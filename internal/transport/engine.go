package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

type Engine struct {
	backend storage.Backend
	myDir   Direction
	peerDir Direction
	id      string

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	pollTicker  time.Duration
	flushTicker time.Duration

	OnNewSession func(sessionID, targetAddr string, s *Session)

	sem         chan struct{}
	processed   map[string]struct{}
	processedMu sync.Mutex
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	e := &Engine{
		backend:        backend,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]struct{}),
		pollTicker:     500 * time.Millisecond,
		flushTicker:    300 * time.Millisecond,
		sem:            make(chan struct{}, 8),
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	return e
}

func (e *Engine) SetPollRate(ms int) {
	if ms > 0 {
		e.pollTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) SetFlushRate(ms int) {
	if ms > 0 {
		e.flushTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) Start(ctx context.Context) {
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx)
}

func (e *Engine) GetSession(id string) *Session {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessions[id]
}

func (e *Engine) AddSession(s *Session) {
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	e.sessions[s.ID] = s
	log.Printf("[session] added %s (total: %d)", s.ID, len(e.sessions))
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	e.sessionMu.Unlock()

	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

// ── flush loop ────────────────────────────────────────────────────────────────

func (e *Engine) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(e.flushTicker)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flushAll(ctx)
		}
	}
}

func (e *Engine) flushAll(ctx context.Context) {
	e.sessionMu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.Unlock()

	muxes := make(map[string][]Envelope)
	var closedIDs []string

	for _, s := range sessions {
		s.mu.Lock()

		// idle timeout
		if time.Since(s.lastActivity) > 10*time.Second {
			s.closed = true
		}

		shouldSend := len(s.txBuf) > 0 || (s.txSeq == 0 && e.myDir == DirReq) || s.closed
		if !shouldSend {
			s.mu.Unlock()
			continue
		}

		payload := s.txBuf
		s.txBuf = nil
		s.txCond.Broadcast()

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id
		}

		muxes[cid] = append(muxes[cid], Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    payload,
			Close:      s.closed,
			TargetAddr: s.TargetAddr,
		})
		s.txSeq++

		if s.closed {
			closedIDs = append(closedIDs, s.ID)
		}
		s.mu.Unlock()
	}

	for cid, mux := range muxes {
		if cid == "" {
			cid = "unknown"
		}
		fname := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, cid, time.Now().UnixNano())

		go func(fname string, m []Envelope) {
			e.sem <- struct{}{}
			defer func() { <-e.sem }()

			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				for _, env := range m {
					if err := env.Encode(pw); err != nil {
						log.Printf("[encode] %v", err)
						break
					}
				}
			}()

			if err := uploadWithRetry(ctx, e.backend, fname, pr); err != nil {
				log.Printf("[upload] %s failed: %v", fname, err)
			}
		}(fname, mux)
	}

	for _, id := range closedIDs {
		e.RemoveSession(id)
	}
}

func uploadWithRetry(ctx context.Context, backend storage.Backend, fname string, r io.Reader) error {
	for attempt := 0; attempt < 4; attempt++ {
		if err := backend.Upload(ctx, fname, r); err == nil {
			return nil
		} else if attempt == 3 {
			return err
		} else {
			wait := time.Duration(1<<uint(attempt)) * time.Second
			log.Printf("[upload] attempt %d failed, retry in %s", attempt+1, wait)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	return nil
}

// ── poll loop ─────────────────────────────────────────────────────────────────

func (e *Engine) pollLoop(ctx context.Context) {
	currentInterval := e.pollTicker
	const maxInterval = 5 * time.Second
	timer := time.NewTimer(currentInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		pollAgain:
			// کلاینت: اگه session نداریم poll نکن — صرفه‌جویی در quota
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					timer.Reset(currentInterval)
					continue
				}
			}

			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				prefix += e.id + "-mux-"
			}

			files, err := listWithRetry(ctx, e.backend, prefix)
			if err != nil {
				log.Printf("[poll] list error: %v", err)
				timer.Reset(currentInterval)
				continue
			}

			if len(files) == 0 {
				// سرور: وقتی idle است poll رو کند کن
				if e.myDir == DirRes {
					e.sessionMu.RLock()
					active := len(e.sessions)
					e.sessionMu.RUnlock()
					if active == 0 {
						currentInterval += 500 * time.Millisecond
						if currentInterval > maxInterval {
							currentInterval = maxInterval
						}
					} else {
						currentInterval = e.pollTicker
					}
				}
				timer.Reset(currentInterval)
				continue
			}
			currentInterval = e.pollTicker

			var wg sync.WaitGroup
			for _, f := range files {
				// فایل‌های قدیمی‌تر از ۵ دقیقه رو حذف کن
				if ts := extractTimestamp(f); ts > 0 && time.Since(time.Unix(0, ts)) > 5*time.Minute {
					e.backend.Delete(ctx, f)
					continue
				}

				e.processedMu.Lock()
				_, already := e.processed[f]
				if !already {
					e.processed[f] = struct{}{}
				}
				e.processedMu.Unlock()
				if already {
					continue
				}

				wg.Add(1)
				go func(fname string) {
					defer wg.Done()
					e.sem <- struct{}{}
					defer func() { <-e.sem }()

					rc, err := e.backend.Download(ctx, fname)
					if err != nil {
						log.Printf("[download] %s: %v", fname, err)
						e.processedMu.Lock()
						delete(e.processed, fname)
						e.processedMu.Unlock()
						return
					}
					defer rc.Close()

					clientID := extractClientID(fname)
					e.processMuxFile(ctx, fname, clientID, rc)
					e.backend.Delete(ctx, fname)
				}(f)
			}
			wg.Wait()

			// اگه هنوز فایل هست فوری دوباره poll کن
			time.Sleep(50 * time.Millisecond)
			goto pollAgain
		}
	}
}

func (e *Engine) processMuxFile(ctx context.Context, fname, clientID string, rc io.Reader) {
	for {
		var env Envelope
		if err := env.Decode(rc); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Printf("[decode] %s: %v", fname, err)
			}
			return
		}

		e.closedSessionsMu.Lock()
		_, isClosed := e.closedSessions[env.SessionID]
		e.closedSessionsMu.Unlock()
		if isClosed {
			continue
		}

		e.sessionMu.Lock()
		s, exists := e.sessions[env.SessionID]
		if !exists && e.myDir == DirRes && e.OnNewSession != nil {
			s = NewSession(env.SessionID)
			s.ClientID = clientID
			e.sessions[env.SessionID] = s
			e.sessionMu.Unlock()
			log.Printf("[session] new %s from client %s -> %s", env.SessionID, clientID, env.TargetAddr)
			e.OnNewSession(env.SessionID, env.TargetAddr, s)
		} else {
			e.sessionMu.Unlock()
		}

		if s != nil {
			s.ProcessRx(&env)
		}
	}
}

func listWithRetry(ctx context.Context, backend storage.Backend, prefix string) ([]string, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if files, err := backend.ListQuery(ctx, prefix); err == nil {
			return files, nil
		} else {
			lastErr = err
			wait := time.Duration(1<<uint(attempt)) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	return nil, lastErr
}

// ── cleanup loop ──────────────────────────────────────────────────────────────

func (e *Engine) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// tombstone های قدیمی رو پاک کن
			e.closedSessionsMu.Lock()
			for id, t := range e.closedSessions {
				if time.Since(t) > 30*time.Second {
					delete(e.closedSessions, id)
				}
			}
			e.closedSessionsMu.Unlock()

			// processed map رو محدود کن
			e.processedMu.Lock()
			if len(e.processed) > 3000 {
				e.processed = make(map[string]struct{})
			}
			e.processedMu.Unlock()

			// کلاینت: اگه session نداریم Drive رو scan نکن
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					continue
				}
			}

			// فایل‌های قدیمی خودم رو پاک کن
			files, _ := e.backend.ListQuery(ctx, string(e.myDir)+"-")
			for _, f := range files {
				if ts := extractTimestamp(f); ts > 0 && time.Since(time.Unix(0, ts)) > 10*time.Second {
					e.backend.Delete(ctx, f)
				}
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func extractTimestamp(fname string) int64 {
	parts := strings.Split(fname, "-")
	if len(parts) == 0 {
		return 0
	}
	s := parts[len(parts)-1]
	s = strings.TrimSuffix(s, ".bin")
	s = strings.TrimSuffix(s, ".json")
	ts, _ := strconv.ParseInt(s, 10, 64)
	return ts
}

func extractClientID(fname string) string {
	// فرمت: {dir}-{clientID}-mux-{ts}.bin
	parts := strings.Split(fname, "-")
	if len(parts) >= 4 && parts[2] == "mux" {
		return parts[1]
	}
	return ""
}
