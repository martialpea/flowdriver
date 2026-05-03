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

// Engine manages the local sessions, periodically flushes Tx buffers to files,
// and polls for new Rx files.
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

	sem chan struct{}

	// بهینه‌سازی: struct{} به جای bool برای کاهش ۸ برابری مصرف رم
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
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	e.sem = make(chan struct{}, 8)
	return e
}

func (e *Engine) SetRefreshRate(ms int) {
	if ms > 0 {
		e.pollTicker = time.Duration(ms) * time.Millisecond
		if e.flushTicker == 300*time.Millisecond {
			e.flushTicker = time.Duration(ms) * time.Millisecond
		}
	}
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
	log.Printf("Engine.AddSession: Added session %s (Total now: %d)", s.ID, len(e.sessions))
}

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
	var closedSessionIDs []string

	for _, s := range sessions {
		s.mu.Lock()

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

		env := Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    payload,
			Close:      s.closed,
			TargetAddr: s.TargetAddr,
		}
		s.txSeq++

		if s.closed {
			closedSessionIDs = append(closedSessionIDs, s.ID)
		}

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id
		}
		muxes[cid] = append(muxes[cid], env)
		s.mu.Unlock()
	}

	for cid, mux := range muxes {
		fnameCID := cid
		if fnameCID == "" {
			fnameCID = "unknown"
		}
		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, fnameCID, time.Now().UnixNano())

		go func(fname string, m []Envelope) {
			e.sem <- struct{}{}
			defer func() { <-e.sem }()

			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				for _, env := range m {
					if err := env.Encode(pw); err != nil {
						log.Printf("mux encode error: %v", err)
						break
					}
				}
			}()

			// بهینه‌سازی: Retry با exponential backoff — جلوگیری از crash هنگام 429/503
			if err := uploadWithRetry(ctx, e.backend, fname, pr); err != nil {
				log.Printf("upload error %s (after retries): %v", fname, err)
			}
		}(filename, mux)
	}

	for _, id := range closedSessionIDs {
		e.RemoveSession(id)
	}
}

// uploadWithRetry: تلاش مجدد با exponential backoff برای خطاهای موقتی Drive
func uploadWithRetry(ctx context.Context, backend storage.Backend, fname string, r io.Reader) error {
	maxAttempts := 4
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := backend.Upload(ctx, fname, r)
		if err == nil {
			return nil
		}
		if attempt == maxAttempts-1 {
			return err
		}
		wait := time.Duration(1<<uint(attempt)) * time.Second // 1s, 2s, 4s
		log.Printf("upload attempt %d failed for %s: %v — retrying in %s", attempt+1, fname, err, wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

func (e *Engine) pollLoop(ctx context.Context) {
	currentPollInterval := e.pollTicker
	maxPollInterval := 5 * time.Second
	timer := time.NewTimer(currentPollInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		pollAgain:
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					timer.Reset(currentPollInterval)
					continue
				}
			}

			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				prefix += e.id + "-mux-"
			}

			// بهینه‌سازی: Retry برای list هم اضافه شد
			files, err := listWithRetry(ctx, e.backend, prefix)
			if err != nil {
				log.Printf("poll list error: %v", err)
				timer.Reset(currentPollInterval)
				continue
			}

			if len(files) == 0 {
				if e.myDir == DirRes {
					e.sessionMu.RLock()
					activeSessions := len(e.sessions)
					e.sessionMu.RUnlock()

					if activeSessions == 0 {
						currentPollInterval += 500 * time.Millisecond
						if currentPollInterval > maxPollInterval {
							currentPollInterval = maxPollInterval
						}
					} else {
						currentPollInterval = e.pollTicker
					}
				}
				timer.Reset(currentPollInterval)
				continue
			}

			currentPollInterval = e.pollTicker

			var wg sync.WaitGroup
			for _, f := range files {
				parts := strings.Split(f, "-")
				if len(parts) >= 3 {
					tsStr := strings.TrimSuffix(parts[len(parts)-1], ".bin")
					ts, _ := strconv.ParseInt(tsStr, 10, 64)
					if ts > 0 && time.Since(time.Unix(0, ts)) > 5*time.Minute {
						e.backend.Delete(ctx, f)
						continue
					}
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
						log.Printf("download error %s: %v", fname, err)
						e.processedMu.Lock()
						delete(e.processed, fname)
						e.processedMu.Unlock()
						return
					}
					defer rc.Close()

					var fileClientID string
					parts := strings.Split(fname, "-")
					if len(parts) >= 4 && parts[2] == "mux" {
						fileClientID = parts[1]
					}

					for {
						var env Envelope
						if err := env.Decode(rc); err != nil {
							if err != io.EOF && err != io.ErrUnexpectedEOF {
								log.Printf("mux decode error %s: %v", fname, err)
							}
							break
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
							s.ClientID = fileClientID
							e.sessions[env.SessionID] = s
							e.sessionMu.Unlock()
							log.Printf("Engine: Triggering new session %s for Client %s", env.SessionID, fileClientID)
							e.OnNewSession(env.SessionID, env.TargetAddr, s)
						} else {
							e.sessionMu.Unlock()
						}

						if s != nil {
							s.ProcessRx(&env)
						}
					}

					e.backend.Delete(ctx, fname)
				}(f)
			}

			wg.Wait()
			time.Sleep(100 * time.Millisecond)
			goto pollAgain
		}
	}
}

// listWithRetry: تلاش مجدد برای list با backoff
func listWithRetry(ctx context.Context, backend storage.Backend, prefix string) ([]string, error) {
	maxAttempts := 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		files, err := backend.ListQuery(ctx, prefix)
		if err == nil {
			return files, nil
		}
		lastErr = err
		if attempt == maxAttempts-1 {
			break
		}
		wait := time.Duration(1<<uint(attempt)) * 500 * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, lastErr
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	e.sessionMu.Unlock()

	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.closedSessionsMu.Lock()
			for id, t := range e.closedSessions {
				if time.Since(t) > 30*time.Second {
					delete(e.closedSessions, id)
				}
			}
			e.closedSessionsMu.Unlock()

			// بهینه‌سازی: حد پایین‌تر برای processed map و استفاده از struct{}
			e.processedMu.Lock()
			if len(e.processed) > 3000 {
				e.processed = make(map[string]struct{})
			}
			e.processedMu.Unlock()

			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					continue
				}
			}

			files, _ := e.backend.ListQuery(ctx, string(e.myDir)+"-")
			for _, f := range files {
				parts := strings.Split(f, "-")
				if len(parts) >= 3 {
					tsStr := parts[len(parts)-1]
					tsStr = strings.TrimSuffix(tsStr, ".json")
					tsStr = strings.TrimSuffix(tsStr, ".bin")
					ts, err := strconv.ParseInt(tsStr, 10, 64)
					if err == nil && time.Since(time.Unix(0, ts)) > 10*time.Second {
						e.backend.Delete(ctx, f)
					}
				}
			}
		}
	}
}
