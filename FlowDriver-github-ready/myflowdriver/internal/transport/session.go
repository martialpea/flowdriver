package transport

import (
	"sync"
	"time"
)

type Direction string

const (
	DirReq Direction = "req"
	DirRes Direction = "res"
)

// Session represents an active proxy connection mapped to files.
type Session struct {
	ID           string
	mu           sync.Mutex
	txBuf        []byte
	txSeq        uint64
	rxSeq        uint64
	rxQueue      map[uint64]*Envelope
	lastActivity time.Time
	closed       bool
	rxClosed     bool
	TargetAddr   string
	ClientID     string

	txCond *sync.Cond
	RxChan chan []byte
}

func NewSession(id string) *Session {
	s := &Session{
		ID:           id,
		rxQueue:      make(map[uint64]*Envelope),
		lastActivity: time.Now(),
		// بهینه‌سازی: کاهش buffer از 1024 به 256 — کاهش مصرف رم بدون تاثیر عملکرد
		// هر slot حاوی یک slice است نه یک مقدار ثابت
		RxChan: make(chan []byte, 256),
	}
	s.txCond = sync.NewCond(&s.mu)
	return s
}

func (s *Session) EnqueueTx(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// BACKPRESSURE: Block if txBuf is larger than 2MB
	for len(s.txBuf) > 2*1024*1024 && !s.closed {
		s.txCond.Wait()
	}

	s.txBuf = append(s.txBuf, data...)
	s.lastActivity = time.Now()
}

func (s *Session) ClearTx() {
	s.mu.Lock()
	s.txBuf = nil
	s.txCond.Broadcast()
	s.mu.Unlock()
}

func (s *Session) ProcessRx(env *Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed {
		return
	}

	if env.Seq == s.rxSeq {
		if len(env.Payload) > 0 {
			// بهینه‌سازی: بررسی پر بودن channel قبل از ارسال (non-blocking select)
			// از بلوک شدن ProcessRx جلوگیری می‌کند اگر consumer کند باشد
			select {
			case s.RxChan <- env.Payload:
			default:
				// اگر channel پر است، payload را در rxQueue نگه می‌داریم
				s.rxQueue[env.Seq] = env
				return
			}
		}
		s.rxSeq++
		if env.Close {
			s.rxClosed = true
			s.closed = true
			close(s.RxChan)
			return
		}

		for {
			nextEnv, ok := s.rxQueue[s.rxSeq]
			if !ok {
				break
			}
			if len(nextEnv.Payload) > 0 {
				select {
				case s.RxChan <- nextEnv.Payload:
				default:
					// channel پر است، صبر می‌کنیم تا بعد
					return
				}
			}
			delete(s.rxQueue, s.rxSeq)
			s.rxSeq++
			if nextEnv.Close {
				s.rxClosed = true
				s.closed = true
				close(s.RxChan)
				return
			}
		}
	} else if env.Seq > s.rxSeq {
		s.rxQueue[env.Seq] = env
	}
	// بهینه‌سازی: جلوگیری از رشد بی‌نهایت rxQueue
	if len(s.rxQueue) > 512 {
		// اگر queue خیلی بزرگ شد، session را ببند
		s.closed = true
	}
}
