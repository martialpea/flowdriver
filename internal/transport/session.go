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
	txCond       *sync.Cond
	RxChan       chan []byte
}

func NewSession(id string) *Session {
	s := &Session{
		ID:           id,
		rxQueue:      make(map[uint64]*Envelope),
		lastActivity: time.Now(),
		RxChan:       make(chan []byte, 256),
	}
	s.txCond = sync.NewCond(&s.mu)
	return s
}

func (s *Session) EnqueueTx(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.txBuf) > 2*1024*1024 && !s.closed {
		s.txCond.Wait()
	}
	if len(data) > 0 {
		s.txBuf = append(s.txBuf, data...)
	}
	s.lastActivity = time.Now()
}

func (s *Session) ProcessRx(env *Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed {
		return
	}

	if env.Seq == s.rxSeq {
		s.deliverInOrder(env)
	} else if env.Seq > s.rxSeq {
		// BUG FIX: فقط اگر rxQueue پر نشده ذخیره کن
		if len(s.rxQueue) < 512 {
			s.rxQueue[env.Seq] = env
		}
	}
	// اگر queue خیلی بزرگ شد session رو ببند
	if len(s.rxQueue) > 512 {
		s.closed = true
	}
}

// deliverInOrder: تحویل ordered پکت‌ها — جدا شده برای خوانایی بیشتر
func (s *Session) deliverInOrder(env *Envelope) {
	if len(env.Payload) > 0 {
		select {
		case s.RxChan <- env.Payload:
		default:
			// channel پر است — payload را در queue نگه می‌داریم تا بعد
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
	// پکت‌های بعدی که در queue منتظر بودند را هم تحویل بده
	for {
		next, ok := s.rxQueue[s.rxSeq]
		if !ok {
			break
		}
		if len(next.Payload) > 0 {
			select {
			case s.RxChan <- next.Payload:
			default:
				return
			}
		}
		delete(s.rxQueue, s.rxSeq)
		s.rxSeq++
		if next.Close {
			s.rxClosed = true
			s.closed = true
			close(s.RxChan)
			return
		}
	}
}
