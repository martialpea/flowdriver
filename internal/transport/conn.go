package transport

import (
	"io"
	"net"
	"sync"
	"time"
)

// VirtualConn پل بین net.Conn و Session ماست
type VirtualConn struct {
	session *Session
	engine  *Engine
	readBuf []byte
	mu      sync.Mutex
}

func NewVirtualConn(s *Session, e *Engine) *VirtualConn {
	return &VirtualConn{session: s, engine: e}
}

func (v *VirtualConn) Read(b []byte) (int, error) {
	for {
		v.mu.Lock()
		if len(v.readBuf) > 0 {
			n := copy(b, v.readBuf)
			remaining := len(v.readBuf) - n
			if remaining > 0 {
				// in-place shift — بدون allocation جدید
				copy(v.readBuf, v.readBuf[n:])
				v.readBuf = v.readBuf[:remaining]
			} else {
				v.readBuf = v.readBuf[:0]
			}
			v.mu.Unlock()
			return n, nil
		}
		v.mu.Unlock()

		data, ok := <-v.session.RxChan
		if !ok {
			return 0, io.EOF
		}
		if len(data) == 0 {
			// BUG FIX: اگر data خالی بود و session بسته، EOF برگردون
			v.session.mu.Lock()
			closed := v.session.closed
			v.session.mu.Unlock()
			if closed {
				return 0, io.EOF
			}
			continue
		}

		v.mu.Lock()
		n := copy(b, data)
		if n < len(data) {
			// باقیمانده رو در readBuf نگه دار
			rem := make([]byte, len(data)-n)
			copy(rem, data[n:])
			v.readBuf = rem
		}
		v.mu.Unlock()
		return n, nil
	}
}

func (v *VirtualConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	// کپی بگیر تا caller بتونه buffer رو reuse کنه بدون data race
	buf := make([]byte, len(b))
	copy(buf, b)
	v.session.EnqueueTx(buf)
	return len(b), nil
}

func (v *VirtualConn) Close() error {
	v.session.mu.Lock()
	v.session.closed = true
	v.session.txCond.Broadcast()
	v.session.mu.Unlock()
	return nil
}

func (v *VirtualConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (v *VirtualConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (v *VirtualConn) SetDeadline(t time.Time) error      { return nil }
func (v *VirtualConn) SetReadDeadline(t time.Time) error  { return nil }
func (v *VirtualConn) SetWriteDeadline(t time.Time) error { return nil }
