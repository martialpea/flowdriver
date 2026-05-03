package transport

import (
	"io"
	"net"
	"sync"
	"time"
)

// VirtualConn bridges net.Conn interface for SOCKS5 library,
// routing data covertly through the Google Drive transport.
type VirtualConn struct {
	session *Session
	engine  *Engine
	readBuf []byte
	mu      sync.Mutex
}

func NewVirtualConn(s *Session, e *Engine) *VirtualConn {
	return &VirtualConn{
		session: s,
		engine:  e,
	}
}

func (v *VirtualConn) Read(b []byte) (n int, err error) {
	for {
		v.mu.Lock()
		if len(v.readBuf) > 0 {
			n = copy(b, v.readBuf)
			// بهینه‌سازی: از append به جای slice assignment برای جلوگیری از memory leak
			remaining := len(v.readBuf) - n
			if remaining > 0 {
				// کپی باقیمانده به ابتدای buffer (in-place)
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

		if len(data) > 0 {
			v.mu.Lock()
			n = copy(b, data)
			if n < len(data) {
				// بهینه‌سازی: allocate با ظرفیت دقیق باقیمانده
				remainder := make([]byte, len(data)-n)
				copy(remainder, data[n:])
				v.readBuf = remainder
			}
			v.mu.Unlock()
			return n, nil
		}

		v.session.mu.Lock()
		closed := v.session.closed
		v.session.mu.Unlock()
		if closed {
			return 0, io.EOF
		}
	}
}

func (v *VirtualConn) Write(b []byte) (n int, err error) {
	if len(b) > 0 {
		// بهینه‌سازی: کپی داده قبل از EnqueueTx برای جلوگیری از data race
		// اگر caller buffer رو reuse کنه
		buf := make([]byte, len(b))
		copy(buf, b)
		v.session.EnqueueTx(buf)
	}
	return len(b), nil
}

func (v *VirtualConn) Close() error {
	v.session.mu.Lock()
	v.session.closed = true
	v.session.txCond.Broadcast()
	v.session.mu.Unlock()
	return nil
}

func (v *VirtualConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65535}
}
func (v *VirtualConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65535}
}
func (v *VirtualConn) SetDeadline(t time.Time) error      { return nil }
func (v *VirtualConn) SetReadDeadline(t time.Time) error  { return nil }
func (v *VirtualConn) SetWriteDeadline(t time.Time) error { return nil }
