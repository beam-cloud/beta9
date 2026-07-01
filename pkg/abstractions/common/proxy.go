package abstractions

import (
	"io"
	"net"
	"sync"
	"time"
)

func ProxyConn(dst io.Writer, src io.Reader, done <-chan struct{}, bufferSize int) error {
	buf := make([]byte, bufferSize)

	for {
		select {
		case <-done:
			return nil
		default:
		}

		n, err := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
		}

		if err != nil {
			return err
		}
	}
}

func SetConnOptions(conn net.Conn, keepAlive bool, keepAliveInterval time.Duration, readDeadline time.Duration) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(keepAlive)
		tcpConn.SetKeepAlivePeriod(keepAliveInterval)
		tcpConn.SetDeadline(time.Time{})

		if readDeadline != -1 {
			tcpConn.SetReadDeadline(time.Now().Add(readDeadline))
		}
	}
}

type ConnIdleDeadline struct {
	mu      sync.Mutex
	timeout time.Duration
	conns   []net.Conn
}

// NewConnIdleDeadline applies one shared idle deadline to every connection.
// Call Refresh whenever bytes move in either direction.
func NewConnIdleDeadline(timeout time.Duration, conns ...net.Conn) *ConnIdleDeadline {
	d := &ConnIdleDeadline{timeout: timeout}
	if timeout <= 0 {
		return d
	}
	for _, conn := range conns {
		if conn != nil {
			d.conns = append(d.conns, conn)
		}
	}
	d.Refresh()
	return d
}

// Refresh extends the shared idle deadline from now.
func (d *ConnIdleDeadline) Refresh() {
	if d == nil || d.timeout <= 0 {
		return
	}
	deadline := time.Now().Add(d.timeout)
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.SetDeadline(deadline)
	}
}

// Clear removes the shared idle deadline from all tracked connections.
func (d *ConnIdleDeadline) Clear() {
	if d == nil || d.timeout <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.SetDeadline(time.Time{})
	}
}
