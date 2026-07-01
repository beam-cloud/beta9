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
	timeout time.Duration
	conns   []net.Conn
	done    chan struct{}
	active  chan struct{}
	stop    sync.Once
}

// NewConnIdleDeadline applies one shared idle deadline to every connection.
// Call Refresh whenever bytes move in either direction.
func NewConnIdleDeadline(timeout time.Duration, conns ...net.Conn) *ConnIdleDeadline {
	d := &ConnIdleDeadline{
		timeout: timeout,
		done:    make(chan struct{}),
		active:  make(chan struct{}, 1),
	}
	if timeout <= 0 {
		return d
	}
	for _, conn := range conns {
		if conn != nil {
			d.conns = append(d.conns, conn)
		}
	}
	d.setDeadline(time.Now().Add(timeout))
	go d.closeWhenIdle()
	return d
}

// Refresh extends the shared idle deadline from now.
func (d *ConnIdleDeadline) Refresh() {
	if d == nil || d.timeout <= 0 {
		return
	}
	d.setDeadline(time.Now().Add(d.timeout))
	select {
	case <-d.done:
	case d.active <- struct{}{}:
	default:
	}
}

func (d *ConnIdleDeadline) closeWhenIdle() {
	timer := time.NewTimer(d.timeout)
	defer timer.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-d.active:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d.timeout)
		case <-timer.C:
			d.stop.Do(func() {
				close(d.done)
				for _, conn := range d.conns {
					_ = conn.Close()
				}
			})
			return
		}
	}
}

func (d *ConnIdleDeadline) setDeadline(deadline time.Time) {
	for _, conn := range d.conns {
		_ = conn.SetDeadline(deadline)
	}
}

// Clear removes the shared idle deadline from all tracked connections.
func (d *ConnIdleDeadline) Clear() {
	if d == nil || d.timeout <= 0 {
		return
	}
	d.stop.Do(func() {
		close(d.done)
		d.setDeadline(time.Time{})
	})
}
