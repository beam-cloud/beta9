package abstractions

import (
	"io"
	"net"
	"time"
)

func ProxyConn(dst io.Writer, src io.Reader, done chan bool, bufferSize int) error {
	buf := make([]byte, bufferSize)
	defer func() {
		done <- true
	}()

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

func SetConnOptions(conn net.Conn, keepAlive bool, keepAliveInterval time.Duration) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(keepAlive)
		tcpConn.SetKeepAlivePeriod(keepAliveInterval)
		tcpConn.SetDeadline(time.Time{})
	}
}
