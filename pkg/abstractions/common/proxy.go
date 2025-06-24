package abstractions

import (
	"io"
	"log"
	"net"
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
		log.Println("str ", string(buf[:n]))
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
