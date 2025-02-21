package abstractions

import (
	"io"
	"net"
	"time"
)

func ProxyConn(dst io.Writer, src io.Reader, done chan<- struct{}, bufferSize int) {
	buf := make([]byte, bufferSize)
	io.CopyBuffer(dst, src, buf)
	done <- struct{}{}
}

func SetConnOptions(conn net.Conn, keepAlive bool, keepAliveInterval time.Duration) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(keepAlive)
		tcpConn.SetKeepAlivePeriod(keepAliveInterval)
		tcpConn.SetDeadline(time.Time{})
	}
}
