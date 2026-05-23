//go:build linux

package cache

import (
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func sendFileToConn(conn net.Conn, file *os.File, offset int64, length int64) (int64, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok || length <= 0 {
		return 0, nil
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return 0, err
	}

	inFD := int(file.Fd())
	remaining := length
	off := offset
	sent := int64(0)
	for remaining > 0 {
		chunk := remaining
		if chunk > 1<<30 {
			chunk = 1 << 30
		}

		var sendErr error
		progressed := false
		if err := rawConn.Write(func(fd uintptr) bool {
			n, err := unix.Sendfile(int(fd), inFD, &off, int(chunk))
			if n > 0 {
				n64 := int64(n)
				sent += n64
				remaining -= n64
				chunk -= n64
				progressed = true
			}
			if remaining == 0 {
				sendErr = nil
				return true
			}
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				return false
			}
			sendErr = err
			return true
		}); err != nil {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK || err == unix.EINTR {
				continue
			}
			return sent, err
		}
		if sendErr != nil {
			return sent, sendErr
		}
		if !progressed {
			break
		}
	}

	return sent, nil
}
