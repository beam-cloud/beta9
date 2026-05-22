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

	var sendErr error
	var outFD int
	if err := rawConn.Control(func(fd uintptr) {
		outFD = int(fd)
	}); err != nil {
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
		n, err := unix.Sendfile(outFD, inFD, &off, int(chunk))
		if err != nil {
			sendErr = err
			break
		}
		if n == 0 {
			break
		}
		n64 := int64(n)
		sent += n64
		remaining -= n64
	}

	return sent, sendErr
}
