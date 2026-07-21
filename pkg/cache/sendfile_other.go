//go:build !linux

package cache

import (
	"net"
	"os"
	"time"
)

func sendFileToConn(conn net.Conn, file *os.File, offset int64, length int64, progressTimeout time.Duration) (int64, error) {
	return 0, nil
}
