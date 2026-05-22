//go:build !linux

package cache

import (
	"net"
	"os"
)

func sendFileToConn(conn net.Conn, file *os.File, offset int64, length int64) (int64, error) {
	return 0, nil
}
