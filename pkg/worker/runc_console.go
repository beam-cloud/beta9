package worker

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/console"
	"github.com/opencontainers/runc/libcontainer/utils"
)

type ConsoleWriter struct {
	path string
}

// Implementation of runc ConsoleSocket, for writing only
func NewConsoleWriter(writer io.Writer) (*ConsoleWriter, error) {
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("tty-%d.sock", time.Now().UnixNano()))

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		// We only accept a single connection, since we can only really have
		// one reader for os.Stdin.
		conn, err := ln.Accept()
		defer ln.Close()
		if err != nil {
			return
		}
		defer conn.Close()

		// Close ln, to allow for other instances to take over.
		ln.Close()

		// Get the fd of the connection.
		unixconn, ok := conn.(*net.UnixConn)
		if !ok {
			return
		}

		socket, err := unixconn.File()
		if err != nil {
			return
		}
		defer socket.Close()

		// Get the master file descriptor from runC.
		master, err := utils.RecvFile(socket)
		if err != nil {
			return
		}

		c, err := console.ConsoleFromFile(master)
		if err != nil {
			return
		}
		defer c.Close()

		if err := console.ClearONLCR(c.Fd()); err != nil {
			return
		}

		// Copy from our stdio to the master fd.
		_, err = io.Copy(writer, c) // Will return on container exit
		if err != nil {
			return
		}
	}()

	return &ConsoleWriter{path: socketPath}, nil
}

func (w *ConsoleWriter) Path() string {
	return w.path
}
