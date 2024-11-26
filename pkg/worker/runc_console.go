package worker

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/console"
	"github.com/opencontainers/runc/libcontainer/utils"
)

//// Implmentation of runc ConsoleSocket for writing only ////

type ConsoleWriter struct {
	path string
}

func NewConsoleWriter(writer io.Writer) (*ConsoleWriter, error) {
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("tty-%d.sock", time.Now().UnixNano()))

	log := log.New(os.Stderr, "console writer: ", log.LstdFlags)

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
			log.Printf("error accepting connection: %v", err)
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
			log.Printf("error getting file from connection: %v", err)
			return
		}
		defer socket.Close()

		// Get the master file descriptor from runC.
		master, err := utils.RecvFd(socket)
		if err != nil {
			log.Printf("error receiving fd: %v", err)
			return
		}
		c, err := console.ConsoleFromFile(master)
		if err != nil {
			log.Printf("error creating console from file: %v", err)
			return
		}
		if err := console.ClearONLCR(c.Fd()); err != nil {
			log.Printf("error clearing ONLCR: %v", err)
			return
		}

		// Copy from our stdio to the master fd.
		_, err = io.Copy(writer, c) // will return on container exit
		if err != nil {
			log.Printf("error copying from console to writer: %v", err)
		}
		c.Close()
	}()

	return &ConsoleWriter{path: socketPath}, nil
}

func (w *ConsoleWriter) Path() string {
	return w.path
}
