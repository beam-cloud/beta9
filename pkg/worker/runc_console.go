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

type ConsoleWriter struct {
	path string
}

func NewConsoleWriter(writer io.Writer) (*ConsoleWriter, error) {
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("tty-%d.sock", time.Now().UnixNano()))

	log := log.New(os.Stderr, "console writer: ", log.LstdFlags)

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Printf("error creating Unix socket: %v", err)
		return nil, err
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("error accepting connection: %v", err)
			return
		}
		defer conn.Close()
		defer ln.Close()

		unixconn, ok := conn.(*net.UnixConn)
		if !ok {
			log.Println("error: connection is not a UnixConn")
			return
		}

		socket, err := unixconn.File()
		if err != nil {
			log.Printf("error getting file from connection: %v", err)
			return
		}
		defer socket.Close()

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
		defer c.Close()

		if err := console.ClearONLCR(c.Fd()); err != nil {
			log.Printf("error clearing ONLCR: %v", err)
			return
		}

		_, err = io.Copy(writer, c)
		if err != nil {
			log.Printf("error copying from console to writer: %v", err)
		}
	}()

	return &ConsoleWriter{path: socketPath}, nil
}

func (w *ConsoleWriter) Path() string {
	return w.path
}
