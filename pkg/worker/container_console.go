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

// ContainerConsole defines the interface for container console/TTY handling
type ContainerConsole interface {
	// Path returns the path to the console socket
	Path() string
	// Close cleans up any resources used by the console
	Close() error
}

// ConsoleWriter implements ContainerConsole for runc
type ConsoleWriter struct {
	path string
	ln   net.Listener
}

// NewConsoleWriter creates a new console writer that implements ContainerConsole
func NewConsoleWriter(writer io.Writer) (ContainerConsole, error) {
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("tty-%d.sock", time.Now().UnixNano()))

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}

	cw := &ConsoleWriter{
		path: socketPath,
		ln:   ln,
	}

	go func() {
		// We only accept a single connection, since we can only really have
		// one reader for os.Stdin.
		conn, err := ln.Accept()
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

	return cw, nil
}

func (w *ConsoleWriter) Path() string {
	return w.path
}

func (w *ConsoleWriter) Close() error {
	if w.ln != nil {
		return w.ln.Close()
	}
	return nil
}

// GvisorConsole implements ContainerConsole for gvisor
type GvisorConsole struct {
	path string
}

func NewGvisorConsole(writer io.Writer) (ContainerConsole, error) {
	// TODO: Implement gvisor console handling
	// For now, return a basic implementation that just returns a path
	return &GvisorConsole{
		path: filepath.Join(os.TempDir(), fmt.Sprintf("gvisor-tty-%d.sock", time.Now().UnixNano())),
	}, nil
}

func (g *GvisorConsole) Path() string {
	return g.path
}

func (g *GvisorConsole) Close() error {
	return nil
}
