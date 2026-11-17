package microvm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// TunnelPort accepts a big-endian uint16 TCP port and replies with one byte:
// zero once connected to guest localhost, one on failure. Raw TCP bytes follow.
const TunnelPort = 1026

// ProcessPort connects to the process manager of a guest that keeps it off
// its network, where the manager listens on ProcessSocket instead of a TCP
// port. The guest replies as it does on TunnelPort, and raw bytes follow.
const (
	ProcessPort   = 1028
	ProcessSocket = "/run/beam-vm/goproc.sock"
)

// ErrPortUnavailable means a reachable guest refused the loopback connection.
var ErrPortUnavailable = errors.New("guest loopback port unavailable")

// ErrProcessUnavailable means a reachable guest has no process manager
// listening on its socket, as before the manager has started.
var ErrProcessUnavailable = errors.New("guest process manager unavailable")

// ConnectPort leaves application bytes unread, including server-first banners.
// The caller owns the stream and bounds the handshake with a deadline.
func ConnectPort(conn io.ReadWriter, port uint32) error {
	if port == 0 || port > 65535 {
		return fmt.Errorf("invalid TCP port %d", port)
	}
	var request [2]byte
	binary.BigEndian.PutUint16(request[:], uint16(port))
	if _, err := conn.Write(request[:]); err != nil {
		return err
	}
	err := readReply(conn)
	if errors.Is(err, ErrPortUnavailable) {
		return fmt.Errorf("%w: %d", err, port)
	}
	return err
}

// ConnectProcess completes a connection to ProcessPort, as ConnectPort does
// one to TunnelPort.
func ConnectProcess(conn io.Reader) error {
	err := readReply(conn)
	if errors.Is(err, ErrPortUnavailable) {
		return ErrProcessUnavailable
	}
	return err
}

func readReply(conn io.Reader) error {
	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return err
	}
	if reply[0] == 1 {
		return ErrPortUnavailable
	}
	if reply[0] != 0 {
		return fmt.Errorf("invalid guest tunnel reply %d", reply[0])
	}
	return nil
}

type portConn interface {
	io.ReadWriteCloser
	CloseWrite() error
	SetDeadline(time.Time) error
}

// ServePort opens the TCP connection inside the guest. Each direction can end
// independently: a client that closes its input can still receive the response.
func ServePort(conn portConn) error {
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	var request [2]byte
	if _, err := io.ReadFull(conn, request[:]); err != nil {
		return err
	}
	port := binary.BigEndian.Uint16(request[:])
	var target net.Conn
	var err error
	if port == 0 {
		err = fmt.Errorf("invalid TCP port 0")
	} else {
		// Minimal images may have no localhost entry or resolver configuration.
		target, err = net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
		if err != nil {
			target, err = net.DialTimeout("tcp6", fmt.Sprintf("[::1]:%d", port), 2*time.Second)
		}
	}
	if err != nil {
		_, _ = conn.Write([]byte{1})
		return err
	}
	return pipe(conn, target.(*net.TCPConn))
}

// ServeSocket connects conn to the Unix socket at path inside the guest, as
// ServePort connects one to a TCP port.
func ServeSocket(conn portConn, path string) error {
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	target, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{1})
		return err
	}
	return pipe(conn, target.(*net.UnixConn))
}

// pipe tells the host that target is connected, and copies both ways until
// each direction has ended.
func pipe(conn, target portConn) error {
	defer target.Close()
	if _, err := conn.Write([]byte{0}); err != nil {
		return err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return err
	}
	copyStream := func(dst, src portConn) error {
		_, err := io.Copy(dst, src)
		if err == nil {
			err = dst.CloseWrite()
		}
		if err != nil {
			dst.Close()
			src.Close()
		}
		return err
	}
	done := make(chan error, 1)
	go func() { done <- copyStream(target, conn) }()
	err := copyStream(conn, target)
	return errors.Join(err, <-done)
}
