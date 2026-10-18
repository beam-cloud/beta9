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
	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return err
	}
	if reply[0] != 0 {
		return fmt.Errorf("connect to guest localhost:%d failed", port)
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
	tcp := target.(*net.TCPConn)
	go func() { done <- copyStream(tcp, conn) }()
	err = copyStream(conn, tcp)
	return errors.Join(err, <-done)
}
