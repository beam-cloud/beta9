package pod

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"slices"
	"time"
)

const (
	postgresALPN      = "postgresql"
	tcpTLSReadTimeout = 5 * time.Second
)

type tcpTLSHandler struct {
	config *tls.Config
}

func newTCPTLSHandler(cert tls.Certificate) *tcpTLSHandler {
	plain := &tls.Config{Certificates: []tls.Certificate{cert}}
	postgres := plain.Clone()
	postgres.NextProtos = []string{postgresALPN}
	return &tcpTLSHandler{config: &tls.Config{
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			switch {
			case hello.ServerName == "":
				// An empty config rejects unroutable clients with unrecognized_name.
				return &tls.Config{}, nil
			case slices.Contains(hello.SupportedProtos, postgresALPN):
				return postgres, nil
			default:
				return plain, nil
			}
		},
	}}
}

func (h *tcpTLSHandler) accept(conn net.Conn) (net.Conn, error) {
	prepared, err := preparePostgresAwareTLSConn(conn)
	if err != nil {
		return nil, err
	}
	client := tls.Server(prepared, h.config)
	if err := client.Handshake(); err != nil {
		return nil, err
	}
	if !prepared.postgres && client.ConnectionState().NegotiatedProtocol != postgresALPN {
		return client, nil
	}

	_ = client.SetReadDeadline(time.Now().Add(tcpTLSReadTimeout))
	defer client.SetReadDeadline(time.Time{})
	startup, err := readPostgresStartup(client)
	if err != nil {
		return nil, err
	}
	return &postgresClientConn{Conn: client, startup: startup}, nil
}

const postgresSSLRequestCode uint32 = 80877103

type bufferedReadConn struct {
	net.Conn
	reader   *bufio.Reader
	postgres bool
}

func (c *bufferedReadConn) Read(p []byte) (int, error) {
	if c.reader != nil && c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}

// preparePostgresAwareTLSConn accepts direct TLS or PostgreSQL SSLRequest.
func preparePostgresAwareTLSConn(conn net.Conn) (*bufferedReadConn, error) {
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(tcpTLSReadTimeout))
	defer conn.SetReadDeadline(time.Time{})

	first, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] == 0x16 {
		return &bufferedReadConn{Conn: conn, reader: reader}, nil
	}

	header, err := reader.Peek(8)
	if err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(header[0:4]) != 8 ||
		binary.BigEndian.Uint32(header[4:8]) != postgresSSLRequestCode {
		return nil, fmt.Errorf("connection did not start with TLS or PostgreSQL SSLRequest")
	}

	if _, err := io.ReadFull(reader, make([]byte, 8)); err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte("S")); err != nil {
		return nil, err
	}

	return &bufferedReadConn{Conn: conn, reader: reader, postgres: true}, nil
}

const postgresStartupMaxLength = 10000

// readPostgresStartup strips client SSL options from PostgreSQL v3 startup packets.
func readPostgresStartup(r io.Reader) ([]byte, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length <= 8 || length > postgresStartupMaxLength {
		return header, nil
	}
	body := make([]byte, length-8)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	message := append(header, body...)
	if binary.BigEndian.Uint32(header[4:8])>>16 != 3 {
		return message, nil
	}

	filtered := append(make([]byte, 0, length), header...)
	params := body
	for len(params) > 0 && params[0] != 0 {
		name, rest, ok := bytes.Cut(params, []byte{0})
		if !ok {
			return message, nil
		}
		value, rest, ok := bytes.Cut(rest, []byte{0})
		if !ok {
			return message, nil
		}
		if !bytes.HasPrefix(name, []byte("ssl")) || bytes.Contains(name, []byte(".")) {
			filtered = append(filtered, name...)
			filtered = append(filtered, 0)
			filtered = append(filtered, value...)
			filtered = append(filtered, 0)
		}
		params = rest
	}
	if len(params) != 1 {
		return message, nil
	}
	filtered = append(filtered, 0)
	binary.BigEndian.PutUint32(filtered[:4], uint32(len(filtered)))
	return filtered, nil
}

// postgresClientConn replays the filtered startup packet before normal TLS reads.
type postgresClientConn struct {
	*tls.Conn
	startup []byte
}

func (c *postgresClientConn) Read(p []byte) (int, error) {
	if len(c.startup) > 0 {
		n := copy(p, c.startup)
		c.startup = c.startup[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}
