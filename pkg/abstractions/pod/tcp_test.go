package pod

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tcpGatewayHandshake(t *testing.T, client *tls.Config) (tls.ConnectionState, error) {
	cert, err := selfSignedCertificate("tcp.example.com")
	require.NoError(t, err)
	pts := &PodTCPServer{tlsCert: cert}

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	go func() { _ = tls.Server(serverConn, pts.tlsConfig()).Handshake() }()

	conn := tls.Client(clientConn, client)
	err = conn.Handshake()
	return conn.ConnectionState(), err
}

// Without SNI a connection cannot be routed; the client must hear why.
func TestTCPGatewayRejectsClientsWithoutSNI(t *testing.T) {
	_, err := tcpGatewayHandshake(t, &tls.Config{InsecureSkipVerify: true})
	require.ErrorContains(t, err, "unrecognized name")
}

func TestTCPGatewayNegotiatesALPNOnlyForPostgres(t *testing.T) {
	host := "app-abc1234-latest-8123.tcp.example.com"
	state, err := tcpGatewayHandshake(t, &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         host,
		NextProtos:         []string{"h2", "http/1.1"},
	})
	require.NoError(t, err)
	require.Empty(t, state.NegotiatedProtocol)

	state, err = tcpGatewayHandshake(t, &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         host,
		NextProtos:         []string{postgresALPN},
	})
	require.NoError(t, err)
	require.Equal(t, postgresALPN, state.NegotiatedProtocol)
}

func TestPreparePostgresAwareTLSConnAcceptsDirectTLS(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan net.Conn, 1)
	go func() {
		conn, postgres, err := preparePostgresAwareTLSConn(server)
		require.NoError(t, err)
		require.False(t, postgres)
		done <- conn
	}()

	_, err := client.Write([]byte{0x16, 0x03, 0x01})
	require.NoError(t, err)

	conn := <-done
	buf := make([]byte, 3)
	_, err = conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, []byte{0x16, 0x03, 0x01}, buf)
}

func TestPreparePostgresAwareTLSConnAcceptsPostgresSSLRequest(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		_, postgres, err := preparePostgresAwareTLSConn(server)
		if err == nil && !postgres {
			err = errors.New("an SSLRequest was not reported")
		}
		done <- err
	}()

	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], 8)
	binary.BigEndian.PutUint32(header[4:8], postgresSSLRequestCode)
	_, err := client.Write(header)
	require.NoError(t, err)

	response := make([]byte, 1)
	_, err = client.Read(response)
	require.NoError(t, err)
	require.Equal(t, []byte("S"), response)

	require.NoError(t, <-done)
}

func TestPreparePostgresAwareTLSConnRejectsPlaintext(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		_, _, err := preparePostgresAwareTLSConn(server)
		done <- err
	}()

	_, err := client.Write([]byte("GET / HT"))
	require.NoError(t, err)

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("preparePostgresAwareTLSConn did not reject plaintext")
	}
}

func postgresStartup(version uint32, params ...string) []byte {
	var body []byte
	for _, param := range params {
		body = append(append(body, param...), 0)
	}
	body = append(body, 0)
	message := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint32(message[0:4], uint32(cap(message)))
	binary.BigEndian.PutUint32(message[4:8], version)
	return append(message, body...)
}

// postgres.js sends each URL parameter it does not know as a startup parameter,
// and Beam's DATABASE_URL carries libpq's sslmode and sslrootcert, which the
// server ends the connection over.
func TestReadPostgresStartupDropsSSLParameters(t *testing.T) {
	const protocol3 = 3 << 16
	next := []byte("next message")
	stream := bytes.NewReader(append(postgresStartup(protocol3,
		"user", "app", "database", "app", "sslrootcert", "system", "sslmode", "verify-full",
		"sslinfo.level", "1", "application_name", "postgres.js",
	), next...))

	startup, err := readPostgresStartup(stream)

	require.NoError(t, err)
	require.Equal(t, postgresStartup(protocol3,
		"user", "app", "database", "app", "sslinfo.level", "1", "application_name", "postgres.js",
	), startup)
	rest, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, next, rest)

	cancel := make([]byte, 16)
	binary.BigEndian.PutUint32(cancel[0:4], 16)
	binary.BigEndian.PutUint32(cancel[4:8], 80877102)
	startup, err = readPostgresStartup(bytes.NewReader(cancel))
	require.NoError(t, err)
	require.Equal(t, cancel, startup)
}

// The rewritten startup message reaches the pod first, and routing still sees
// the TLS connection's SNI.
func TestPostgresClientConnReplaysStartupOverTLS(t *testing.T) {
	host := "app-abc1234-latest-5432.tcp.example.com"
	cert, err := selfSignedCertificate("tcp.example.com")
	require.NoError(t, err)
	pts := &PodTCPServer{tlsCert: cert}
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	go func() {
		client := tls.Client(clientConn, &tls.Config{InsecureSkipVerify: true, ServerName: host, NextProtos: []string{postgresALPN}})
		_, _ = client.Write([]byte("rest"))
	}()
	tlsConn := tls.Server(serverConn, pts.tlsConfig())
	require.NoError(t, tlsConn.Handshake())

	var conn net.Conn = &postgresClientConn{Conn: tlsConn, startup: []byte("startup ")}

	got := make([]byte, len("startup rest"))
	_, err = io.ReadFull(conn, got)
	require.NoError(t, err)
	require.Equal(t, "startup rest", string(got))
	state := conn.(interface{ ConnectionState() tls.ConnectionState }).ConnectionState()
	require.Equal(t, host, state.ServerName)
	require.Implements(t, (*interface{ CloseWrite() error })(nil), conn)
}
