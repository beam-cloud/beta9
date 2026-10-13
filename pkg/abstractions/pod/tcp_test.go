package pod

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparePostgresAwareTLSConnAcceptsDirectTLS(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan net.Conn, 1)
	go func() {
		conn, err := preparePostgresAwareTLSConn(server)
		require.NoError(t, err)
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
		_, err := preparePostgresAwareTLSConn(server)
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
		_, err := preparePostgresAwareTLSConn(server)
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
