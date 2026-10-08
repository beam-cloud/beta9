package pod

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestVMStreamReadsResponseAfterInputEOF(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	payload := bytes.Repeat([]byte("a"), (1<<20)+1)
	received := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			received <- nil
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		body, _ := io.ReadAll(conn)
		received <- body
		conn.Write([]byte("response after EOF"))
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer backend.Close()
		upgrader := websocket.Upgrader{}
		client, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer client.Close()
		bridgeVMStream(context.Background(), client, backend)
	}))
	defer server.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.Close()
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	require.NoError(t, client.WriteMessage(websocket.BinaryMessage, payload))
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("EOF")))
	kind, body, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, kind)
	require.Equal(t, "response after EOF", string(body))
	require.Equal(t, payload, <-received)
}

func TestVMStreamDisconnectAfterEOFCancelsIdleBackend(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	eof := make(chan struct{})
	finish := make(chan struct{})
	defer close(finish)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.ReadAll(conn)
		close(eof)
		<-finish // The service keeps its response side open after EOF.
	}()
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			return
		}
		defer backend.Close()
		client, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer client.Close()
		bridgeVMStream(context.Background(), client, backend)
		close(released)
	}))
	defer server.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte("EOF")))
	select {
	case <-eof:
	case <-time.After(5 * time.Second):
		t.Fatal("EOF was not forwarded")
	}
	client.Close()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected tunnel still holds the backend open")
	}
}
