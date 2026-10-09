package pod

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type tunnelBackend struct{ net.Conn }

func (b tunnelBackend) Tunnel(_ context.Context, in *pb.ContainerTunnelRequest) (*pb.ContainerTunnelResponse, error) {
	out := &pb.ContainerTunnelResponse{}
	switch {
	case in.Close:
		return out, b.Close()
	case in.Read:
		buffer := make([]byte, 64<<10)
		n, err := b.Read(buffer)
		out.Output, out.OutputEof = buffer[:n], n == 0 && err == io.EOF
		if err == io.EOF {
			err = nil
		}
		return out, err
	case in.InputEof:
		return out, b.Conn.(*net.TCPConn).CloseWrite()
	case len(in.Input) > 0:
		_, err := b.Write(in.Input)
		return out, err
	}
	return out, nil
}

func newVMTunnel(t *testing.T, serve func(net.Conn)) func() *websocket.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		serve(conn)
	}()
	backend, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	backend.SetDeadline(time.Now().Add(5 * time.Second))
	e := echo.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := e.NewContext(r, w)
		request, err := abstractions.ParseTunnelRequest(ctx, "vm", 2222)
		if err == nil {
			err = abstractions.ServeTunnel(ctx, tunnelBackend{backend}, request)
		}
		if err != nil {
			e.HTTPErrorHandler(err, ctx)
		}
	}))
	t.Cleanup(server.Close)
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "?session=" + uuid.NewString()
	return func() *websocket.Conn {
		client, _, err := websocket.DefaultDialer.Dial(url, nil)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })
		client.SetReadDeadline(time.Now().Add(5 * time.Second))
		return client
	}
}

func tunnelControl(t *testing.T, client *websocket.Conn, kind string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	require.NoError(t, client.WriteJSON(map[string]string{"type": kind, "id": id.String()}))
	return id
}

func TestVMTunnelReadsResponseAfterInputEOF(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), (1<<20)+1)
	received := make(chan []byte, 1)
	client := newVMTunnel(t, func(conn net.Conn) {
		body, _ := io.ReadAll(conn)
		received <- body
		conn.Write([]byte("response after EOF"))
	})()
	for start := 0; start < len(payload); start += 64 << 10 {
		id := uuid.New()
		frame := append(append(id[:], make([]byte, 16)...), payload[start:min(start+64<<10, len(payload))]...)
		require.NoError(t, client.WriteMessage(websocket.BinaryMessage, frame))
		var ack map[string]string
		require.NoError(t, client.ReadJSON(&ack))
		require.Equal(t, id.String(), ack["id"])
	}
	tunnelControl(t, client, "eof")
	_, _, err := client.ReadMessage()
	require.NoError(t, err)
	id := tunnelControl(t, client, "read")
	_, body, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, append(id[:], []byte("response after EOF")...), body)
	require.Equal(t, payload, <-received)
}

func TestVMTunnelReattachesAfterGatewayDisconnect(t *testing.T) {
	connect := newVMTunnel(t, func(conn net.Conn) {
		_, _ = io.ReadAll(conn)
		conn.Write([]byte("still connected"))
	})
	client := connect()
	tunnelControl(t, client, "eof")
	_, ack, err := client.ReadMessage()
	require.NoError(t, err)
	require.True(t, json.Valid(ack))
	client.Close()
	client = connect()
	id := tunnelControl(t, client, "read")
	_, body, err := client.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, append(id[:], []byte("still connected")...), body)
}
