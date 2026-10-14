package egress

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestHeaderSubstitutionAndStreamingScrub(t *testing.T) {
	bindings := []types.SecretBinding{
		{SecretName: "A", Placeholder: "beam_secret_a", Value: "secret-a", Hosts: []string{"api.example.com"}},
		{SecretName: "B", Placeholder: "beam_secret_b", Value: "secret-b", Hosts: []string{"other.example.com"}},
	}
	headers := http.Header{"Authorization": []string{"Bearer beam_secret_a / beam_secret_a / beam_secret_b"}}
	substituteHeaders(headers, bindings, "api.example.com")
	require.Equal(t, "Bearer secret-a / secret-a / beam_secret_b", headers.Get("Authorization"))
	scrubHeaders(headers, bindings)
	require.Equal(t, "Bearer beam_secret_a / beam_secret_a / beam_secret_b", headers.Get("Authorization"))
	payload := "data: secret-a / secret-b / ordinary SSE\n\n"
	for split := 1; split < len(payload); split++ {
		source := io.MultiReader(bytes.NewBufferString(payload[:split]), bytes.NewBufferString(payload[split:]))
		result, err := io.ReadAll(newScrubber(source, bindings))
		require.NoError(t, err)
		require.Equal(t, "data: beam_secret_a / beam_secret_b / ordinary SSE\n\n", string(result))
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	go func() { writer.Write([]byte("data: immediately\n\n")); writer.Close() }()
	result := make(chan string, 1)
	go func() {
		buffer := make([]byte, 100)
		n, _ := newScrubber(reader, bindings).Read(buffer)
		result <- string(buffer[:n])
	}()
	select {
	case got := <-result:
		require.Equal(t, "data: immediately\n\n", got)
	case <-time.After(time.Second):
		t.Fatal("SSE flush stalled")
	}
}

func TestRotationAndWebSocketScrub(t *testing.T) {
	bindings := []types.SecretBinding{{SecretName: "A", Placeholder: "beam_secret_a", Value: "old", Hosts: []string{"api.example.com"}}}
	f := NewFirewall(types.EgressConfig{}, nil, Dependencies{RefreshBindings: func(_ context.Context, _ string, bindings []types.SecretBinding) ([]types.SecretBinding, error) {
		updated := append([]types.SecretBinding(nil), bindings...)
		updated[0].Value = "new"
		return updated, nil
	}})
	require.NoError(t, f.Register("box", nil, Policy{Bindings: bindings}))
	c, _ := f.lookup("box")
	got, err := f.bindings(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, "new", got[0].Value)
	c.policy.Bindings[0].Value = "cached"
	got, err = f.bindings(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, "cached", got[0].Value)
	c.refreshed = time.Now().Add(-bindingTTL)
	got, err = f.bindings(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, "new", got[0].Value)
	frame := append(webSocketFrame(1, []byte("n")), webSocketFrame(0, []byte("ew"))...)
	frame[0] &= 0x7f
	upstream := &testUpgrade{Buffer: *bytes.NewBuffer(frame)}
	result, err := io.ReadAll(newWebSocketScrubber(upstream, got))
	require.NoError(t, err)
	require.Equal(t, webSocketFrame(1, []byte("beam_secret_a")), result)
	c.policy.BindingsOnly = true
	c.policy.AllowList = []string{"other.example.com"}
	response := httptest.NewRecorder()
	f.handler(c, "api.example.com", 443).ServeHTTP(response, httptest.NewRequest("GET", "https://api.example.com/", nil))
	require.Equal(t, http.StatusForbidden, response.Code)
}

type testUpgrade struct{ bytes.Buffer }

func (*testUpgrade) Close() error { return nil }

func TestSSEFlushAndWebSocketUpgrade(t *testing.T) {
	binding := types.SecretBinding{SecretName: "A", Placeholder: "beam_secret_a", Value: "real-key", Hosts: []string{"example.com"}}
	stop := make(chan struct{})

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "real-key", r.Header.Get("X-Api-Key"))
		if r.URL.Path == "/sse" {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: real-key\n\n")
			w.(http.Flusher).Flush()
			<-stop
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteMessage(websocket.TextMessage, []byte("real-key"))
		_, _, _ = conn.ReadMessage()
	}))
	defer func() { close(stop); upstream.Close() }()
	f := testFirewall(t, upstream.Listener.Addr().String(), Policy{AllowList: []string{"example.com"}, Bindings: []types.SecretBinding{binding}})
	f.deps.RootCertificates = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})
	bundle, err := f.TrustBundle("box", t.TempDir())
	require.NoError(t, err)
	certs, err := os.ReadFile(bundle)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(certs))
	tlsConfig := &tls.Config{RootCAs: roots}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, fmt.Sprintf("127.0.0.1:%d", f.config.TLSPort))
	}
	client := &http.Client{Transport: &http.Transport{DialContext: dial, TLSClientConfig: tlsConfig}, Timeout: time.Second}
	req, _ := http.NewRequest("GET", "https://example.com/sse", nil)
	req.Header.Set("X-Api-Key", binding.Placeholder)
	response, err := client.Do(req)
	require.NoError(t, err)
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "data: beam_secret_a\n", line)
	response.Body.Close()
	dialer := websocket.Dialer{NetDialContext: dial, TLSClientConfig: tlsConfig, HandshakeTimeout: time.Second}
	conn, _, err := dialer.Dial("wss://example.com/ws", http.Header{"X-Api-Key": []string{binding.Placeholder}})
	require.NoError(t, err)
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, payload, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, binding.Placeholder, string(payload))
}
