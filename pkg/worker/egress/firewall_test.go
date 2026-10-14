package egress

import (
	"bufio"
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
	"strconv"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func testFirewall(t *testing.T, upstream string, policy Policy) *Firewall {
	t.Helper()
	dst, err := net.ResolveTCPAddr("tcp", upstream)
	require.NoError(t, err)
	f := NewFirewall(types.EgressConfig{MaxConnectionsPerContainer: 2}, []string{"127.0.0.1"}, Dependencies{
		OriginalDestination: func(net.Conn) (*net.TCPAddr, error) { return dst, nil },
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, upstream)
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, f.Register("box", []string{"127.0.0.1"}, policy))
	require.NoError(t, f.Start(ctx))
	t.Cleanup(f.Stop)
	return f
}

func TestFirewallRoutesAndLiveUpdate(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "allowed") }))
	defer upstream.Close()
	for _, tc := range []struct {
		name, host string
		allow      []string
		ok         bool
	}{
		{"SNI", "api.example.com", []string{"api.example.com"}, true},
		{"wildcard", "api.example.com", []string{"*.example.com"}, true},
		{"same IP different SNI", "other.example.com", []string{"api.example.com"}, false},
		{"no SNI", "", []string{"api.example.com"}, false},
		{"CIDR", "", []string{"127.0.0.1/32"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testFirewall(t, upstream.Listener.Addr().String(), Policy{AllowList: tc.allow})
			conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.config.TLSPort)))
			require.NoError(t, err)
			client := tls.Client(conn, &tls.Config{ServerName: tc.host, InsecureSkipVerify: true})
			defer client.Close()
			require.NoError(t, client.SetDeadline(time.Now().Add(time.Second)))
			err = client.Handshake()
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, f.Register("box", []string{"127.0.0.1"}, Policy{}))
			_, err = client.Read(make([]byte, 1))
			require.Error(t, err, "policy changes revoke existing streams")
		})
	}
}

func TestHTTPHostRawTCPAndConnectionCap(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "allowed") }))
	defer upstream.Close()
	for _, tc := range []struct {
		host         string
		ok, brokered bool
	}{{"api.example.com", true, false}, {"api.example.com:80", true, false}, {"evil.example.com", false, false}, {"api.example.com", false, true}} {
		policy := Policy{AllowList: []string{"api.example.com"}}
		if tc.brokered {
			policy.Bindings = []types.SecretBinding{{Hosts: []string{"api.example.com"}, Placeholder: "beam_secret_test", Value: "real-key"}}
		}
		f := testFirewall(t, upstream.Listener.Addr().String(), policy)
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", f.config.HTTPPort))
		require.NoError(t, err)
		conn.SetDeadline(time.Now().Add(time.Second))
		fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", tc.host)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if tc.ok {
			require.NoError(t, err)
			response.Body.Close()
		} else {
			require.Error(t, err)
		}
		conn.Close()
		f.Stop()
	}
	f := testFirewall(t, upstream.Listener.Addr().String(), Policy{AllowList: []string{"127.0.0.1/32"}})
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", f.config.OtherPort))
	require.NoError(t, err)
	fmt.Fprint(conn, "GET / HTTP/1.0\r\n\r\n")
	conn.SetDeadline(time.Now().Add(time.Second))
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	response.Body.Close()
	conn.Close()
	c, _ := f.lookup("box")
	connectionCount := func() int {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.connections)
	}
	// Closing the client does not synchronously release the proxy's slot.
	// Wait before filling the cap so neither pending connection is rejected.
	require.Eventually(t, func() bool { return connectionCount() == 0 }, time.Second, time.Millisecond)
	for range 2 {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", f.config.TLSPort))
		require.NoError(t, err)
		defer conn.Close()
	}
	require.Eventually(t, func() bool { return connectionCount() == 2 }, time.Second, time.Millisecond)
	extra, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", f.config.TLSPort))
	require.NoError(t, err)
	defer extra.Close()
	extra.SetDeadline(time.Now().Add(time.Second))
	_, err = extra.Read(make([]byte, 1))
	require.Error(t, err)
	require.False(t, os.IsTimeout(err), "the connection cap must close the socket, not leave it idle")
}

func TestBindingRefreshDoesNotBlockPolicyOrAccept(t *testing.T) {
	releaseCtx, release := context.WithCancel(context.Background())
	defer release()
	started := make(chan struct{})
	f := NewFirewall(types.EgressConfig{}, nil, Dependencies{RefreshBindings: func(_ context.Context, _ string, bindings []types.SecretBinding) ([]types.SecretBinding, error) {
		close(started)
		<-releaseCtx.Done()
		bindings[0].Value = "stale"
		return bindings, nil
	}})
	policy := Policy{Bindings: []types.SecretBinding{{Value: "initial"}}}
	require.NoError(t, f.Register("box", []string{"127.0.0.1"}, policy))
	c, _ := f.lookup("box")
	refreshed := make(chan []types.SecretBinding, 1)
	go func() { bindings, _ := f.bindings(context.Background(), c); refreshed <- bindings }()
	<-started
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() { conn, _ := (&policyListener{Listener: listener, firewall: f}).Accept(); accepted <- conn }()
	client, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer client.Close()
	select {
	case conn := <-accepted:
		require.NotNil(t, conn)
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("refresh blocked the listener")
	}
	policy.Bindings[0].Value = "current"
	updated := make(chan error, 1)
	go func() { updated <- f.Register("box", nil, policy) }()
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("refresh blocked a policy update")
	}
	release()
	bindings := <-refreshed
	require.Len(t, bindings, 1)
	require.Equal(t, "current", bindings[0].Value)
	f.Unregister("box")
}

func TestHostnameDialRejectsPrivateAddresses(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "192.168.0.1", "169.254.169.254", "::1", "fd00:abcd::1", "fe80::1", "100.64.0.1", "0.0.0.0", "64:ff9b::a00:1", "64:ff9b::7f00:1"} {
		require.ErrorIs(t, checkPublicAddress(context.Background(), "tcp", net.JoinHostPort(ip, "443"), nil), errPrivateAddress)
	}
	_, err := dialPublic(context.Background(), "tcp", "localhost:443")
	require.Error(t, err)
	require.NoError(t, checkPublicAddress(context.Background(), "tcp", "8.8.8.8:443", nil))
}

func TestSelectiveTLSInterceptionAndHTTP2(t *testing.T) {
	bindings := []types.SecretBinding{{EnvName: "KEY", SecretName: "KEY", Placeholder: "beam_secret_placeholder", Value: "real-key", Hosts: []string{"api.example.com"}}}
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer real-key", r.Header.Get("Authorization"))
		w.Header().Set("X-Echo", r.Header.Get("Authorization"))
		io.WriteString(w, "event: echo\ndata: real-key\n\n")
	}))
	defer upstream.Close()
	f := testFirewall(t, upstream.Listener.Addr().String(), Policy{AllowList: []string{"api.example.com"}, Bindings: bindings})
	// The test upstream certificate has example.com in its SANs.
	f.deps.RootCertificates = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})
	bindings[0].Hosts = []string{"example.com"}
	require.NoError(t, f.Register("box", []string{"127.0.0.1"}, Policy{AllowList: []string{"example.com"}, Bindings: bindings}))
	bundle, err := f.TrustBundle("box", t.TempDir())
	require.NoError(t, err)
	data, err := os.ReadFile(bundle)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(data))
	for _, h2 := range []bool{false, true} {
		transport := &http.Transport{ForceAttemptHTTP2: h2, TLSClientConfig: &tls.Config{RootCAs: roots}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, fmt.Sprintf("127.0.0.1:%d", f.config.TLSPort))
		}}
		client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
		req, err := http.NewRequest("GET", "https://example.com/", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer beam_secret_placeholder")
		response, err := client.Do(req)
		require.NoError(t, err)
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, "event: echo\ndata: beam_secret_placeholder\n\n", string(body))
		require.Equal(t, "Bearer beam_secret_placeholder", response.Header.Get("X-Echo"))
		if h2 {
			require.Equal(t, 2, response.ProtoMajor)
		} else {
			require.Equal(t, 1, response.ProtoMajor)
		}
		transport.CloseIdleConnections()
	}
}
