//go:build linux && integration

package egress

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// Run inside a disposable privileged Linux network namespace.
func TestTransparentFirewall(t *testing.T) {
	namespace := "beam-egress-test"
	run := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		require.NoError(t, err, "%v: %s", args, out)
		return string(out)
	}
	run("ip", "netns", "add", namespace)
	t.Cleanup(func() {
		exec.Command("ip", "netns", "del", namespace).Run()
		exec.Command("ip", "link", "del", "b9-egress-host").Run()
	})
	run("ip", "link", "add", "b9-egress-host", "type", "veth", "peer", "name", "b9-egress-box")
	run("ip", "link", "set", "b9-egress-box", "netns", namespace)
	run("ip", "addr", "add", "192.0.2.1/24", "dev", "b9-egress-host")
	run("ip", "addr", "add", "2001:db8::1/64", "dev", "b9-egress-host", "nodad")
	run("ip", "link", "set", "b9-egress-host", "up")
	run("ip", "netns", "exec", namespace, "ip", "addr", "add", "192.0.2.2/24", "dev", "b9-egress-box")
	run("ip", "netns", "exec", namespace, "ip", "addr", "add", "2001:db8::2/64", "dev", "b9-egress-box", "nodad")
	run("ip", "netns", "exec", namespace, "ip", "link", "set", "b9-egress-box", "up")
	run("ip", "netns", "exec", namespace, "ip", "link", "set", "lo", "up")
	run("ip", "netns", "exec", namespace, "ip", "route", "add", "default", "via", "192.0.2.1")
	run("ip", "netns", "exec", namespace, "ip", "-6", "route", "add", "default", "via", "2001:db8::1")
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer real-stage-key", r.Header.Get("Authorization"))
		w.Header().Set("X-Echo", "real-stage-key")
		io.WriteString(w, "real-stage-key")
	}))
	defer upstream.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})
	cfg := types.EgressConfig{TLSPort: 18043, HTTPPort: 18080, OtherPort: 18081}
	f := NewFirewall(cfg, []string{"192.0.2.1", "2001:db8::1"}, Dependencies{RootCertificates: cert, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	}})
	binding := types.SecretBinding{SecretName: "KEY", EnvName: "KEY", Placeholder: "beam_secret_stage", Value: "real-stage-key", Hosts: []string{"example.com"}}
	policy := Policy{AllowList: []string{"example.com"}, Bindings: []types.SecretBinding{binding}}
	ips := []string{"192.0.2.2", "2001:db8::2"}
	require.NoError(t, f.Register("box", ips, policy))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, f.Start(ctx))
	defer f.Stop()
	bundle, err := f.TrustBundle("box", t.TempDir())
	require.NoError(t, err)
	for family, ip := range ips {
		command := []string{"iptables", "ip6tables"}[family]
		for _, route := range []struct{ port, listener int }{{443, cfg.TLSPort}, {80, cfg.HTTPPort}, {0, cfg.OtherPort}} {
			args := []string{"-t", "nat", "-A", "PREROUTING", "-i", "b9-egress-host", "-s", ip, "-p", "tcp"}
			if route.port != 0 {
				args = append(args, "--dport", strconv.Itoa(route.port))
			}
			args = append(args, "-j", "REDIRECT", "--to-ports", strconv.Itoa(route.listener))
			run(append([]string{command}, args...)...)
			t.Cleanup(func() { args[2] = "-D"; exec.Command(command, args...).Run() })
		}
	}
	for _, destination := range []string{"8.8.8.8", "[2001:4860:4860::8888]"} {
		curl := func(host string) (string, error) {
			returnData, err := exec.Command("ip", "netns", "exec", namespace, "curl", "--noproxy", "*", "--silent", "--show-error", "--max-time", "3", "--cacert", bundle, "--resolve", host+":443:"+destination, "-H", "Authorization: Bearer "+binding.Placeholder, "https://"+host).CombinedOutput()
			return string(returnData), err
		}
		response, err := curl("example.com")
		require.NoError(t, err, response)
		require.Equal(t, binding.Placeholder, response)
		_, err = curl("other.example.com")
		require.Error(t, err, "same destination IP with another SNI must be denied")
		output, err := exec.Command("ip", "netns", "exec", namespace, "curl", "--noproxy", "*", "--silent", "--max-time", "3", "--write-out", "%{http_code}", "--resolve", "example.com:80:"+destination, "-H", "Authorization: "+binding.Placeholder, "http://example.com").CombinedOutput()
		require.Error(t, err, "bound hosts must reject plaintext HTTP")
		require.Equal(t, "000", string(output))
		require.NoError(t, f.Register("box", ips, Policy{}))
		_, err = curl("example.com")
		require.Error(t, err, "live update must deny the previous host")
		require.NoError(t, f.Register("box", ips, policy))
		response, err = curl("example.com")
		require.NoError(t, err, response)
		require.Equal(t, binding.Placeholder, response)
	}
	// Raw TCP is admitted without waiting for a client-first protocol.
	greeting, err := net.Listen("tcp", "192.0.2.1:19022")
	require.NoError(t, err)
	defer greeting.Close()
	source := make(chan string, 1)
	go func() {
		conn, err := greeting.Accept()
		if err == nil {
			defer conn.Close()
			source <- conn.RemoteAddr().String()
			io.WriteString(conn, "SSH-2.0-Beam\r\n")
		}
	}()
	require.NoError(t, f.Register("box", ips, Policy{AllowList: []string{"192.0.2.1/32"}}))
	output := run("ip", "netns", "exec", namespace, "sh", "-c", "timeout 3 nc 192.0.2.1 19022 | head -1")
	require.Equal(t, "SSH-2.0-Beam\r\n", output)
	host, _, err := net.SplitHostPort(<-source)
	require.NoError(t, err)
	require.Equal(t, "192.0.2.1", host)
	require.NoError(t, os.Remove(bundle))
}
