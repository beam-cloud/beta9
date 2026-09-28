package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
)

func TestNetworkReporterCollect(t *testing.T) {
	lookups := 0
	ipify := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		lookups++
		_, _ = w.Write([]byte("203.0.113.7\n"))
	}))
	defer ipify.Close()
	status := &fakeTSNetStatusClient{status: &ipnstate.Status{
		// MagicDNS suffixed the requested name after a collision.
		Self:         &ipnstate.PeerStatus{DNSName: "beam-agent-m1-1.tail1234.ts.net."},
		TailscaleIPs: []netip.Addr{netip.MustParseAddr("fd7a:115c:a1e0::1"), netip.MustParseAddr("100.64.0.7")},
	}}
	reporter := newNetworkReporter(nil, status, "beam-agent-m1", func() bool { return true })
	reporter.publicIPURL = ipify.URL

	info := reporter.collect(context.Background())
	require.Equal(t, "203.0.113.7", info.PublicIp)
	require.Equal(t, "100.64.0.7", info.TailnetIp)
	require.Equal(t, "beam-agent-m1-1", info.TailnetHostname)
	require.True(t, info.TailnetSsh)

	reporter.collect(context.Background())
	require.Equal(t, 1, lookups, "public IP is cached between refreshes")
}

type fakeTailnetPrefsClient struct {
	prefs ipn.Prefs
	edits []bool
}

func (c *fakeTailnetPrefsClient) GetPrefs(context.Context) (*ipn.Prefs, error) {
	prefs := c.prefs
	return &prefs, nil
}

func (c *fakeTailnetPrefsClient) EditPrefs(_ context.Context, mp *ipn.MaskedPrefs) (*ipn.Prefs, error) {
	if mp.RunSSHSet {
		c.edits = append(c.edits, mp.RunSSH)
		c.prefs.RunSSH = mp.RunSSH
	}
	prefs := c.prefs
	return &prefs, nil
}

func TestTailnetSSHReconcile(t *testing.T) {
	client := &fakeTailnetPrefsClient{}
	ssh := &tailnetSSH{client: client, want: true, stderr: io.Discard}
	require.NoError(t, ssh.reconcile(context.Background()))
	require.NoError(t, ssh.reconcile(context.Background()))
	require.Equal(t, []bool{true}, client.edits, "a matching pref is left alone")
	require.True(t, ssh.enabled())

	// The gateway withdrew SSH: the persisted pref is switched off again.
	off := &tailnetSSH{client: client, stderr: io.Discard}
	require.NoError(t, off.reconcile(context.Background()))
	require.Equal(t, []bool{true, false}, client.edits)
	require.False(t, off.enabled())
}
