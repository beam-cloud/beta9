package network

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/dns/dnsmessage"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

var (
	serverRegistry = make(map[string]*Tailscale)
	registryLock   = sync.Mutex{}
)

// GetOrCreateTailscale checks the registry for an existing server by name.
// If it exists, it returns that; otherwise, it creates and registers a new one.
func GetOrCreateTailscale(cfg TailscaleConfig) *Tailscale {
	registryLock.Lock()
	defer registryLock.Unlock()

	// Check if the server already exists
	if ts, exists := serverRegistry[cfg.Hostname]; exists {
		return ts
	}

	// Create a new Tailscale server since it doesn't exist
	ts := newTailscale(cfg)
	serverRegistry[cfg.Hostname] = ts
	return ts
}

type TailscaleConfig struct {
	Dir        string // Directory for state storage
	Hostname   string // Hostname for the Tailscale node
	AuthKey    string // Auth key for Tailscale
	ControlURL string // Control server URL
	Ephemeral  bool   // Whether the node is ephemeral
	Debug      bool
}

var (
	tailnetPeerPollInterval    = 500 * time.Millisecond
	tailnetPeerAdvisoryTimeout = time.Second
)

type Tailscale struct {
	mu          sync.Mutex // guards server and initialized
	server      *tsnet.Server
	initialized bool // server has been brought up

	cfg   TailscaleConfig
	debug bool

	// statusFunc and dialFunc override tailnet operations in tests.
	statusFunc    func(ctx context.Context) (*ipnstate.Status, error)
	dialFunc      func(ctx context.Context, network, addr string) (net.Conn, error)
	agentPeerFunc func(context.Context, string) (netip.Addr, *tailcfg.Node, error)
}

func (t *Tailscale) logF(format string, v ...interface{}) {
	if t.debug {
		log.Info().Msgf(format, v...)
	}
}

// NewTailscale creates a new Tailscale instance using tsnet
func newTailscale(cfg TailscaleConfig) *Tailscale {
	t := &Tailscale{
		cfg:   cfg,
		debug: cfg.Debug,
	}
	t.server = t.buildServer()
	return t
}

func (t *Tailscale) buildServer() *tsnet.Server {
	return &tsnet.Server{
		Dir:        t.cfg.Dir,
		Hostname:   t.cfg.Hostname,
		AuthKey:    t.cfg.AuthKey,
		ControlURL: t.cfg.ControlURL,
		Ephemeral:  t.cfg.Ephemeral,
		UserLogf:   t.logF,
		Logf:       t.logF,
	}
}

func (t *Tailscale) currentServer() *tsnet.Server {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.server
}

func (t *Tailscale) ensureUp(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.initialized {
		return nil
	}
	if _, err := t.server.Up(ctx); err != nil {
		return err
	}
	t.initialized = true
	return nil
}

func (t *Tailscale) Start(ctx context.Context) error {
	return t.ensureUp(ctx)
}

// Dial attempts to establish a TCP connection to a tailscale service
func (t *Tailscale) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	if t.dialFunc != nil {
		return t.dialFunc(ctx, network, addr)
	}
	if err := t.ensureUp(ctx); err != nil {
		return nil, err
	}

	conn, err := t.currentServer().Dial(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// WaitForPeer blocks until host is visible in this node's tailnet netmap, or
// the timeout elapses. Dialing a MagicDNS name for a peer that is missing from
// the netmap silently falls back to the system resolver and surfaces as a
// confusing NXDOMAIN ("no such host"); callers should use this to fail with a
// clear error instead. This lookup is deliberately advisory: request-path
// failures must never close or replace the shared gateway tsnet server because
// doing so tears down healthy traffic to every other peer.
func (t *Tailscale) WaitForPeer(ctx context.Context, host string, timeout time.Duration) error {
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	if host == "" {
		return nil
	}
	if _, err := netip.ParseAddr(host); err == nil {
		// IP targets don't go through MagicDNS; tsnet dials them directly
		// from the netmap and fails fast on its own.
		return nil
	}

	deadline := time.Now().Add(timeout)
	probeCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		probeCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	var lastErr error

poll:
	for {
		found, err := t.peerInNetmap(probeCtx, host)
		if err == nil && found {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		lastErr = err

		if time.Now().Add(tailnetPeerPollInterval).After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-probeCtx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			break poll
		case <-time.After(tailnetPeerPollInterval):
		}
	}

	if lastErr != nil {
		return fmt.Errorf("tailnet netmap status unavailable while resolving peer %q: %w", host, lastErr)
	}
	return fmt.Errorf("tailnet peer %q is not visible in this node's netmap (peer is offline or has been removed)", host)
}

func (t *Tailscale) peerInNetmap(ctx context.Context, host string) (bool, error) {
	status, err := t.netmapStatus(ctx)
	if err != nil {
		return false, err
	}
	if status == nil {
		return false, nil
	}
	if status.Self != nil && tailnetPeerMatchesHost(status.Self.HostName, status.Self.DNSName, host) {
		return true, nil
	}
	for _, peer := range status.Peer {
		if peer == nil {
			continue
		}
		if tailnetPeerMatchesHost(peer.HostName, peer.DNSName, host) {
			return true, nil
		}
	}
	return false, nil
}

func (t *Tailscale) netmapStatus(ctx context.Context) (*ipnstate.Status, error) {
	if t.statusFunc != nil {
		return t.statusFunc(ctx)
	}
	if err := t.ensureUp(ctx); err != nil {
		return nil, err
	}
	client, err := t.currentServer().LocalClient()
	if err != nil {
		return nil, err
	}
	statusCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return client.Status(statusCtx)
}

// PeerStatuses returns the gateway's current view of its tailnet peers,
// including their advertised endpoints and tailnet addresses.
func (t *Tailscale) PeerStatuses(ctx context.Context) ([]*ipnstate.PeerStatus, error) {
	status, err := t.netmapStatus(ctx)
	if err != nil || status == nil {
		return nil, err
	}
	peers := make([]*ipnstate.PeerStatus, 0, len(status.Peer))
	for _, peer := range status.Peer {
		if peer != nil {
			peers = append(peers, peer)
		}
	}
	return peers, nil
}

func tailnetPeerMatchesHost(hostName, dnsName, target string) bool {
	target = strings.TrimSuffix(target, ".")
	hostName = strings.TrimSuffix(hostName, ".")
	dnsName = strings.TrimSuffix(dnsName, ".")
	return target == hostName || target == dnsName || strings.HasPrefix(dnsName, target+".")
}

func (t *Tailscale) DialContextTimeout(ctx context.Context, network, addr string, timeout time.Duration) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return t.Dial(ctx, network, addr)
}

// Stops the Tailscale server
func (t *Tailscale) Close() error {
	return t.currentServer().Close()
}

// agentRouteTarget pins the destination to the expected machine's tailnet IP.
// Lookup failures may reflect peer propagation; identity mismatches fail immediately.
func (t *Tailscale) agentRouteTarget(ctx context.Context, machineID, target string) (string, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return "", fmt.Errorf("invalid agent proxy target: %w", err)
	}
	if port != strconv.Itoa(types.DefaultAgentTSNetRouteProxyPort) || machineID == "" {
		return "", fmt.Errorf("invalid agent proxy identity or port")
	}
	lookup := t.agentPeerFunc
	if lookup == nil {
		lookup = t.lookupAgentPeer
	}
	for {
		ip, peer, err := lookup(ctx, host)
		if err != nil {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("lookup agent route: %v: %w", err, ctx.Err())
			case <-time.After(backendRouteDialRetryInterval):
				continue
			}
		}
		if peer == nil || !peer.Hostinfo.Valid() || peer.Hostinfo.Hostname() != types.AgentTailnetHostnamePrefix+machineID {
			return "", fmt.Errorf("agent proxy destination does not belong to machine %s", machineID)
		}
		for _, address := range peer.Addresses {
			if address.IsSingleIP() && address.Addr().Unmap() == ip.Unmap() {
				return net.JoinHostPort(ip.String(), port), nil
			}
		}
		return "", fmt.Errorf("agent proxy destination is not an address of machine %s", machineID)
	}
}

func (t *Tailscale) lookupAgentPeer(ctx context.Context, host string) (netip.Addr, *tailcfg.Node, error) {
	if err := t.ensureUp(ctx); err != nil {
		return netip.Addr{}, nil, err
	}
	client, err := t.currentServer().LocalClient()
	if err != nil {
		return netip.Addr{}, nil, err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		// Resolve legacy names through this node's MagicDNS.
		if !strings.Contains(host, ".") {
			status, statusErr := client.StatusWithoutPeers(ctx)
			if statusErr != nil {
				return netip.Addr{}, nil, statusErr
			}
			if status.CurrentTailnet == nil || status.CurrentTailnet.MagicDNSSuffix == "" {
				return netip.Addr{}, nil, fmt.Errorf("tailnet DNS suffix is unavailable")
			}
			host += "." + status.CurrentTailnet.MagicDNSSuffix
		}
		for _, query := range []string{"A", "AAAA"} {
			packet, _, queryErr := client.QueryDNS(ctx, host, query)
			if queryErr != nil {
				err = queryErr
				continue
			}
			var message dnsmessage.Message
			if err = message.Unpack(packet); err != nil {
				continue
			}
			for _, answer := range message.Answers {
				switch value := answer.Body.(type) {
				case *dnsmessage.AResource:
					ip = netip.AddrFrom4(value.A)
				case *dnsmessage.AAAAResource:
					ip = netip.AddrFrom16(value.AAAA)
				}
				if ip.IsValid() {
					break
				}
			}
			if ip.IsValid() {
				break
			}
		}
		if !ip.IsValid() {
			return netip.Addr{}, nil, fmt.Errorf("resolve agent proxy destination: %v", err)
		}
	}
	identity, err := client.WhoIs(ctx, ip.String())
	if err != nil {
		return netip.Addr{}, nil, err
	}
	if identity == nil || identity.Node == nil {
		return netip.Addr{}, nil, fmt.Errorf("tailnet node identity is unavailable")
	}
	return ip, identity.Node, nil
}
