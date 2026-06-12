package network

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tsnet"
)

var (
	serverRegistry = make(map[string]*Tailscale)
	registryLock   = sync.Mutex{}
)

// GetOrCreateTailscale checks the registry for an existing server by name.
// If it exists, it returns that; otherwise, it creates and registers a new one.
func GetOrCreateTailscale(cfg TailscaleConfig, tailscaleRepo repository.TailscaleRepository) *Tailscale {
	registryLock.Lock()
	defer registryLock.Unlock()

	// Check if the server already exists
	if ts, exists := serverRegistry[cfg.Hostname]; exists {
		return ts
	}

	// Create a new Tailscale server since it doesn't exist
	ts := newTailscale(cfg, tailscaleRepo)
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

// Stale-netmap self-healing: when peers the control plane says are connected
// keep missing from this node's netmap, the tailnet control connection is
// likely a zombie. After enough misses spread over a window, the tsnet server
// is recycled (rate-limited) to force a fresh control connection and netmap.
var (
	staleNetmapMissThreshold   = 3
	staleNetmapMissWindow      = time.Minute
	staleNetmapRestartCooldown = 5 * time.Minute
	tailnetPeerPollInterval    = 500 * time.Millisecond
)

type Tailscale struct {
	server        *tsnet.Server
	cfg           TailscaleConfig
	debug         bool
	initialized   bool
	served        bool
	mu            sync.Mutex
	tailscaleRepo repository.TailscaleRepository

	// statusFunc overrides netmap status lookups in tests.
	statusFunc func(ctx context.Context) (*ipnstate.Status, error)

	missMu      sync.Mutex
	missCount   int
	firstMissAt time.Time
	lastRestart time.Time
}

func (t *Tailscale) logF(format string, v ...interface{}) {
	if t.debug {
		log.Info().Msgf(format, v...)
	}
}

// NewTailscale creates a new Tailscale instance using tsnet
func newTailscale(cfg TailscaleConfig, tailscaleRepo repository.TailscaleRepository) *Tailscale {
	ts := &Tailscale{
		cfg:           cfg,
		debug:         cfg.Debug,
		initialized:   false,
		mu:            sync.Mutex{},
		tailscaleRepo: tailscaleRepo,
	}

	ts.server = ts.buildServer()
	return ts
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

// Serve connects to a tailnet and serves a local service
func (t *Tailscale) Serve(ctx context.Context, service types.InternalService) (net.Listener, error) {
	server := t.currentServer()
	log.Info().Str("url", server.ControlURL).Msg("connecting to tailnet")

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	addr := fmt.Sprintf(":%d", service.LocalPort)
	listener, err := server.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	_, err = server.Up(timeoutCtx)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.served = true
	t.initialized = true
	t.mu.Unlock()

	log.Info().Str("addr", addr).Msg("connected to tailnet")
	return listener, nil
}

// Dial attempts to establish a TCP connection to a tailscale service
func (t *Tailscale) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
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
// clear error instead. Repeated misses feed the stale-netmap self-healing.
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
	var lastErr error
	for {
		found, err := t.peerInNetmap(ctx, host)
		if err == nil && found {
			t.notePeerSeen()
			return nil
		}
		lastErr = err

		if time.Now().Add(tailnetPeerPollInterval).After(deadline) {
			t.notePeerMiss(host)
			if lastErr != nil {
				return fmt.Errorf("tailnet status unavailable while resolving peer %q: %w", host, lastErr)
			}
			return fmt.Errorf("tailnet peer %q is not visible in this node's netmap (peer is offline or the tailnet control connection is stale)", host)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tailnetPeerPollInterval):
		}
	}
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

func tailnetPeerMatchesHost(hostName, dnsName, target string) bool {
	target = strings.TrimSuffix(target, ".")
	hostName = strings.TrimSuffix(hostName, ".")
	dnsName = strings.TrimSuffix(dnsName, ".")
	return target == hostName || target == dnsName || strings.HasPrefix(dnsName, target+".")
}

func (t *Tailscale) notePeerSeen() {
	t.missMu.Lock()
	defer t.missMu.Unlock()
	t.missCount = 0
	t.firstMissAt = time.Time{}
}

func (t *Tailscale) notePeerMiss(host string) {
	t.missMu.Lock()
	now := time.Now()
	if t.firstMissAt.IsZero() {
		t.firstMissAt = now
	}
	t.missCount++
	shouldRestart := t.missCount >= staleNetmapMissThreshold &&
		now.Sub(t.firstMissAt) >= staleNetmapMissWindow &&
		(t.lastRestart.IsZero() || now.Sub(t.lastRestart) >= staleNetmapRestartCooldown)
	if shouldRestart {
		t.lastRestart = now
		t.missCount = 0
		t.firstMissAt = time.Time{}
	}
	t.missMu.Unlock()

	if shouldRestart {
		t.restartServer(host)
	}
}

// restartServer recycles the tsnet server to force a fresh control connection
// and netmap. It is only safe for dial-only nodes; nodes serving listeners
// would drop them.
func (t *Tailscale) restartServer(host string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.served {
		log.Warn().
			Str("missing_peer", host).
			Msg("tailnet netmap appears stale but this node serves tsnet listeners; skipping tsnet restart")
		return
	}

	log.Warn().
		Str("missing_peer", host).
		Msg("recycling tsnet server after repeated netmap misses; tailnet control connection may be stale")
	if t.server != nil {
		_ = t.server.Close()
	}
	t.server = t.buildServer()
	t.initialized = false
}

// DialTimeout attempts to establish a TCP connection to a tailscale service with the specified timeout duration
func (t *Tailscale) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialContextTimeout(context.Background(), network, addr, timeout)
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

// GetHostnameForService retrieves a random, available hostname for a particular service
// These are served from the "proxy" service, which binds tsnet services to local ports
func (t *Tailscale) GetHostnameForService(serviceName string) (string, error) {
	hostnames, err := t.tailscaleRepo.GetHostnamesForService(serviceName)
	if err != nil {
		return "", err
	}

	for len(hostnames) > 0 {
		index := rand.Intn(len(hostnames))
		hostname := hostnames[index]

		conn, err := t.DialTimeout("tcp", hostname, time.Second*30)
		if err == nil {
			conn.Close()
			return hostname, nil
		}

		hostnames = append(hostnames[:index], hostnames[index+1:]...)
	}

	return "", fmt.Errorf("no valid hostname found for service<%s>", serviceName)
}

func (t *Tailscale) GetServer() *tsnet.Server {
	return t.currentServer()
}

func (t *Tailscale) ResolveService(serviceName string, timeout time.Duration) (string, error) {
	client, err := t.currentServer().LocalClient()
	if err != nil {
		return "", err
	}

	interval := time.Second * 1
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		// Get the status from Tailscale
		status, err := client.Status(context.Background())
		if err != nil {
			return "", err
		}

		// Iterate through the peers to find a matching service
		for _, peer := range status.Peer {
			if !peer.Online {
				continue
			}

			if strings.Contains(peer.HostName, serviceName) {
				return strings.TrimSuffix(peer.DNSName, "."), nil
			}
		}

		time.Sleep(interval)
	}

	return "", fmt.Errorf("no valid service found for <%s>", serviceName)
}

// Stops the Tailscale server
func (t *Tailscale) Close() error {
	return t.currentServer().Close()
}
