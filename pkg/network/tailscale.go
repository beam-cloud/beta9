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

var (
	tailnetPeerPollInterval    = 500 * time.Millisecond
	tailnetPeerAdvisoryTimeout = time.Second
)

type Tailscale struct {
	mu          sync.Mutex // guards server and initialized
	server      *tsnet.Server
	initialized bool // server has been brought up

	cfg           TailscaleConfig
	debug         bool
	tailscaleRepo repository.TailscaleRepository

	// statusFunc and dialFunc override tailnet operations in tests.
	statusFunc func(ctx context.Context) (*ipnstate.Status, error)
	dialFunc   func(ctx context.Context, network, addr string) (net.Conn, error)
}

func (t *Tailscale) logF(format string, v ...interface{}) {
	if t.debug {
		log.Info().Msgf(format, v...)
	}
}

// NewTailscale creates a new Tailscale instance using tsnet
func newTailscale(cfg TailscaleConfig, tailscaleRepo repository.TailscaleRepository) *Tailscale {
	t := &Tailscale{
		cfg:           cfg,
		debug:         cfg.Debug,
		tailscaleRepo: tailscaleRepo,
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
	t.initialized = true
	t.mu.Unlock()

	log.Info().Str("addr", addr).Msg("connected to tailnet")
	return listener, nil
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

func tailnetPeerMatchesHost(hostName, dnsName, target string) bool {
	target = strings.TrimSuffix(target, ".")
	hostName = strings.TrimSuffix(hostName, ".")
	dnsName = strings.TrimSuffix(dnsName, ".")
	return target == hostName || target == dnsName || strings.HasPrefix(dnsName, target+".")
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
