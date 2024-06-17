package network

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
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

type Tailscale struct {
	server        *tsnet.Server
	debug         bool
	initialized   bool
	mu            sync.Mutex
	tailscaleRepo repository.TailscaleRepository
}

func (t *Tailscale) logF(format string, v ...interface{}) {
	if t.debug {
		log.Printf(format, v...)
	}
}

// NewTailscale creates a new Tailscale instance using tsnet
func newTailscale(cfg TailscaleConfig, tailscaleRepo repository.TailscaleRepository) *Tailscale {
	ts := &Tailscale{
		server: &tsnet.Server{
			Dir:        cfg.Dir,
			Hostname:   cfg.Hostname,
			AuthKey:    cfg.AuthKey,
			ControlURL: cfg.ControlURL,
			Ephemeral:  cfg.Ephemeral,
		},
		debug:         cfg.Debug,
		initialized:   false,
		mu:            sync.Mutex{},
		tailscaleRepo: tailscaleRepo,
	}

	ts.server.Logf = ts.logF
	return ts
}

// Serve connects to a tailnet and serves a local service
func (t *Tailscale) Serve(ctx context.Context, service types.InternalService) (net.Listener, error) {
	log.Println("Connecting to tailnet @", t.server.ControlURL)

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	addr := fmt.Sprintf(":%d", service.LocalPort)
	listener, err := t.server.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	_, err = t.server.Up(timeoutCtx)
	if err != nil {
		return nil, err
	}

	log.Printf("Connected to tailnet - listening on %s\n", addr)
	return listener, nil
}

// Dial returns a TCP connection to a tailscale service
func (t *Tailscale) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Connect to tailnet, if we aren't already
	if !t.initialized {
		t.mu.Lock()

		_, err := t.server.Up(timeoutCtx)
		if err != nil {
			t.mu.Unlock()
			return nil, err
		}

		t.initialized = true
		t.mu.Unlock()
	}

	conn, err := t.server.Dial(timeoutCtx, network, addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
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

		conn, err := t.Dial(context.TODO(), "tcp", hostname)
		if err == nil {
			conn.Close()
			return hostname, nil
		}

		hostnames = append(hostnames[:index], hostnames[index+1:]...)
	}

	return "", fmt.Errorf("no valid hostname found for service<%s>", serviceName)
}

func (t *Tailscale) GetServer() *tsnet.Server {
	return t.server
}

// Stops the Tailscale server
func (t *Tailscale) Close() error {
	return t.server.Close()
}
