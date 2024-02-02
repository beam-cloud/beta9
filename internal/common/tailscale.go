package common

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/beam-cloud/beta9/internal/types"
	"tailscale.com/tsnet"
)

type TailscaleConfig struct {
	Dir        string // Directory for state storage
	Hostname   string // Hostname for the Tailscale node
	AuthKey    string // Auth key for Tailscale
	ControlURL string // Control server URL
	Ephemeral  bool   // Whether the node is ephemeral
	Debug      bool
}

type Tailscale struct {
	server *tsnet.Server
	debug  bool
}

func (t *Tailscale) logF(format string, v ...interface{}) {
	if t.debug {
		log.Printf(format, v...)
	}
}

// NewTailscale creates a new Tailscale instance using tsnet
func NewTailscale(cfg TailscaleConfig) *Tailscale {
	ts := &Tailscale{
		server: &tsnet.Server{
			Dir:        cfg.Dir,
			Hostname:   cfg.Hostname,
			AuthKey:    cfg.AuthKey,
			ControlURL: cfg.ControlURL,
			Ephemeral:  cfg.Ephemeral,
		},
		debug: cfg.Debug,
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

func (t *Tailscale) Join(ctx context.Context) error {
	log.Println("Connecting to tailnet @", t.server.ControlURL)

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := t.server.Up(timeoutCtx)
	if err != nil {
		return err
	}

	log.Println("Connected to tailnet.")
	return nil
}

// Stops the Tailscale server
func (t *Tailscale) Close() error {
	return t.server.Close()
}
