package common

import (
	"context"
	"log"
	"time"

	"tailscale.com/ipn/ipnstate"
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

func (t *Tailscale) log(format string, v ...interface{}) {
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

	ts.server.Logf = ts.log
	return ts
}

// Start connects the server to the tailnet
func (t *Tailscale) Start(ctx context.Context) (*ipnstate.Status, error) {
	log.Println("Connecting to tailnet @", t.server.ControlURL)

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	status, err := t.server.Up(timeoutCtx)
	if err != nil {
		return nil, err
	}

	log.Println("Connected to tailnet")
	return status, nil
}

// Stops the Tailscale server
func (t *Tailscale) Close() error {
	return t.server.Close()
}
