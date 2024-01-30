package common

import (
	"context"
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
}

// Tailscale encapsulates functionality to interact with Tailscale using tsnet
type Tailscale struct {
	server *tsnet.Server
}

// NewTailscale creates a new Tailscale instance using tsnet
func NewTailscale(cfg TailscaleConfig) *Tailscale {
	return &Tailscale{
		server: &tsnet.Server{
			Dir:        cfg.Dir,
			Hostname:   cfg.Hostname,
			AuthKey:    cfg.AuthKey,
			ControlURL: cfg.ControlURL,
			Ephemeral:  cfg.Ephemeral,
		},
	}
}

// Start connects the server to the tailnet
func (t *Tailscale) Start(ctx context.Context) (*ipnstate.Status, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	status, err := t.server.Up(timeoutCtx)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, timeoutCtx.Err()
		default:
			if status.BackendState == "Running" {
				// Successfully connected, return status
				return status, nil
			}

			time.Sleep(1 * time.Second)
		}
	}
}

// Stops the Tailscale server
func (t *Tailscale) Close() error {
	return t.server.Close()
}
