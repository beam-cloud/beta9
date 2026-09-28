package agent

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"time"

	"tailscale.com/ipn"
)

type tailnetPrefsClient interface {
	GetPrefs(context.Context) (*ipn.Prefs, error)
	EditPrefs(context.Context, *ipn.MaskedPrefs) (*ipn.Prefs, error)
}

// tailnetSSH reconciles the node's persisted RunSSH pref with what the gateway
// asked for. With it set, tsnet serves inbound tailnet port 22 with the
// Tailscale SSH server linked into the Linux agent binary.
type tailnetSSH struct {
	client tailnetPrefsClient
	want   bool
	stderr io.Writer
	active atomic.Bool
}

func newTailnetSSH(client tailnetPrefsClient, want bool, stderr io.Writer) *tailnetSSH {
	if want && runtime.GOOS != "linux" {
		fmt.Fprintln(stderr, "tailnet SSH is supported only on Linux agents")
		want = false
	}
	return &tailnetSSH{client: client, want: want, stderr: stderr}
}

func (s *tailnetSSH) enabled() bool {
	return s.active.Load()
}

// run retries because the tailnet's SSH capability arrives with the netmap and
// an operator may only enable it for the tailnet later.
func (s *tailnetSSH) run(ctx context.Context) {
	backoff := 5 * time.Second
	for ctx.Err() == nil {
		err := s.reconcile(ctx)
		if err == nil {
			return
		}
		fmt.Fprintf(s.stderr, "tailnet SSH not ready: %v\n", err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, 5*time.Minute)
	}
}

func (s *tailnetSSH) reconcile(ctx context.Context) error {
	prefs, err := s.client.GetPrefs(ctx)
	if err != nil {
		return err
	}
	if prefs.RunSSH != s.want {
		if _, err := s.client.EditPrefs(ctx, &ipn.MaskedPrefs{Prefs: ipn.Prefs{RunSSH: s.want}, RunSSHSet: true}); err != nil {
			return err
		}
		if s.want {
			statusf(s.stderr, "Tailnet SSH enabled")
		}
	}
	s.active.Store(s.want)
	return nil
}
