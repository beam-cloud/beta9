package network

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/client/tailscale"
)

func ConnectToHost(ctx context.Context, host string, timeout time.Duration, tailscale *Tailscale, tsConfig types.TailscaleConfig) (net.Conn, error) {
	var conn net.Conn = nil

	if tsConfig.Enabled && tailscale != nil && strings.Contains(host, tsConfig.HostName) {
		dialCtx := ctx
		cancel := func() {}
		deadline := time.Time{}
		if timeout > 0 {
			dialCtx, cancel = context.WithTimeout(ctx, timeout)
			deadline = time.Now().Add(timeout)
		}
		defer cancel()

		// Dial first because tsnet.Status can omit peers that are nevertheless
		// reachable. After a fast failure, spend only a bounded part of the
		// remaining budget enriching the error, then retry the actual dial.
		dialTimeout := timeout
		if !deadline.IsZero() {
			dialTimeout = time.Until(deadline)
		}
		conn, dialErr := tailscale.DialContextTimeout(dialCtx, "tcp", host, dialTimeout)
		if dialErr == nil {
			return conn, nil
		}

		var peerErr error
		remaining := tailnetPeerAdvisoryTimeout
		if !deadline.IsZero() {
			remaining = time.Until(deadline)
		}
		if peerHost := tailnetHostFromAddr(host); peerHost != "" && remaining > 0 {
			peerErr = tailscale.WaitForPeer(
				dialCtx,
				peerHost,
				tsnetPeerProbeReserve(remaining),
			)
		}

		if !deadline.IsZero() {
			dialTimeout = time.Until(deadline)
		}
		if deadline.IsZero() || dialTimeout > 0 {
			conn, retryErr := tailscale.DialContextTimeout(dialCtx, "tcp", host, dialTimeout)
			if retryErr == nil {
				return conn, nil
			}
			dialErr = retryErr
		}

		if peerErr != nil {
			return nil, fmt.Errorf("%w; tsnet dial failed: %w", peerErr, dialErr)
		}
		return nil, dialErr
	}

	dialCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", host)
	if err != nil {
		return conn, err
	}
	return conn, nil
}

func GetDialer(host string, tailscale *Tailscale, tsConfig types.TailscaleConfig) func(ctx context.Context, network, address string) (net.Conn, error) {
	if tsConfig.Enabled && strings.Contains(host, tsConfig.HostName) {
		return tailscale.Dial
	}

	dialer := &net.Dialer{DualStack: true}
	return dialer.DialContext
}

func ResolveTailscaleService(serviceName string, timeout time.Duration) (string, error) {
	client := tailscale.LocalClient{}
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
				return peer.HostName, nil
			}
		}

		time.Sleep(interval)
	}

	return "", fmt.Errorf("no valid service found for <%s>", serviceName)
}
