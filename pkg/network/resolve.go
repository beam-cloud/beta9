package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/client/tailscale"
)

func ConnectToHost(ctx context.Context, host string, timeout time.Duration, tailscale *Tailscale, tsConfig types.TailscaleConfig) (net.Conn, error) {
	var conn net.Conn = nil

	if tsConfig.Enabled && strings.Contains(host, tsConfig.HostName) {
		conn, err := tailscale.Dial(ctx, "tcp", host)
		if err != nil {
			return nil, err
		}

		return conn, err
	}

	conn, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return conn, err
	}
	return conn, nil
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
				log.Printf("Found tailscale service<%s> @ %s\n", serviceName, peer.HostName)
				return peer.HostName, nil
			}
		}

		time.Sleep(interval)
	}

	return "", fmt.Errorf("no valid service found for <%s>", serviceName)
}
