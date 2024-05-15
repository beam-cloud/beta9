package network

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
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
