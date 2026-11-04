package worker

import (
	"context"
	"fmt"
	"net"
	"time"
)

// Wait for a listening socket before constructing the authenticated gRPC
// client. Rebuilding channels while the guest is still executing goproc's
// startup repeatedly allocates resolvers/transports and sends doomed RPCs.
// This is only a socket preflight; ReadyContext remains the readiness fence.
func waitProcessManagerSocket(ctx context.Context, endpoints []processManagerEndpoint) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("process manager has no socket endpoints")
	}
	dialer := net.Dialer{Timeout: 10 * time.Millisecond}
	backoff := goprocInitialBackoff
	for {
		for _, endpoint := range endpoints {
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(endpoint.host, fmt.Sprint(endpoint.port)))
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
		if err := waitProcessManagerBackoff(ctx, backoff); err != nil {
			return err
		}
		backoff = nextProcessManagerBackoff(backoff)
	}
}
