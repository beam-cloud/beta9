package agent

import (
	"context"
	"fmt"
	"io"
	"strings"
)

func RunJoin(ctx context.Context, opts JoinOptions) error {
	opts.GatewayURL = strings.TrimRight(strings.TrimSpace(opts.GatewayURL), "/")
	opts.JoinToken = strings.TrimSpace(opts.JoinToken)
	if opts.GatewayURL == "" || opts.JoinToken == "" {
		return fmt.Errorf("gateway and join-token are required")
	}
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}

	lock, err := acquireAgentLock()
	if err != nil {
		return err
	}
	defer lock.release()

	client := NewClient(opts.GatewayURL)
	res, err := join(ctx, client, opts)
	if err != nil {
		return fmt.Errorf("join failed: %w", err)
	}
	if !res.Ok {
		return fmt.Errorf("join failed: %s", res.ErrMsg)
	}
	res.Bootstrap = normalizeBootstrapForAgentRuntime(opts.GatewayURL, res.Bootstrap)

	fmt.Fprintf(opts.Stdout, "joined pool %q as machine %q\n", res.PoolName, res.MachineID)
	fmt.Fprintf(opts.Stdout, "transport=%s executor=%s fallback=%s\n", res.Bootstrap.Transport, res.Bootstrap.Executor, res.Bootstrap.Fallback)
	if !res.Ok || res.AgentToken == "" {
		return nil
	}
	grpcClient, grpcConn, err := newGatewayGRPCClient(opts.GatewayURL, res.Bootstrap.GatewayGRPCHost, res.Bootstrap.GatewayGRPCPort, res.Bootstrap.GatewayGRPCTLS)
	if err != nil {
		return fmt.Errorf("gateway grpc client: %w", err)
	}
	defer grpcConn.Close()

	workers := newWorkerRuntimeManager(res.Bootstrap, opts, opts.Stdout, opts.Stderr)
	defer workers.stopAll()

	registryForwarder, err := startLocalRegistryForwarder(ctx, opts.Stderr)
	if err != nil {
		fmt.Fprintf(opts.Stderr, "local registry forwarder disabled: %v\n", err)
	} else if registryForwarder != nil {
		defer registryForwarder.Close()
	}

	transport := normalizeTransport(firstNonEmpty(opts.TransportOverride, res.Bootstrap.Transport))
	if err := runRouteProxy(ctx, grpcClient, res.AgentToken, transport, workers, opts.Stdout, opts.Stderr); err != nil {
		return fmt.Errorf("route proxy stopped: %w", err)
	}
	return nil
}
