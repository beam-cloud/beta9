package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

var ErrInterrupted = errors.New("agent interrupted")

func RunJoin(ctx context.Context, opts types.AgentJoinOptions) error {
	var err error
	if opts, err = normalizeJoinOptions(opts); err != nil {
		return err
	}

	lock, err := acquireAgentLock()
	if err != nil {
		return err
	}
	defer lock.release()

	client := NewClient(opts.GatewayURL)
	res, err := resolveAgentIdentity(ctx, client, opts)
	if err != nil {
		return err
	}
	if !res.Ok || res.AgentToken == "" {
		return fmt.Errorf("join failed: %s", firstNonEmpty(res.ErrMsg, "gateway did not return an agent token"))
	}
	res.Bootstrap = normalizeBootstrapForAgentRuntime(opts.GatewayURL, res.Bootstrap)
	if err := saveRuntimeState(opts.GatewayURL, res); err != nil {
		fmt.Fprintf(opts.Stderr, "failed to save agent state: %v\n", err)
	}

	grpcClient, grpcConn, err := newGatewayGRPCClient(opts.GatewayURL, res.Bootstrap.GatewayGRPCHost, res.Bootstrap.GatewayGRPCPort, res.Bootstrap.GatewayGRPCTLS)
	if err != nil {
		return fmt.Errorf("gateway grpc client: %w", err)
	}
	defer grpcConn.Close()

	telemetry := newAgentTelemetry(grpcClient, res.AgentToken, res.Bootstrap, opts.CacheDir, opts.Stderr)
	go telemetry.run(ctx)
	agentStdout := telemetry.teeLogWriter(opts.Stdout, types.AgentTelemetrySourceAgent, "", types.EventLogStreamStdout)
	defer agentStdout.Close()
	agentStderr := telemetry.teeLogWriter(opts.Stderr, types.AgentTelemetrySourceAgent, "", types.EventLogStreamStderr)
	defer agentStderr.Close()

	statusf(agentStdout, "Connected to pool %q", res.PoolName)
	statusf(agentStdout, "Registered machine %q", res.MachineID)
	if !res.Schedulable && len(res.Preflight) > 0 {
		statusf(agentStdout, "Machine is not schedulable: %s", preflightFailureSummary(res.Preflight))
	}
	verbosef(agentStdout, "transport=%s executor=%s fallback=%s\n", res.Bootstrap.Transport, res.Bootstrap.Executor, res.Bootstrap.Fallback)

	workers := newWorkerRuntimeManager(res.Bootstrap, opts, opts.Stdout, opts.Stderr, agentStdout, agentStderr, telemetry)
	telemetry.setStatsProvider(workers.stats)
	defer workers.stopAll()

	registryForwarder, err := startLocalRegistryForwarder(ctx, agentStderr)
	if err != nil {
		fmt.Fprintf(agentStderr, "local registry forwarder disabled: %v\n", err)
	} else if registryForwarder != nil {
		defer registryForwarder.Close()
	}

	transport := normalizeTransport(firstNonEmpty(opts.TransportOverride, res.Bootstrap.Transport))
	if err := runRouteProxy(ctx, grpcClient, res.AgentToken, transport, workers, telemetry, agentStdout, agentStderr); err != nil {
		if agentInterrupted(ctx, err) {
			statusf(agentStdout, "Disconnecting machine %q", res.MachineID)
			return ErrInterrupted
		}
		return fmt.Errorf("route proxy stopped: %w", err)
	}
	return nil
}

func agentInterrupted(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled)
}

func normalizeJoinOptions(opts types.AgentJoinOptions) (types.AgentJoinOptions, error) {
	opts.GatewayURL = strings.TrimRight(strings.TrimSpace(opts.GatewayURL), "/")
	opts.JoinToken = strings.TrimSpace(opts.JoinToken)
	opts.JoinTokenFile = strings.TrimSpace(opts.JoinTokenFile)
	if opts.JoinToken == "" && opts.JoinTokenFile != "" {
		data, err := os.ReadFile(opts.JoinTokenFile)
		if err != nil {
			return types.AgentJoinOptions{}, fmt.Errorf("read join token file: %w", err)
		}
		opts.JoinToken = strings.TrimSpace(string(data))
	}
	if opts.GatewayURL == "" {
		return types.AgentJoinOptions{}, fmt.Errorf("gateway is required")
	}
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	return opts, nil
}

func resolveAgentIdentity(ctx context.Context, client *Client, opts types.AgentJoinOptions) (*joinResponse, error) {
	savedState, stateErr := loadRuntimeState(opts.GatewayURL)
	if stateErr != nil {
		fmt.Fprintf(opts.Stderr, "failed to load saved agent state: %v\n", stateErr)
	}
	if opts.JoinToken == "" {
		if savedState == nil {
			return nil, fmt.Errorf("join-token is required")
		}
		return savedState, nil
	}

	res, err := join(ctx, client, opts)
	if err == nil && res != nil && res.Ok {
		return res, nil
	}
	if res != nil {
		return nil, fmt.Errorf("join failed: %s", res.ErrMsg)
	}
	if err != nil {
		// Fall back to the saved identity only when the gateway could not be
		// reached; a 4xx rejection means the token was revoked or invalid.
		if savedState != nil && !joinRejected(err) {
			logJoinFallback(opts.Stderr, err)
			return savedState, nil
		}
		return nil, fmt.Errorf("join failed: %w", err)
	}
	return nil, fmt.Errorf("join failed")
}

func joinRejected(err error) bool {
	var statusErr *compute.HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode >= 400 && statusErr.StatusCode < 500
}

func logJoinFallback(stderr io.Writer, err error) {
	fmt.Fprintf(stderr, "join failed, resuming saved agent identity: %v\n", err)
}

func preflightFailureSummary(checks []check) string {
	failed := make([]string, 0, len(checks))
	for _, check := range checks {
		if check.Ok || check.Severity != "error" {
			continue
		}
		if check.Message == "" {
			failed = append(failed, check.Name)
			continue
		}
		failed = append(failed, fmt.Sprintf("%s (%s)", check.Name, check.Message))
	}
	if len(failed) == 0 {
		return "waiting for schedulable capacity"
	}
	return strings.Join(failed, ", ")
}
