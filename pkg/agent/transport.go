package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	tsclient "tailscale.com/client/tailscale"
	"tailscale.com/tsnet"
)

const tsnetSnapshotInterval = time.Minute

func runRouteProxy(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string, workers *workerRuntimeManager, telemetry *agentTelemetry, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	transport = normalizeTransport(transport)
	switch transport {
	case types.BackendRouteTransportTSNet:
		return runTSNetRouteProxy(ctx, client, agentToken, transport, workers, telemetry, stdout, stderr)
	default:
		return fmt.Errorf("unsupported agent transport %q", transport)
	}
}

func runTSNetRouteProxy(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string, workers *workerRuntimeManager, telemetry *agentTelemetry, stdout, stderr io.Writer) error {
	credential, err := requestTransportCredential(ctx, client, agentToken, transport)
	if err != nil {
		return err
	}
	if !credential.Ok {
		return fmt.Errorf("%s", credential.ErrMsg)
	}

	server := &tsnet.Server{
		Hostname:   credential.Hostname,
		AuthKey:    credential.AuthKey,
		ControlURL: credential.ControlURL,
		Ephemeral:  credential.Ephemeral,
		Logf:       agentTSNetLogf(stderr),
		UserLogf:   agentTSNetLogf(stderr),
	}
	defer server.Close()
	if _, err := server.Up(ctx); err != nil {
		return err
	}

	hostname := credential.Hostname
	if localClient, err := server.LocalClient(); err == nil {
		if status, err := localClient.Status(ctx); err == nil && status.Self != nil && status.Self.DNSName != "" {
			hostname = strings.TrimSuffix(status.Self.DNSName, ".")
		}
	}

	listener, err := server.Listen("tcp", fmt.Sprintf(":%d", types.DefaultAgentTSNetRouteProxyPort))
	if err != nil {
		return err
	}
	defer listener.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return err
	}

	proxyTarget := net.JoinHostPort(hostname, port)
	statusf(stdout, "Network ready")
	statusf(stdout, "Agent running; leave this terminal open")
	verbosef(stdout, "agent route listener ready at %s\n", proxyTarget)
	if localClient, err := server.LocalClient(); err == nil {
		go emitTSNetSnapshots(ctx, telemetry, localClient, proxyTarget)
	}
	return newRouteProxy(client, agentToken, listener, proxyTarget, workers, stdout, stderr).run(ctx)
}

func emitTSNetSnapshots(ctx context.Context, telemetry *agentTelemetry, client *tsclient.LocalClient, proxyTarget string) {
	if telemetry == nil || client == nil {
		return
	}
	emitTSNetSnapshot(ctx, telemetry, client, proxyTarget)
	ticker := time.NewTicker(tsnetSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emitTSNetSnapshot(ctx, telemetry, client, proxyTarget)
		}
	}
}

func emitTSNetSnapshot(ctx context.Context, telemetry *agentTelemetry, client *tsclient.LocalClient, proxyTarget string) {
	snapshotCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	attrs := map[string]string{"proxy_target": proxyTarget}
	status, err := client.Status(snapshotCtx)
	if err != nil {
		telemetry.event(types.EventComputeTransport, types.EventComputeActionTransportSnapshot, "error", err.Error(), attrs)
		return
	}

	attrs["backend_state"] = status.BackendState
	attrs["health_count"] = strconv.Itoa(len(status.Health))
	attrs["peer_count"] = strconv.Itoa(len(status.Peer))
	if status.Self != nil {
		attrs["self_dns"] = strings.TrimSuffix(status.Self.DNSName, ".")
		attrs["self_online"] = strconv.FormatBool(status.Self.Online)
		if status.Self.Relay != "" {
			attrs["self_relay"] = status.Self.Relay
		}
	}
	if len(status.TailscaleIPs) > 0 {
		ips := make([]string, 0, len(status.TailscaleIPs))
		for _, ip := range status.TailscaleIPs {
			ips = append(ips, ip.String())
		}
		attrs["tailscale_ips"] = strings.Join(ips, ",")
	}

	onlinePeers := 0
	directPeers := 0
	relayPeers := 0
	activePeers := 0
	recentHandshakePeers := 0
	newestHandshakeAge := time.Duration(0)
	relayRegions := map[string]struct{}{}
	now := time.Now()
	for _, peer := range status.Peer {
		if peer == nil {
			continue
		}
		if peer.Online {
			onlinePeers++
		}
		if peer.CurAddr != "" {
			directPeers++
		} else if peer.Relay != "" {
			relayPeers++
		}
		if peer.Relay != "" {
			relayRegions[peer.Relay] = struct{}{}
		}
		if peer.Active {
			activePeers++
		}
		if !peer.LastHandshake.IsZero() {
			age := now.Sub(peer.LastHandshake)
			if age < 0 {
				age = 0
			}
			if age <= 2*time.Minute {
				recentHandshakePeers++
			}
			if newestHandshakeAge == 0 || age < newestHandshakeAge {
				newestHandshakeAge = age
			}
		}
	}
	attrs["online_peer_count"] = strconv.Itoa(onlinePeers)
	attrs["direct_peer_count"] = strconv.Itoa(directPeers)
	attrs["relay_peer_count"] = strconv.Itoa(relayPeers)
	attrs["active_peer_count"] = strconv.Itoa(activePeers)
	attrs["recent_handshake_peer_count"] = strconv.Itoa(recentHandshakePeers)
	if newestHandshakeAge > 0 {
		attrs["newest_handshake_age_ms"] = strconv.FormatInt(newestHandshakeAge.Milliseconds(), 10)
	}
	if len(relayRegions) > 0 {
		regions := make([]string, 0, len(relayRegions))
		for region := range relayRegions {
			regions = append(regions, region)
		}
		sort.Strings(regions)
		attrs["relay_regions"] = strings.Join(regions, ",")
	}
	telemetry.event(types.EventComputeTransport, types.EventComputeActionTransportSnapshot, status.BackendState, "", attrs)
}

func agentTSNetLogf(stderr io.Writer) func(string, ...any) {
	return func(format string, args ...any) {
		verbosef(stderr, format+"\n", args...)
	}
}

func requestTransportCredential(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string) (*transportCredentialResponse, error) {
	res, err := client.RequestAgentTransportCredential(ctx, &pb.RequestAgentTransportCredentialRequest{
		AgentToken: agentToken,
		Transport:  transport,
	})
	if err != nil {
		return nil, err
	}
	return &transportCredentialResponse{
		Ok:         res.Ok,
		ErrMsg:     res.ErrMsg,
		AuthKey:    res.AuthKey,
		ControlURL: res.ControlUrl,
		Hostname:   res.Hostname,
		Ephemeral:  res.Ephemeral,
	}, nil
}

func normalizeTransport(transport string) string {
	transport = strings.TrimSpace(strings.ReplaceAll(transport, "-", "_"))
	if transport == "" {
		return types.BackendRouteTransportTSNet
	}
	return transport
}
