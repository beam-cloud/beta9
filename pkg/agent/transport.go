package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"tailscale.com/tsnet"
)

func runRouteProxy(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string, workers *workerRuntimeManager, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	transport = normalizeTransport(transport)
	switch transport {
	case types.BackendRouteTransportTSNet:
		return runTSNetRouteProxy(ctx, client, agentToken, transport, workers, stdout, stderr)
	default:
		return fmt.Errorf("unsupported agent transport %q", transport)
	}
}

func runTSNetRouteProxy(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string, workers *workerRuntimeManager, stdout, stderr io.Writer) error {
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
	fmt.Fprintf(stdout, "agent route listener ready at %s\n", proxyTarget)
	return newRouteProxy(client, agentToken, listener, proxyTarget, workers, stderr).run(ctx)
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
