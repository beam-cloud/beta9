package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute/httpjson"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	http httpjson.Client
}

func NewClient(gatewayURL string) *Client {
	return &Client{
		http: httpjson.Client{
			BaseURL: strings.TrimRight(gatewayURL, "/"),
			Client:  &http.Client{Timeout: 30 * time.Second},
		},
	}
}

func newGatewayGRPCClient(gatewayURL, host string, port int, useTLS bool) (pb.GatewayServiceClient, *grpc.ClientConn, error) {
	addr, err := gatewayGRPCAddr(gatewayURL, host, port)
	if err != nil {
		return nil, nil, err
	}

	creds := insecure.NewCredentials()
	if useTLS || strings.HasSuffix(addr, ":443") {
		creds = credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(creds), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return pb.NewGatewayServiceClient(conn), conn, nil
}

func gatewayGRPCAddr(gatewayURL, host string, port int) (string, error) {
	if port <= 0 {
		port = 443
	}

	u, err := url.Parse(gatewayURL)
	if err != nil {
		return "", err
	}
	if isLoopbackHost(u.Hostname()) {
		host = u.Hostname()
	}
	if host == "" {
		host = u.Hostname()
	}
	if host == "" {
		return "", fmt.Errorf("gateway grpc host is required")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func isLoopbackHost(host string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	return host == "localhost" || host == "::1" || strings.HasPrefix(host, "127.")
}

func normalizeBootstrapForAgentRuntime(gatewayURL string, bootstrap bootstrapConfig) bootstrapConfig {
	if !envBool(types.AgentInContainerEnv) {
		return bootstrap
	}

	u, err := url.Parse(gatewayURL)
	if err != nil {
		return bootstrap
	}
	runtimeHost := u.Hostname()
	if runtimeHost == "" {
		return bootstrap
	}

	bootstrap.GatewayGRPCHost = runtimeHost

	if bootstrap.GatewayHTTPURL == "" || urlHostIsLoopback(bootstrap.GatewayHTTPURL) {
		bootstrap.GatewayHTTPURL = gatewayURL
	}

	return bootstrap
}

func urlHostIsLoopback(value string) bool {
	u, err := url.Parse(value)
	if err != nil {
		return false
	}
	return isLoopbackHost(u.Hostname())
}

func join(ctx context.Context, client *Client, opts types.AgentJoinOptions) (*joinResponse, error) {
	hostname, _ := os.Hostname()
	hostname = firstNonEmpty(os.Getenv(types.AgentHostnameEnv), hostname)
	executor := types.DefaultAgentWorkerContainerMode
	if opts.ExecutorOverride != "" {
		executor = opts.ExecutorOverride
	}
	preflight := runPreflight(opts.DevMode, executor)
	capacity, checks, schedulable := resolveAgentCapacity(opts, preflight)
	req := joinRequest{
		JoinToken:                 opts.JoinToken,
		MachineFingerprint:        machineFingerprint(hostname),
		Hostname:                  hostname,
		OS:                        runtime.GOOS,
		Arch:                      runtime.GOARCH,
		CPUCount:                  capacity.CPUCount,
		CPUMillicores:             capacity.CPUMillicores,
		MemoryMB:                  capacity.MemoryMB,
		GPU:                       capacity.GPUs,
		GPUIDs:                    capacity.GPUIDs,
		GPUCount:                  capacity.GPUCount,
		Preflight:                 checks,
		Schedulable:               schedulable,
		Executor:                  executor,
		NetworkSlotPoolSize:       capacity.NetworkSlotPoolSize,
		ContainerStartConcurrency: capacity.ContainerStartConcurrency,
	}

	res := joinResponse{}
	if err := client.http.Do(ctx, http.MethodPost, "/api/v1/gateway/agent/join", req, &res); err != nil {
		return nil, err
	}
	res.Preflight = checks
	res.Schedulable = schedulable
	return &res, nil
}
