package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func agentWorkspaceStorageEndpointURL(bootstrap bootstrapConfig) string {
	if endpoint := strings.TrimSpace(os.Getenv(agentWorkspaceStorageEndpointOverrideEnv)); endpoint != "" {
		return endpoint
	}

	u, err := url.Parse(bootstrap.GatewayHTTPURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if host == "" {
		return ""
	}
	if !isLoopbackHost(host) && !strings.EqualFold(host, "host.docker.internal") {
		return ""
	}

	port := strings.TrimSpace(os.Getenv(agentWorkspaceStorageEndpointPortEnv))
	if port == "" {
		port = "4566"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func agentGatewayEnv(bootstrap bootstrapConfig) map[string]string {
	httpHost, httpPort, _ := agentGatewayHTTPParts(bootstrap)
	grpcPort := bootstrap.GatewayGRPCPort
	if grpcPort <= 0 {
		grpcPort = 443
	}

	return map[string]string{
		types.ContainerEnvGatewayGRPCHost: bootstrap.GatewayGRPCHost,
		types.ContainerEnvGatewayGRPCPort: strconv.Itoa(grpcPort),
		types.ContainerEnvGatewayHTTPHost: httpHost,
		types.ContainerEnvGatewayHTTPPort: strconv.Itoa(httpPort),
	}
}

func agentGatewayHTTPParts(bootstrap bootstrapConfig) (string, int, bool) {
	u, err := url.Parse(bootstrap.GatewayHTTPURL)
	if err != nil || u.Hostname() == "" {
		return bootstrap.GatewayHTTPURL, 443, true
	}

	port := 0
	if u.Port() != "" {
		port, _ = strconv.Atoi(u.Port())
	}
	if port <= 0 {
		if u.Scheme == "http" {
			port = 80
		} else {
			port = 443
		}
	}

	return u.Hostname(), port, u.Scheme == "https"
}

func agentOCIRegistryRewrite(bootstrap bootstrapConfig) string {
	if rewrite := strings.TrimSpace(os.Getenv(agentOCIRegistryRewriteOverrideEnv)); rewrite != "" {
		return rewrite
	}

	u, err := url.Parse(bootstrap.GatewayHTTPURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if host == "" {
		return ""
	}
	if !isLoopbackHost(host) && !strings.EqualFold(host, "host.docker.internal") {
		return ""
	}

	port := strings.TrimSpace(os.Getenv(agentOCIRegistryEndpointPortEnv))
	if port == "" {
		port = "5001"
	}
	target := net.JoinHostPort(host, port)
	return strings.Join([]string{
		"registry.localhost:5000=" + target,
		"localhost:5000=" + target,
		"127.0.0.1:5000=" + target,
	}, ",")
}

func agentDockerHostAliases(bootstrap bootstrapConfig) []string {
	u, err := url.Parse(bootstrap.GatewayHTTPURL)
	if err != nil {
		return nil
	}
	host := u.Hostname()
	if host == "" || (!isLoopbackHost(host) && !strings.EqualFold(host, "host.docker.internal")) {
		return nil
	}

	return []string{
		"registry.localhost:127.0.0.1",
		"localstack:host-gateway",
	}
}

func startLocalRegistryForwarder(ctx context.Context, bootstrap bootstrapConfig, stderr io.Writer) (io.Closer, error) {
	target := agentLocalRegistryForwardTarget(bootstrap)
	if target == "" {
		return nil, nil
	}

	listener, err := net.Listen("tcp", "127.0.0.1:5000")
	if err != nil {
		return nil, err
	}

	forwarder := &tcpForwarder{
		listener: listener,
		target:   target,
		stderr:   stderr,
	}
	go forwarder.run(ctx)
	return forwarder, nil
}

func agentLocalRegistryForwardTarget(bootstrap bootstrapConfig) string {
	u, err := url.Parse(bootstrap.GatewayHTTPURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if host == "" || (!isLoopbackHost(host) && !strings.EqualFold(host, "host.docker.internal")) {
		return ""
	}

	port := strings.TrimSpace(os.Getenv(agentOCIRegistryEndpointPortEnv))
	if port == "" {
		port = "5001"
	}
	if port == "5000" {
		return ""
	}
	return net.JoinHostPort(host, port)
}

type tcpForwarder struct {
	listener net.Listener
	target   string
	stderr   io.Writer
}

func (f *tcpForwarder) Close() error {
	return f.listener.Close()
}

func (f *tcpForwarder) run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		_ = f.listener.Close()
	}()

	for {
		conn, err := f.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || strings.Contains(strings.ToLower(err.Error()), "use of closed network connection") {
				return
			}
			if f.stderr != nil {
				fmt.Fprintf(f.stderr, "local registry forwarder accept failed: %v\n", err)
			}
			return
		}
		go f.handleConn(conn)
	}
}

func (f *tcpForwarder) handleConn(conn net.Conn) {
	defer conn.Close()

	upstream, err := net.DialTimeout("tcp", f.target, 30*time.Second)
	if err != nil {
		if f.stderr != nil {
			fmt.Fprintf(f.stderr, "local registry forwarder dial failed: %v\n", err)
		}
		return
	}
	defer upstream.Close()

	copyBoth(conn, upstream)
}

func agentStateDir() (string, error) {
	if dir := strings.TrimSpace(os.Getenv("BEAM_AGENT_STATE_DIR")); dir != "" {
		return dir, os.MkdirAll(dir, 0755)
	}
	if runtime.GOOS == "linux" && writableDirOrCreatable("/var/lib/beam/agent") {
		dir := "/var/lib/beam/agent"
		return dir, os.MkdirAll(dir, 0755)
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, "beam", "agent")
	return dir, os.MkdirAll(dir, 0755)
}

func sanitizeDockerName(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '.' || r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-_.")
	if out == "" {
		out = "slot"
	}
	if len(out) > 96 {
		out = out[:96]
	}
	return out
}
