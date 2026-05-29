package agent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/hybrid/httpjson"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/tsnet"
)

const (
	workerContainerExecutor = "worker-container"
	localDevExecutor        = "local-dev"
)

type JoinOptions struct {
	GatewayURL        string
	JoinToken         string
	DevMode           bool
	TransportOverride string
	ListenAddr        string
	AdvertiseHost     string
	Stdout            io.Writer
	Stderr            io.Writer
}

type Client struct {
	http httpjson.Client
}

type joinRequest struct {
	JoinToken          string   `json:"joinToken"`
	MachineFingerprint string   `json:"machineFingerprint"`
	Hostname           string   `json:"hostname"`
	OS                 string   `json:"os"`
	Arch               string   `json:"arch"`
	CPUCount           uint32   `json:"cpuCount"`
	MemoryMB           uint64   `json:"memoryMb"`
	GPU                []string `json:"gpu"`
	GPUCount           uint32   `json:"gpuCount"`
	Preflight          []check  `json:"preflight"`
	Schedulable        bool     `json:"schedulable"`
	Executor           string   `json:"executor"`
}

type joinResponse struct {
	Ok          bool   `json:"ok"`
	ErrMsg      string `json:"errMsg"`
	WorkspaceID string `json:"workspaceId"`
	PoolName    string `json:"poolName"`
	MachineID   string `json:"machineId"`
	AgentToken  string `json:"agentToken"`
	Bootstrap   struct {
		Transport string `json:"transport"`
		Executor  string `json:"executor"`
		Fallback  string `json:"fallback"`
	} `json:"bootstrap"`
}

type transportCredentialResponse struct {
	Ok         bool   `json:"ok"`
	ErrMsg     string `json:"errMsg"`
	AuthKey    string `json:"authKey"`
	ControlURL string `json:"controlUrl"`
	Hostname   string `json:"hostname"`
	Ephemeral  bool   `json:"ephemeral"`
}

type route struct {
	RouteID     string `json:"routeId"`
	Transport   string `json:"transport"`
	LocalTarget string `json:"localTarget"`
	ProxyTarget string `json:"proxyTarget"`
	State       string `json:"state"`
}

type listRoutesResponse struct {
	Ok     bool    `json:"ok"`
	ErrMsg string  `json:"errMsg"`
	Routes []route `json:"routes"`
}

type check struct {
	Name     string `json:"name"`
	Ok       bool   `json:"ok"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

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

	client := NewClient(opts.GatewayURL)
	res, err := join(ctx, client, opts.JoinToken, opts.DevMode)
	if err != nil {
		return fmt.Errorf("join failed: %w", err)
	}
	if !res.Ok {
		return fmt.Errorf("join failed: %s", res.ErrMsg)
	}

	fmt.Fprintf(opts.Stdout, "joined hybrid pool %q as machine %q\n", res.PoolName, res.MachineID)
	fmt.Fprintf(opts.Stdout, "transport=%s executor=%s fallback=%s\n", res.Bootstrap.Transport, res.Bootstrap.Executor, res.Bootstrap.Fallback)
	if !res.Ok || res.AgentToken == "" {
		return nil
	}
	transport := normalizeTransport(firstNonEmpty(opts.TransportOverride, res.Bootstrap.Transport))
	if err := runRouteProxy(ctx, client, res.AgentToken, transport, opts.ListenAddr, opts.AdvertiseHost, opts.DevMode, opts.Stdout, opts.Stderr); err != nil {
		return fmt.Errorf("route proxy stopped: %w", err)
	}
	return nil
}

func NewClient(gatewayURL string) *Client {
	return &Client{
		http: httpjson.Client{
			BaseURL: strings.TrimRight(gatewayURL, "/"),
			Client:  &http.Client{Timeout: 30 * time.Second},
		},
	}
}

func join(ctx context.Context, client *Client, token string, devMode bool) (*joinResponse, error) {
	hostname, _ := os.Hostname()
	preflight := runPreflight(devMode)
	gpus := preflight.gpus
	executor := workerContainerExecutor
	if devMode {
		executor = localDevExecutor
	}
	req := joinRequest{
		JoinToken:          token,
		MachineFingerprint: machineFingerprint(hostname),
		Hostname:           hostname,
		OS:                 runtime.GOOS,
		Arch:               runtime.GOARCH,
		CPUCount:           uint32(runtime.NumCPU()),
		MemoryMB:           systemMemoryMB(),
		GPU:                gpus,
		GPUCount:           uint32(len(gpus)),
		Preflight:          preflight.checks,
		Schedulable:        preflight.schedulable,
		Executor:           executor,
	}

	res := joinResponse{}
	if err := client.http.Do(ctx, http.MethodPost, "/api/v1/gateway/hybrid/join", req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func runRouteProxy(ctx context.Context, client *Client, agentToken, transport, listenAddr, advertiseHost string, devMode bool, stdout, stderr io.Writer) error {
	transport = normalizeTransport(transport)
	switch transport {
	case types.BackendRouteTransportTSNet:
		return runTSNetRouteProxy(ctx, client, agentToken, transport, stderr)
	case types.BackendRouteTransportLocalDirect:
		return runLocalDirectRouteProxy(ctx, client, agentToken, listenAddr, advertiseHost, devMode, stdout, stderr)
	default:
		return fmt.Errorf("unsupported hybrid transport %q", transport)
	}
}

func runTSNetRouteProxy(ctx context.Context, client *Client, agentToken, transport string, stderr io.Writer) error {
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

	listener, err := server.Listen("tcp", fmt.Sprintf(":%d", types.DefaultHybridTSNetRouteProxyPort))
	if err != nil {
		return err
	}
	defer listener.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return err
	}

	return newRouteProxy(client, agentToken, listener, net.JoinHostPort(hostname, port), stderr).run(ctx)
}

func runLocalDirectRouteProxy(ctx context.Context, client *Client, agentToken, listenAddr, advertiseHost string, devMode bool, stdout, stderr io.Writer) error {
	if listenAddr == "" {
		listenAddr = defaultLocalDirectListenAddr(devMode)
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return err
	}
	if advertiseHost == "" {
		advertiseHost = defaultLocalDirectAdvertiseHost(host, devMode)
	}
	proxyTarget := net.JoinHostPort(advertiseHost, port)
	fmt.Fprintf(stdout, "local_direct route listener ready at %s, advertising %s\n", listener.Addr().String(), proxyTarget)

	return newRouteProxy(client, agentToken, listener, proxyTarget, stderr).run(ctx)
}

func newRouteProxy(client *Client, agentToken string, listener net.Listener, proxyTarget string, stderr io.Writer) *routeProxy {
	if stderr == nil {
		stderr = io.Discard
	}
	return &routeProxy{
		agentToken:  agentToken,
		client:      client,
		listener:    listener,
		proxyTarget: proxyTarget,
		stderr:      stderr,
		routes:      map[string]string{},
	}
}

type routeProxy struct {
	client      *Client
	agentToken  string
	listener    net.Listener
	proxyTarget string
	stderr      io.Writer
	mu          sync.Mutex
	routes      map[string]string
}

func (p *routeProxy) run(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	acceptErr := make(chan error, 1)
	go func() {
		acceptErr <- p.accept(ctx)
	}()

	for {
		if err := p.reconcile(ctx); err != nil {
			fmt.Fprintf(p.stderr, "route reconcile failed: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-acceptErr:
			return err
		case <-ticker.C:
		}
	}
}

func (p *routeProxy) reconcile(ctx context.Context) error {
	routes, err := listRoutes(ctx, p.client, p.agentToken)
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, route := range routes {
		if route.LocalTarget == "" {
			continue
		}
		seen[route.RouteID] = struct{}{}
		p.setRoute(route.RouteID, route.LocalTarget)
		if route.State == "ready" && route.ProxyTarget == p.proxyTarget {
			continue
		}
		if err := updateRouteStatus(ctx, p.client, p.agentToken, route.RouteID, "ready", p.proxyTarget, ""); err != nil {
			p.deleteRoute(route.RouteID)
			_ = updateRouteStatus(ctx, p.client, p.agentToken, route.RouteID, "degraded", p.proxyTarget, err.Error())
			return err
		}
	}
	p.deleteRoutesNotIn(seen)
	return nil
}

func (p *routeProxy) setRoute(routeID, localTarget string) {
	p.mu.Lock()
	p.routes[routeID] = localTarget
	p.mu.Unlock()
}

func (p *routeProxy) deleteRoute(routeID string) {
	p.mu.Lock()
	delete(p.routes, routeID)
	p.mu.Unlock()
}

func (p *routeProxy) deleteRoutesNotIn(seen map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for routeID := range p.routes {
		if _, ok := seen[routeID]; !ok {
			delete(p.routes, routeID)
		}
	}
}

func (p *routeProxy) localTarget(routeID string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	target, ok := p.routes[routeID]
	return target, ok
}

func (p *routeProxy) accept(ctx context.Context) error {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}
		go p.handleConn(conn)
	}
}

func (p *routeProxy) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	lineBytes, err := reader.ReadSlice('\n')
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return
	}

	line := strings.TrimRight(string(lineBytes), "\r\n")
	if !strings.HasPrefix(line, network.BackendRoutePreface) {
		return
	}

	routeID := strings.TrimSpace(strings.TrimPrefix(line, network.BackendRoutePreface))
	localTarget, ok := p.localTarget(routeID)
	if !ok || localTarget == "" {
		return
	}
	if err := proxyConn(conn, reader, localTarget); err != nil {
		fmt.Fprintf(p.stderr, "route %s proxy failed: %v\n", routeID, err)
	}
}

func proxyConn(conn net.Conn, source io.Reader, localTarget string) error {
	defer conn.Close()
	local, err := dialLocalTarget(localTarget)
	if err != nil {
		return err
	}
	defer local.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(local, source)
		closeWrite(local)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(conn, local)
		closeWrite(conn)
	}()
	wg.Wait()
	return nil
}

func dialLocalTarget(localTarget string) (net.Conn, error) {
	return dialLocalTargetWithTimeout(localTarget, 30*time.Second)
}

func dialLocalTargetWithTimeout(localTarget string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", localTarget, timeout)
	if err == nil {
		return conn, nil
	}

	_, port, splitErr := net.SplitHostPort(localTarget)
	if splitErr != nil {
		return nil, err
	}
	fallbackTarget := net.JoinHostPort("127.0.0.1", port)
	if fallbackTarget == localTarget {
		return nil, err
	}

	fallbackConn, fallbackErr := net.DialTimeout("tcp", fallbackTarget, timeout)
	if fallbackErr == nil {
		return fallbackConn, nil
	}
	return nil, fmt.Errorf("dial %s failed: %w; loopback fallback %s failed: %v", localTarget, err, fallbackTarget, fallbackErr)
}

type closeWriter interface {
	CloseWrite() error
}

func closeWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func requestTransportCredential(ctx context.Context, client *Client, agentToken, transport string) (*transportCredentialResponse, error) {
	res := transportCredentialResponse{}
	err := client.http.Do(ctx, http.MethodPost, "/api/v1/gateway/hybrid/transport-credential", map[string]string{
		"agentToken": agentToken,
		"transport":  transport,
	}, &res)
	return &res, err
}

func listRoutes(ctx context.Context, client *Client, agentToken string) ([]route, error) {
	var res listRoutesResponse
	if err := client.http.Do(ctx, http.MethodPost, "/api/v1/gateway/hybrid/routes:list", map[string]string{
		"agentToken": agentToken,
	}, &res); err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("%s", res.ErrMsg)
	}
	return res.Routes, nil
}

func updateRouteStatus(ctx context.Context, client *Client, agentToken, routeID, state, proxyTarget, errMsg string) error {
	var res struct {
		Ok     bool   `json:"ok"`
		ErrMsg string `json:"errMsg"`
	}
	if err := client.http.Do(ctx, http.MethodPost, "/api/v1/gateway/hybrid/routes/status", map[string]string{
		"agentToken":  agentToken,
		"routeId":     routeID,
		"state":       state,
		"proxyTarget": proxyTarget,
		"error":       errMsg,
	}, &res); err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("%s", res.ErrMsg)
	}
	return nil
}

func normalizeTransport(transport string) string {
	transport = strings.TrimSpace(strings.ReplaceAll(transport, "-", "_"))
	if transport == "" {
		return types.BackendRouteTransportTSNet
	}
	return transport
}

func defaultLocalDirectListenAddr(devMode bool) string {
	if devMode && runtime.GOOS == "darwin" {
		return "0.0.0.0:0"
	}
	return "127.0.0.1:0"
}

func defaultLocalDirectAdvertiseHost(boundHost string, devMode bool) string {
	if devMode && runtime.GOOS == "darwin" {
		return "host.docker.internal"
	}
	if boundHost == "" || boundHost == "::" || boundHost == "0.0.0.0" || boundHost == "[::]" {
		return "127.0.0.1"
	}
	return boundHost
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type preflightResult struct {
	checks      []check
	gpus        []string
	schedulable bool
}

func runPreflight(devMode bool) preflightResult {
	checks := []check{
		{
			Name:     "k3s",
			Ok:       true,
			Message:  "not required for hybrid worker-container mode",
			Severity: "info",
		},
		{
			Name:     "flux",
			Ok:       true,
			Message:  "not installed or required for hybrid worker-container mode",
			Severity: "info",
		},
		{
			Name:     "tailscale-daemon",
			Ok:       true,
			Message:  "not installed or required; hybrid transport is embedded and route-scoped",
			Severity: "info",
		},
	}
	if devMode {
		checks = append(checks, check{
			Name:     "local-dev",
			Ok:       true,
			Message:  "macOS and Linux local joins use the agent route listener without k3s, Flux, or a system Tailscale daemon",
			Severity: "info",
		})
	}

	production := runtime.GOOS == "linux"
	checks = append(checks, check{
		Name:     "linux",
		Ok:       production || devMode,
		Message:  "production worker-container execution requires Linux; local-dev join is route-only on macOS",
		Severity: severity(production || devMode),
	})

	if runtime.GOOS == "linux" && !devMode {
		root := os.Geteuid() == 0
		checks = append(checks, check{
			Name:     "root",
			Ok:       root,
			Message:  "required for privileged worker containers, netns, iptables, and device mapping",
			Severity: severity(root),
		})

		runtimeOK := commandExists("docker") || commandExists("containerd") || commandExists("ctr")
		checks = append(checks, check{
			Name:     "container-runtime",
			Ok:       runtimeOK,
			Message:  "requires rootful Docker or containerd for the v1 worker-container executor",
			Severity: severity(runtimeOK),
		})

		netns := pathExists("/proc/self/ns/net")
		checks = append(checks, check{
			Name:     "network-namespace",
			Ok:       netns,
			Message:  "required because the worker creates container network namespaces directly",
			Severity: severity(netns),
		})

		netnsRunDir := writableDirOrCreatable("/var/run/netns")
		checks = append(checks, check{
			Name:     "netns-run-dir",
			Ok:       netnsRunDir,
			Message:  "requires writable /var/run/netns for named container network namespaces",
			Severity: severity(netnsRunDir),
		})

		ipForward := sysctlEnabledOrWritable("/proc/sys/net/ipv4/ip_forward")
		checks = append(checks, check{
			Name:     "ip-forward",
			Ok:       ipForward,
			Message:  "requires IPv4 forwarding for worker bridge NAT and exposed container ports",
			Severity: severity(ipForward),
		})

		iptables := iptablesNatAvailable()
		checks = append(checks, check{
			Name:     "iptables",
			Ok:       iptables,
			Message:  "requires usable IPv4 nat/filter tables for worker-managed MASQUERADE and DNAT rules",
			Severity: severity(iptables),
		})

		networkManager := networkManagerPresent()
		checks = append(checks, check{
			Name:     "network-manager",
			Ok:       true,
			Message:  networkManagerMessage(networkManager),
			Severity: "info",
		})

		fuse := pathExists("/dev/fuse")
		checks = append(checks, check{
			Name:     "fuse",
			Ok:       fuse,
			Message:  "required while the current image path uses CLIP/go-fuse lazy mounts",
			Severity: severity(fuse),
		})
	}

	gpus := detectNvidiaGPUs()
	if len(gpus) > 0 {
		nvidiaRuntime := commandExists("nvidia-ctk") || commandExists("nvidia-container-runtime")
		checks = append(checks, check{
			Name:     "nvidia-runtime",
			Ok:       nvidiaRuntime,
			Message:  "required to pin GPUs into worker containers",
			Severity: severity(nvidiaRuntime),
		})
	}

	return preflightResult{
		checks:      checks,
		gpus:        gpus,
		schedulable: allRequiredChecksPassed(checks),
	}
}

func allRequiredChecksPassed(checks []check) bool {
	for _, c := range checks {
		if c.Severity == "error" && !c.Ok {
			return false
		}
	}
	return true
}

func severity(ok bool) string {
	if ok {
		return "info"
	}
	return "error"
}

func commandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func writableDirOrCreatable(path string) bool {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir() && writableDir(path)
	}
	if !os.IsNotExist(err) {
		return false
	}

	parent := filepath.Dir(path)
	return writableDir(parent)
}

func writableDir(path string) bool {
	probe, err := os.CreateTemp(path, ".beam-preflight-*")
	if err != nil {
		return false
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return true
}

func sysctlEnabledOrWritable(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	if strings.TrimSpace(string(data)) == "1" {
		return true
	}
	return writableFile(path)
}

func writableFile(path string) bool {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return false
	}
	_ = file.Close()
	return true
}

func iptablesNatAvailable() bool {
	for _, name := range []string{"iptables", "iptables-nft", "iptables-legacy"} {
		if !commandExists(name) {
			continue
		}
		if err := exec.Command(name, "-t", "nat", "-L", "-n").Run(); err == nil {
			return true
		}
	}
	return false
}

func networkManagerPresent() bool {
	return commandExists("nmcli") || pathExists("/run/NetworkManager")
}

func networkManagerMessage(present bool) string {
	if !present {
		return "not detected; worker-managed bridge/veth devices will be created directly"
	}
	return "detected; Beam uses b9_br0 and b9h*/b9c* devices directly and does not require NetworkManager profiles"
}

func machineFingerprint(hostname string) string {
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if data, err := os.ReadFile(path); err == nil {
			value := strings.TrimSpace(string(data))
			if value != "" {
				return value
			}
		}
	}
	sum := sha256.Sum256([]byte(runtime.GOOS + "\x00" + runtime.GOARCH + "\x00" + hostname))
	return hex.EncodeToString(sum[:])
}

func systemMemoryMB() uint64 {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return 0
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "MemTotal:" {
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if err != nil {
					return 0
				}
				return kb / 1024
			}
		}
		return 0
	}

	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0
		}
		bytes, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			return 0
		}
		return bytes / 1024 / 1024
	}

	return 0
}

func detectNvidiaGPUs() []string {
	cmd := exec.Command("nvidia-smi", "--query-gpu=name", "--format=csv,noheader")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	gpus := make([]string, 0, len(lines))
	for _, line := range lines {
		name := strings.TrimSpace(line)
		if name != "" {
			gpus = append(gpus, name)
		}
	}
	return gpus
}
