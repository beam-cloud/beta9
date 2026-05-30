package agent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/hybrid/httpjson"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"tailscale.com/tsnet"
)

const (
	agentWorkspaceStorageEndpointEnv             = "BEAM_WORKSPACE_STORAGE_ENDPOINT_URL"
	agentWorkspaceStorageEndpointRewriteHostsEnv = "BEAM_WORKSPACE_STORAGE_ENDPOINT_REWRITE_HOSTS"
	agentWorkspaceStorageEndpointOverrideEnv     = "BEAM_AGENT_WORKSPACE_STORAGE_ENDPOINT_URL"
	agentWorkspaceStorageEndpointPortEnv         = "BEAM_AGENT_WORKSPACE_STORAGE_ENDPOINT_PORT"
	agentOCIRegistryRewriteEnv                   = "BEAM_OCI_REGISTRY_REWRITE"
	agentOCIRegistryRewriteOverrideEnv           = "BEAM_AGENT_OCI_REGISTRY_REWRITE"
	agentOCIRegistryEndpointPortEnv              = "BEAM_AGENT_OCI_REGISTRY_ENDPOINT_PORT"

	agentContainerImagesPath           = "/images"
	agentContainerTmpPath              = "/tmp"
	agentContainerDataPath             = "/data"
	agentContainerWorkspaceStoragePath = "/workspace/data"
	agentContainerCachePath            = "/var/lib/beta9/cache"
	agentContainerCheckpointPath       = "/checkpoints"
	agentContainerLogsPath             = "/var/log/worker"
	agentContainerConfigPath           = "/etc/beam/agent-worker.json"
	agentContainerCacheFSMountPath     = "/cache"
)

type JoinOptions struct {
	GatewayURL        string
	JoinToken         string
	DevMode           bool
	ExecutorOverride  string
	TransportOverride string
	WorkerImage       string
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
	Ok          bool            `json:"ok"`
	ErrMsg      string          `json:"errMsg"`
	WorkspaceID string          `json:"workspaceId"`
	PoolName    string          `json:"poolName"`
	MachineID   string          `json:"machineId"`
	AgentToken  string          `json:"agentToken"`
	Bootstrap   bootstrapConfig `json:"bootstrap"`
}

type bootstrapConfig struct {
	GatewayHTTPURL  string `json:"gatewayHttpUrl"`
	GatewayGRPCHost string `json:"gatewayGrpcHost"`
	GatewayGRPCPort int    `json:"gatewayGrpcPort"`
	GatewayGRPCTLS  bool   `json:"gatewayGrpcTls"`
	WorkspaceID     string `json:"workspaceId"`
	PoolName        string `json:"poolName"`
	Transport       string `json:"transport"`
	Executor        string `json:"executor"`
	Fallback        string `json:"fallback"`
}

type transportCredentialResponse struct {
	Ok         bool   `json:"ok"`
	ErrMsg     string `json:"errMsg"`
	AuthKey    string `json:"authKey"`
	ControlURL string `json:"controlUrl"`
	Hostname   string `json:"hostname"`
	Ephemeral  bool   `json:"ephemeral"`
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
	res, err := join(ctx, client, opts.JoinToken, opts.DevMode, opts.ExecutorOverride)
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

	slotManager := newWorkerSlotManager(res.Bootstrap, opts, opts.Stdout, opts.Stderr)
	defer slotManager.stopAll()

	registryForwarder, err := startLocalRegistryForwarder(ctx, res.Bootstrap, opts.Stderr)
	if err != nil {
		fmt.Fprintf(opts.Stderr, "local registry forwarder disabled: %v\n", err)
	} else if registryForwarder != nil {
		defer registryForwarder.Close()
	}

	transport := normalizeTransport(firstNonEmpty(opts.TransportOverride, res.Bootstrap.Transport))
	if err := runRouteProxy(ctx, grpcClient, res.AgentToken, transport, slotManager, opts.Stdout, opts.Stderr); err != nil {
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

func newGatewayGRPCClient(gatewayURL, host string, port int, useTLS bool) (pb.GatewayServiceClient, *grpc.ClientConn, error) {
	addr, err := gatewayGRPCAddr(gatewayURL, host, port)
	if err != nil {
		return nil, nil, err
	}

	creds := insecure.NewCredentials()
	if useTLS || strings.HasSuffix(addr, ":443") {
		creds = credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(creds))
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
	if !envBool("BEAM_AGENT_CONTAINER") {
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

func join(ctx context.Context, client *Client, token string, devMode bool, executorOverride string) (*joinResponse, error) {
	hostname, _ := os.Hostname()
	hostname = firstNonEmpty(os.Getenv("BEAM_AGENT_HOSTNAME"), hostname)
	executor := types.DefaultAgentWorkerContainerMode
	if executorOverride != "" {
		executor = executorOverride
	}
	preflight := runPreflight(devMode, executor)
	gpus := preflight.gpus
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
	if err := client.http.Do(ctx, http.MethodPost, "/api/v1/gateway/agent/join", req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func runRouteProxy(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string, slotManager *workerSlotManager, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	transport = normalizeTransport(transport)
	switch transport {
	case types.BackendRouteTransportTSNet:
		return runTSNetRouteProxy(ctx, client, agentToken, transport, slotManager, stdout, stderr)
	default:
		return fmt.Errorf("unsupported agent transport %q", transport)
	}
}

func runTSNetRouteProxy(ctx context.Context, client pb.GatewayServiceClient, agentToken, transport string, slotManager *workerSlotManager, stdout, stderr io.Writer) error {
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
	return newRouteProxy(client, agentToken, listener, proxyTarget, slotManager, stderr).run(ctx)
}

type workerSlotManager struct {
	bootstrap   bootstrapConfig
	opts        JoinOptions
	stdout      io.Writer
	stderr      io.Writer
	mu          sync.Mutex
	supervisors map[string]*slotSupervisor
	noticeOnce  sync.Once
}

type slotSupervisor struct {
	slot   *pb.AgentWorkerSlot
	cancel context.CancelFunc
}

func newWorkerSlotManager(bootstrap bootstrapConfig, opts JoinOptions, stdout, stderr io.Writer) *workerSlotManager {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	return &workerSlotManager{
		bootstrap:   bootstrap,
		opts:        opts,
		stdout:      stdout,
		stderr:      stderr,
		supervisors: map[string]*slotSupervisor{},
	}
}

func (m *workerSlotManager) reconcile(ctx context.Context, slots []*pb.AgentWorkerSlot) error {
	if m == nil {
		return nil
	}

	if m.bootstrap.Executor != types.DefaultAgentWorkerContainerMode {
		if len(slots) > 0 {
			m.noticeOnce.Do(func() {
				fmt.Fprintf(m.stderr, "agent executor %q does not start worker containers; desired slots are ignored\n", m.bootstrap.Executor)
			})
		}
		m.stopAll()
		return nil
	}

	if runtime.GOOS != "linux" {
		if len(slots) > 0 {
			return fmt.Errorf("worker-container executor requires Linux; this machine joined as %s/%s", runtime.GOOS, runtime.GOARCH)
		}
		m.stopAll()
		return nil
	}

	seen := map[string]struct{}{}
	for _, slot := range slots {
		if slot == nil || slot.WorkerId == "" {
			continue
		}
		seen[slot.WorkerId] = struct{}{}
		m.ensureSlot(ctx, slot)
	}
	m.stopSlotsNotIn(seen)
	return nil
}

func (m *workerSlotManager) ensureSlot(ctx context.Context, slot *pb.AgentWorkerSlot) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if current, ok := m.supervisors[slot.WorkerId]; ok {
		if sameWorkerSlot(current.slot, slot) {
			return
		}
		current.cancel()
		delete(m.supervisors, slot.WorkerId)
	}

	slotCtx, cancel := context.WithCancel(ctx)
	m.supervisors[slot.WorkerId] = &slotSupervisor{slot: slot, cancel: cancel}
	go m.superviseSlot(slotCtx, slot)
}

func (m *workerSlotManager) superviseSlot(ctx context.Context, slot *pb.AgentWorkerSlot) {
	backoff := time.Second
	for {
		err := m.runWorkerContainer(ctx, slot)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			return
		}
		fmt.Fprintf(m.stderr, "worker slot %s exited: %v\n", slot.WorkerId, err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (m *workerSlotManager) runWorkerContainer(ctx context.Context, slot *pb.AgentWorkerSlot) error {
	if !commandExists("docker") {
		return fmt.Errorf("docker is required for worker-container executor")
	}

	stateDir, err := agentStateDir()
	if err != nil {
		return err
	}
	dirs := agentWorkerDirs(stateDir, slot.WorkerId)
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	configPath := filepath.Join(dirs["slot"], "config.json")
	if err := writeWorkerConfig(configPath, m.bootstrap, slot, dirs); err != nil {
		return err
	}

	name := "beam-agent-" + sanitizeDockerName(slot.WorkerId)
	image := firstNonEmpty(m.opts.WorkerImage, os.Getenv("BEAM_WORKER_IMAGE"), slot.WorkerImage)
	if image == "" {
		return fmt.Errorf("worker image is required for slot %s", slot.WorkerId)
	}

	_ = exec.Command("docker", "rm", "-f", name).Run()
	defer func() { _ = exec.Command("docker", "rm", "-f", name).Run() }()

	args := dockerRunArgs(name, image, configPath, m.bootstrap, slot, dirs)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = m.stdout
	cmd.Stderr = m.stderr

	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = exec.Command("docker", "rm", "-f", name).Run()
		<-done
		return ctx.Err()
	}
}

func (m *workerSlotManager) stopSlotsNotIn(seen map[string]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for workerID, supervisor := range m.supervisors {
		if _, ok := seen[workerID]; ok {
			continue
		}
		supervisor.cancel()
		delete(m.supervisors, workerID)
	}
}

func (m *workerSlotManager) stopAll() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for workerID, supervisor := range m.supervisors {
		supervisor.cancel()
		delete(m.supervisors, workerID)
	}
}

func agentWorkerDirs(stateDir, workerID string) map[string]string {
	slotName := sanitizeDockerName(workerID)
	return map[string]string{
		"slot":        filepath.Join(stateDir, "slots", slotName),
		"images":      filepath.Join(stateDir, "images"),
		"tmp":         filepath.Join(stateDir, "tmp", slotName),
		"data":        filepath.Join(stateDir, "data"),
		"workspace":   filepath.Join(stateDir, "workspace-data"),
		"cache":       filepath.Join(stateDir, "cache"),
		"checkpoints": filepath.Join(stateDir, "checkpoints"),
		"logs":        filepath.Join(stateDir, "logs", slotName),
	}
}

func sameWorkerSlot(a, b *pb.AgentWorkerSlot) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.WorkerId == b.WorkerId &&
		a.WorkerToken == b.WorkerToken &&
		a.PoolName == b.PoolName &&
		a.MachineId == b.MachineId &&
		a.Cpu == b.Cpu &&
		a.Memory == b.Memory &&
		a.Gpu == b.Gpu &&
		a.GpuCount == b.GpuCount &&
		a.GpuAssignment == b.GpuAssignment &&
		a.NetworkPrefix == b.NetworkPrefix &&
		a.WorkerImage == b.WorkerImage
}

func dockerRunArgs(name, image, configPath string, bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot, dirs map[string]string) []string {
	localTargetHost := firstNonEmpty(os.Getenv("BEAM_AGENT_LOCAL_TARGET_HOST"), "127.0.0.1")
	args := []string{
		"run", "--rm",
		"--name", name,
		"--privileged",
		"--network", "host",
		"--cgroupns", "host",
	}
	for _, alias := range agentDockerHostAliases(bootstrap) {
		args = append(args, "--add-host", alias)
	}

	if slot.Cpu > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.3f", float64(slot.Cpu)/1000.0))
	}
	if slot.Memory > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", slot.Memory))
		args = append(args, "--shm-size", fmt.Sprintf("%dm", max(slot.Memory/2, 64)))
	}
	if slot.GpuCount > 0 {
		if slot.GpuAssignment != "" {
			args = append(args, "--gpus", "device="+slot.GpuAssignment)
		} else {
			args = append(args, "--gpus", fmt.Sprintf("%d", slot.GpuCount))
		}
	}

	volumeArgs := []string{
		dirs["images"] + ":" + agentContainerImagesPath,
		dirs["tmp"] + ":" + agentContainerTmpPath,
		dirs["data"] + ":" + agentContainerDataPath,
		dirs["workspace"] + ":" + agentContainerWorkspaceStoragePath,
		dirs["cache"] + ":" + agentContainerCachePath,
		dirs["checkpoints"] + ":" + agentContainerCheckpointPath,
		dirs["logs"] + ":" + agentContainerLogsPath,
		configPath + ":" + agentContainerConfigPath + ":ro",
	}
	if pathExists("/var/lib/kubelet/device-plugins") {
		volumeArgs = append(volumeArgs, "/var/lib/kubelet/device-plugins:/var/lib/kubelet/device-plugins:ro")
	}
	if pathExists("/var/run/netns") {
		volumeArgs = append(volumeArgs, "/var/run/netns:/var/run/netns")
	}
	if pathExists("/sys/fs/cgroup") {
		volumeArgs = append(volumeArgs, "/sys/fs/cgroup:/sys/fs/cgroup:rw")
	}
	for _, volume := range volumeArgs {
		args = append(args, "-v", volume)
	}
	if pathExists("/dev/fuse") {
		args = append(args, "--device", "/dev/fuse")
	}

	env := map[string]string{
		"CONFIG_PATH":                       agentContainerConfigPath,
		types.WorkerEnvID:                   slot.WorkerId,
		types.WorkerEnvToken:                slot.WorkerToken,
		types.WorkerEnvPoolName:             slot.PoolName,
		types.WorkerEnvMachineID:            slot.MachineId,
		"CPU_LIMIT":                         strconv.FormatInt(slot.Cpu, 10),
		"MEMORY_LIMIT":                      strconv.FormatInt(slot.Memory, 10),
		"GPU_TYPE":                          slot.Gpu,
		"GPU_COUNT":                         strconv.FormatUint(uint64(slot.GpuCount), 10),
		"POD_HOSTNAME":                      "127.0.0.1",
		"POD_IP":                            "127.0.0.1",
		"NETWORK_PREFIX":                    slot.NetworkPrefix,
		"CACHE_LOCALITY":                    slot.PoolName,
		"CACHE_NODE_ID":                     slot.MachineId,
		"CACHE_HOST_NETWORK":                "true",
		types.WorkerEnvPersistent:           "true",
		types.WorkerEnvRouteTransport:       normalizeTransport(bootstrap.Transport),
		types.WorkerEnvRouteLocalTargetHost: localTargetHost,
		"BEAM_GATEWAY_HTTP_URL":             strings.TrimRight(bootstrap.GatewayHTTPURL, "/"),
	}
	for key, value := range agentGatewayEnv(bootstrap) {
		env[key] = value
	}
	if endpoint := agentWorkspaceStorageEndpointURL(bootstrap); endpoint != "" {
		env[agentWorkspaceStorageEndpointEnv] = endpoint
	}
	if rewriteHosts := strings.TrimSpace(os.Getenv("BEAM_AGENT_WORKSPACE_STORAGE_ENDPOINT_REWRITE_HOSTS")); rewriteHosts != "" {
		env[agentWorkspaceStorageEndpointRewriteHostsEnv] = rewriteHosts
	}
	if rewrite := agentOCIRegistryRewrite(bootstrap); rewrite != "" {
		env[agentOCIRegistryRewriteEnv] = rewrite
	}
	if slot.GpuCount > 0 && slot.GpuAssignment != "" {
		env["NVIDIA_VISIBLE_DEVICES"] = slot.GpuAssignment
	}
	for key, value := range env {
		args = append(args, "-e", key+"="+value)
	}

	args = append(args, image, "/usr/local/bin/worker")
	return args
}

func writeWorkerConfig(path string, bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot, dirs map[string]string) error {
	workspaceStorageMode := firstNonEmpty(os.Getenv("BEAM_AGENT_WORKSPACE_STORAGE_MODE"), storage.StorageModeGeese)
	cacheDir := pathpkg.Join(agentContainerCachePath, sanitizeDockerName(slot.PoolName), sanitizeDockerName(slot.MachineId))
	httpHost, httpPort, httpTLS := agentGatewayHTTPParts(bootstrap)
	config := map[string]any{
		"clusterName": "agent",
		"debugMode":   false,
		"prettyLogs":  true,
		"gateway": map[string]any{
			"grpc": map[string]any{
				"externalHost": bootstrap.GatewayGRPCHost,
				"externalPort": bootstrap.GatewayGRPCPort,
				"tls":          bootstrap.GatewayGRPCTLS,
			},
			"http": map[string]any{
				"externalHost": httpHost,
				"externalPort": httpPort,
				"tls":          httpTLS,
			},
		},
		"storage": map[string]any{
			"mode":       storage.StorageModeLocal,
			"fsName":     "agent",
			"fsPath":     agentContainerDataPath,
			"objectPath": pathpkg.Join(agentContainerDataPath, "objects"),
			"workspaceStorage": map[string]any{
				"baseMountPath":      agentContainerWorkspaceStoragePath,
				"defaultStorageMode": workspaceStorageMode,
			},
		},
		"monitoring": map[string]any{
			"metricsCollector":         string(types.MetricsCollectorNone),
			"containerMetricsInterval": "3s",
			"prometheus": map[string]any{
				"scrapeWorkers": false,
				"port":          0,
			},
		},
		"worker": map[string]any{
			"hostNetwork":                true,
			"useHostResolvConf":          true,
			"containerRuntime":           "runc",
			"cacheEnabled":               true,
			"terminationGracePeriod":     30,
			"containerLogLinesPerHour":   6000,
			"defaultWorkerCPURequest":    slot.Cpu,
			"defaultWorkerMemoryRequest": slot.Memory,
			"failover": map[string]any{
				"maxSchedulingLatencyMs": 300000,
			},
			"pools": map[string]any{
				slot.PoolName: map[string]any{
					"mode":                 string(types.PoolModeHybrid),
					"gpuType":              slot.Gpu,
					"containerRuntime":     "runc",
					"networkPreallocation": false,
					"networkSlotPoolSize":  32,
					"requiresPoolSelector": true,
					"priority":             1000,
					"criuEnabled":          false,
					"tmpSizeLimit":         "30Gi",
					"storageMode":          workspaceStorageMode,
					"checkpointPath":       agentContainerCheckpointPath,
					"cache": map[string]any{
						"enabled": true,
						"disk": map[string]any{
							"enabled":     true,
							"hostPath":    agentContainerCachePath,
							"mountPath":   agentContainerCachePath,
							"maxUsagePct": 0.95,
						},
					},
				},
			},
		},
		"cache": map[string]any{
			"enabled": true,
			"disk": map[string]any{
				"enabled":     true,
				"hostPath":    agentContainerCachePath,
				"mountPath":   agentContainerCachePath,
				"maxUsagePct": 0.95,
			},
			"memory": map[string]any{
				"enabled": false,
			},
			"global": map[string]any{
				"defaultLocality": firstNonEmpty(slot.PoolName, "agent"),
			},
			"server": map[string]any{
				"diskCacheDir": cacheDir,
			},
			"client": map[string]any{
				"cachefs": map[string]any{
					"enabled":    true,
					"mountPoint": agentContainerCacheFSMountPath,
				},
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

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

func newRouteProxy(client pb.GatewayServiceClient, agentToken string, listener net.Listener, proxyTarget string, slotManager *workerSlotManager, stderr io.Writer) *routeProxy {
	if stderr == nil {
		stderr = io.Discard
	}
	return &routeProxy{
		agentToken:  agentToken,
		client:      client,
		listener:    listener,
		proxyTarget: proxyTarget,
		slotManager: slotManager,
		stderr:      stderr,
		routes:      map[string]string{},
	}
}

type routeProxy struct {
	client      pb.GatewayServiceClient
	agentToken  string
	listener    net.Listener
	proxyTarget string
	slotManager *workerSlotManager
	stderr      io.Writer
	mu          sync.Mutex
	routes      map[string]string
}

func (p *routeProxy) run(ctx context.Context) error {
	acceptErr := make(chan error, 1)
	go func() {
		acceptErr <- p.accept(ctx)
	}()

	streamErr := make(chan error, 1)
	go func() {
		streamErr <- p.watchRoutes(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-acceptErr:
			return err
		case err := <-streamErr:
			return err
		}
	}
}

func (p *routeProxy) watchRoutes(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := p.watchRoutesOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Fprintf(p.stderr, "agent stream disconnected: %v\n", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (p *routeProxy) watchRoutesOnce(ctx context.Context) error {
	stream, err := p.client.StreamAgent(ctx, &pb.StreamAgentRequest{AgentToken: p.agentToken})
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if !msg.Ok {
			return fmt.Errorf("%s", msg.ErrMsg)
		}
		if err := p.reconcileRoutes(ctx, msg.Routes); err != nil {
			return err
		}
		if p.slotManager != nil {
			if err := p.slotManager.reconcile(ctx, msg.Slots); err != nil {
				return err
			}
		}
	}
}

func (p *routeProxy) reconcileRoutes(ctx context.Context, routes []*pb.AgentRoute) error {
	seen := map[string]struct{}{}
	for _, route := range routes {
		if route.LocalTarget == "" {
			continue
		}
		seen[route.RouteId] = struct{}{}
		p.setRoute(route.RouteId, route.LocalTarget)
		if route.State == "ready" && route.ProxyTarget == p.proxyTarget {
			continue
		}
		if err := checkLocalTargetReady(route.LocalTarget); err != nil {
			if route.State == "ready" {
				_ = updateRouteStatus(ctx, p.client, p.agentToken, route.RouteId, "degraded", p.proxyTarget, err.Error())
			}
			continue
		}
		if err := updateRouteStatus(ctx, p.client, p.agentToken, route.RouteId, "ready", p.proxyTarget, ""); err != nil {
			p.deleteRoute(route.RouteId)
			_ = updateRouteStatus(ctx, p.client, p.agentToken, route.RouteId, "degraded", p.proxyTarget, err.Error())
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
		p.markRouteDegraded(routeID, err)
		if !isLocalTargetUnavailable(err) {
			fmt.Fprintf(p.stderr, "route %s proxy failed: %v\n", routeID, err)
		}
	}
}

func (p *routeProxy) markRouteDegraded(routeID string, cause error) {
	p.deleteRoute(routeID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := updateRouteStatus(ctx, p.client, p.agentToken, routeID, "degraded", p.proxyTarget, cause.Error()); err != nil && !isLocalTargetUnavailable(cause) {
		fmt.Fprintf(p.stderr, "route %s status update failed: %v\n", routeID, err)
	}
}

func proxyConn(conn net.Conn, source io.Reader, localTarget string) error {
	defer conn.Close()
	local, err := dialLocalTarget(localTarget)
	if err != nil {
		return err
	}
	defer local.Close()

	copyBuffered(conn, source, local)
	return nil
}

func copyBuffered(conn net.Conn, source io.Reader, local net.Conn) {
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
}

func copyBoth(a, b net.Conn) {
	copyBuffered(a, a, b)
}

func checkLocalTargetReady(localTarget string) error {
	conn, err := dialLocalTargetWithTimeout(localTarget, 250*time.Millisecond)
	if err != nil {
		return err
	}
	return conn.Close()
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

func isLocalTargetUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "connection reset by peer")
}

type closeWriter interface {
	CloseWrite() error
}

func closeWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
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

func updateRouteStatus(ctx context.Context, client pb.GatewayServiceClient, agentToken, routeID, state, proxyTarget, errMsg string) error {
	res, err := client.UpdateAgentRouteStatus(ctx, &pb.UpdateAgentRouteStatusRequest{
		AgentToken:  agentToken,
		RouteId:     routeID,
		State:       state,
		ProxyTarget: proxyTarget,
		Error:       errMsg,
	})
	if err != nil {
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func envBool(name string) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return value == "1" || value == "true" || value == "yes"
}

type preflightResult struct {
	checks      []check
	gpus        []string
	schedulable bool
}

func runPreflight(devMode bool, executor string) preflightResult {
	checks := []check{
		{
			Name:     "k3s",
			Ok:       true,
			Message:  "not required for agent worker-container mode",
			Severity: "info",
		},
		{
			Name:     "flux",
			Ok:       true,
			Message:  "not installed or required for agent worker-container mode",
			Severity: "info",
		},
		{
			Name:     "tailscale-daemon",
			Ok:       true,
			Message:  "not installed or required; agent transport is embedded and route-scoped",
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
	if envBool("BEAM_AGENT_CONTAINER") {
		checks = append(checks, check{
			Name:     "agent-container",
			Ok:       true,
			Message:  "running the Linux agent inside Docker; worker containers are started as siblings through the Docker socket",
			Severity: "info",
		})
	}

	workerContainer := executor == types.DefaultAgentWorkerContainerMode
	production := runtime.GOOS == "linux"
	linuxOK := production || (devMode && !workerContainer)
	checks = append(checks, check{
		Name:     "linux",
		Ok:       linuxOK,
		Message:  "worker-container execution requires Linux; macOS uses the Docker Linux agent path",
		Severity: severity(linuxOK),
	})

	if runtime.GOOS == "linux" && workerContainer {
		root := os.Geteuid() == 0
		checks = append(checks, check{
			Name:     "root",
			Ok:       root,
			Message:  "not required when the user can run rootful Docker; sudo install remains supported",
			Severity: "info",
		})

		runtimeOK := commandExists("docker")
		checks = append(checks, check{
			Name:     "container-runtime",
			Ok:       runtimeOK,
			Message:  "requires rootful Docker for the v1 worker-container executor",
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
	if value := strings.TrimSpace(os.Getenv("BEAM_AGENT_MACHINE_FINGERPRINT")); value != "" {
		return value
	}
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
