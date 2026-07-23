package shell

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	pb "github.com/beam-cloud/beta9/proto"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	computemodel "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	shellRoutePrefix              string        = "/shell"
	shellContainerPrefix          string        = "shell"
	shellContainerTtlS            int           = 30 // 30 seconds
	shellProxyBufferSizeKb        int           = 32 * 1024
	shellKeepAliveIntervalS       time.Duration = 60 * time.Second
	defaultContainerCpu           int64         = 100
	defaultContainerMemory        int64         = 128
	containerDialTimeoutDurationS time.Duration = 10 * time.Second
	containerControlDialTimeout   time.Duration = 10 * time.Second
	shellSetupTimeout             time.Duration = 20 * time.Second
	containerWaitTimeoutDurationS time.Duration = 5 * time.Minute
	initialShellTTL               time.Duration = containerWaitTimeoutDurationS + time.Minute
	pendingShellTTL               time.Duration = 5 * time.Minute
	containerWaitPollIntervalS    time.Duration = 1 * time.Second
	containerKeepAliveIntervalS   time.Duration = 5 * time.Second
	shellReconcileInterval        time.Duration = 30 * time.Second
	sshProbeTimeoutDurationS      time.Duration = 500 * time.Millisecond
	sshBannerTimeoutDurationS     time.Duration = 500 * time.Millisecond
	sshStartupTimeoutDurationS    time.Duration = 10 * time.Second
	sshStartupPollIntervalS       time.Duration = 100 * time.Millisecond
	// Remove systemd from nsswitch.conf to prevent systemd from being used as credential provider by dropbear
	startupScript    string = `SHELL="$(command -v bash || command -v sh)"; mkdir -p /etc/dropbear; sed -i 's/systemd//g' /etc/nsswitch.conf 2>/dev/null || true; /usr/local/bin/dropbear -e -c "export PATH=$PATH:/usr/local/bin && cd /mnt/code && $SHELL" -p %d -R -E -F 2>> /etc/dropbear/logs.txt`
	podStartupScript string = `SHELL="$(command -v bash || command -v sh)"; mkdir -p /etc/dropbear; sed -i 's/systemd//g' /etc/nsswitch.conf 2>/dev/null || true; /usr/local/bin/dropbear -e -c "export PATH=$PATH:/usr/local/bin && cd /mnt/code && $SHELL" -p %d -R -E 2>> /etc/dropbear/logs.txt`
	createUserScript string = `SHELL="$(command -v bash || command -v sh)"; mkdir -p /etc/dropbear; \
(command -v useradd >/dev/null && useradd -o -m -s $SHELL -u 0 -g 0 "$USERNAME" 2>> /etc/dropbear/logs.txt) || \
(command -v adduser >/dev/null && adduser --disabled-password --gecos "" --shell $SHELL --uid 0 --gid 0 "$USERNAME" 2>> /etc/dropbear/logs.txt) || \
(echo "$USERNAME:x:0:0:$USERNAME:/root:$SHELL" >> /etc/passwd && mkdir -p "/root" && chown 0:0 "/root") && \
echo "$USERNAME:$PASSWORD" | chpasswd 2>> /etc/dropbear/logs.txt`
)

type ShellService interface {
	pb.ShellServiceServer
	CreateStandaloneShell(ctx context.Context, in *pb.CreateStandaloneShellRequest) (*pb.CreateStandaloneShellResponse, error)
	CreateShellInExistingContainer(ctx context.Context, in *pb.CreateShellInExistingContainerRequest) (*pb.CreateShellInExistingContainerResponse, error)
}

func hasShellPermission(authInfo *auth.AuthInfo) bool {
	return auth.HasInteractivePermission(authInfo) && authInfo.Workspace != nil
}

type SSHShellService struct {
	pb.UnimplementedShellServiceServer
	ctx                context.Context
	config             types.AppConfig
	rdb                *common.RedisClient
	keyEventManager    *common.KeyEventManager
	scheduler          *scheduler.Scheduler
	backendRepo        repository.BackendRepository
	computeRepo        repository.ComputeRepository
	workspaceRepo      repository.WorkspaceRepository
	containerRepo      repository.ContainerRepository
	workerRepo         repository.WorkerRepository
	workerPoolRepo     repository.WorkerPoolRepository
	eventRepo          repository.EventRepository
	tailscale          *network.Tailscale
	ttlEventChan       chan common.KeyEvent
	containerEventChan chan common.KeyEvent
}

type ShellServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	ComputeRepo    repository.ComputeRepository
	WorkspaceRepo  repository.WorkspaceRepository
	ContainerRepo  repository.ContainerRepository
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
	Scheduler      *scheduler.Scheduler
	RouteGroup     *echo.Group
	Tailscale      *network.Tailscale
	EventRepo      repository.EventRepository
}

func NewSSHShellService(
	ctx context.Context,
	opts ShellServiceOpts,
) (ShellService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	ss := &SSHShellService{
		ctx:                ctx,
		config:             opts.Config,
		rdb:                opts.RedisClient,
		keyEventManager:    keyEventManager,
		scheduler:          opts.Scheduler,
		backendRepo:        opts.BackendRepo,
		computeRepo:        opts.ComputeRepo,
		workspaceRepo:      opts.WorkspaceRepo,
		containerRepo:      opts.ContainerRepo,
		workerRepo:         opts.WorkerRepo,
		workerPoolRepo:     opts.WorkerPoolRepo,
		tailscale:          opts.Tailscale,
		eventRepo:          opts.EventRepo,
		ttlEventChan:       make(chan common.KeyEvent),
		containerEventChan: make(chan common.KeyEvent),
	}

	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	registerShellRoutes(opts.RouteGroup.Group(shellRoutePrefix, authMiddleware), ss)

	// Listen for shell container ttl events
	go func() {
		if err := ss.keyEventManager.ListenForPattern(
			ss.ctx,
			Keys.shellContainerTTL(""),
			ss.ttlEventChan,
		); err != nil {
			log.Error().Err(err).Msg("shell TTL event listener stopped")
		}
	}()
	go func() {
		if err := ss.keyEventManager.ListenForPattern(
			ss.ctx,
			common.RedisKeys.SchedulerContainerState(shellContainerPrefix),
			ss.containerEventChan,
		); err != nil {
			log.Error().Err(err).Msg("shell container event listener stopped")
		}
	}()
	go ss.handleTTLEvents()

	return ss, nil
}

func (ss *SSHShellService) durableDiskPlacementRepos() abstractions.DurableDiskPlacementRepos {
	return abstractions.DurableDiskPlacementRepos{
		BackendRepo:    ss.backendRepo,
		ComputeRepo:    ss.computeRepo,
		WorkerRepo:     ss.workerRepo,
		WorkerPoolRepo: ss.workerPoolRepo,
	}
}

func (ss *SSHShellService) handleTTLEvents() {
	reconcileTicker := time.NewTicker(shellReconcileInterval)
	defer reconcileTicker.Stop()

	for {
		select {
		case event := <-ss.ttlEventChan:
			if event.Operation == common.KeyOperationExpired {
				ss.stopShellWithoutLease(event.Key)
			}
		case event := <-ss.containerEventChan:
			if event.Operation == common.KeyOperationSet {
				// Reconcile shell containers left behind while no gateway was
				// subscribed to their TTL events.
				ss.stopShellWithoutLease(shellContainerPrefix + event.Key)
			}
		case <-reconcileTicker.C:
			ss.reconcileShellLeases()
		case <-ss.ctx.Done():
			return
		}
	}
}

func (ss *SSHShellService) reconcileShellLeases() {
	statePrefix := common.RedisKeys.SchedulerContainerState(shellContainerPrefix)
	stateKeys, err := ss.rdb.Scan(ss.ctx, statePrefix+"*")
	if err != nil {
		log.Error().Err(err).Msg("failed to reconcile shell leases")
		return
	}
	for _, stateKey := range stateKeys {
		containerId := shellContainerPrefix + strings.TrimPrefix(stateKey, statePrefix)
		ss.stopShellWithoutLease(containerId)
	}
}

func (ss *SSHShellService) stopShellWithoutLease(containerId string) {
	if !IsStandaloneContainer(containerId) {
		return
	}

	exists, err := ss.rdb.Exists(ss.ctx, Keys.shellContainerTTL(containerId)).Result()
	if err != nil {
		log.Error().Err(err).Str("container_id", containerId).Msg("failed to inspect shell TTL")
		return
	}
	// Ignore a stale expiry notification if a valid lease is present.
	if exists != 0 {
		return
	}
	if err := ss.scheduler.Stop(&types.StopContainerArgs{
		ContainerId: containerId,
		Force:       true,
		Reason:      types.StopContainerReasonTtl,
	}); err != nil {
		log.Error().Err(err).Str("container_id", containerId).Msg("failed to stop expired shell")
	}
}

func (ss *SSHShellService) getShellAddress(containerId string) (string, bool) {
	addressMap, err := ss.containerRepo.GetContainerAddressMap(containerId)
	if err != nil {
		return "", false
	}

	addr, ok := addressMap[types.WorkerShellPort]
	return addr, ok
}

func (ss *SSHShellService) ensureShellPortExposed(ctx context.Context, containerId string, client *common.ContainerClient) (string, error) {
	if addr, ok := ss.getShellAddress(containerId); ok {
		return addr, nil
	}

	resp, err := client.SandboxExposePortContext(ctx, containerId, types.WorkerShellPort)
	if err != nil {
		return "", err
	}
	if !resp.Ok {
		return "", fmt.Errorf("%s", resp.ErrorMsg)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr, ok := ss.getShellAddress(containerId); ok {
			return addr, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(sshStartupPollIntervalS):
		}
	}
	return "", fmt.Errorf("shell port was not exposed")
}

func (ss *SSHShellService) checkForExistingSSHServer(ctx context.Context, addr string) bool {
	conn, err := network.ConnectToBackend(ctx, addr, sshProbeTimeoutDurationS, ss.tailscale, ss.config.Tailscale, ss.containerRepo)
	if err != nil {
		return false
	}
	defer conn.Close()

	return hasSSHBanner(conn, sshBannerTimeoutDurationS)
}

func hasSSHBanner(conn net.Conn, timeout time.Duration) bool {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}

	// RFC 4253 permits server notice lines before the identification string.
	// Bound both each line and the whole preamble so a non-SSH listener cannot
	// hold shell setup open or consume unbounded memory.
	reader := bufio.NewReaderSize(conn, 256)
	total := 0
	for total < 8192 {
		line, err := reader.ReadString('\n')
		total += len(line)
		if len(line) > 255 {
			return false
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if strings.HasPrefix(line, "SSH-") {
			return true
		}
		if err != nil {
			return false
		}
	}
	return false
}

func (ss *SSHShellService) waitForSSHServer(ctx context.Context, addr string) bool {
	waitCtx, cancel := context.WithTimeout(ctx, sshStartupTimeoutDurationS)
	defer cancel()
	ticker := time.NewTicker(sshStartupPollIntervalS)
	defer ticker.Stop()

	for {
		if ss.checkForExistingSSHServer(waitCtx, addr) {
			return true
		}
		select {
		case <-waitCtx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (ss *SSHShellService) waitForContainerSSHServer(ctx context.Context, containerId string) bool {
	waitCtx, cancel := context.WithTimeout(ctx, sshStartupTimeoutDurationS)
	defer cancel()
	ticker := time.NewTicker(sshStartupPollIntervalS)
	defer ticker.Stop()

	for {
		if addr, ok := ss.getShellAddress(containerId); ok &&
			ss.checkForExistingSSHServer(waitCtx, addr) {
			return true
		}
		select {
		case <-waitCtx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (ss *SSHShellService) getOrCreateSSHUser(ctx context.Context, containerId string, client *common.ContainerClient) (string, string, error) {
	username, password, err := GenerateShellCredentials()
	if err != nil {
		return "", "", err
	}

	response, err := client.ExecContext(
		ctx,
		containerId,
		createUserScript,
		[]string{
			fmt.Sprintf("USERNAME=%s", username),
			fmt.Sprintf("PASSWORD=%s", password),
		},
	)
	if err != nil {
		return "", "", err
	}
	if response == nil || !response.Ok {
		return "", "", fmt.Errorf("container rejected SSH user creation")
	}

	return username, password, nil
}

func (ss *SSHShellService) CreateShellInExistingContainer(ctx context.Context, in *pb.CreateShellInExistingContainerRequest) (*pb.CreateShellInExistingContainerResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !hasShellPermission(authInfo) {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Unauthorized access",
		}, nil
	}
	setupCtx, cancel := context.WithTimeout(ctx, shellSetupTimeout)
	defer cancel()
	containerId := in.ContainerId

	containerState, err := ss.containerRepo.GetContainerState(containerId)
	if err != nil || containerState == nil ||
		containerState.WorkspaceId != authInfo.Workspace.ExternalId {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Container not found",
		}, nil
	}

	if containerState.Status != types.ContainerStatusRunning {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Container is not running",
		}, nil
	}

	stub, err := ss.backendRepo.GetStubByExternalId(
		setupCtx,
		containerState.StubId,
		types.QueryFilter{
			Field: "workspace_id",
			Value: authInfo.Workspace.ExternalId,
		},
	)
	if err != nil || stub == nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Container not found",
		}, nil
	}

	if stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Container not found",
		}, nil
	}

	containerAddr, err := ss.containerRepo.GetWorkerAddress(setupCtx, containerId)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to get container address: %s", err),
		}, nil
	}

	conn, err := network.ConnectToBackend(
		setupCtx,
		containerAddr,
		containerControlDialTimeout,
		ss.tailscale,
		ss.config.Tailscale,
		ss.containerRepo,
	)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to connect to container: %s", err),
		}, nil
	}

	// This is an already-established gateway-to-worker connection. The worker
	// container service does not authenticate these internal RPCs, so never
	// forward the caller's reusable API token to customer-managed compute.
	containerClient, err := common.NewContainerClient(containerAddr, "", conn)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to create container client: %s", err),
		}, nil
	}
	defer containerClient.Close()

	shellAddr, err := ss.ensureShellPortExposed(setupCtx, containerId, containerClient)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to expose shell port: %s", err),
		}, nil
	}

	ok := ss.checkForExistingSSHServer(setupCtx, shellAddr)
	if !ok {
		response, execErr := containerClient.ExecContext(
			setupCtx,
			containerId,
			fmt.Sprintf(podStartupScript, types.WorkerShellPort),
			[]string{},
		)
		if !ss.waitForSSHServer(setupCtx, shellAddr) {
			message := "SSH server did not become ready"
			if execErr != nil {
				message = fmt.Sprintf("Failed to start SSH server: %v", execErr)
			} else if response == nil || !response.Ok {
				message = "Container rejected SSH server startup"
			}
			return &pb.CreateShellInExistingContainerResponse{
				Ok:     false,
				ErrMsg: message,
			}, nil
		}
	}

	username, password, err := ss.getOrCreateSSHUser(setupCtx, containerId, containerClient)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to create new SSH user: %s", err),
		}, nil
	}

	return &pb.CreateShellInExistingContainerResponse{
		Ok:       true,
		Username: username,
		Password: password,
		StubId:   stub.ExternalId,
	}, nil
}

func (ss *SSHShellService) CreateStandaloneShell(ctx context.Context, in *pb.CreateStandaloneShellRequest) (*pb.CreateStandaloneShellResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !hasShellPermission(authInfo) {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Unauthorized access",
		}, nil
	}

	stub, err := ss.backendRepo.GetStubByExternalId(
		ctx,
		in.StubId,
		types.QueryFilter{
			Field: "workspace_id",
			Value: authInfo.Workspace.ExternalId,
		},
	)
	if err != nil || stub == nil ||
		stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Invalid stub ID",
		}, nil
	}

	if ss.eventRepo != nil {
		go ss.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)
	}

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok: false,
		}, nil
	}

	machineID := strings.TrimSpace(in.GetMachineId())
	var machineWorker *types.Worker
	if machineID != "" {
		if ss.computeRepo == nil || ss.workerRepo == nil {
			return &pb.CreateStandaloneShellResponse{Ok: false, ErrMsg: "machine shells are unavailable"}, nil
		}
		machine, err := ss.computeRepo.GetAgentMachineStateForWorkspace(ctx, authInfo.Workspace.ExternalId, machineID)
		if err != nil {
			return &pb.CreateStandaloneShellResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		if machine == nil {
			return &pb.CreateStandaloneShellResponse{Ok: false, ErrMsg: "machine not found"}, nil
		}
		if !computemodel.AgentMachineConnected(machine, time.Now()) {
			return &pb.CreateStandaloneShellResponse{Ok: false, ErrMsg: "machine is not connected"}, nil
		}
		if selector := stubConfig.PoolSelector(); selector != "" && selector != machine.PoolName {
			return &pb.CreateStandaloneShellResponse{
				Ok: false, ErrMsg: fmt.Sprintf("machine %s does not belong to pool %s", machineID, selector),
			}, nil
		}
		machineWorker, err = ss.workerRepo.GetWorkerById(computemodel.AgentMachineWorkerID(machineID))
		if err != nil || machineWorker == nil {
			return &pb.CreateStandaloneShellResponse{Ok: false, ErrMsg: "machine worker is not ready"}, nil
		}
		stubConfig.Pool = &types.PoolConfig{Name: machine.PoolName, Selector: machine.PoolName}
		stubConfig.MachineID = machineID
	}

	containerId := ss.genContainerId(stub.ExternalId)

	// Don't allow negative and 0-valued compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultContainerMemory
	}

	if err := abstractions.ConfigureDurableDiskPlacement(ctx, ss.durableDiskPlacementRepos(), authInfo.Workspace, &stubConfig); err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		containerId,
		stub,
		authInfo.Workspace,
		stubConfig,
	)
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok: false,
		}, nil
	}

	secrets, err := abstractions.ConfigureContainerRequestSecrets(
		authInfo.Workspace,
		stubConfig,
	)
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok: false,
		}, nil
	}

	username, password, err := GenerateShellCredentials()
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Failed to generate shell credentials",
		}, nil
	}

	env := []string{
		fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
		fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		fmt.Sprintf("USERNAME=%s", username),
		fmt.Sprintf("PASSWORD=%s", password),
	}

	env = append(secrets, env...)

	gpuRequest := types.GpuTypesToStrings(stubConfig.Runtime.Gpus)
	if stubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, stubConfig.Runtime.Gpu.String())
	}

	gpuCount := stubConfig.Runtime.GpuCount
	if stubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	entryPoint := StandaloneEntryPoint()

	if err := SetInitialContainerTTL(ctx, ss.rdb, containerId); err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Failed to set shell container ttl",
		}, nil
	}

	runRequest := &types.ContainerRequest{
		ContainerId:      containerId,
		Env:              env,
		Cpu:              stubConfig.Runtime.Cpu,
		Memory:           stubConfig.Runtime.Memory,
		GpuRequest:       gpuRequest,
		GpuCount:         uint32(gpuCount),
		ImageId:          stubConfig.Runtime.ImageId,
		StubId:           stub.ExternalId,
		AppId:            "",
		WorkspaceId:      authInfo.Workspace.ExternalId,
		Workspace:        *authInfo.Workspace,
		EntryPoint:       entryPoint,
		Mounts:           mounts,
		Stub:             *stub,
		PoolSelector:     stubConfig.PoolSelector(),
		AllowMarketplace: stubConfig.AllowMarketplace,
		MachineId:        stubConfig.MachineID,
	}
	if stub.App != nil {
		runRequest.AppId = stub.App.ExternalId
	}
	if machineWorker != nil {
		runRequest.Cpu = max(machineWorker.FreeCpu, defaultContainerCpu)
		runRequest.Memory = max(machineWorker.FreeMemory, defaultContainerMemory)
		if machineWorker.FreeGpuCount > 0 && machineWorker.Gpu != "" {
			runRequest.GpuRequest = []string{machineWorker.Gpu}
			runRequest.GpuCount = machineWorker.FreeGpuCount
		} else {
			runRequest.GpuRequest = nil
			runRequest.GpuCount = 0
		}
	}
	if err := abstractions.ConfigureContainerRequestNetwork(runRequest, stubConfig); err != nil {
		_ = ClearContainerTTL(ctx, ss.rdb, containerId)
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	err = ss.scheduler.Run(runRequest)
	if err != nil {
		_ = ClearContainerTTL(ctx, ss.rdb, containerId)
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Failed to run shell container",
		}, nil
	}

	err = ss.waitForContainer(ctx, containerId, containerWaitTimeoutDurationS)
	if err != nil {
		_ = ss.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: containerId,
			Force:       true,
			Reason:      types.StopContainerReasonTtl,
		})
		_ = ClearContainerTTL(context.Background(), ss.rdb, containerId)

		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to wait for shell container: %v", err),
		}, nil
	}

	if !ss.waitForContainerSSHServer(ctx, containerId) {
		_ = ss.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: containerId,
			Force:       true,
			Reason:      types.StopContainerReasonTtl,
		})
		_ = ClearContainerTTL(context.Background(), ss.rdb, containerId)
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Shell SSH server did not become ready",
		}, nil
	}
	if err := RefreshPendingContainerTTL(ctx, ss.rdb, containerId); err != nil {
		_ = ss.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: containerId,
			Force:       true,
			Reason:      types.StopContainerReasonTtl,
		})
		_ = ClearContainerTTL(context.Background(), ss.rdb, containerId)
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Shell lease expired before connection",
		}, nil
	}

	return &pb.CreateStandaloneShellResponse{
		Ok:          true,
		ContainerId: containerId,
		Username:    username,
		Password:    password,
	}, nil
}

func (ss *SSHShellService) genContainerId(stubId string) string {
	return ContainerIDForStub(stubId)
}

// The helpers below let the gateway provision shells outside this service
// (marketplace rental shells). Containers created with this prefix and TTL
// key are lifecycle-managed by the running SSHShellService instance exactly
// like CreateStandaloneShell containers.

func ContainerIDForStub(stubId string) string {
	return fmt.Sprintf("%s-%s-%s", shellContainerPrefix, stubId, uuid.New().String()[:8])
}

func IsStandaloneContainer(containerId string) bool {
	return strings.HasPrefix(containerId, shellContainerPrefix+"-")
}

// StandaloneEntryPoint is the dropbear-based startup used by standalone
// shells: provision the SSH user, then run the SSH server in the foreground.
func StandaloneEntryPoint() []string {
	return []string{
		"/bin/sh",
		"-c",
		fmt.Sprintf("%s && %s", createUserScript, fmt.Sprintf(startupScript, types.WorkerShellPort)),
	}
}

// SetInitialContainerTTL marks a shell container as pending; the shell
// service stops the container when the TTL lapses without a client keepalive.
func SetInitialContainerTTL(ctx context.Context, rdb *common.RedisClient, containerId string) error {
	return rdb.Set(ctx, Keys.shellContainerTTL(containerId), "1", initialShellTTL).Err()
}

var ErrShellLeaseExpired = errors.New("shell lease no longer exists")

func extendContainerTTL(
	ctx context.Context,
	rdb *common.RedisClient,
	containerId string,
	ttl time.Duration,
) error {
	refreshed, err := rdb.Expire(
		ctx,
		Keys.shellContainerTTL(containerId),
		ttl,
	).Result()
	if err != nil {
		return err
	}
	if !refreshed {
		return ErrShellLeaseExpired
	}
	return nil
}

// RefreshPendingContainerTTL grants time for a user to run a returned shell
// command. The shorter connected lease starts only once the HTTP tunnel opens.
func RefreshPendingContainerTTL(ctx context.Context, rdb *common.RedisClient, containerId string) error {
	return extendContainerTTL(ctx, rdb, containerId, pendingShellTTL)
}

// RefreshContainerTTL extends an existing connected-shell lease without
// resurrecting a lease that has already expired.
func RefreshContainerTTL(ctx context.Context, rdb *common.RedisClient, containerId string) error {
	return extendContainerTTL(
		ctx,
		rdb,
		containerId,
		time.Duration(shellContainerTtlS)*time.Second,
	)
}

func ClearContainerTTL(ctx context.Context, rdb *common.RedisClient, containerId string) error {
	return rdb.Del(ctx, Keys.shellContainerTTL(containerId)).Err()
}

// GenerateShellCredentials returns credentials that are independent from the
// caller's API token. A compromised or replaced SSH daemon must never receive
// a reusable Beam/Beta9 bearer token during password authentication.
func GenerateShellCredentials() (string, string, error) {
	usernameSuffix, err := generateToken(14)
	if err != nil {
		return "", "", err
	}
	password, err := generateToken(32)
	if err != nil {
		return "", "", err
	}
	return "b9" + strings.ToLower(usernameSuffix), password, nil
}

func (ss *SSHShellService) keepAlive(ctx context.Context, containerId string, done <-chan struct{}) {
	ticker := time.NewTicker(containerKeepAliveIntervalS)
	defer ticker.Stop()

	for {
		select {
		case <-ss.ctx.Done():
			return
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			if err := RefreshContainerTTL(ctx, ss.rdb, containerId); err != nil {
				log.Error().Err(err).Str("container_id", containerId).Msg("failed to refresh shell TTL")
				if errors.Is(err, ErrShellLeaseExpired) {
					return
				}
			}
		}
	}
}

func (ss *SSHShellService) waitForContainer(ctx context.Context, containerId string, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(containerWaitPollIntervalS)
	defer ticker.Stop()

	for {
		containerState, err := ss.containerRepo.GetContainerState(containerId)
		if err != nil {
			return err
		}
		if containerState == nil {
			return fmt.Errorf("container state disappeared")
		}
		if containerState.Status == types.ContainerStatusRunning {
			return nil
		}
		if containerState.Status == types.ContainerStatusStopping {
			return fmt.Errorf("container stopped before becoming available")
		}

		select {
		case <-ss.ctx.Done():
			return fmt.Errorf("shell service stopped while waiting for container")
		case <-timeoutCtx.Done():
			return fmt.Errorf("timed out waiting for container to be available")
		case <-ticker.C:
		}
	}
}

func generateToken(length int) (string, error) {
	byteLength := (length*6 + 7) / 8 // Calculate the number of bytes needed

	randomBytes := make([]byte, byteLength)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	token := base64.RawURLEncoding.EncodeToString(randomBytes)
	return token[:length], nil
}

// Redis keys
var (
	shellContainerTTL string = "shell:container_ttl:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) shellContainerTTL(containerId string) string {
	return fmt.Sprintf(shellContainerTTL, containerId)
}
