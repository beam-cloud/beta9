package shell

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"

	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	pb "github.com/beam-cloud/beta9/proto"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
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
	containerDialTimeoutDurationS time.Duration = 300 * time.Second
	containerWaitTimeoutDurationS time.Duration = 5 * time.Minute
	containerWaitPollIntervalS    time.Duration = 1 * time.Second
	containerKeepAliveIntervalS   time.Duration = 5 * time.Second
	sshBannerTimeoutDurationS     time.Duration = 2 * time.Second
	startupScript                 string        = `SHELL=$(ls /bin/bash || ls /bin/sh); /usr/local/bin/dropbear -e -c "cd /mnt/code && $SHELL" -p %d -R -E -F 2>> /etc/dropbear/logs.txt`
	createUserScript              string        = `SHELL=$(ls /bin/bash || ls /bin/sh); \
(command -v useradd >/dev/null && useradd -m -s $SHELL "$USERNAME" 2>> /etc/dropbear/logs.txt) || \
(command -v adduser >/dev/null && adduser --disabled-password --gecos "" --shell $SHELL "$USERNAME" 2>> /etc/dropbear/logs.txt) || \
(echo "$USERNAME:x:1000:1000:$USERNAME:/home/$USERNAME:$SHELL" >> /etc/passwd && mkdir -p "/home/$USERNAME" && chown 1000:1000 "/home/$USERNAME") && \
echo "$USERNAME:$PASSWORD" | chpasswd 2>> /etc/dropbear/logs.txt`
)

type ShellService interface {
	pb.ShellServiceServer
	CreateStandaloneShell(ctx context.Context, in *pb.CreateStandaloneShellRequest) (*pb.CreateStandaloneShellResponse, error)
	CreateShellInExistingContainer(ctx context.Context, in *pb.CreateShellInExistingContainerRequest) (*pb.CreateShellInExistingContainerResponse, error)
}

type SSHShellService struct {
	pb.UnimplementedShellServiceServer
	ctx             context.Context
	config          types.AppConfig
	rdb             *common.RedisClient
	keyEventManager *common.KeyEventManager
	scheduler       *scheduler.Scheduler
	backendRepo     repository.BackendRepository
	workspaceRepo   repository.WorkspaceRepository
	containerRepo   repository.ContainerRepository
	eventRepo       repository.EventRepository
	tailscale       *network.Tailscale
	keyEventChan    chan common.KeyEvent
}

type ShellServiceOpts struct {
	Config        types.AppConfig
	RedisClient   *common.RedisClient
	BackendRepo   repository.BackendRepository
	WorkspaceRepo repository.WorkspaceRepository
	ContainerRepo repository.ContainerRepository
	Scheduler     *scheduler.Scheduler
	RouteGroup    *echo.Group
	Tailscale     *network.Tailscale
	EventRepo     repository.EventRepository
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
		ctx:             ctx,
		config:          opts.Config,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		scheduler:       opts.Scheduler,
		backendRepo:     opts.BackendRepo,
		workspaceRepo:   opts.WorkspaceRepo,
		containerRepo:   opts.ContainerRepo,
		tailscale:       opts.Tailscale,
		eventRepo:       opts.EventRepo,
		keyEventChan:    make(chan common.KeyEvent),
	}

	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	registerShellRoutes(opts.RouteGroup.Group(shellRoutePrefix, authMiddleware), ss)

	// Listen for shell container ttl events
	go ss.keyEventManager.ListenForPattern(ss.ctx, Keys.shellContainerTTL("*"), ss.keyEventChan)
	go ss.keyEventManager.ListenForPattern(ss.ctx, common.RedisKeys.SchedulerContainerState(shellContainerPrefix), ss.keyEventChan)
	go ss.handleTTLEvents()

	return ss, nil
}

func (ss *SSHShellService) handleTTLEvents() {
	for {
		select {
		case event := <-ss.keyEventChan:
			operation := event.Operation
			switch operation {
			case common.KeyOperationSet:
				// Clean up shell containers that have expired, but a gateway wasn't around to handle the ttl event
				// NOTE: the reason this checks for the shellContainerTTL is the prefixed is stripped in the key event manager
				// when fetching pre-existing keys.
				if !strings.Contains(event.Key, Keys.shellContainerTTL("")) {
					containerId := shellContainerPrefix + event.Key

					if ss.rdb.Exists(ss.ctx, Keys.shellContainerTTL(containerId)).Val() == 0 {
						ss.scheduler.Stop(&types.StopContainerArgs{
							ContainerId: containerId,
							Force:       true,
							Reason:      types.StopContainerReasonTtl,
						})
					}
				}
			case common.KeyOperationHSet, common.KeyOperationDel, common.KeyOperationExpire:
				// Do nothing
			case common.KeyOperationExpired:
				// Clean up shell containers that have been expired
				containerId := strings.TrimPrefix(ss.keyEventManager.TrimKeyspacePrefix(event.Key), Keys.shellContainerTTL(""))
				ss.scheduler.Stop(&types.StopContainerArgs{
					ContainerId: containerId,
					Force:       true,
					Reason:      types.StopContainerReasonTtl,
				})
			}
		case <-ss.ctx.Done():
			return
		}
	}
}

func (ss *SSHShellService) checkForExistingSSHServer(ctx context.Context, containerId string) bool {
	addressMap, err := ss.containerRepo.GetContainerAddressMap(containerId)
	if err != nil {
		return false
	}

	addr, ok := addressMap[types.WorkerShellPort]
	if !ok {
		return false
	}

	conn, err := network.ConnectToHost(ctx, addr, time.Second*30, ss.tailscale, ss.config.Tailscale)
	if err != nil {
		return false
	}

	// Set read timeout so it doesn't hang forever
	conn.SetReadDeadline(time.Now().Add(sshBannerTimeoutDurationS))

	// Read SSH banner line by line
	// This check partially implements RFC 4253 Section 4.2 of the SSH protocol handshake
	buf := make([]byte, 256)
	for {
		// Clear buffer
		for i := range buf {
			buf[i] = 0
		}

		// Read one line
		n, err := conn.Read(buf)
		if err != nil {
			return false
		}

		// Convert \r to \n if present
		line := string(buf[:n])
		line = strings.ReplaceAll(line, "\r", "\n")

		// Check if this line contains the SSH banner
		if strings.HasPrefix(line, "SSH-") {
			return true
		}

		// If we've read enough lines, give up
		if n == 0 {
			return false
		}
	}
}

func (ss *SSHShellService) getOrCreateSSHUser(ctx context.Context, containerId string, client *common.RunCClient) (string, string, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	username, password := ss.generateUsernamePassword(*authInfo.Token)

	_, err := client.Exec(containerId, "id $USERNAME && exit 0 ;"+createUserScript, []string{fmt.Sprintf("USERNAME=%s", username), fmt.Sprintf("PASSWORD=%s", password)})
	if err != nil {
		return "", "", err
	}

	return username, password, nil
}

func (ss *SSHShellService) CreateShellInExistingContainer(ctx context.Context, in *pb.CreateShellInExistingContainerRequest) (*pb.CreateShellInExistingContainerResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	containerId := in.ContainerId

	containerState, err := ss.containerRepo.GetContainerState(containerId)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to get container state: %s", err),
		}, nil
	}

	if containerState.Status != types.ContainerStatusRunning {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Container is not running",
		}, nil
	}

	stub, err := ss.backendRepo.GetStubByExternalId(ctx, containerState.StubId)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to get stub: %s", err),
		}, nil
	}

	if stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: "Container not found",
		}, nil
	}

	containerAddr, err := ss.containerRepo.GetWorkerAddress(ctx, containerId)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to get container address: %s", err),
		}, nil
	}

	conn, err := network.ConnectToHost(ctx, containerAddr, time.Second*30, ss.tailscale, ss.config.Tailscale)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to connect to container: %s", err),
		}, nil
	}

	runcClient, err := common.NewRunCClient(containerAddr, authInfo.Token.Key, conn)
	if err != nil {
		return &pb.CreateShellInExistingContainerResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Failed to create runc client: %s", err),
		}, nil
	}

	ok := ss.checkForExistingSSHServer(ctx, containerAddr)
	if !ok {
		go func() {
			// This only dies if the container is stopped
			_, err = runcClient.Exec(containerId, fmt.Sprintf(startupScript, types.WorkerShellPort), []string{})
			if err != nil {
				log.Error().Msgf("Failed to execute startup script: %v", err)
			}
		}()
	}

	username, password, err := ss.getOrCreateSSHUser(ctx, containerId, runcClient)
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

	stub, err := ss.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok: false,
		}, nil
	}

	go ss.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok: false,
		}, nil
	}

	containerId := ss.genContainerId(stub.ExternalId)

	// Don't allow negative and 0-valued compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultContainerMemory
	}

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		containerId,
		stub.Object.ExternalId,
		authInfo.Workspace,
		stubConfig,
		stub.ExternalId,
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

	username, password := ss.generateUsernamePassword(*authInfo.Token)

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

	startupCommand := fmt.Sprintf("%s && %s", createUserScript, fmt.Sprintf(startupScript, types.WorkerShellPort))
	entryPoint := []string{
		"/bin/sh",
		"-c",
		startupCommand,
	}

	err = ss.rdb.Set(ctx, Keys.shellContainerTTL(containerId), "1", containerWaitTimeoutDurationS).Err()
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Failed to set shell container ttl",
		}, nil
	}

	err = ss.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env:         env,
		Cpu:         stubConfig.Runtime.Cpu,
		Memory:      stubConfig.Runtime.Memory,
		GpuRequest:  gpuRequest,
		GpuCount:    uint32(gpuCount),
		ImageId:     stubConfig.Runtime.ImageId,
		StubId:      stub.ExternalId,
		AppId:       stub.App.ExternalId,
		WorkspaceId: authInfo.Workspace.ExternalId,
		Workspace:   *authInfo.Workspace,
		EntryPoint:  entryPoint,
		Mounts:      mounts,
		Stub:        *stub,
	})
	if err != nil {
		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Failed to run shell container",
		}, nil
	}

	err = ss.waitForContainer(ctx, containerId, containerWaitTimeoutDurationS)
	if err != nil {
		ss.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: containerId,
			Force:       true,
			Reason:      types.StopContainerReasonTtl,
		})

		return &pb.CreateStandaloneShellResponse{
			Ok:     false,
			ErrMsg: "Failed to wait for shell container",
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
	return fmt.Sprintf("%s-%s-%s", shellContainerPrefix, stubId, uuid.New().String()[:8])
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
			ss.rdb.Set(ctx, Keys.shellContainerTTL(containerId), "1", time.Duration(shellContainerTtlS)*time.Second).Err()
		}
	}
}

func (ss *SSHShellService) waitForContainer(ctx context.Context, containerId string, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ss.ctx.Done():
			return nil
		case <-timeoutCtx.Done():
			return fmt.Errorf("timed out waiting for container to be available")
		default:
			containerState, err := ss.containerRepo.GetContainerState(containerId)
			if err != nil {
				return err
			}

			if containerState.Status == types.ContainerStatusRunning {
				return nil
			}

			time.Sleep(containerWaitPollIntervalS)
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

	token := base64.URLEncoding.EncodeToString(randomBytes)
	return token[:length], nil
}

func (ss *SSHShellService) generateUsernamePassword(token types.Token) (string, string) {
	return strings.Join(strings.Split(token.ExternalId, "-"), "")[:6], token.Key
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
