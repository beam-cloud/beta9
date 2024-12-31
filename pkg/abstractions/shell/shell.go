package shell

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	shellProxyBufferSizeKb        int           = 32 * 1024
	defaultContainerCpu           int64         = 100
	defaultContainerMemory        int64         = 128
	containerDialTimeoutDurationS time.Duration = 300 * time.Second
)

type ShellService interface {
	pb.ShellServiceServer
	CreateShell(ctx context.Context, in *pb.CreateShellRequest) (*pb.CreateShellResponse, error)
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
	}

	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	registerShellRoutes(opts.RouteGroup.Group(shellRoutePrefix, authMiddleware), ss)

	return ss, nil
}

func (ss *SSHShellService) CreateShell(ctx context.Context, in *pb.CreateShellRequest) (*pb.CreateShellResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	keyEventChan := make(chan common.KeyEvent)

	stub, err := ss.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return &pb.CreateShellResponse{
			Ok: false,
		}, nil
	}

	go ss.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return &pb.CreateShellResponse{
			Ok: false,
		}, nil
	}

	containerId := ss.genContainerId(stub.ExternalId)

	go ss.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

	// Don't allow negative and 0-valued compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultContainerMemory
	}

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		stub.Object.ExternalId,
		authInfo.Workspace,
		stubConfig,
		stub.ExternalId,
	)
	if err != nil {
		return &pb.CreateShellResponse{
			Ok: false,
		}, nil
	}

	secrets, err := abstractions.ConfigureContainerRequestSecrets(
		authInfo.Workspace,
		stubConfig,
	)
	if err != nil {
		return &pb.CreateShellResponse{
			Ok: false,
		}, nil
	}

	env := []string{
		fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
		fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
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

	token, err := generateToken(16)
	if err != nil {
		return &pb.CreateShellResponse{
			Ok: false,
		}, nil
	}

	startupCommand := fmt.Sprintf(`
    set -e;
    USERNAME='runc';
    TOKEN='%s';
    useradd -m -s /bin/bash "$USERNAME";
    echo "$USERNAME:$TOKEN" | chpasswd;
    sed -i 's/^#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config;
    sed -i 's/^#PubkeyAuthentication.*/PubkeyAuthentication no/' /etc/ssh/sshd_config;
    echo "AllowUsers $USERNAME" >> /etc/ssh/sshd_config;
    exec /usr/sbin/sshd -D -p 8001
    `, token)

	entryPoint := []string{
		"/bin/bash",
		"-c",
		startupCommand,
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
		WorkspaceId: authInfo.Workspace.ExternalId,
		Workspace:   *authInfo.Workspace,
		EntryPoint:  entryPoint,
		Mounts:      mounts,
		Stub:        *stub,
	})
	if err != nil {
		return &pb.CreateShellResponse{
			Ok: false,
		}, nil
	}

	return &pb.CreateShellResponse{
		Ok:          true,
		ContainerId: containerId,
		Token:       token,
	}, nil
}

func (ss *SSHShellService) genContainerId(stubId string) string {
	return fmt.Sprintf("%s-%s-%s", shellContainerPrefix, stubId, uuid.New().String()[:8])
}

func (ss *SSHShellService) waitForContainerRunning(ctx context.Context, containerId string, delay time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout reached while waiting for container to be running")
		default:
			containerState, err := ss.containerRepo.GetContainerState(containerId)
			if err != nil {
				return err
			}

			if containerState.Status == types.ContainerStatusRunning {
				return nil
			}

			time.Sleep(delay)
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
