package pod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type PodServiceOpts struct {
	Config        types.AppConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	WorkspaceRepo repository.WorkspaceRepository
	Tailscale     *network.Tailscale
	Scheduler     *scheduler.Scheduler
	RedisClient   *common.RedisClient
	EventRepo     repository.EventRepository
	RouteGroup    *echo.Group
}

const (
	podContainerPrefix            string = "pod"
	sandboxContainerPrefix        string = "sandbox"
	podRoutePrefix                string = "/pod"
	sandboxRoutePrefix            string = "/sandbox"
	podContainerConnectionTimeout        = 600 * time.Second
	podProxyBufferSize                   = 300
)

type PodService interface {
	pb.PodServiceServer
	CreatePod(ctx context.Context, in *pb.CreatePodRequest) (*pb.CreatePodResponse, error)
}

type GenericPodService struct {
	pb.PodServiceServer
	ctx             context.Context
	mu              sync.Mutex
	config          types.AppConfig
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	workspaceRepo   repository.WorkspaceRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	tailscale       *network.Tailscale
	eventRepo       repository.EventRepository
	controller      *abstractions.InstanceController
	podInstances    *common.SafeMap[*podInstance]
	clientCache     sync.Map
}

func NewPodService(
	ctx context.Context,
	opts PodServiceOpts,
) (PodService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	ps := &GenericPodService{
		ctx:             ctx,
		mu:              sync.Mutex{},
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		workspaceRepo:   opts.WorkspaceRepo,
		scheduler:       opts.Scheduler,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		tailscale:       opts.Tailscale,
		config:          opts.Config,
		eventRepo:       opts.EventRepo,
		podInstances:    common.NewSafeMap[*podInstance](),
	}

	// Listen for container events with a certain prefix
	// For example if a container is created, destroyed, or updated
	eventManager, err := abstractions.NewContainerEventManager(ctx, podContainerPrefix, keyEventManager, ps.InstanceFactory)
	if err != nil {
		return nil, err
	}
	eventManager.Listen()

	// Initialize deployment manager
	ps.controller = abstractions.NewInstanceController(ctx, ps.InstanceFactory, []string{types.StubTypePodDeployment}, opts.BackendRepo, opts.RedisClient)
	err = ps.controller.Init()
	if err != nil {
		return nil, err
	}

	authMiddleware := auth.AuthMiddleware(ps.backendRepo, ps.workspaceRepo)

	registerPodGroup(opts.RouteGroup.Group(podRoutePrefix, authMiddleware), ps)
	registerPodGroup(opts.RouteGroup.Group(sandboxRoutePrefix, authMiddleware), ps)

	return ps, nil
}

func (ps *GenericPodService) InstanceFactory(ctx context.Context, stubId string, options ...func(abstractions.IAutoscaledInstance)) (abstractions.IAutoscaledInstance, error) {
	return ps.getOrCreatePodInstance(stubId)
}

func (ps *GenericPodService) IsPublic(stubId string) (*types.Workspace, error) {
	instance, err := ps.getOrCreatePodInstance(stubId)
	if err != nil {
		return nil, err
	}

	if instance.StubConfig.Authorized {
		return nil, errors.New("unauthorized")
	}

	return instance.Workspace, nil
}

func (ps *GenericPodService) forwardRequest(ctx echo.Context, stubId string) error {
	instance, err := ps.getOrCreatePodInstance(stubId)
	if err != nil {
		return err
	}

	return instance.buffer.ForwardRequest(ctx)
}

func (ps *GenericPodService) getOrCreatePodInstance(stubId string, options ...func(*podInstance)) (*podInstance, error) {
	instance, exists := ps.podInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	// The reason we lock here, and then check again -- is because if the instance does not exist, we may have two separate
	// goroutines trying to create the instance. So, we check first, then get the mutex. If another
	// routine got the lock, it should have created the instance, so we check once again. That way
	// we don't create two instances of the same stub, but we also ensure that we return quickly if the instance
	// _does_ already exist.
	ps.mu.Lock()
	defer ps.mu.Unlock()

	instance, exists = ps.podInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := ps.backendRepo.GetStubByExternalId(ps.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	token, err := ps.backendRepo.RetrieveActiveToken(ps.ctx, stub.Workspace.Id)
	if err != nil {
		return nil, err
	}

	// Create queue instance to hold taskqueue specific methods/fields
	instance = &podInstance{}

	// Create base autoscaled instance
	autoscaledInstance, err := abstractions.NewAutoscaledInstance(ps.ctx, &abstractions.AutoscaledInstanceConfig{
		Name:                fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		AppConfig:           ps.config,
		Rdb:                 ps.rdb,
		Stub:                stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           ps.scheduler,
		ContainerRepo:       ps.containerRepo,
		BackendRepo:         ps.backendRepo,
		EventRepo:           ps.eventRepo,
		InstanceLockKey:     Keys.podInstanceLock(stub.Workspace.Name, stubId),
		StartContainersFunc: instance.startContainers,
		StopContainersFunc:  instance.stopContainers,
	})
	if err != nil {
		return nil, err
	}

	instance.buffer = NewPodProxyBuffer(autoscaledInstance.Ctx, ps.rdb, &stub.Workspace, stubId, podProxyBufferSize, ps.containerRepo, ps.keyEventManager, stubConfig, ps.tailscale, ps.config.Tailscale)

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// Set all options on the instance
	for _, o := range options {
		o(instance)
	}

	if instance.Autoscaler == nil {
		instance.Autoscaler = abstractions.NewAutoscaler(instance, podAutoscalerSampleFunc, podScaleFunc)
	}

	if len(instance.EntryPoint) == 0 {
		instance.EntryPoint = instance.StubConfig.EntryPoint
	}

	ps.podInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Monitor()
	go func(i *podInstance) {
		<-i.Ctx.Done()
		ps.podInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

func (s *GenericPodService) run(ctx context.Context, authInfo *auth.AuthInfo, stub *types.StubWithRelated) (string, error) {
	stubConfig := types.StubConfigV1{}
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return "", err
	}

	secrets, err := abstractions.ConfigureContainerRequestSecrets(authInfo.Workspace, stubConfig)
	if err != nil {
		return "", err
	}

	containerId := s.generateContainerId(stub.ExternalId, stub.Type)

	mounts, err := abstractions.ConfigureContainerRequestMounts(
		containerId,
		stub.Object.ExternalId,
		authInfo.Workspace,
		stubConfig,
		stub.ExternalId,
	)
	if err != nil {
		return "", err
	}

	env := []string{}
	env = append(stubConfig.Env, env...)
	env = append(secrets, env...)
	env = append(env, []string{
		fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		fmt.Sprintf("STUB_TYPE=%s", stub.Type),
		fmt.Sprintf("KEEP_WARM_SECONDS=%d", stubConfig.KeepWarmSeconds),
	}...)

	gpuRequest := types.GpuTypesToStrings(stubConfig.Runtime.Gpus)
	if stubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, stubConfig.Runtime.Gpu.String())
	}

	gpuCount := stubConfig.Runtime.GpuCount
	if stubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	checkpointEnabled := stubConfig.CheckpointEnabled
	if gpuCount > 1 {
		checkpointEnabled = false
	}

	ports := []uint32{}
	if len(stubConfig.Ports) > 0 {
		ports = stubConfig.Ports
	}

	ttl := time.Duration(stubConfig.KeepWarmSeconds) * time.Second
	key := Keys.podKeepWarmLock(authInfo.Workspace.Name, stub.ExternalId, containerId)
	if ttl <= 0 {
		s.rdb.Set(context.Background(), key, 1, 0) // Never expire
	} else {
		s.rdb.SetEx(context.Background(), key, 1, ttl)
	}

	err = s.scheduler.Run(&types.ContainerRequest{
		ContainerId:       containerId,
		StubId:            stub.ExternalId,
		Env:               env,
		Cpu:               stubConfig.Runtime.Cpu,
		Memory:            stubConfig.Runtime.Memory,
		GpuRequest:        gpuRequest,
		GpuCount:          uint32(gpuCount),
		Mounts:            mounts,
		Stub:              *stub,
		ImageId:           stubConfig.Runtime.ImageId,
		WorkspaceId:       authInfo.Workspace.ExternalId,
		Workspace:         *authInfo.Workspace,
		EntryPoint:        stubConfig.EntryPoint,
		Ports:             ports,
		CheckpointEnabled: checkpointEnabled,
	})
	if err != nil {
		return "", err
	}

	go s.eventRepo.PushRunStubEvent(
		authInfo.Workspace.ExternalId,
		&stub.Stub,
	)

	return containerId, nil
}

func (s *GenericPodService) CreatePod(ctx context.Context, in *pb.CreatePodRequest) (*pb.CreatePodResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := s.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return &pb.CreatePodResponse{
			Ok: false,
		}, nil
	}

	containerId, err := s.run(ctx, authInfo, stub)
	if err != nil {
		return &pb.CreatePodResponse{
			Ok: false,
		}, nil
	}

	return &pb.CreatePodResponse{
		Ok:          true,
		ContainerId: containerId,
	}, nil
}

func (s *GenericPodService) generateContainerId(stubId string, stubType types.StubType) string {
	switch string(stubType) {
	case string(types.StubTypeSandbox):
		return fmt.Sprintf("%s-%s-%s", sandboxContainerPrefix, stubId, uuid.New().String()[:8])
	default:
		return fmt.Sprintf("%s-%s-%s", podContainerPrefix, stubId, uuid.New().String()[:8])
	}
}
