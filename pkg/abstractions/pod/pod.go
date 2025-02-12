package pod

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type PodServiceOpts struct {
	Config        types.AppConfig
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Tailscale     *network.Tailscale
	Scheduler     *scheduler.Scheduler
	RedisClient   *common.RedisClient
	EventRepo     repository.EventRepository
}

const (
	podContainerPrefix string = "pod"
	podRoutePrefix     string = "/pod"
)

type PodService interface {
	pb.PodServiceServer
	CreatePod(ctx context.Context, in *pb.CreatePodRequest) (*pb.CreatePodResponse, error)
}

type GenericPodService struct {
	pb.PodServiceServer
	ctx             context.Context
	config          types.AppConfig
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	tailscale       *network.Tailscale
	eventRepo       repository.EventRepository
	controller      *abstractions.InstanceController
	podInstances    *common.SafeMap[*podInstance]
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
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
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

	return ps, nil
}

func (ps *GenericPodService) InstanceFactory(ctx context.Context, stubId string, options ...func(abstractions.IAutoscaledInstance)) (abstractions.IAutoscaledInstance, error) {
	return ps.getOrCreatePodInstance(stubId)
}

func (ps *GenericPodService) getOrCreatePodInstance(stubId string, options ...func(*podInstance)) (*podInstance, error) {
	instance, exists := ps.podInstances.Get(stubId)
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

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// Set all options on the instance
	for _, o := range options {
		o(instance)
	}

	if instance.Autoscaler == nil {
		if stub.Type.IsDeployment() || stub.Type == types.StubType(types.StubTypeTaskQueue) {
			instance.Autoscaler = abstractions.NewAutoscaler(instance, podAutoscalerSampleFunc, podScaleFunc)
		}
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

// CreatePod creates a new container that will run to completion, with an associated task
func (s *GenericPodService) CreatePod(ctx context.Context, in *pb.CreatePodRequest) (*pb.CreatePodResponse, error) {
	log.Println("CreatePod", in)
	return &pb.CreatePodResponse{
		PodId: "123",
	}, nil
}
