package abstractions

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"k8s.io/utils/ptr"
)

type Controller struct {
	ctx                 context.Context
	getOrCreateInstance func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)
	StubTypes           []string
	backendRepo         repository.BackendRepository
	redisClient         *common.RedisClient
}

func NewController(
	ctx context.Context,
	getOrCreateInstance func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error),
	stubTypes []string,
	backendRepo repository.BackendRepository,
	redisClient *common.RedisClient,
) *Controller {
	return &Controller{
		ctx:                 ctx,
		getOrCreateInstance: getOrCreateInstance,
		StubTypes:           stubTypes,
		backendRepo:         backendRepo,
		redisClient:         redisClient,
	}
}

func (c *Controller) Init() error {
	eventBus := common.NewEventBus(
		c.redisClient,
		common.EventBusSubscriber{Type: common.EventTypeReloadInstance, Callback: func(e *common.Event) bool {
			stubId := e.Args["stub_id"].(string)
			stubType := e.Args["stub_type"].(string)

			correctStub := false
			for _, t := range c.StubTypes {
				if t == stubType {
					correctStub = true
					break
				}
			}

			if !correctStub {
				return false
			}

			if err := c.reload(stubId, stubType); err != nil {
				return false
			}

			return true
		}},
	)
	go eventBus.ReceiveEvents(c.ctx)

	if err := c.load(); err != nil {
		return err
	}

	return nil
}

func (c *Controller) Warmup(
	ctx context.Context,
	stubId string,
) error {
	instance, err := c.getOrCreateInstance(ctx, stubId)
	if err != nil {
		return err
	}

	return instance.HandleScalingEvent(1)
}

func (c *Controller) load() error {
	stubs, err := c.backendRepo.ListDeploymentsWithRelated(
		c.ctx,
		types.DeploymentFilter{
			StubType:         c.StubTypes,
			MinContainersGTE: 1,
			Active:           ptr.To(true),
		},
	)
	if err != nil {
		return err
	}

	for _, stub := range stubs {
		_, err := c.getOrCreateInstance(c.ctx, stub.Stub.ExternalId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) reload(stubId, stubType string) error {
	instance, err := c.getOrCreateInstance(c.ctx, stubId)
	if err != nil {
		return err
	}

	instance.Reload()

	return nil
}
