package abstractions

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerEventManager struct {
	ctx             context.Context
	containerPrefix string
	keyEventChan    chan common.KeyEvent
	keyEventManager *common.KeyEventManager
	instanceFactory func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)
}

func NewContainerEventManager(ctx context.Context, containerPrefix string, keyEventManager *common.KeyEventManager, instanceFactory func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)) (*ContainerEventManager, error) {
	keyEventChan := make(chan common.KeyEvent)

	return &ContainerEventManager{
		ctx:             ctx,
		containerPrefix: containerPrefix,
		instanceFactory: instanceFactory,
		keyEventChan:    keyEventChan,
		keyEventManager: keyEventManager,
	}, nil
}

func (em *ContainerEventManager) Listen() {
	go em.keyEventManager.ListenForPattern(em.ctx, common.RedisKeys.SchedulerContainerState(em.containerPrefix), em.keyEventChan)
	go em.handleContainerEvents(em.ctx)
}

func (em *ContainerEventManager) handleContainerEvents(ctx context.Context) {
	for {
		select {
		case event := <-em.keyEventChan:
			operation := event.Operation

			/*
				Container IDs are formatted like so:

					endpoint-6f073820-3d2f-483c-8089-0a30862c3145-80fd7e36

				containerPrefix is the first portion of this string, in the above example "endpoint"
				This portion "6f073820-3d2f-483c-8089-0a30862c3145" is the stub ID, and the final "80fd7e36"
				is a UUID specific to this container.

				Because we listen for keyspace notifications on a certain container prefix, actual events
				come in like:

					{Key:-6f073820-3d2f-483c-8089-0a30862c3145-80fd7e36 Operation:hset}

				So what we are doing here is reconstructing the containerId using a known prefix, and then parsing
				out the stubId.
			*/
			containerId := fmt.Sprintf("%s%s", em.containerPrefix, event.Key)
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:6], "-")

			instance, err := em.instanceFactory(em.ctx, stubId)
			if err != nil {
				continue
			}

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				instance.ConsumeContainerEvent(types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				})
			case common.KeyOperationDel, common.KeyOperationExpired:
				instance.ConsumeContainerEvent(types.ContainerEvent{
					ContainerId: containerId,
					Change:      -1,
				})
			}

		case <-ctx.Done():
			return
		}
	}
}
