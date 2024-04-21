package abstractions

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type ContainerEventManager struct {
	containerPrefix string
	keyEventChan    chan common.KeyEvent
	keyEventManager *common.KeyEventManager
	instanceFactory func(stubId string, options ...func(AbstractionInstance)) (AbstractionInstance, error)
}

func NewContainerEventManager(containerPrefix string, keyEventManager *common.KeyEventManager, instanceFactory func(stubId string, options ...func(AbstractionInstance)) (AbstractionInstance, error)) (*ContainerEventManager, error) {
	keyEventChan := make(chan common.KeyEvent)

	return &ContainerEventManager{
		containerPrefix: containerPrefix,
		instanceFactory: instanceFactory,
		keyEventChan:    keyEventChan,
		keyEventManager: keyEventManager,
	}, nil
}

func (em *ContainerEventManager) ConsumeEvents(ctx context.Context) {
	go em.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(em.containerPrefix), em.keyEventChan)
	go em.handleContainerEvents(ctx)
}

func (em *ContainerEventManager) handleContainerEvents(ctx context.Context) {
	for {
		select {
		case event := <-em.keyEventChan:
			operation := event.Operation

			containerId := fmt.Sprintf("%s%s", em.containerPrefix, event.Key)
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:6], "-")

			instance, err := em.instanceFactory(stubId)
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

func parseContainerIdFromEvent(eventKey string) (string, error) {
	return "", nil
}
