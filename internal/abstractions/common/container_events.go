package abstractions

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

type ContainerInstanceManager interface {
	CreateInstance(stubId string) error
	RetrieveInstance(stubId string) interface{}
}

func NewInstanceContainerEventManager(containerPrefix string, keyEventManager *common.KeyEventManager, instanceFactory func(stubId string, options ...func(*interface{})) error) (*InstanceContainerEventManager, error) {
	keyEventChan := make(chan common.KeyEvent)

	return &InstanceContainerEventManager{
		containerPrefix: containerPrefix,
		instanceFactory: instanceFactory,
		keyEventChan:    keyEventChan,
		keyEventManager: keyEventManager,
	}, nil
}

type InstanceContainerEventManager struct {
	ctx             context.Context
	containerPrefix string
	keyEventChan    chan common.KeyEvent
	keyEventManager *common.KeyEventManager
	instanceFactory func(stubId string, options ...func(*interface{})) error
}

func (em *InstanceContainerEventManager) ConsumeEvents() {
	go em.keyEventManager.ListenForPattern(em.ctx, common.RedisKeys.SchedulerContainerState(em.containerPrefix), em.keyEventChan)
	go em.handleContainerEvents()
}

func (em *InstanceContainerEventManager) handleContainerEvents() {
	for {
		select {
		case event := <-em.keyEventChan:
			containerId := fmt.Sprintf("%s%s", em.containerPrefix, event.Key)

			operation := event.Operation
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:6], "-")

			instance, err := em.instanceFactory(stubId)
			if err != nil {
				continue
			}

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				instance.ContainerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				}
			case common.KeyOperationDel, common.KeyOperationExpired:
				instance.ContainerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      -1,
				}
			}

		case <-em.ctx.Done():
			return
		}
	}
}

func parseContainerIdFromEvent(string) (string, error) {
	return "", nil
}
