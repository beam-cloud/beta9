package abstractions

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerEventManager struct {
	ctx               context.Context
	containerPrefixes []string
	keyEventChans     map[string]chan common.KeyEvent
	keyEventManager   *common.KeyEventManager
	instanceFactory   func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)
}

func NewContainerEventManager(ctx context.Context, containerPrefixes []string, keyEventManager *common.KeyEventManager, instanceFactory func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)) (*ContainerEventManager, error) {
	keyEventChans := make(map[string]chan common.KeyEvent)
	for _, prefix := range containerPrefixes {
		keyEventChans[prefix] = make(chan common.KeyEvent)
	}

	return &ContainerEventManager{
		ctx:               ctx,
		containerPrefixes: containerPrefixes,
		keyEventChans:     keyEventChans,
		instanceFactory:   instanceFactory,
		keyEventManager:   keyEventManager,
	}, nil
}

func (em *ContainerEventManager) Listen() {
	for _, prefix := range em.containerPrefixes {
		go em.keyEventManager.ListenForPattern(em.ctx, common.RedisKeys.SchedulerContainerState(prefix), em.keyEventChans[prefix])
		go em.handleContainerEventsForPrefix(em.ctx, prefix)
	}
}

func (em *ContainerEventManager) handleContainerEventsForPrefix(ctx context.Context, prefix string) {
	keyEventChan := em.keyEventChans[prefix]

	for {
		select {
		case event := <-keyEventChan:
			operation := event.Operation

			/*
				Container IDs are formatted like so:

					endpoint-6f073820-3d2f-483c-8089-0a30862c3145-80fd7e36

				containerPrefixes contains the first portion of this string, in the above example "endpoint"
				This portion "6f073820-3d2f-483c-8089-0a30862c3145" is the stub ID, and the final "80fd7e36"
				is a UUID specific to this container.

				Because we listen for keyspace notifications on certain container prefixes, actual events
				come in like:

					{Key:-6f073820-3d2f-483c-8089-0a30862c3145-80fd7e36 Operation:hset}

				So what we are doing here is reconstructing the containerId using a known prefix, and then parsing
				out the stubId.
			*/

			// Reconstruct the container ID using the known prefix
			containerId := fmt.Sprintf("%s%s", prefix, event.Key)
			containerIdParts := strings.Split(containerId, "-")

			// Check if this produces a valid container ID structure
			// We expect: prefix-uuid-uuid-uuid-uuid-uuid-container-uuid
			// So we need at least 7 parts: [prefix, uuid1, uuid2, uuid3, uuid4, uuid5, container-uuid]
			if len(containerIdParts) < 7 {
				continue
			}

			// Verify the first part matches our prefix
			if containerIdParts[0] != prefix {
				continue
			}

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
