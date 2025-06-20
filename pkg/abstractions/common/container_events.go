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
	keyEventChan      chan common.KeyEvent
	keyEventManager   *common.KeyEventManager
	instanceFactory   func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)
}

func NewContainerEventManager(ctx context.Context, containerPrefixes []string, keyEventManager *common.KeyEventManager, instanceFactory func(ctx context.Context, stubId string, options ...func(IAutoscaledInstance)) (IAutoscaledInstance, error)) (*ContainerEventManager, error) {
	keyEventChan := make(chan common.KeyEvent)

	return &ContainerEventManager{
		ctx:               ctx,
		containerPrefixes: containerPrefixes,
		instanceFactory:   instanceFactory,
		keyEventChan:      keyEventChan,
		keyEventManager:   keyEventManager,
	}, nil
}

func (em *ContainerEventManager) Listen() {
	// Listen for events on all container prefixes
	for _, prefix := range em.containerPrefixes {
		go em.keyEventManager.ListenForPattern(em.ctx, common.RedisKeys.SchedulerContainerState(prefix), em.keyEventChan)
	}

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

				containerPrefixes contains the first portion of this string, in the above example "endpoint"
				This portion "6f073820-3d2f-483c-8089-0a30862c3145" is the stub ID, and the final "80fd7e36"
				is a UUID specific to this container.

				Because we listen for keyspace notifications on certain container prefixes, actual events
				come in like:

					{Key:-6f073820-3d2f-483c-8089-0a30862c3145-80fd7e36 Operation:hset}

				So what we are doing here is reconstructing the containerId using a known prefix, and then parsing
				out the stubId.
			*/

			// Find which prefix this event belongs to
			var containerId string
			var stubId string

			for _, prefix := range em.containerPrefixes {
				if strings.HasPrefix(event.Key, "-") {
					// Event key starts with "-", so we need to reconstruct the full container ID
					containerId = fmt.Sprintf("%s%s", prefix, event.Key)
					containerIdParts := strings.Split(containerId, "-")
					if len(containerIdParts) >= 6 {
						stubId = strings.Join(containerIdParts[1:6], "-")
						break
					}
				}
			}

			// Skip if we couldn't determine the container ID or stub ID
			if containerId == "" || stubId == "" {
				continue
			}

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
