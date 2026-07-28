package abstractions

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type ContainerStopper interface {
	Stop(*types.StopContainerArgs) error
}

type ContainerLeaseManager struct {
	redisClient     *common.RedisClient
	stopper         ContainerStopper
	containerPrefix string
	leaseKey        func(string) string
}

func NewContainerLeaseManager(
	redisClient *common.RedisClient,
	stopper ContainerStopper,
	containerPrefix string,
	leaseKey func(string) string,
) *ContainerLeaseManager {
	return &ContainerLeaseManager{
		redisClient:     redisClient,
		stopper:         stopper,
		containerPrefix: containerPrefix,
		leaseKey:        leaseKey,
	}
}

func (m *ContainerLeaseManager) Run(ctx context.Context) error {
	containers := make(chan common.KeyEvent)
	expirations := make(chan common.KeyEvent)
	events := common.NewKeyEventManager(m.redisClient)
	if err := events.ListenForPatternEvents(ctx, m.leaseKey(""), expirations); err != nil {
		return err
	}
	listening := make(chan error, 1)
	go func() {
		listening <- events.ListenForContainerPattern(ctx, m.containerPrefix, containers)
	}()

	for {
		select {
		case event := <-containers:
			if event.Operation == common.KeyOperationSet {
				m.stopIfExpired(ctx, m.containerPrefix+event.Key)
			}
		case event := <-expirations:
			if event.Operation == common.KeyOperationExpired {
				m.stopIfExpired(ctx, event.Key)
			}
		case err := <-listening:
			if err != nil {
				return err
			}
			listening = nil
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *ContainerLeaseManager) stopIfExpired(ctx context.Context, containerId string) {
	exists, err := m.redisClient.Exists(ctx, m.leaseKey(containerId)).Result()
	if err != nil {
		log.Error().Err(err).Str("container_id", containerId).Msg("failed to check container lease")
		return
	}
	if exists != 0 {
		return
	}
	if err = m.stopper.Stop(&types.StopContainerArgs{
		ContainerId: containerId,
		Force:       true,
		Reason:      types.StopContainerReasonTtl,
	}); err != nil {
		log.Error().Err(err).Str("container_id", containerId).Msg("failed to stop expired container")
	}
}
