package pod

import (
	"context"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/rs/zerolog/log"
)

// keepWarmLockDeletes is the containers that are gone and whose keep-warm
// locks are still to be deleted.
type keepWarmLockDeletes struct {
	mu      sync.Mutex
	pending []string
	running bool
}

// deleteKeepWarmLock removes a container's keep-warm lock once the container
// is gone. A sandbox or VM that never expires stores its lock without a TTL,
// so the lock would otherwise stay in Redis forever.
//
// The caller hears of every container event of its kind, one at a time, so
// the lock is deleted apart from it.
func (i *podInstance) deleteKeepWarmLock(containerId string) {
	if i.ContainerRepo == nil || i.Workspace == nil || i.Stub == nil || containerId == "" {
		return
	}

	deletes := &i.lockDeletes
	deletes.mu.Lock()
	deletes.pending = append(deletes.pending, containerId)
	start := !deletes.running
	deletes.running = true
	deletes.mu.Unlock()

	if start {
		go i.deleteKeepWarmLocks()
	}
}

// deleteKeepWarmLocks deletes the locks of the containers that are gone until
// none is left.
func (i *podInstance) deleteKeepWarmLocks() {
	deletes := &i.lockDeletes
	for {
		deletes.mu.Lock()
		containerIds := deletes.pending
		deletes.pending = nil
		if len(containerIds) == 0 {
			deletes.running = false
			deletes.mu.Unlock()
			return
		}
		deletes.mu.Unlock()

		for _, containerId := range containerIds {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			// Zero seconds deletes the lock.
			setPodKeepWarmLock(ctx, i.ContainerRepo, i.Workspace.Name, i.Stub.ExternalId, containerId, 0)
			cancel()
		}
	}
}

func setPodKeepWarmLock(ctx context.Context, containerRepo repository.ContainerRepository, workspaceName, stubId, containerId string, keepWarmSeconds int) {
	if containerRepo == nil {
		return
	}

	if err := containerRepo.SetPodKeepWarmLock(ctx, workspaceName, stubId, containerId, keepWarmSeconds); err != nil {
		log.Error().
			Err(err).
			Str("stub_id", stubId).
			Str("container_id", containerId).
			Int("keep_warm_seconds", keepWarmSeconds).
			Msg("failed to update pod keep-warm lock")
	}
}
