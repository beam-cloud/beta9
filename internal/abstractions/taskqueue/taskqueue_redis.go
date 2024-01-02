package taskqueue

import (
	"log"
	"math/rand"
	"time"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
)

type TaskQueueRedis struct {
	rdb       *common.RedisClient
	scheduler *scheduler.Scheduler
}

func NewTaskQueueRedis(rdb *common.RedisClient, scheduler *scheduler.Scheduler) (*TaskQueueRedis, error) {
	return &TaskQueueRedis{
		rdb:       rdb,
		scheduler: scheduler,
	}, nil
}

/*

Things we need here:
 - a way to spin up containers
 - a way to shut down containers
 - a way to determine how to shut down containers

*/

func (tq *TaskQueueRedis) stopContainers(containersToStop int) error {
	rand.Seed(time.Now().UnixNano())

	containerIds := []string{}

	for i := 0; i < containersToStop && len(containerIds) > 0; i++ {
		// Randomly pick a container from the containerIds slice
		idx := rand.Intn(len(containerIds))
		containerId := containerIds[idx]

		err := tq.scheduler.Stop(containerId)
		if err != nil {
			log.Printf("Unable to stop container: %v", err)
			return err
		}

		// Remove the containerId from the containerIds slice to avoid
		// sending multiple stop requests to the same container
		containerIds = append(containerIds[:idx], containerIds[idx+1:]...)
	}

	return nil
}
