package main

import (
	"log"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
)

func main() {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		panic(err)
	}
	config := configManager.GetConfig()

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Worker"))
	if err != nil {
		panic(err)
	}

	workerRepo := repository.NewWorkerRedisRepository(redisClient, config.Worker)

	workerId := "worker-test-client"

	imageClient, err := worker.NewImageClient(config.ImageService, workerId, workerRepo)
	if err != nil {
		panic(err)
	}
	err = imageClient.PullLazy("e4e8cdd66bd321e2")
	log.Println(err)
}
