package repository

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beam/internal/common"
)

func NewRedisClientForTest() (*common.RedisClient, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	rdb, err := common.NewRedisClient(common.WithAddress(s.Addr()))
	if err != nil {
		return nil, err
	}

	return rdb, nil
}

func NewWorkerRedisRepositoryForTest(rdb *common.RedisClient) WorkerRepository {
	lock := common.NewRedisLock(rdb)
	return &WorkerRedisRepository{rdb: rdb, lock: lock}
}

func NewContainerRedisRepositoryForTest(rdb *common.RedisClient) ContainerRepository {
	lock := common.NewRedisLock(rdb)
	return &ContainerRedisRepository{rdb: rdb, lock: lock}
}

func NewMetricsStatsdRepositoryForTest() MetricsStatsdRepository {
	return &MetricsStatsd{
		statSender: common.InitStatsdSender(
			"test:8125",
		),
	}
}
