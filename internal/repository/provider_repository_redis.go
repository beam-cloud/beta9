package repository

import (
	"github.com/beam-cloud/beta9/internal/common"
)

type ProviderRedisRepository struct {
	rdb         *common.RedisClient
	lock        *common.RedisLock
	lockOptions common.RedisLockOptions
}

func NewProviderRedisRepository(rdb *common.RedisClient) ProviderRepository {
	lock := common.NewRedisLock(rdb)
	lockOptions := common.RedisLockOptions{TtlS: 10, Retries: 0}
	return &ProviderRedisRepository{rdb: rdb, lock: lock, lockOptions: lockOptions}
}

func (r *ProviderRedisRepository) ListMachines(providerName string) error {
	// pool, err := r.GetPool(name)
	// if err != nil {
	// 	return err
	// }

	// log.Printf("getting machines for pool: %+v\n", pool)
	return nil
}

func (r *ProviderRedisRepository) GetMachine(providerName, machineId string) error {
	return nil
}

func (r *ProviderRedisRepository) SetMachine(providerName, machineId string) error {
	return nil
}
