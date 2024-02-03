package repository

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
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
	return nil
}

func (r *ProviderRedisRepository) GetMachine(providerName, machineId string) error {
	return nil
}

func (r *ProviderRedisRepository) RegisterMachine(providerName, poolName, machineId string, info *types.ProviderMachineState) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	err := r.rdb.HSet(context.TODO(), stateKey, "machine_id", machineId, "token", info.Token, "hostname", info.HostName).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}
	return nil
}
