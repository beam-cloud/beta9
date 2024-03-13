package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	redis "github.com/redis/go-redis/v9"
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

func (r *ProviderRedisRepository) GetMachine(providerName, poolName, machineId string) (*types.ProviderMachineState, error) {
	ctx := context.TODO() // TODO: pass context as an argument to GetMachine

	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	res, err := r.rdb.HGetAll(ctx, stateKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("no machine state found for %s", machineId)
		}
		return nil, fmt.Errorf("failed to get machine state for %s: %w", machineId, err)
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("machine state for %s is invalid or empty", machineId)
	}

	state := &types.ProviderMachineState{}
	if err := common.ToStruct(res, state); err != nil {
		return nil, fmt.Errorf("error parsing machine state for %s: %w", machineId, err)
	}

	return state, nil
}

func (r *ProviderRedisRepository) WaitForMachineRegistration(providerName, poolName, machineId string) (*types.ProviderMachineState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for machine registration for %s", machineId)
		case <-ticker.C:
			exists, err := r.rdb.Exists(ctx, stateKey).Result()
			if err != nil {
				return nil, fmt.Errorf("error checking machine state existence for %s: %w", machineId, err)
			}

			if exists == 0 {
				continue
			}

			res, err := r.rdb.HGetAll(ctx, stateKey).Result()
			if err != nil {
				if err == redis.Nil {
					continue // Key does not exist yet, continue polling
				}

				return nil, fmt.Errorf("failed to get machine state for %s: %w", machineId, err)
			}

			if len(res) == 0 {
				return nil, fmt.Errorf("machine state for %s is invalid or empty", machineId)
			}

			state := &types.ProviderMachineState{}
			if err = common.ToStruct(res, state); err != nil {
				return nil, fmt.Errorf("error parsing machine state for %s: %w", machineId, err)
			}

			return state, nil
		}
	}
}

func (r *ProviderRedisRepository) RegisterMachine(providerName, poolName, machineId string, info *types.ProviderMachineState) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	err := r.rdb.HSet(context.TODO(), stateKey, "machine_id", machineId, "token", info.Token, "hostname", info.HostName).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}
	return nil
}

func (r *ProviderRedisRepository) SetMachineLock(providerName, poolName, machineId string) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId), common.RedisLockOptions{TtlS: 300, Retries: 0})
	if err != nil {
		return err
	}

	return nil
}

func (r *ProviderRedisRepository) RemoveMachineLock(providerName, poolName, machineId string) error {
	return r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))
}
