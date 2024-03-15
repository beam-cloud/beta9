package repository

import (
	"context"
	"fmt"
	"strings"
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

func (r *ProviderRedisRepository) ListAllMachines(providerName, poolName string) ([]*types.ProviderMachineState, error) {
	machines := []*types.ProviderMachineState{}

	// Get all machines from the machine index
	keys, err := r.rdb.SMembers(context.TODO(), common.RedisKeys.ProviderMachineIndex(providerName, poolName)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve machine state keys: %v", err)
	}

	for _, key := range keys {
		machineId := strings.Split(key, ":")[2]

		err := r.lock.Acquire(context.TODO(), common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId), common.RedisLockOptions{TtlS: 10, Retries: 0})
		if err != nil {
			continue
		}

		m, err := r.getMachineFromKey(key)
		if err != nil {
			r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))

			continue
		}

		r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))

		machines = append(machines, m)
	}

	return machines, nil
}

func (r *ProviderRedisRepository) getMachineFromKey(key string) (*types.ProviderMachineState, error) {
	machine := &types.ProviderMachineState{}

	res, err := r.rdb.HGetAll(context.TODO(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get worker <%s>: %v", key, err)
	}

	if err = common.ToStruct(res, machine); err != nil {
		return nil, fmt.Errorf("failed to deserialize machine state <%v>: %v", key, err)
	}

	return machine, nil
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
			_, err := r.rdb.Exists(ctx, stateKey).Result()
			if err != nil {
				return nil, fmt.Errorf("error checking machine state existence for %s: %w", machineId, err)
			}

			res, err := r.rdb.HGetAll(ctx, stateKey).Result()
			if err != nil {
				return nil, fmt.Errorf("failed to get machine state for %s: %w", machineId, err)
			}

			if len(res) == 0 {
				return nil, fmt.Errorf("machine state for %s is invalid or empty", machineId)
			}

			state := &types.ProviderMachineState{}
			if err = common.ToStruct(res, state); err != nil {
				return nil, fmt.Errorf("error parsing machine state for %s: %w", machineId, err)
			}

			if state.Status == types.MachineStatusPending {
				// Still waiting for machine registration
				continue
			}

			return state, nil
		}
	}
}

func (r *ProviderRedisRepository) AddMachine(providerName, poolName, machineId string, info *types.ProviderMachineState) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))

	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	err = r.rdb.HSet(context.TODO(),
		stateKey, "machine_id", machineId, "status",
		string(types.MachineStatusPending), "cpu", info.Cpu, "memory", info.Memory,
		"gpu", info.Gpu, "gpu_count", info.GpuCount).Err()

	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}

	machineIndexKey := common.RedisKeys.ProviderMachineIndex(providerName, poolName)
	err = r.rdb.SAdd(context.TODO(), machineIndexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add machine state key to index <%v>: %w", machineIndexKey, err)
	}

	return nil
}

func (r *ProviderRedisRepository) RemoveMachine(providerName, poolName, machineId string) error {
	err := r.lock.Acquire(context.TODO(), common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId), common.RedisLockOptions{TtlS: 10, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))

	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	err = r.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return err
	}

	machineIndexKey := common.RedisKeys.ProviderMachineIndex(providerName, poolName)
	err = r.rdb.SRem(context.TODO(), machineIndexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove machine state key from index <%v>: %w", machineIndexKey, err)
	}

	return nil
}

func (r *ProviderRedisRepository) RegisterMachine(providerName, poolName, machineId string, info *types.ProviderMachineState) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	err := r.rdb.HSet(context.TODO(), stateKey, "machine_id", machineId, "token", info.Token, "hostname", info.HostName, "status", string(types.MachineStatusRegistered)).Err()
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
