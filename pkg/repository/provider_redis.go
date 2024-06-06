package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
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
	ctx := context.TODO()

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

func (r *ProviderRedisRepository) ListAllMachines(providerName, poolName string) ([]*types.ProviderMachine, error) {
	machines := []*types.ProviderMachine{}

	// Get all machines from the machine index
	keys, err := r.rdb.SMembers(context.TODO(), common.RedisKeys.ProviderMachineIndex(providerName, poolName)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve machine state keys: %v", err)
	}

	for _, key := range keys {
		keyParts := strings.Split(key, ":")
		if len(keyParts) == 0 {
			continue
		}

		machineId := keyParts[len(keyParts)-1]

		err := r.lock.Acquire(context.TODO(), common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId), common.RedisLockOptions{TtlS: 10, Retries: 0})
		if err != nil {
			continue
		}

		machineState, err := r.getMachineFromKey(key)
		if err != nil {
			r.RemoveMachine(providerName, poolName, machineId)
			r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))
			continue
		}

		r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))
		machines = append(machines, &types.ProviderMachine{State: machineState})
	}

	return machines, nil
}

func (r *ProviderRedisRepository) getMachineFromKey(key string) (*types.ProviderMachineState, error) {
	machine := &types.ProviderMachineState{}

	exists, err := r.rdb.Exists(context.TODO(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("error checking machine state existence for key %s: %w", key, err)
	}

	if exists == 0 {
		return nil, errors.New("machine not found")
	}

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

func (r *ProviderRedisRepository) AddMachine(providerName, poolName, machineId string, machineInfo *types.ProviderMachineState) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)

	machineInfo.MachineId = machineId
	machineInfo.Status = types.MachineStatusPending
	machineInfo.Created = fmt.Sprintf("%d", time.Now().Unix())

	err := r.rdb.HSet(context.TODO(),
		stateKey, common.ToSlice(machineInfo)).Err()

	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.MachinePendingExpirationS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state ttl <%v>: %w", stateKey, err)
	}

	machineIndexKey := common.RedisKeys.ProviderMachineIndex(providerName, poolName)
	err = r.rdb.SAdd(context.TODO(), machineIndexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add machine state key to index <%v>: %w", machineIndexKey, err)
	}

	return nil
}

func (r *ProviderRedisRepository) SetMachineKeepAlive(providerName, poolName, machineId string) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)

	machineInfo, err := r.getMachineFromKey(stateKey)
	if err != nil {
		return fmt.Errorf("failed to get machine state <%v>: %w", stateKey, err)
	}

	// Update the LastKeepalive with the current Unix timestamp
	machineInfo.LastKeepalive = fmt.Sprintf("%d", time.Now().Unix())

	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(machineInfo)).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.MachineKeepaliveExpirationS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state ttl <%v>: %w", stateKey, err)
	}
	return nil
}

func (r *ProviderRedisRepository) RemoveMachine(providerName, poolName, machineId string) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)

	machineIndexKey := common.RedisKeys.ProviderMachineIndex(providerName, poolName)
	err := r.rdb.SRem(context.TODO(), machineIndexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove machine state key from index <%v>: %w", machineIndexKey, err)
	}

	err = r.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *ProviderRedisRepository) RegisterMachine(providerName, poolName, machineId string, newMachineInfo *types.ProviderMachineState) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)

	machineInfo, err := r.getMachineFromKey(stateKey)
	if err != nil {
		return fmt.Errorf("failed to get machine state <%v>: %w", stateKey, err)
	}

	machineInfo.HostName = newMachineInfo.HostName
	machineInfo.Token = newMachineInfo.Token
	machineInfo.Status = types.MachineStatusRegistered
	machineInfo.Cpu = newMachineInfo.Cpu
	machineInfo.Memory = newMachineInfo.Memory
	machineInfo.GpuCount = newMachineInfo.GpuCount

	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(machineInfo)).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.MachineKeepaliveExpirationS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state ttl <%v>: %w", stateKey, err)
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
