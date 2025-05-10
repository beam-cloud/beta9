package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
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

func (r *ProviderRedisRepository) GetMachine(providerName, poolName, machineId string) (*types.ProviderMachine, error) {
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

	machine := &types.ProviderMachine{State: state}
	metrics, err := r.getMachineMetrics(providerName, poolName, machineId)
	if err == nil {
		machine.Metrics = metrics
	}

	return machine, nil
}

func (r *ProviderRedisRepository) ListAllMachines(providerName, poolName string, useLock bool) ([]*types.ProviderMachine, error) {
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

		if useLock {
			err := r.lock.Acquire(context.TODO(), common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId), common.RedisLockOptions{TtlS: 10, Retries: 0})
			if err != nil {
				continue
			}
		}

		machineState, err := r.getMachineStateFromKey(key)
		if err != nil {
			r.RemoveMachine(providerName, poolName, machineId)

			if useLock {
				r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))
			}
			continue
		}

		machine := &types.ProviderMachine{State: machineState}
		metrics, err := r.getMachineMetrics(providerName, poolName, machineId)
		if err == nil {
			machine.Metrics = metrics
		}

		if useLock {
			r.lock.Release(common.RedisKeys.ProviderMachineLock(providerName, poolName, machineId))
		}
		machines = append(machines, machine)
	}

	return machines, nil
}

func (r *ProviderRedisRepository) getMachineMetrics(providerName, poolName, machineId string) (*types.ProviderMachineMetrics, error) {
	key := common.RedisKeys.ProviderMachineMetrics(providerName, poolName, machineId)
	metrics := &types.ProviderMachineMetrics{}

	exists, err := r.rdb.Exists(context.TODO(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("error checking machine metrics existence for key %s: %w", key, err)
	}

	if exists == 0 {
		return nil, errors.New("machine metrics not found")
	}

	res, err := r.rdb.HGetAll(context.TODO(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get machine metrics <%s>: %v", key, err)
	}

	if err = common.ToStruct(res, metrics); err != nil {
		return nil, fmt.Errorf("failed to deserialize machine metrics <%v>: %v", key, err)
	}

	return metrics, nil
}

func (r *ProviderRedisRepository) getMachineStateFromKey(key string) (*types.ProviderMachineState, error) {
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
		return nil, fmt.Errorf("failed to get machine state <%s>: %v", key, err)
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

			if state.Status != types.MachineStatusReady {
				log.Info().Msgf("waiting for machine to be ready: %s", machineId)
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

func (r *ProviderRedisRepository) SetMachineKeepAlive(providerName, poolName, machineId, agentVersion string, metrics *types.ProviderMachineMetrics) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)
	metricsKey := common.RedisKeys.ProviderMachineMetrics(providerName, poolName, machineId)

	machineState, err := r.getMachineStateFromKey(stateKey)
	if err != nil {
		return fmt.Errorf("failed to get machine state <%v>: %w", stateKey, err)
	}

	// Update the LastKeepalive with the current Unix timestamp
	machineState.LastKeepalive = fmt.Sprintf("%d", time.Now().Unix())
	machineState.AgentVersion = agentVersion
	machineState.Status = types.MachineStatusReady

	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(machineState)).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
	}

	// Update latest machine metrics
	err = r.rdb.HSet(context.TODO(), metricsKey, common.ToSlice(metrics)).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine metrics <%v>: %w", metricsKey, err)
	}

	// Set TTL on state key
	err = r.rdb.Expire(context.TODO(), stateKey, time.Duration(types.MachineKeepaliveExpirationS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state ttl <%v>: %w", stateKey, err)
	}

	// Set TTL on metrics key
	err = r.rdb.Expire(context.TODO(), metricsKey, time.Duration(types.MachineKeepaliveExpirationS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine metrics ttl <%v>: %w", metricsKey, err)
	}

	return nil
}

func (r *ProviderRedisRepository) SetLastWorkerSeen(providerName, poolName, machineId string) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)

	machineInfo, err := r.getMachineStateFromKey(stateKey)
	if err != nil {
		return fmt.Errorf("failed to get machine state <%v>: %w", stateKey, err)
	}

	// Update the LastWorkerSeen with the current Unix timestamp
	machineInfo.LastWorkerSeen = fmt.Sprintf("%d", time.Now().Unix())

	err = r.rdb.HSet(context.TODO(), stateKey, common.ToSlice(machineInfo)).Err()
	if err != nil {
		return fmt.Errorf("failed to set machine state <%v>: %w", stateKey, err)
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

func (r *ProviderRedisRepository) RegisterMachine(providerName, poolName, machineId string, newMachineInfo *types.ProviderMachineState, poolConfig *types.WorkerPoolConfig) error {
	stateKey := common.RedisKeys.ProviderMachineState(providerName, poolName, machineId)

	machineInfo, err := r.getMachineStateFromKey(stateKey)
	if err != nil {
		// TODO: This is a temporary fix to allow the machine to be registered
		// without having to update the machine state, in the future we should tie
		// registration token to machine ID and store that somewhere else persistently
		machineInfo = &types.ProviderMachineState{}
		machineInfo.Gpu = poolConfig.GPUType
		machineInfo.Created = fmt.Sprintf("%d", time.Now().UTC().Unix())
		machineInfo.LastKeepalive = fmt.Sprintf("%d", time.Now().UTC().Unix())
		machineInfo.PoolName = poolName
		machineInfo.MachineId = machineId
		// Add machine to index
		machineIndexKey := common.RedisKeys.ProviderMachineIndex(providerName, poolName)
		err = r.rdb.SAdd(context.TODO(), machineIndexKey, stateKey).Err()
		if err != nil {
			return fmt.Errorf("failed to add machine state key to index <%v>: %w", machineIndexKey, err)
		}
	}

	machineInfo.HostName = newMachineInfo.HostName
	machineInfo.Token = newMachineInfo.Token
	machineInfo.Status = types.MachineStatusRegistered
	machineInfo.Cpu = newMachineInfo.Cpu
	machineInfo.Memory = newMachineInfo.Memory
	machineInfo.GpuCount = newMachineInfo.GpuCount
	machineInfo.PrivateIP = newMachineInfo.PrivateIP

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

func (r *ProviderRedisRepository) GetGPUAvailability(pools map[string]types.WorkerPoolConfig) (map[string]bool, error) {
	gpuAvailability := map[string]bool{}

	for poolName, pool := range pools {
		if pool.Provider == nil {
			continue
		}

		// When GPU already found and is available, skip looking for it in another pool
		if isAvailable, ok := gpuAvailability[pool.GPUType]; ok && isAvailable {
			continue
		}

		// Initialize availability of GPU
		gpuAvailability[pool.GPUType] = false

		machines, err := r.ListAllMachines(string(*pool.Provider), poolName, false)
		if err != nil {
			return nil, err
		}

		// Update availability of GPU based on machine states
		for _, machine := range machines {
			if machine.Metrics != nil {
				gpuAvailability[machine.State.Gpu] = machine.Metrics.FreeGpuCount > 0
			}
		}
	}

	return gpuAvailability, nil
}

func (r *ProviderRedisRepository) GetGPUCounts(pools map[string]types.WorkerPoolConfig) (map[string]int, error) {
	gpuCounts := map[string]int{}
	gpuTypes := types.AllGPUTypes()
	for _, gpuType := range gpuTypes {
		gpuCounts[gpuType.String()] = 0
	}

	for poolName, pool := range pools {
		if pool.Provider == nil {
			continue
		}

		machines, err := r.ListAllMachines(string(*pool.Provider), poolName, false)
		if err != nil {
			return nil, err
		}

		for _, machine := range machines {
			if machine.Metrics != nil {
				gpuCounts[machine.State.Gpu] += machine.Metrics.FreeGpuCount
			}
		}
	}

	return gpuCounts, nil
}
