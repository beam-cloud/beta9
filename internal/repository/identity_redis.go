package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
)

type IdentityRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewIdentityRedisRepository(r *common.RedisClient) IdentityRepository {
	lock := common.NewRedisLock(r)
	return &IdentityRedisRepository{
		rdb:  r,
		lock: lock,
	}
}

func (ir *IdentityRedisRepository) SetIdentityQuota(identityId string, quota *types.IdentityQuota) error {
	quotaKey := common.RedisKeys.IdentityConcurrencyQuota(identityId)
	err := ir.rdb.HSet(context.TODO(), quotaKey, common.ToSlice(quota)).Err()
	if err != nil {
		return fmt.Errorf("failed to set identity quota <%v>: %w", quotaKey, err)
	}

	// Set ttl on quota
	err = ir.rdb.Expire(context.TODO(), quotaKey, time.Duration(types.IdentityQuotaTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set identity quota ttl <%v>: %w", quotaKey, err)
	}

	return nil
}

func (ir *IdentityRedisRepository) GetIdentityQuota(identityId string) (*types.IdentityQuota, error) {
	quota := &types.IdentityQuota{}

	quotaKey := common.RedisKeys.IdentityConcurrencyQuota(identityId)
	res, err := ir.rdb.HGetAll(context.Background(), quotaKey).Result()
	if err != nil {
		return quota, err
	}

	if len(res) == 0 {
		return nil, nil // No quota set
	}

	err = common.ToStruct(res, quota)
	if err != nil {
		return quota, fmt.Errorf("failed to deserialize identity quota: %v", err)
	}

	return quota, nil
}

func (ir *IdentityRedisRepository) GetActiveContainerByContainerId(containerId string) (string, error) {
	keys, err := ir.rdb.Scan(context.Background(), common.RedisKeys.IdentityActiveContainer("*", containerId, "*"))
	if err != nil {
		return "", err
	}

	if len(keys) == 0 {
		return "", nil
	}

	return keys[0], nil
}

func (ir *IdentityRedisRepository) ScanAllActiveContainersInIdentity(identityId string, gpuType string) ([]string, error) {
	var keys []string

	keys, err := ir.rdb.Scan(context.Background(), common.RedisKeys.IdentityActiveContainer(identityId, "*", gpuType))
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (ir *IdentityRedisRepository) GetTotalActiveContainersByGpuType(identity, gpuType string) (int, error) {
	if gpuType == strings.ToLower(types.NO_GPU.String()) {
		keys, err := ir.rdb.Scan(context.Background(), common.RedisKeys.IdentityActiveContainer(identity, "*", strings.ToLower(types.NO_GPU.String())))
		if err != nil {
			return 0, err
		}

		return len(keys), nil
	} else {
		keys, err := ir.rdb.Scan(context.Background(), common.RedisKeys.IdentityActiveContainer(identity, "*", gpuType))
		if err != nil {
			return 0, err
		}

		numGpus := 0

		for _, key := range keys {
			keySplit := strings.Split(key, ":")
			currGpuType := keySplit[len(keySplit)-1]

			if currGpuType == strings.ToLower(types.NO_GPU.String()) {
				continue
			}

			if gpuType == "*" { // If we want to get all gpus
				numGpus++
				continue
			}

			if currGpuType == gpuType {
				numGpus++
			}
		}

		return numGpus, nil
	}
}

func (ir *IdentityRedisRepository) SetIdentityActiveContainer(identityId string, quota *types.IdentityQuota, containerId string, gpuType string) error {
	err := ir.lock.Acquire(
		context.Background(), common.RedisKeys.IdentityActiveContainerLock(identityId), common.RedisLockOptions{TtlS: 10, Retries: 0},
	)
	if err != nil {
		return err
	}
	defer ir.lock.Release(common.RedisKeys.IdentityActiveContainerLock(identityId))

	if gpuType == strings.ToLower(types.NO_GPU.String()) {
		numContainers, err := ir.GetTotalActiveContainersByGpuType(identityId, gpuType)
		if err != nil {
			return err
		}

		if numContainers >= quota.CpuConcurrencyLimit {
			return &types.ThrottledByConcurrencyLimitError{}
		}
	} else {
		numContainers, err := ir.GetTotalActiveContainersByGpuType(identityId, "*")
		if err != nil {
			return err
		}

		if numContainers >= quota.GpuConcurrencyLimit {
			return &types.ThrottledByConcurrencyLimitError{}
		}
	}

	key := common.RedisKeys.IdentityActiveContainer(identityId, containerId, gpuType)
	return ir.rdb.SetEx(context.Background(), key, "1", time.Duration(types.ContainerStateTtlSWhilePending)*time.Second).Err()
}

func (ir *IdentityRedisRepository) DeleteIdentityActiveContainer(containerId string, identityId string, gpuType string) error {
	if gpuType == "" {
		gpuType = types.NO_GPU.String()
	}

	gpuType = strings.ToLower(gpuType)

	containerActiveKey := common.RedisKeys.IdentityActiveContainer(identityId, containerId, gpuType)

	err := ir.rdb.Del(context.Background(), containerActiveKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container active key <%v>: %w", containerActiveKey, err)
	}

	return nil
}

func (ir *IdentityRedisRepository) RefreshIdentityActiveContainerKeyExpiration(containerId string, expiry time.Duration) error {
	containerActiveKey, err := ir.GetActiveContainerByContainerId(containerId)
	if err != nil {
		return fmt.Errorf("failed to get container active key: %w", err)
	}

	// Update expiration on container active key
	err = ir.rdb.Expire(context.Background(), containerActiveKey, expiry).Err()
	if err != nil {
		return fmt.Errorf("failed to set container active key ttl <%v>: %w", containerActiveKey, err)
	}

	return nil
}
