package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	tailscaleHostNameExpiration time.Duration = time.Duration(30) * time.Second
)

type TailscaleRedisRepository struct {
	rdb *common.RedisClient
}

func NewTailscaleRedisRepository(r *common.RedisClient, config types.AppConfig) TailscaleRepository {
	return &TailscaleRedisRepository{rdb: r}
}

func (ts *TailscaleRedisRepository) GetHostnamesForService(serviceName string) ([]string, error) {
	ctx := context.TODO()
	hostnames := []string{}

	serviceIds, err := ts.rdb.SMembers(ctx, common.RedisKeys.TailscaleServiceHostnameIndex(serviceName)).Result()
	if err != nil {
		return hostnames, err
	}

	if len(serviceIds) == 0 {
		return hostnames, fmt.Errorf("no hostname found for service<%s>", serviceName)
	}

	for _, serviceId := range serviceIds {
		hostname, err := ts.rdb.Get(ctx, common.RedisKeys.TailscaleServiceHostname(serviceName, serviceId)).Result()
		if err != nil {
			ts.rdb.SRem(ctx, common.RedisKeys.TailscaleServiceHostnameIndex(serviceName), serviceId)
			continue
		}

		hostnames = append(hostnames, hostname)
	}

	if len(hostnames) == 0 {
		return hostnames, fmt.Errorf("no hostname found for service<%s>", serviceName)
	}

	return hostnames, nil
}

func (ts *TailscaleRedisRepository) SetHostname(serviceName, serviceId string, hostName string) error {
	ctx := context.TODO()
	pipe := ts.rdb.TxPipeline()
	pipe.SAdd(ctx, common.RedisKeys.TailscaleServiceHostnameIndex(serviceName), serviceId)
	pipe.SetEx(ctx,
		common.RedisKeys.TailscaleServiceHostname(serviceName, serviceId),
		hostName,
		tailscaleHostNameExpiration,
	)
	_, err := pipe.Exec(ctx)
	return err
}
