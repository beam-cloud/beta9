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
	hostnames := []string{}

	keys, err := ts.rdb.Keys(context.TODO(), common.RedisKeys.TailscaleServiceHostname(serviceName, "*"))
	if err != nil {
		return hostnames, err
	}

	if len(keys) == 0 {
		return hostnames, fmt.Errorf("no hostname found for service<%s>", serviceName)
	}

	for _, key := range keys {
		hostname, err := ts.rdb.Get(context.TODO(), key).Result()
		if err != nil {
			continue
		}

		hostnames = append(hostnames, hostname)
	}

	return hostnames, nil
}

func (ts *TailscaleRedisRepository) SetHostname(serviceName, serviceId string, hostName string) error {
	return ts.rdb.SetEx(context.TODO(),
		common.RedisKeys.TailscaleServiceHostname(serviceName, serviceId),
		hostName,
		tailscaleHostNameExpiration,
	).Err()
}
