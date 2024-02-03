package repository

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
)

const (
	tailscaleHostNameExpiration time.Duration = time.Duration(30) * time.Second
)

type TailscaleRedisRepository struct {
	rdb       *common.RedisClient
	tailscale *common.Tailscale
}

func NewTailscaleRedisRepository(r *common.RedisClient, config types.AppConfig) TailscaleRepository {
	tailscale := common.NewTailscale(common.TailscaleConfig{
		ControlURL: config.Tailscale.ControlURL,
		AuthKey:    config.Tailscale.AuthKey,
		Debug:      config.Tailscale.Debug,
		Ephemeral:  true,
	})
	return &TailscaleRedisRepository{rdb: r, tailscale: tailscale}
}

func (ts *TailscaleRedisRepository) GetHostnameForService(serviceName string) (string, error) {
	keys, err := ts.rdb.Keys(context.TODO(), common.RedisKeys.TailscaleServiceHostname(serviceName, "*"))
	if err != nil {
		return "", err
	}

	if len(keys) == 0 {
		return "", fmt.Errorf("not hostnames found for service<%s>", serviceName)
	}

	for len(keys) > 0 {
		index := rand.Intn(len(keys))
		key := keys[index]

		hostname, err := ts.rdb.Get(context.TODO(), key).Result()
		if err != nil {
			keys = append(keys[:index], keys[index+1:]...)
			continue
		}

		conn, err := ts.tailscale.Dial(context.TODO(), hostname)
		if err == nil {
			conn.Close()
			return hostname, nil
		}

		keys = append(keys[:index], keys[index+1:]...)
	}

	return "", fmt.Errorf("unable to find a valid hostname for service<%s>", serviceName)
}

func (ts *TailscaleRedisRepository) SetHostname(serviceName, serviceId string, hostName string) error {
	return ts.rdb.SetEx(context.TODO(),
		common.RedisKeys.TailscaleServiceHostname(serviceName, serviceId),
		hostName,
		tailscaleHostNameExpiration,
	).Err()
}
