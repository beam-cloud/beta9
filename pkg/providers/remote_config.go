package providers

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

func GetRemoteConfig(baseConfig types.AppConfig, tailscale *network.Tailscale) (*types.AppConfig, error) {
	configBytes, err := json.Marshal(baseConfig)
	if err != nil {
		return nil, err
	}

	// Overwrite certain config fields with tailscale hostnames
	// TODO: figure out a more elegant to override these fields without hardcoding service names
	// possibly, use proxy config values
	remoteConfig := types.AppConfig{}
	if err = json.Unmarshal(configBytes, &remoteConfig); err != nil {
		return nil, err
	}

	redisHostname, err := tailscale.GetHostnameForService("redis")
	if err != nil {
		return nil, err
	}

	if baseConfig.Storage.Mode == storage.StorageModeJuiceFS {
		juiceFsRedisHostname, err := tailscale.GetHostnameForService("juicefs-redis")
		if err != nil {
			return nil, err
		}

		remoteConfig.Storage.JuiceFS.RedisURI = fmt.Sprintf("rediss://%s/0", juiceFsRedisHostname)
	}

	gatewayGrpcHostname, err := tailscale.GetHostnameForService("gateway-grpc")
	if err != nil {
		return nil, err
	}

	remoteConfig.Database.Redis.Addrs[0] = redisHostname
	remoteConfig.Database.Redis.InsecureSkipVerify = true
	remoteConfig.GatewayService.Host = strings.Split(gatewayGrpcHostname, ":")[0]

	return &remoteConfig, nil
}
