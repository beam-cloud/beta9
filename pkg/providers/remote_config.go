package providers

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
)

const connectTimeout time.Duration = time.Second * 5

func GetRemoteConfig(baseConfig types.AppConfig, tailscale *network.Tailscale) (*types.AppConfig, error) {
	configBytes, err := json.Marshal(baseConfig)
	if err != nil {
		return nil, err
	}

	// Overwrite certain config fields with tailscale hostnames
	// TODO: figure out a more elegant to override these fields without hardcoding service names
	remoteConfig := types.AppConfig{}
	if err = json.Unmarshal(configBytes, &remoteConfig); err != nil {
		return nil, err
	}

	redisHostname, err := tailscale.ResolveService("control-plane-redis", connectTimeout)
	if err != nil {
		return nil, err
	}
	remoteConfig.Database.Redis.Addrs[0] = fmt.Sprintf("%s:%d", redisHostname, 6379)
	remoteConfig.Database.Redis.InsecureSkipVerify = true

	return &remoteConfig, nil
}
