package providers

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/storage"
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

	redisHostname, err := network.ResolveTailscaleService("control-plane-redis", connectTimeout)
	if err != nil {
		return nil, err
	}
	remoteConfig.Database.Redis.Addrs[0] = redisHostname
	remoteConfig.Database.Redis.InsecureSkipVerify = true

	if baseConfig.Storage.Mode == storage.StorageModeJuiceFS {
		juiceFsRedisHostname, err := network.ResolveTailscaleService("juicefs-redis", connectTimeout)
		if err != nil {
			return nil, err
		}

		parsedUrl, err := url.Parse(remoteConfig.Storage.JuiceFS.RedisURI)
		if err != nil {
			return nil, err
		}

		juicefsRedisPassword, _ := parsedUrl.User.Password()
		remoteConfig.Storage.JuiceFS.RedisURI = fmt.Sprintf("rediss://:%s@%s/0", juicefsRedisPassword, juiceFsRedisHostname)
	}

	remoteConfig.GatewayService.Host = strings.TrimPrefix(remoteConfig.GatewayService.ExternalURL, "https://")

	if baseConfig.ImageService.BlobCacheEnabled {
		blobcacheRedisHostname, err := network.ResolveTailscaleService("blobcache-redis", connectTimeout)
		if err != nil {
			return nil, err
		}

		remoteConfig.ImageService.BlobCache.Metadata.RedisAddr = blobcacheRedisHostname
	}

	return &remoteConfig, nil
}
