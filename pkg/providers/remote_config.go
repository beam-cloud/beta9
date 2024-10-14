package providers

import (
	"encoding/json"
	"fmt"
	"net/url"
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

	redisHostname, err := tailscale.ResolveService("control-plane-redis", connectTimeout)
	if err != nil {
		return nil, err
	}
	remoteConfig.Database.Redis.Addrs[0] = fmt.Sprintf("%s:%d", redisHostname, 6379)
	remoteConfig.Database.Redis.InsecureSkipVerify = true

	if baseConfig.Storage.Mode == storage.StorageModeJuiceFS {
		juiceFsRedisHostname, err := tailscale.ResolveService("juicefs-redis", connectTimeout)
		if err != nil {
			return nil, err
		}

		juiceFsRedisHostname = fmt.Sprintf("%s:%d", juiceFsRedisHostname, 6379)

		parsedUrl, err := url.Parse(remoteConfig.Storage.JuiceFS.RedisURI)
		if err != nil {
			return nil, err
		}

		juicefsRedisPassword, _ := parsedUrl.User.Password()
		remoteConfig.Storage.JuiceFS.RedisURI = fmt.Sprintf("rediss://:%s@%s/0", juicefsRedisPassword, juiceFsRedisHostname)
	}

	if baseConfig.Worker.BlobCacheEnabled {
		blobcacheRedisHostname, err := tailscale.ResolveService("blobcache-redis", connectTimeout)
		if err != nil {
			return nil, err
		}

		remoteConfig.BlobCache.Metadata.RedisAddr = fmt.Sprintf("%s:%d", blobcacheRedisHostname, 6379)

		if remoteConfig.BlobCache.BlobFs.Enabled {
			for idx, sourceConfig := range remoteConfig.BlobCache.BlobFs.Sources {
				if sourceConfig.Mode == storage.StorageModeJuiceFS {
					remoteConfig.BlobCache.BlobFs.Sources[idx].JuiceFS.RedisURI = remoteConfig.Storage.JuiceFS.RedisURI
				}
			}
		}

	}

	return &remoteConfig, nil
}
