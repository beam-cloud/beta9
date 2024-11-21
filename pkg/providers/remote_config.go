package providers

import (
	"encoding/json"
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

const connectTimeout time.Duration = time.Second * 5

func GetRemoteConfig(baseConfig types.AppConfig, tailscale *network.Tailscale) (*types.AppConfig, error) {
	configBytes, err := json.Marshal(baseConfig)
	if err != nil {
		log.Println("Error marshalling config from bytes")
		return nil, err
	}

	// Overwrite certain config fields with tailscale hostnames
	// TODO: figure out a more elegant to override these fields without hardcoding service names
	log.Println("Resolving tailscale hostnames")
	remoteConfig := types.AppConfig{}
	if err = json.Unmarshal(configBytes, &remoteConfig); err != nil {
		log.Println("Error unmarshalling config")
		return nil, err
	}

	// redisHostname, err := tailscale.ResolveService("control-plane-redis", connectTimeout)
	// if err != nil {
	// 	return nil, err
	// }
	// remoteConfig.Database.Redis.Addrs[0] = fmt.Sprintf("%s:%d", redisHostname, 6379)
	remoteConfig.Database.Redis.InsecureSkipVerify = true
	log.Println("Resolved control-plane-redis")
	if baseConfig.Storage.Mode == storage.StorageModeJuiceFS {
		// juiceFsRedisHostname, err := tailscale.ResolveService("juicefs-redis", connectTimeout)
		// if err != nil {
		// 	return nil, err
		// }

		// juiceFsRedisHostname = fmt.Sprintf("%s:%d", juiceFsRedisHostname, 6379)

		// parsedUrl, err := url.Parse(remoteConfig.Storage.JuiceFS.RedisURI)
		// if err != nil {
		// 	return nil, err
		// }

		// juicefsRedisPassword, _ := parsedUrl.User.Password()
		// remoteConfig.Storage.JuiceFS.RedisURI = fmt.Sprintf("rediss://:%s@%s/0", juicefsRedisPassword, juiceFsRedisHostname)
	}
	log.Println("Resolved juicefs-redis")
	if baseConfig.Worker.BlobCacheEnabled {
		// blobcacheRedisHostname, err := tailscale.ResolveService("blobcache-redis", connectTimeout)
		// if err != nil {
		// 	return nil, err
		// }

		// remoteConfig.BlobCache.Metadata.RedisAddr = fmt.Sprintf("%s:%d", blobcacheRedisHostname, 6379)

		// if remoteConfig.BlobCache.BlobFs.Enabled {
		// 	for idx, sourceConfig := range remoteConfig.BlobCache.BlobFs.Sources {
		// 		if sourceConfig.Mode == storage.StorageModeJuiceFS {
		// 			remoteConfig.BlobCache.BlobFs.Sources[idx].JuiceFS.RedisURI = remoteConfig.Storage.JuiceFS.RedisURI
		// 		}
		// 	}
		// }

	}
	log.Println("Resolved blobcache-redis")

	return &remoteConfig, nil
}
