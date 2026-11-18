package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.yaml.in/yaml/v3"
)

type cacheServerConfig struct {
	DebugMode  bool         `key:"debugMode"`
	PrettyLogs bool         `key:"prettyLogs"`
	Cache      cache.Config `key:"cache"`
	Gateway    struct {
		GRPC struct {
			ExternalHost string `key:"externalHost"`
			ExternalPort int    `key:"externalPort"`
		} `key:"grpc"`
	} `key:"gateway"`
	Worker struct {
		HostNetwork  bool                             `key:"hostNetwork"`
		CacheEnabled bool                             `key:"cacheEnabled"`
		Pools        map[string]cacheServerPoolConfig `key:"pools"`
	} `key:"worker"`
	Database struct {
		S2 types.S2Config `key:"s2"`
	} `key:"database"`
	Monitoring struct {
		VictoriaMetrics struct {
			PushURL   string `key:"pushURL"`
			AuthToken string `key:"authToken"`
			PushSecs  int    `key:"pushSecs"`
		} `key:"victoriametrics"`
	} `key:"monitoring"`
	ImageService struct {
		RegistryStore string `key:"registryStore"`
	} `key:"imageService"`
}

type cacheServerPoolConfig struct {
	GPUType     string                      `key:"gpuType"`
	ConfigGroup string                      `key:"configGroup"`
	Mode        types.PoolMode              `key:"mode"`
	Cache       types.WorkerPoolCacheConfig `key:"cache"`
}

func loadCacheServerConfig(path string) (types.AppConfig, error) {
	if path == "" {
		return types.AppConfig{}, errors.New("CONFIG_PATH must point to a dedicated cache server config")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return types.AppConfig{}, fmt.Errorf("read cache server CONFIG_PATH: %w", err)
	}

	// Do not use ConfigManager here. Its standard inputs include CONFIG_JSON
	// and all files in /etc/beta9.d, which may contain control-plane config.
	var settings map[string]any
	yamlDecoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := yamlDecoder.Decode(&settings); err != nil {
		return types.AppConfig{}, errors.New("invalid cache server config document")
	}
	var extra any
	if err := yamlDecoder.Decode(&extra); err != io.EOF {
		return types.AppConfig{}, errors.New("cache server config must contain one document")
	}

	// Keep the runtime defaults that are not supplied by normalizeCacheConfig.
	// Reconciliation is opt-in because it needs a dedicated S2 credential.
	var config cacheServerConfig
	config.PrettyLogs = true
	config.Gateway.GRPC.ExternalHost = "beta9-gateway"
	config.Gateway.GRPC.ExternalPort = 1993
	config.Worker.CacheEnabled = true
	config.Database.S2.StreamPrefix = "events"
	config.ImageService.RegistryStore = registry.LocalImageRegistryStore
	config.Cache = cache.Config{
		Enabled: true,
		Disk:    cache.DiskConfig{Enabled: true},
		Global: cache.GlobalConfig{
			GRPCInitialWindowSize:     4 << 20,
			GRPCInitialConnWindowSize: 32 << 20,
			GRPCWriteBufferSize:       256 << 10,
			GRPCReadBufferSize:        256 << 10,
			GRPCMaxConcurrentStreams:  1024,
			PrettyLogs:                true,
		},
		Server: cache.ServerConfig{ReadTransport: cache.ServerReadTransportConfig{
			MaxRequestSizeBytes:   64 << 20,
			MaxInflightBytes:      1 << 30,
			MaxConcurrentRequests: 256,
		}},
		Client: cache.ClientConfig{
			ReadTransport: cache.ClientReadTransportConfig{RequestSizeBytes: 64 << 20, MaxPartsPerRead: 8},
			CacheFS: cache.FSConfig{
				Enabled:            true,
				MountPoint:         "/cache",
				MaxBackgroundTasks: 256,
				MaxReadAheadKB:     1024,
				MaxWriteKB:         1024,
				Options:            []string{"max_read=1048576"},
			},
		},
		Reconciliation: cache.ReconciliationConfig{IntervalSeconds: 15, OriginFallbackEnabled: true},
	}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:          "key",
		Result:           &config,
		ErrorUnused:      true,
		WeaklyTypedInput: true,
	})
	if err != nil {
		return types.AppConfig{}, err
	}
	if err := decoder.Decode(settings); err != nil {
		// Parser and decoder errors can include config values.
		return types.AppConfig{}, errors.New("invalid or unsupported settings in cache server config")
	}

	if strings.TrimSpace(config.Cache.Coordinator.Token) == "" {
		return types.AppConfig{}, errors.New("cache.coordinator.token is required for cache server mode")
	}
	if config.Cache.Server.Metadata != (cache.MetadataConfig{}) {
		return types.AppConfig{}, errors.New("cache.server.metadata must be configured on the gateway")
	}
	for _, source := range config.Cache.Server.Sources {
		if source.MountPoint.AccessKey != "" || source.MountPoint.SecretKey != "" {
			return types.AppConfig{}, errors.New("cache server mount sources require ambient credentials")
		}
	}
	if config.Cache.Reconciliation.Enabled && (config.Database.S2.ApiKey == "" || config.Database.S2.Basin == "") {
		return types.AppConfig{}, errors.New("cache reconciliation requires a cache-scoped database.s2.apiKey and database.s2.basin")
	}
	metricsConfig := config.Monitoring.VictoriaMetrics
	if metricsConfig.PushURL != "" && metricsConfig.PushSecs <= 0 {
		return types.AppConfig{}, errors.New("monitoring.victoriametrics.pushSecs must be positive when pushURL is configured")
	}

	appConfig := types.AppConfig{DebugMode: config.DebugMode, PrettyLogs: config.PrettyLogs, Cache: config.Cache}
	appConfig.GatewayService.GRPC.ExternalHost = config.Gateway.GRPC.ExternalHost
	appConfig.GatewayService.GRPC.ExternalPort = config.Gateway.GRPC.ExternalPort
	appConfig.Worker.CacheEnabled = config.Worker.CacheEnabled
	appConfig.Worker.HostNetwork = config.Worker.HostNetwork
	appConfig.Worker.Pools = make(map[string]types.WorkerPoolConfig, len(config.Worker.Pools))
	for name, pool := range config.Worker.Pools {
		appConfig.Worker.Pools[name] = types.WorkerPoolConfig{
			GPUType: pool.GPUType, ConfigGroup: pool.ConfigGroup, Mode: pool.Mode, Cache: pool.Cache,
		}
	}
	appConfig.Database.S2 = config.Database.S2
	appConfig.ImageService.RegistryStore = config.ImageService.RegistryStore
	appConfig.Monitoring.VictoriaMetrics = types.VictoriaMetricsConfig{
		PushURL:   metricsConfig.PushURL,
		AuthToken: metricsConfig.AuthToken,
		PushSecs:  metricsConfig.PushSecs,
	}
	return appConfig, nil
}

// RunCacheServer runs the worker cache manager without accepting user
// containers. It uses the same worker image and cache registration path as a
// normal worker, but never starts the container runtime.
func RunCacheServer() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	config, err := loadCacheServerConfig(os.Getenv("CONFIG_PATH"))
	if err != nil {
		return err
	}
	if config.DebugMode {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	if config.PrettyLogs {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
	metrics.InitializeMetricsRepository(config.Monitoring.VictoriaMetrics)

	workerRepoClient, err := NewWorkerRepositoryClient(ctx, config, config.Cache.Coordinator.Token)
	if err != nil {
		return err
	}

	poolName := os.Getenv(types.WorkerPoolEnv)
	if poolName == "" {
		poolName = "default"
	}
	poolConfig := config.Worker.Pools[poolName]

	workerID := os.Getenv(types.WorkerIDEnv)
	if workerID == "" {
		workerID = fmt.Sprintf("cache-server-%s", cacheNodeID())
	}

	podAddr, err := GetPodAddr()
	if err != nil {
		return err
	}

	eventRepo := repo.NewEventClientRepo(config)
	manager := NewWorkerCacheManager(ctx, config, poolConfig, workerRepoClient, eventRepo, nil, workerID, poolName, podAddr)
	client, err := manager.Start()
	if err != nil {
		return err
	}
	if client == nil {
		return errors.New("cache server mode requires cache to be enabled")
	}

	log.Info().
		Str("worker_id", workerID).
		Str("pool_name", poolName).
		Str("node_id", manager.nodeID).
		Str("locality", manager.locality).
		Bool("running_cache_server", manager.runningCacheServer()).
		Msg("cache server process started")

	<-ctx.Done()
	stop()
	log.Info().Msg("cache server shutdown signal received")
	if err := manager.Drain(); err != nil {
		log.Warn().Err(err).Msg("cache server drain failed")
	}

	return manager.Close()
}
