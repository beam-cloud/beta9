package common

import (
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"net/url"
)

// AgentRuntimeConfig decodes the agent's bootstrap format using the same key
// tags as YAML configuration, before applying the runtime boundary.
func AgentRuntimeConfig(data []byte, poolName string) (types.AppConfig, error) {
	k := koanf.New(".")
	yamlParser, _ := GetConfigParser(YAMLConfigFormat)
	jsonParser, _ := GetConfigParser(JSONConfigFormat)
	if err := k.Load(rawbytes.Provider(defaultConfig), yamlParser); err != nil {
		return types.AppConfig{}, err
	}
	if err := k.Load(rawbytes.Provider(data), jsonParser); err != nil {
		return types.AppConfig{}, err
	}
	var config types.AppConfig
	if err := k.UnmarshalWithConf("", &config, koanf.UnmarshalConf{Tag: "key"}); err != nil {
		return types.AppConfig{}, err
	}
	config.Worker.UseGatewayServiceHostname = false
	return WorkerRuntimeConfig(config, poolName), nil
}

// WorkerRuntimeConfig is the configuration boundary for worker processes.
func WorkerRuntimeConfig(config types.AppConfig, poolName string) types.AppConfig {
	out := types.AppConfig{ClusterName: config.ClusterName, DebugMode: config.DebugMode, PrettyLogs: config.PrettyLogs}
	out.GatewayService.GRPC = config.GatewayService.GRPC
	out.GatewayService.HTTP = config.GatewayService.HTTP
	if config.Worker.UseGatewayServiceHostname && config.GatewayService.Host != "" {
		out.GatewayService.GRPC.ExternalHost = config.GatewayService.Host
		out.GatewayService.GRPC.ExternalPort = config.GatewayService.GRPC.Port
		out.GatewayService.GRPC.TLS = false
		out.GatewayService.HTTP.ExternalHost = config.GatewayService.Host
		out.GatewayService.HTTP.ExternalPort = config.GatewayService.HTTP.Port
		out.GatewayService.HTTP.TLS = false
	}
	out.Worker = types.WorkerConfig{
		HostNetwork:                config.Worker.HostNetwork,
		UseGatewayServiceHostname:  config.Worker.UseGatewayServiceHostname,
		UseHostResolvConf:          config.Worker.UseHostResolvConf,
		ImageTag:                   config.Worker.ImageTag,
		ImageName:                  config.Worker.ImageName,
		ImageRegistry:              config.Worker.ImageRegistry,
		ContainerResourceLimits:    config.Worker.ContainerResourceLimits,
		DefaultWorkerCPURequest:    config.Worker.DefaultWorkerCPURequest,
		DefaultWorkerMemoryRequest: config.Worker.DefaultWorkerMemoryRequest,
		TerminationGracePeriod:     config.Worker.TerminationGracePeriod,
		CacheEnabled:               config.Worker.CacheEnabled,
		CRIU:                       config.Worker.CRIU,
		TmpSizeLimit:               config.Worker.TmpSizeLimit,
		ContainerLogLinesPerHour:   config.Worker.ContainerLogLinesPerHour,
		ContainerRuntime:           config.Worker.ContainerRuntime,
		HeadroomWorkerMaxAge:       config.Worker.HeadroomWorkerMaxAge,
		MaxAge:                     config.Worker.MaxAge,
		Egress:                     config.Worker.Egress,
	}
	out.Worker.Pools = map[string]types.WorkerPoolConfig{}
	for name, pool := range config.Worker.Pools {
		if poolName != "" && name != poolName {
			continue
		}
		out.Worker.Pools[name] = types.WorkerPoolConfig{
			GPUType:                   pool.GPUType,
			Runtime:                   pool.Runtime,
			ContainerRuntime:          pool.ContainerRuntime,
			ContainerRuntimeConfig:    pool.ContainerRuntimeConfig,
			CPUAffinityEnforced:       pool.CPUAffinityEnforced,
			GPUVirtualized:            pool.GPUVirtualized,
			ContainerStartConcurrency: pool.ContainerStartConcurrency,
			NetworkPreallocation:      pool.NetworkPreallocation,
			NetworkSlotPoolSize:       pool.NetworkSlotPoolSize,
			Mode:                      pool.Mode,
			RequiresPoolSelector:      pool.RequiresPoolSelector,
			Priority:                  pool.Priority,
			Preemptable:               pool.Preemptable,
			CRIUEnabled:               pool.CRIUEnabled,
			TmpSizeLimit:              pool.TmpSizeLimit,
			ConfigGroup:               pool.ConfigGroup,
			StoragePath:               pool.StoragePath,
			StorageMode:               pool.StorageMode,
			ImagesPath:                pool.ImagesPath,
			DurableDisksPath:          pool.DurableDisksPath,
			Cache:                     pool.Cache,
			Locality:                  pool.Locality,
		}
	}
	if poolName != "" && poolName == config.ImageService.BuildContainerPoolSelector {
		pool := out.Worker.Pools[poolName]
		pool.ContainerRuntime = types.ContainerRuntimeGvisor.String()
		pool.ContainerRuntimeConfig.GVisorExtraArgs = append(append([]string(nil), pool.ContainerRuntimeConfig.GVisorExtraArgs...), "--overlay2=none")
		out.Worker.Pools[poolName] = pool
	}
	out.Storage.WorkspaceStorage.BaseMountPath = config.Storage.WorkspaceStorage.BaseMountPath
	out.Storage.WorkspaceStorage.DefaultStorageMode = config.Storage.WorkspaceStorage.DefaultStorageMode
	out.ImageService.RegistryStore = config.ImageService.RegistryStore
	out.ImageService.ClipVersion = config.ImageService.ClipVersion
	out.ImageService.LocalCacheEnabled = config.ImageService.LocalCacheEnabled
	out.ImageService.UpstreamBuildRegistry = config.ImageService.EffectiveBuildRegistry()
	out.ImageService.BuildRegistry = config.ImageService.EffectiveBuildRegistry()
	if broker, err := url.Parse(out.GatewayService.HTTP.GetExternalURL()); err == nil && broker.Hostname() != "" {
		out.ImageService.BuildRegistry = broker.Host
		out.ImageService.BuildRegistryInsecure = broker.Scheme == "http"
	}
	out.ImageService.BuildRepositoryName = config.ImageService.BuildRepositoryName
	if out.ImageService.UpstreamBuildRegistry == out.ImageService.BuildRegistry {
		out.ImageService.BuildRegistryInsecure = config.ImageService.BuildRegistryInsecure
	}
	out.ImageService.Runner.BaseImageRegistry = config.ImageService.Runner.BaseImageRegistry
	out.ImageService.LayeredBuilds = config.ImageService.LayeredBuilds
	out.ImageService.BuildContainerPoolSelector = config.ImageService.BuildContainerPoolSelector
	out.ImageService.BuildApt = config.ImageService.BuildApt
	out.ImageService.BuildLayerCacheMaxPct = config.ImageService.BuildLayerCacheMaxPct
	out.Cache = cache.Config{
		Enabled: config.Cache.Enabled,
		Disk: cache.DiskConfig{
			Enabled:      config.Cache.Disk.Enabled,
			HostPath:     config.Cache.Disk.HostPath,
			MountPath:    config.Cache.Disk.MountPath,
			MaxUsagePct:  config.Cache.Disk.MaxUsagePct,
			MinFreeBytes: config.Cache.Disk.MinFreeBytes,
		},
		Memory: cache.MemoryConfig{
			Enabled:     config.Cache.Memory.Enabled,
			MaxCachePct: config.Cache.Memory.MaxCachePct,
		},
		Coordinator: cache.CoordinatorConfig{
			RegistrationTTLSeconds:   config.Cache.Coordinator.RegistrationTTLSeconds,
			HeartbeatIntervalSeconds: config.Cache.Coordinator.HeartbeatIntervalSeconds,
			HostWatchIntervalSeconds: config.Cache.Coordinator.HostWatchIntervalSeconds,
		},
		Global: cache.GlobalConfig{
			DefaultLocality:                 config.Cache.Global.DefaultLocality,
			ServerPort:                      config.Cache.Global.ServerPort,
			DiscoveryIntervalS:              config.Cache.Global.DiscoveryIntervalS,
			DiscoveryJitterS:                config.Cache.Global.DiscoveryJitterS,
			MaxDiscoveryConcurrency:         config.Cache.Global.MaxDiscoveryConcurrency,
			HostMonitorIntervalS:            config.Cache.Global.HostMonitorIntervalS,
			RoundTripThresholdMilliseconds:  config.Cache.Global.RoundTripThresholdMilliseconds,
			HostStorageCapacityThresholdPct: config.Cache.Global.HostStorageCapacityThresholdPct,
			GRPCDialTimeoutS:                config.Cache.Global.GRPCDialTimeoutS,
			GRPCMessageSizeBytes:            config.Cache.Global.GRPCMessageSizeBytes,
			GRPCInitialWindowSize:           config.Cache.Global.GRPCInitialWindowSize,
			GRPCInitialConnWindowSize:       config.Cache.Global.GRPCInitialConnWindowSize,
			GRPCWriteBufferSize:             config.Cache.Global.GRPCWriteBufferSize,
			GRPCReadBufferSize:              config.Cache.Global.GRPCReadBufferSize,
			GRPCMaxConcurrentStreams:        config.Cache.Global.GRPCMaxConcurrentStreams,
			GRPCNumStreamWorkers:            config.Cache.Global.GRPCNumStreamWorkers,
			GRPCPayloadCodecV2:              config.Cache.Global.GRPCPayloadCodecV2,
			GRPCPayloadCodecMinBytes:        config.Cache.Global.GRPCPayloadCodecMinBytes,
			DebugMode:                       config.Cache.Global.DebugMode,
			PrettyLogs:                      config.Cache.Global.PrettyLogs,
		},
		Client: cache.ClientConfig{
			Token:                 config.Cache.Client.Token,
			MinRetryLengthBytes:   config.Cache.Client.MinRetryLengthBytes,
			MaxGetContentAttempts: config.Cache.Client.MaxGetContentAttempts,
			NTopHosts:             config.Cache.Client.NTopHosts,
			CacheFS:               config.Cache.Client.CacheFS,
			PreferLocalCacheHost:  config.Cache.Client.PreferLocalCacheHost,
			PageFDCacheSize:       config.Cache.Client.PageFDCacheSize,
			ReadTransport:         config.Cache.Client.ReadTransport,
		},
		Server: cache.ServerConfig{
			DiskCacheDir:                 config.Cache.Server.DiskCacheDir,
			DiskCacheMaxUsagePct:         config.Cache.Server.DiskCacheMaxUsagePct,
			DiskCacheEvictWatermarkPct:   config.Cache.Server.DiskCacheEvictWatermarkPct,
			ObjectTtlS:                   config.Cache.Server.ObjectTtlS,
			MaxCachePct:                  config.Cache.Server.MaxCachePct,
			PageSizeBytes:                config.Cache.Server.PageSizeBytes,
			PageFileBuckets:              config.Cache.Server.PageFileBuckets,
			SmallRangeCopyThresholdBytes: config.Cache.Server.SmallRangeCopyThresholdBytes,
			ReadTransport:                config.Cache.Server.ReadTransport,
			S3DownloadConcurrency:        config.Cache.Server.S3DownloadConcurrency,
			S3DownloadChunkSize:          config.Cache.Server.S3DownloadChunkSize,
		},
		Reconciliation: cache.ReconciliationConfig{
			Enabled:               config.Cache.Reconciliation.Enabled,
			IntervalSeconds:       config.Cache.Reconciliation.IntervalSeconds,
			RecentStubTTLSeconds:  config.Cache.Reconciliation.RecentStubTTLSeconds,
			LockTTLSeconds:        config.Cache.Reconciliation.LockTTLSeconds,
			MaxItemsPerCycle:      config.Cache.Reconciliation.MaxItemsPerCycle,
			MaxBytesPerCycle:      config.Cache.Reconciliation.MaxBytesPerCycle,
			MaxConcurrentFetches:  config.Cache.Reconciliation.MaxConcurrentFetches,
			VolumeMinBytes:        config.Cache.Reconciliation.VolumeMinBytes,
			OriginFallbackEnabled: config.Cache.Reconciliation.OriginFallbackEnabled,
		},
	}
	out.Monitoring.MetricsCollector = string(types.MetricsCollectorNone)
	out.Monitoring.ContainerMetricsInterval = config.Monitoring.ContainerMetricsInterval
	if config.Monitoring.MetricsCollector == string(types.MetricsCollectorPrometheus) {
		out.Monitoring.MetricsCollector = config.Monitoring.MetricsCollector
		out.Monitoring.Prometheus.Port = config.Monitoring.Prometheus.Port
		out.Monitoring.Prometheus.ScrapeWorkers = config.Monitoring.Prometheus.ScrapeWorkers
	}
	out.Abstractions.Pod.TCP = config.Abstractions.Pod.TCP
	return out
}
