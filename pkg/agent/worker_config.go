package agent

import (
	"encoding/json"
	"os"
	pathpkg "path"

	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type agentWorkerConfig struct {
	ClusterName string                `json:"clusterName"`
	DebugMode   bool                  `json:"debugMode"`
	PrettyLogs  bool                  `json:"prettyLogs"`
	Gateway     agentConfigGateway    `json:"gateway"`
	Storage     agentConfigStorage    `json:"storage"`
	Image       agentConfigImage      `json:"imageService"`
	Monitoring  agentConfigMonitoring `json:"monitoring"`
	Worker      agentConfigWorker     `json:"worker"`
	Cache       agentConfigCache      `json:"cache"`
}

type agentConfigGateway struct {
	GRPC agentConfigEndpoint `json:"grpc"`
	HTTP agentConfigEndpoint `json:"http"`
}

type agentConfigEndpoint struct {
	ExternalHost string `json:"externalHost"`
	ExternalPort int    `json:"externalPort"`
	TLS          bool   `json:"tls"`
}

type agentConfigStorage struct {
	Mode             string                      `json:"mode"`
	FSName           string                      `json:"fsName"`
	FSPath           string                      `json:"fsPath"`
	ObjectPath       string                      `json:"objectPath"`
	WorkspaceStorage agentConfigWorkspaceStorage `json:"workspaceStorage"`
}

type agentConfigWorkspaceStorage struct {
	BaseMountPath      string `json:"baseMountPath"`
	DefaultStorageMode string `json:"defaultStorageMode"`
}

type agentConfigImage struct {
	LocalCacheEnabled bool   `json:"localCacheEnabled"`
	RegistryStore     string `json:"registryStore"`
	ClipVersion       uint32 `json:"clipVersion"`
}

type agentConfigMonitoring struct {
	MetricsCollector         string                `json:"metricsCollector"`
	ContainerMetricsInterval string                `json:"containerMetricsInterval"`
	Prometheus               agentConfigPrometheus `json:"prometheus"`
}

type agentConfigPrometheus struct {
	ScrapeWorkers bool `json:"scrapeWorkers"`
	Port          int  `json:"port"`
}

type agentConfigWorker struct {
	HostNetwork                bool                       `json:"hostNetwork"`
	UseHostResolvConf          bool                       `json:"useHostResolvConf"`
	ContainerRuntime           string                     `json:"containerRuntime"`
	CacheEnabled               bool                       `json:"cacheEnabled"`
	TerminationGracePeriod     int                        `json:"terminationGracePeriod"`
	ContainerLogLinesPerHour   int                        `json:"containerLogLinesPerHour"`
	DefaultWorkerCPURequest    int64                      `json:"defaultWorkerCPURequest"`
	DefaultWorkerMemoryRequest int64                      `json:"defaultWorkerMemoryRequest"`
	Failover                   agentConfigWorkerFailover  `json:"failover"`
	Pools                      map[string]agentConfigPool `json:"pools"`
}

type agentConfigWorkerFailover struct {
	MaxSchedulingLatencyMs int `json:"maxSchedulingLatencyMs"`
}

type agentConfigPool struct {
	Mode                      string           `json:"mode"`
	GPUType                   string           `json:"gpuType"`
	ContainerRuntime          string           `json:"containerRuntime"`
	ContainerStartConcurrency int              `json:"containerStartConcurrency"`
	NetworkPreallocation      bool             `json:"networkPreallocation"`
	NetworkSlotPoolSize       int              `json:"networkSlotPoolSize"`
	RequiresPoolSelector      bool             `json:"requiresPoolSelector"`
	Priority                  int              `json:"priority"`
	CRIUEnabled               bool             `json:"criuEnabled"`
	TmpSizeLimit              string           `json:"tmpSizeLimit"`
	StorageMode               string           `json:"storageMode"`
	CheckpointPath            string           `json:"checkpointPath"`
	Cache                     agentConfigCache `json:"cache"`
}

type agentConfigCache struct {
	Enabled bool                    `json:"enabled"`
	Disk    agentConfigCacheDisk    `json:"disk"`
	Memory  *agentConfigCacheMemory `json:"memory,omitempty"`
	Global  *agentConfigCacheGlobal `json:"global,omitempty"`
	Server  *agentConfigCacheServer `json:"server,omitempty"`
	Client  *agentConfigCacheClient `json:"client,omitempty"`
}

type agentConfigCacheDisk struct {
	Enabled     bool    `json:"enabled"`
	HostPath    string  `json:"hostPath"`
	MountPath   string  `json:"mountPath"`
	MaxUsagePct float64 `json:"maxUsagePct"`
}

type agentConfigCacheMemory struct {
	Enabled bool `json:"enabled"`
}

type agentConfigCacheGlobal struct {
	DefaultLocality string `json:"defaultLocality"`
}

type agentConfigCacheServer struct {
	DiskCacheDir string `json:"diskCacheDir"`
}

type agentConfigCacheClient struct {
	CacheFS agentConfigCacheFS `json:"cachefs"`
}

type agentConfigCacheFS struct {
	Enabled    bool   `json:"enabled"`
	MountPoint string `json:"mountPoint"`
}

func writeWorkerConfig(path string, bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot) error {
	config := newAgentWorkerConfig(bootstrap, slot).sanitizedForAgent()
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func newAgentWorkerConfig(bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot) agentWorkerConfig {
	workspaceStorageMode := firstNonEmpty(os.Getenv(types.AgentStorageModeEnv), types.StorageModeGeese)
	cache := agentDiskCacheConfig()
	httpHost, httpPort, httpTLS := agentGatewayHTTPParts(bootstrap)
	cacheFSEnabled := !envBool(types.AgentInContainerEnv)

	return agentWorkerConfig{
		ClusterName: types.DefaultAgentName,
		DebugMode:   false,
		PrettyLogs:  true,
		Gateway: agentConfigGateway{
			GRPC: agentConfigEndpoint{
				ExternalHost: bootstrap.GatewayGRPCHost,
				ExternalPort: bootstrap.GatewayGRPCPort,
				TLS:          bootstrap.GatewayGRPCTLS,
			},
			HTTP: agentConfigEndpoint{
				ExternalHost: httpHost,
				ExternalPort: httpPort,
				TLS:          httpTLS,
			},
		},
		Storage: agentConfigStorage{
			Mode:       types.StorageModeLocal,
			FSName:     types.DefaultAgentName,
			FSPath:     types.AgentDataPath,
			ObjectPath: pathpkg.Join(types.AgentDataPath, "objects"),
			WorkspaceStorage: agentConfigWorkspaceStorage{
				BaseMountPath:      types.AgentWorkspacePath,
				DefaultStorageMode: workspaceStorageMode,
			},
		},
		Image: agentConfigImage{
			LocalCacheEnabled: bootstrap.ImageLocalCacheEnabled,
			RegistryStore:     firstNonEmpty(bootstrap.ImageRegistryStore, registry.LocalImageRegistryStore),
			ClipVersion:       firstNonZeroUint32(bootstrap.ImageClipVersion, uint32(types.ClipVersion2)),
		},
		Monitoring: agentConfigMonitoring{
			MetricsCollector:         string(types.MetricsCollectorNone),
			ContainerMetricsInterval: "3s",
			Prometheus:               agentConfigPrometheus{ScrapeWorkers: false, Port: 0},
		},
		Worker: agentConfigWorker{
			HostNetwork:                true,
			UseHostResolvConf:          true,
			ContainerRuntime:           types.ContainerRuntimeRunc.String(),
			CacheEnabled:               true,
			TerminationGracePeriod:     30,
			ContainerLogLinesPerHour:   6000,
			DefaultWorkerCPURequest:    slot.Cpu,
			DefaultWorkerMemoryRequest: slot.Memory,
			Failover: agentConfigWorkerFailover{
				MaxSchedulingLatencyMs: 300000,
			},
			Pools: map[string]agentConfigPool{
				slot.PoolName: {
					Mode:                      string(types.PoolModePrivate),
					GPUType:                   slot.Gpu,
					ContainerRuntime:          types.ContainerRuntimeRunc.String(),
					ContainerStartConcurrency: int(slot.ContainerStartConcurrency),
					NetworkPreallocation:      true,
					NetworkSlotPoolSize:       int(slot.NetworkSlotPoolSize),
					RequiresPoolSelector:      true,
					Priority:                  1000,
					CRIUEnabled:               false,
					TmpSizeLimit:              types.AgentTmpSizeLimit,
					StorageMode:               workspaceStorageMode,
					CheckpointPath:            types.AgentCheckpointPath,
					Cache:                     cache,
				},
			},
		},
		Cache: agentConfigCache{
			Enabled: true,
			Disk:    cache.Disk,
			Memory:  &agentConfigCacheMemory{Enabled: false},
			Global: &agentConfigCacheGlobal{
				DefaultLocality: firstNonEmpty(slot.PoolName, types.DefaultAgentName),
			},
			Server: &agentConfigCacheServer{
				DiskCacheDir: pathpkg.Join(types.AgentCachePath, sanitizeDockerName(slot.PoolName), sanitizeDockerName(slot.MachineId)),
			},
			Client: &agentConfigCacheClient{
				CacheFS: agentConfigCacheFS{
					Enabled:    cacheFSEnabled,
					MountPoint: types.AgentCacheFSMountPath,
				},
			},
		},
	}
}

func (c agentWorkerConfig) sanitizedForAgent() agentWorkerConfig {
	c.Image.LocalCacheEnabled = false
	c.Monitoring.MetricsCollector = string(types.MetricsCollectorNone)
	c.Monitoring.Prometheus = agentConfigPrometheus{}
	c.Worker.HostNetwork = true
	c.Worker.UseHostResolvConf = true
	c.Worker.CacheEnabled = true
	for name, pool := range c.Worker.Pools {
		pool.Mode = string(types.PoolModePrivate)
		pool.CRIUEnabled = false
		pool.RequiresPoolSelector = true
		pool.Cache.Enabled = true
		pool.Cache.Disk.Enabled = true
		c.Worker.Pools[name] = pool
	}
	return c
}

func agentDiskCacheConfig() agentConfigCache {
	return agentConfigCache{
		Enabled: true,
		Disk: agentConfigCacheDisk{
			Enabled:     true,
			HostPath:    types.AgentCachePath,
			MountPath:   types.AgentCachePath,
			MaxUsagePct: 0.95,
		},
	}
}
