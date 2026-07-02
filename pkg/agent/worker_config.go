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
	ClusterName    string                     `json:"clusterName"`
	DebugMode      bool                       `json:"debugMode"`
	PrettyLogs     bool                       `json:"prettyLogs"`
	Database       agentConfigDatabase        `json:"database"`
	Gateway        agentConfigGateway         `json:"gateway"`
	Storage        agentConfigStorage         `json:"storage"`
	Image          agentConfigImage           `json:"imageService"`
	Monitoring     agentConfigMonitoring      `json:"monitoring"`
	Worker         agentConfigWorker          `json:"worker"`
	Cache          agentConfigCache           `json:"cache"`
	ManagedCompute *agentConfigManagedCompute `json:"managedCompute,omitempty"`
}

type agentConfigDatabase struct {
	S2 agentConfigS2 `json:"s2"`
}

type agentConfigS2 struct {
	Basin             string `json:"basin"`
	StreamPrefix      string `json:"streamPrefix"`
	LogApiKey         string `json:"logApiKey"`
	LogStreamPrefix   string `json:"logStreamPrefix"`
	EventApiKey       string `json:"eventApiKey"`
	EventStreamPrefix string `json:"eventStreamPrefix"`
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
	// Registries is always serialized with zero values to explicitly clear the
	// placeholder registry credentials baked into the worker image's embedded
	// default config (config.default.yaml). Private workers never hold static
	// registry credentials; they use gateway-brokered origins instead.
	Registries agentConfigImageRegistries `json:"registries"`
}

type agentConfigImageRegistries struct {
	Docker agentConfigDockerRegistry `json:"docker"`
	S3     agentConfigS3Registry     `json:"s3"`
}

type agentConfigDockerRegistry struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type agentConfigS3Registry struct {
	BucketName     string `json:"bucketName"`
	Region         string `json:"region"`
	AccessKey      string `json:"accessKey"`
	SecretKey      string `json:"secretKey"`
	Endpoint       string `json:"endpoint"`
	ForcePathStyle bool   `json:"forcePathStyle"`
}

type agentConfigMonitoring struct {
	MetricsCollector         string                `json:"metricsCollector"`
	ContainerMetricsInterval string                `json:"containerMetricsInterval"`
	Prometheus               agentConfigPrometheus `json:"prometheus"`
	ContainerCostHook        *agentConfigCostHook  `json:"containerCostHook,omitempty"`
}

type agentConfigCostHook struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
}

type agentConfigManagedCompute struct {
	BillableMarginPct float64            `json:"billableMarginPct"`
	Billing           agentConfigBilling `json:"billing"`
}

type agentConfigBilling struct {
	Endpoint  string `json:"endpoint"`
	AuthToken string `json:"authToken"`
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
	cacheLocality := agentCacheLocality(bootstrap, slot)
	poolMode := slotPoolMode(slot)
	poolRuntime := slotContainerRuntime(slot)

	return agentWorkerConfig{
		ClusterName: types.DefaultAgentName,
		DebugMode:   false,
		PrettyLogs:  true,
		Database: agentConfigDatabase{
			S2: agentConfigS2{
				Basin:             firstNonEmpty(bootstrap.Telemetry.Events.Destination, bootstrap.Telemetry.Logs.Destination),
				StreamPrefix:      bootstrap.Telemetry.StreamPrefix,
				LogApiKey:         bootstrap.Telemetry.Logs.Credential,
				LogStreamPrefix:   bootstrap.Telemetry.Logs.StreamPrefix,
				EventApiKey:       bootstrap.Telemetry.Events.Credential,
				EventStreamPrefix: bootstrap.Telemetry.Events.StreamPrefix,
			},
		},
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
			ContainerCostHook:        costHookConfigForSlot(bootstrap, poolMode),
		},
		Worker: agentConfigWorker{
			HostNetwork:                true,
			UseHostResolvConf:          true,
			ContainerRuntime:           poolRuntime,
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
					Mode:                      poolMode,
					GPUType:                   slot.Gpu,
					ContainerRuntime:          poolRuntime,
					ContainerStartConcurrency: int(slot.ContainerStartConcurrency),
					NetworkPreallocation:      true,
					NetworkSlotPoolSize:       int(slot.NetworkSlotPoolSize),
					RequiresPoolSelector:      poolMode != string(types.PoolModeMarketplace),
					Priority:                  1000,
					CRIUEnabled:               true,
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
				DefaultLocality: cacheLocality,
			},
			Server: &agentConfigCacheServer{
				DiskCacheDir: pathpkg.Join(types.AgentCachePath, sanitizeDockerName(cacheLocality), sanitizeDockerName(slot.MachineId)),
			},
			Client: &agentConfigCacheClient{
				CacheFS: agentConfigCacheFS{
					Enabled:    cacheFSEnabled,
					MountPoint: types.AgentCacheFSMountPath,
				},
			},
		},
		ManagedCompute: managedComputeConfigForSlot(bootstrap, poolMode),
	}
}

// slotPoolMode returns the pool mode the gateway assigned to this worker slot,
// defaulting to private for older gateways that don't send one.
func slotPoolMode(slot *pb.AgentWorkerSlot) string {
	if slot != nil && slot.Mode == string(types.PoolModeMarketplace) {
		return string(types.PoolModeMarketplace)
	}
	return string(types.PoolModePrivate)
}

// slotContainerRuntime returns the runtime the gateway assigned to this slot.
// Marketplace listings can fall back to runc for GPU families that do not run
// correctly under gVisor; the listing and offer surfaces expose that runtime.
func slotContainerRuntime(slot *pb.AgentWorkerSlot) string {
	if slot != nil && slot.ContainerRuntime != "" {
		return slot.ContainerRuntime
	}
	if slotPoolMode(slot) == string(types.PoolModeMarketplace) {
		return types.ContainerRuntimeGvisor.String()
	}
	return types.ContainerRuntimeRunc.String()
}

func costHookConfigForSlot(bootstrap bootstrapConfig, poolMode string) *agentConfigCostHook {
	if poolMode != string(types.PoolModeMarketplace) || bootstrap.Billing == nil || bootstrap.Billing.CostHookEndpoint == "" {
		return nil
	}
	return &agentConfigCostHook{
		Endpoint: bootstrap.Billing.CostHookEndpoint,
		Token:    bootstrap.Billing.CostHookToken,
	}
}

func managedComputeConfigForSlot(bootstrap bootstrapConfig, poolMode string) *agentConfigManagedCompute {
	if poolMode != string(types.PoolModeMarketplace) || bootstrap.Billing == nil || bootstrap.Billing.UsageEndpoint == "" {
		return nil
	}
	return &agentConfigManagedCompute{
		BillableMarginPct: bootstrap.Billing.BillableMarginPct,
		Billing: agentConfigBilling{
			Endpoint:  bootstrap.Billing.UsageEndpoint,
			AuthToken: bootstrap.Billing.UsageToken,
		},
	}
}

func agentCacheLocality(bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot) string {
	poolName := firstNonEmpty(bootstrap.PoolName, types.DefaultAgentName)
	if slot != nil && slot.PoolName != "" {
		poolName = slot.PoolName
	}
	if bootstrap.WorkspaceID != "" {
		return bootstrap.WorkspaceID + "/" + poolName
	}
	return poolName
}

func (c agentWorkerConfig) sanitizedForAgent() agentWorkerConfig {
	c.Image.LocalCacheEnabled = false
	c.Monitoring.MetricsCollector = string(types.MetricsCollectorNone)
	c.Monitoring.Prometheus = agentConfigPrometheus{}
	c.Worker.HostNetwork = true
	c.Worker.UseHostResolvConf = true
	c.Worker.CacheEnabled = true
	for name, pool := range c.Worker.Pools {
		// Agent workers only ever run in private or marketplace mode; anything
		// else collapses to private so cluster-only modes can't leak in.
		if pool.Mode != string(types.PoolModeMarketplace) {
			pool.Mode = string(types.PoolModePrivate)
			pool.RequiresPoolSelector = true
		}
		pool.CRIUEnabled = true
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
