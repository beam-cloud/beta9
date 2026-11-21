package types

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
	corev1 "k8s.io/api/core/v1"
)

type AppConfig struct {
	ClusterName    string               `key:"clusterName" json:"cluster_name"`
	DebugMode      bool                 `key:"debugMode" json:"debug_mode"`
	PrettyLogs     bool                 `key:"prettyLogs" json:"pretty_logs"`
	Database       DatabaseConfig       `key:"database" json:"database"`
	Events         EventsConfig         `key:"events" json:"events"`
	GatewayService GatewayServiceConfig `key:"gateway" json:"gateway_service"`
	FileService    FileServiceConfig    `key:"fileService" json:"file_service"`
	ImageService   ImageServiceConfig   `key:"imageService" json:"image_service"`
	Storage        StorageConfig        `key:"storage" json:"storage"`
	Worker         WorkerConfig         `key:"worker" json:"worker"`
	Scheduling     SchedulingConfig     `key:"scheduling" json:"scheduling"`
	Providers      ProviderConfig       `key:"providers" json:"providers"`
	Tailscale      TailscaleConfig      `key:"tailscale" json:"tailscale"`
	Monitoring     MonitoringConfig     `key:"monitoring" json:"monitoring"`
	ManagedCompute ManagedComputeConfig `key:"managedCompute" json:"managed_compute"`
	Abstractions   AbstractionConfig    `key:"abstractions" json:"abstractions"`
	Cache          cache.Config         `key:"cache" json:"cache"`
	// ManagedEndpoints configures the platform-owned inference endpoints
	// (GitOps-driven stubs filled onto spare GPU capacity, served under /v1).
	ManagedEndpoints ManagedEndpointsConfig `key:"managedEndpoints" json:"managed_endpoints"`
}

// Validate reports settings that cannot mean what was intended, and stops the
// gateway starting. So it refuses only what a config that ran on the previous
// release cannot hold: anything else is logged where it is read, and an
// upgrade never fails on a config that worked.
func (c AppConfig) Validate() error {
	pendingLimit := c.Worker.CleanupPendingWorkerAgeLimit
	for _, name := range slices.Sorted(maps.Keys(c.Worker.Pools)) {
		pool := c.Worker.Pools[name]
		if err := pool.Validate(); err != nil {
			return fmt.Errorf("worker pool %q: %w", name, err)
		}
		if stalledAfter := time.Duration(pool.StalledAfter); pendingLimit > 0 && stalledAfter >= pendingLimit {
			return fmt.Errorf("worker pool %q: stalledAfter is %s, but a worker that is still Pending after %s is removed (worker.cleanupPendingWorkerAgeLimit), so the pool would never count as stalled",
				name, stalledAfter, pendingLimit)
		}
	}
	switch {
	case c.Providers.MaxHourlyCents < 0:
		return fmt.Errorf("providers.maxHourlyCents cannot be negative: leave it out for no cap")
	case c.Providers.MaxDailyCents < 0:
		return fmt.Errorf("providers.maxDailyCents cannot be negative: leave it out for no cap")
	}
	return nil
}

// ManagedEndpointsConfig is the cluster-level configuration for the managed
// endpoints platform. Only Enabled is required to turn it on; every other
// value has a production default (see ApplyDefaults). Endpoint stubs are
// owned by the cluster admin workspace and deployed by the endpoints repo's CI.
type ManagedEndpointsConfig struct {
	Enabled     bool                          `key:"enabled" json:"enabled"`
	RoutePrefix string                        `key:"routePrefix" json:"route_prefix"`
	Preemption  ManagedEndpointsPreemption    `key:"preemption" json:"preemption"`
	Reconcile   ManagedEndpointsReconcile     `key:"reconcile" json:"reconcile"`
	Routing     ManagedEndpointsRoutingConfig `key:"routing" json:"routing"`
	// MaxReplicaMemoryMiB can raise the stub memory limit for managed endpoints.
	MaxReplicaMemoryMiB uint32 `key:"maxReplicaMemoryMiB" json:"max_replica_memory_mib"`
	// HeartbeatInterval is what replicas are told to heartbeat at.
	HeartbeatInterval time.Duration `key:"heartbeatInterval" json:"heartbeat_interval"`
	// ReplicaStaleAfter marks replicas failed when no heartbeat/probe arrives.
	ReplicaStaleAfter time.Duration `key:"replicaStaleAfter" json:"replica_stale_after"`
	// ProviderRevenueShare is the fraction of billed token revenue credited to
	// the workspace whose contributed machine served the request.
	ProviderRevenueShare float64 `key:"providerRevenueShare" json:"provider_revenue_share"`
}

type ManagedEndpointsPreemption struct {
	Enabled bool `key:"enabled" json:"enabled"`
}

type ManagedEndpointsReconcile struct {
	Interval time.Duration `key:"interval" json:"interval"`
	// FailureBackoff delays rescheduling after a replica fails.
	FailureBackoff time.Duration `key:"failureBackoff" json:"failure_backoff"`
}

type ManagedEndpointsRoutingConfig struct {
	// Cluster-wide in-flight caps, leased in Redis per request across every
	// gateway; the engines' own bound is the replicas' MaxConcurrency.
	PerWorkspaceConcurrency uint32 `key:"perWorkspaceConcurrency" json:"per_workspace_concurrency"`
	PerEndpointConcurrency  uint32 `key:"perEndpointConcurrency" json:"per_endpoint_concurrency"`
}

// ApplyDefaults fills zero values with production defaults so consumers can
// read the config directly.
func (c *ManagedEndpointsConfig) ApplyDefaults() {
	c.RoutePrefix = "/" + strings.Trim(strings.TrimSpace(c.RoutePrefix), "/")
	if c.RoutePrefix == "/" {
		c.RoutePrefix = "/v1"
	}
	// The harness receives the interval in whole seconds, and a replica must
	// be allowed to miss at least one heartbeat before it is considered stale.
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	c.HeartbeatInterval = max(c.HeartbeatInterval.Round(time.Second), time.Second)
	if c.ProviderRevenueShare <= 0 || c.ProviderRevenueShare > 1 {
		c.ProviderRevenueShare = 0.7
	}
	if c.ReplicaStaleAfter <= 0 {
		c.ReplicaStaleAfter = 3 * c.HeartbeatInterval
	}
	c.ReplicaStaleAfter = max(c.ReplicaStaleAfter, 2*c.HeartbeatInterval)
	for _, d := range []struct {
		v   *time.Duration
		def time.Duration
	}{
		{&c.Reconcile.Interval, 10 * time.Second},
		{&c.Reconcile.FailureBackoff, 30 * time.Second},
	} {
		if *d.v <= 0 {
			*d.v = d.def
		}
	}
}

// WorkerPoolManagedEndpointsConfig opts a pool into hosting endpoint replicas.
type WorkerPoolManagedEndpointsConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`
}

type DatabaseConfig struct {
	Redis    RedisConfig    `key:"redis" json:"redis"`
	Postgres PostgresConfig `key:"postgres" json:"postgres"`
	S2       S2Config       `key:"s2" json:"s2"`
}

type RedisMode string

const (
	RedisModeSingle  RedisMode = "single"
	RedisModeCluster RedisMode = "cluster"
)

type RedisConfig struct {
	Addrs              []string      `key:"addrs" json:"addrs"`
	Mode               RedisMode     `key:"mode" json:"mode"`
	ClientName         string        `key:"clientName" json:"client_name"`
	EnableTLS          bool          `key:"enableTLS" json:"enable_tls"`
	InsecureSkipVerify bool          `key:"insecureSkipVerify" json:"insecure_skip_verify"`
	MinIdleConns       int           `key:"minIdleConns" json:"min_idle_conns"`
	MaxIdleConns       int           `key:"maxIdleConns" json:"max_idle_conns"`
	ConnMaxIdleTime    time.Duration `key:"connMaxIdleTime" json:"conn_max_idle_time"`
	ConnMaxLifetime    time.Duration `key:"connMaxLifetime" json:"conn_max_lifetime"`
	DialTimeout        time.Duration `key:"dialTimeout" json:"dial_timeout"`
	ReadTimeout        time.Duration `key:"readTimeout" json:"read_timeout"`
	WriteTimeout       time.Duration `key:"writeTimeout" json:"write_timeout"`
	MaxRedirects       int           `key:"maxRedirects" json:"max_redirects"`
	MaxRetries         int           `key:"maxRetries" json:"max_retries"`
	PoolSize           int           `key:"poolSize" json:"pool_size"`
	Username           string        `key:"username" json:"username"`
	Password           string        `key:"password" json:"password"`
	RouteByLatency     bool          `key:"routeByLatency" json:"route_by_latency"`
}

type PostgresConfig struct {
	Host          string `key:"host" json:"host"`
	Port          int    `key:"port" json:"port"`
	Name          string `key:"name" json:"name"`
	Username      string `key:"username" json:"username"`
	Password      string `key:"password" json:"password"`
	TimeZone      string `key:"timezone" json:"timezone"`
	EnableTLS     bool   `key:"enableTLS" json:"enable_tls"`
	EncryptionKey string `key:"encryptionKey" json:"encryption_key"`
	// EncryptCredentials stores token keys and workspace signing keys encrypted
	// with EncryptionKey and encrypts existing rows. Gateways without support
	// for encrypted rows cannot authenticate them, so enable it after rollout,
	// and keep it enabled: sealed token keys are only found by hash.
	EncryptCredentials bool `key:"encryptCredentials" json:"encrypt_credentials"`
	// MaxOpenConns caps this process's database connections; 0 is unbounded.
	MaxOpenConns int `key:"maxOpenConns" json:"max_open_conns"`
	// WarmConnections opens a bounded pool before accepting requests. Zero
	// disables prewarming; the target is capped at MaxOpenConns and 128.
	WarmConnections int `key:"warmConnections" json:"warm_connections"`
	// VMBurstBatchSize groups simultaneous VM and stub mutations into durable
	// statement. Values <= 1 disable batching; the collection window is 1 ms.
	VMBurstBatchSize int `key:"vmBurstBatchSize" json:"vm_burst_batch_size"`
}

type S2Config struct {
	ApiKey            string `key:"apiKey" json:"api_key"`
	Basin             string `key:"basin" json:"basin"`
	StreamPrefix      string `key:"streamPrefix" json:"stream_prefix"`
	LogApiKey         string `key:"logApiKey" json:"log_api_key"`
	LogStreamPrefix   string `key:"logStreamPrefix" json:"log_stream_prefix"`
	EventApiKey       string `key:"eventApiKey" json:"event_api_key"`
	EventStreamPrefix string `key:"eventStreamPrefix" json:"event_stream_prefix"`
}

type EventsConfig struct {
	Callbacks []EventCallbackConfig `key:"callbacks" json:"callbacks"`
}

type EventCallbackConfig struct {
	URL        string            `key:"url" json:"url"`
	Format     string            `key:"format" json:"format"`
	EventTypes []string          `key:"eventTypes" json:"event_types"`
	Headers    map[string]string `key:"headers" json:"headers"`
	Timeout    time.Duration     `key:"timeout" json:"timeout"`
}

type GRPCConfig struct {
	ExternalPort   int    `key:"externalPort" json:"external_port"`
	ExternalHost   string `key:"externalHost" json:"external_host"`
	TLS            bool   `key:"tls" json:"tls"`
	Port           int    `key:"port" json:"port"`
	MaxRecvMsgSize int    `key:"maxRecvMsgSize" json:"max_recv_msg_size"`
	MaxSendMsgSize int    `key:"maxSendMsgSize" json:"max_send_msg_size"`
}

type HTTPConfig struct {
	ExternalPort     int        `key:"externalPort" json:"external_port"`
	ExternalHost     string     `key:"externalHost" json:"external_host"`
	TLS              bool       `key:"tls" json:"tls"`
	Port             int        `key:"port" json:"port"`
	EnablePrettyLogs bool       `key:"enablePrettyLogs" json:"enable_pretty_logs"`
	CORS             CORSConfig `key:"cors" json:"cors"`
}

func (h *HTTPConfig) GetExternalURL() string {
	baseUrl := "http"
	if h.TLS {
		baseUrl += "s"
	}
	baseUrl += "://" + h.ExternalHost

	if h.ExternalPort != 80 && h.ExternalPort != 443 {
		baseUrl += fmt.Sprintf(":%d", h.ExternalPort)
	}

	return baseUrl
}

type CORSConfig struct {
	AllowedOrigins []string `key:"allowOrigins" json:"allow_origins"`
	AllowedMethods []string `key:"allowMethods" json:"allow_methods"`
	AllowedHeaders []string `key:"allowHeaders" json:"allow_headers"`
}

type StubLimits struct {
	Cpu          uint64             `key:"cpu" json:"cpu"`
	Memory       uint64             `key:"memory" json:"memory"`
	MaxReplicas  uint64             `key:"maxReplicas" json:"max_replicas"`
	MaxGpuCount  uint32             `key:"maxGpuCount" json:"max_gpu_count"`
	GPUBlackList GPUBlackListConfig `key:"gpuBlackList" json:"gpu_black_list"`
}

type GPUBlackListConfig struct {
	GPUTypes []string `key:"gpuTypes" json:"gpu_types"`
	Message  string   `key:"message" json:"message"`
}

// ValidateCpuAndMemory enforces limits on CPU and memory (min and max)
func ValidateCpuAndMemory(cpu, memory int64, stubLimits StubLimits) (valid bool, errorMsg string) {
	if cpu <= 0 {
		return false, "CPU must be greater than 0."
	}

	if memory <= 0 {
		return false, "Memory must be greater than 0."
	}

	// Enforce upper limits
	if memory > int64(stubLimits.Memory) {
		return false, fmt.Sprintf("Memory must be %dGiB or less.", stubLimits.Memory/1024)
	}

	if cpu > int64(stubLimits.Cpu) {
		return false, fmt.Sprintf("CPU must be %d or less.", stubLimits.Cpu)
	}

	return true, ""
}

type ContainerCostHookConfig struct {
	Endpoint string `key:"endpoint" json:"endpoint"`
	Token    string `key:"token" json:"token"`
}

type GatewayServiceConfig struct {
	TaskCompletion TaskCompletionConfig `key:"taskCompletion" json:"task_completion"`
	// DeploymentRevision orders configuration publications across rolling
	// gateway replicas. Helm rollbacks receive a new revision as well.
	DeploymentRevision int64          `key:"deploymentRevision" json:"deployment_revision"`
	MCPOAuth           MCPOAuthConfig `key:"mcpOAuth" json:"mcp_oauth"`
	Host               string         `key:"host" json:"host"`
	InvokeURLType      string         `key:"invokeURLType" json:"invoke_url_type"`
	GRPC               GRPCConfig     `key:"grpc" json:"grpc"`
	HTTP               HTTPConfig     `key:"http" json:"http"`
	ShutdownTimeout    time.Duration  `key:"shutdownTimeout" json:"shutdown_timeout"`
	StubLimits         StubLimits     `key:"stubLimits" json:"stub_limits"`
	// Applied to workspaces that have no explicit concurrency limit row.
	// Both values must be > 0 for the default to take effect; otherwise
	// workspaces without a row remain unlimited (legacy behavior).
	DefaultConcurrencyLimit DefaultConcurrencyLimitConfig `key:"defaultConcurrencyLimit" json:"default_concurrency_limit"`
	// CreditGate decides whether a workspace has prepaid credit to run
	// serverless workloads. Independent of the concurrency limit, which is a
	// capacity cap, not a paywall.
	CreditGate CreditGateConfig `key:"creditGate" json:"credit_gate"`
}

type TaskCompletionConfig struct {
	Timeout             time.Duration `key:"timeout" json:"timeout"`
	ResultUploadTimeout time.Duration `key:"resultUploadTimeout" json:"result_upload_timeout"`
}

// OAuth is opt-in. The public issuer/resource are canonical identifiers; the
// introspection URL can use the cluster's private account-service endpoint.
type MCPOAuthConfig struct {
	Issuer              string `key:"issuer" json:"issuer"`
	Resource            string `key:"resource" json:"resource"`
	IntrospectionURL    string `key:"introspectionURL" json:"introspection_url"`
	IntrospectionSecret string `key:"introspectionSecret" json:"-"`
}

type DefaultConcurrencyLimitConfig struct {
	CPUMillicores uint32 `key:"cpuMillicores" json:"cpu_millicores"`
	GPUCount      uint32 `key:"gpuCount" json:"gpu_count"`
}

const (
	CreditGateModeNoop = "noop"
	CreditGateModeHTTP = "http"
)

// CreditGateConfig configures the scheduler's prepaid-credit check.
//
// In "http" mode container admission and the periodic enforcement sweep check
// whether the workspace still has credit. Decisions are cached in Redis for
// CacheTTL. Stale approvals are refreshed asynchronously and may be reused for
// up to StaleTTL; stale denials and cache misses are refreshed synchronously.
// With no usable cached decision FailOpen decides if billing is unavailable.
type CreditGateConfig struct {
	Mode            string        `key:"mode" json:"mode"`
	Endpoint        string        `key:"endpoint" json:"endpoint"`
	AuthToken       string        `key:"authToken" json:"auth_token"`
	Timeout         time.Duration `key:"timeout" json:"timeout"`
	CacheTTL        time.Duration `key:"cacheTTL" json:"cache_ttl"`
	StaleTTL        time.Duration `key:"staleTTL" json:"stale_ttl"`
	FailOpen        *bool         `key:"failOpen" json:"fail_open"`
	EnforceInterval time.Duration `key:"enforceInterval" json:"enforce_interval"`
}

// Enabled reports whether the gate consults billing at all: http mode with an
// endpoint. Anything else allows everything.
func (c CreditGateConfig) Enabled() bool {
	return strings.EqualFold(strings.TrimSpace(c.Mode), CreditGateModeHTTP) && strings.TrimSpace(c.Endpoint) != ""
}

// TimeoutOrDefault bounds one billing call. The check may run an off-session
// Stripe charge (auto top-up) before answering, so it needs Stripe latency
// headroom; a cached decision serves the next 30s either way.
func (c CreditGateConfig) TimeoutOrDefault() time.Duration {
	if c.Timeout <= 0 {
		return 10 * time.Second
	}
	return c.Timeout
}

func (c CreditGateConfig) CacheTTLOrDefault() time.Duration {
	if c.CacheTTL <= 0 {
		return 30 * time.Second
	}
	return c.CacheTTL
}

func (c CreditGateConfig) StaleTTLOrDefault() time.Duration {
	if c.StaleTTL <= 0 {
		return 10 * time.Minute
	}
	return c.StaleTTL
}

// FailOpenOrDefault: allow when billing cannot be reached and nothing is
// cached, unless explicitly set to false.
func (c CreditGateConfig) FailOpenOrDefault() bool {
	if c.FailOpen == nil {
		return true
	}
	return *c.FailOpen
}

func (c CreditGateConfig) EnforceIntervalOrDefault() time.Duration {
	if c.EnforceInterval <= 0 {
		return time.Minute
	}
	return c.EnforceInterval
}

type FileServiceConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`
}

type ClipVersion uint32

const (
	ClipVersion1 ClipVersion = 1
	ClipVersion2 ClipVersion = 2
)

type ImageServiceConfig struct {
	LocalCacheEnabled              bool                           `key:"localCacheEnabled" json:"local_cache_enabled"`
	RegistryStore                  string                         `key:"registryStore" json:"registry_store"`
	RegistryCredentialProviderName string                         `key:"registryCredentialProvider" json:"registry_credential_provider_name"`
	Registries                     ImageRegistriesConfig          `key:"registries" json:"registries"`
	PythonVersion                  string                         `key:"pythonVersion" json:"python_version"`
	EnableTLS                      bool                           `key:"enableTLS" json:"enable_tls"`
	BuildContainerCpu              int64                          `key:"buildContainerCpu" json:"build_container_cpu"`
	BuildContainerMemory           int64                          `key:"buildContainerMemory" json:"build_container_memory"`
	BuildContainerPoolSelector     string                         `key:"buildContainerPoolSelector" json:"build_container_pool_selector"`
	Runner                         RunnerConfig                   `key:"runner" json:"runner"`
	ArchiveNanosecondsPerByte      int64                          `key:"archiveNanosecondsPerByte" json:"archive_nanoseconds_per_byte"`
	ClipVersion                    uint32                         `key:"clipVersion" json:"clip_version"`
	BuildRegistry                  string                         `key:"buildRegistry" json:"build_registry"`
	BuildRepositoryName            string                         `key:"buildRepositoryName" json:"build_repository_name"`
	BuildRegistryCredentials       BuildRegistryCredentialsConfig `key:"buildRegistryCredentials" json:"build_registry_credentials"`
	BuildRegistryInsecure          bool                           `key:"buildRegistryInsecure" json:"build_registry_insecure"`
	// LayeredBuilds publishes a build's changes as layers packed straight from
	// the working container instead of buildah commit + push + re-index.
	// Dockerfiles outside the supported subset still go through buildah bud.
	LayeredBuilds bool `key:"layeredBuilds" json:"layered_builds"`
	// BuildApt tunes apt inside the RUN steps of a layered build. The settings
	// are bind-mounted into each step and never land in the image.
	BuildApt BuildAptConfig `key:"buildApt" json:"build_apt"`
	// BuildLayerCacheMaxPct bounds the persistent buildah layer store to this
	// fraction of the filesystem it sits on; after a build, the oldest images
	// are removed until it fits. Zero disables the bound.
	BuildLayerCacheMaxPct float64 `key:"buildLayerCacheMaxPct" json:"build_layer_cache_max_pct"`
}

// EffectiveBuildRegistry resolves the publishing registry for gateway and agent workers.
func (c ImageServiceConfig) EffectiveBuildRegistry() string {
	if c.BuildRegistry != "" {
		return c.BuildRegistry
	}
	if c.Runner.BaseImageRegistry != "" {
		return c.Runner.BaseImageRegistry
	}
	return "localhost"
}

// BuildAptConfig is applied to apt during image builds whose base image has
// apt installed. Public archives are slow or stall from some regions
// (archive.ubuntu.com measured at ~90 KB/s from us-east-1, with apt's default
// 120 s timeouts turning a stalled connection into a many-minute build), so a
// build gets short timeouts with retries and, when configured, a nearby
// mirror or caching proxy.
type BuildAptConfig struct {
	// Mirror replaces http://archive.ubuntu.com/ubuntu and
	// http://security.ubuntu.com/ubuntu in the base image's sources for the
	// duration of each RUN step, e.g. http://mirrors.edge.kernel.org/ubuntu.
	Mirror string `key:"mirror" json:"mirror"`
	// Proxy is set as Acquire::http::Proxy (an apt-cacher-ng, for instance).
	Proxy string `key:"proxy" json:"proxy"`
	// TimeoutS is Acquire::http::Timeout / Acquire::https::Timeout; 0 keeps apt's default.
	TimeoutS int `key:"timeoutS" json:"timeout_s"`
	// Retries is Acquire::Retries; 0 keeps apt's default.
	Retries int `key:"retries" json:"retries"`
}

// BuildRegistryCredentialsConfig stores credentials for generating tokens for the build registry
// Supports: aws (ECR), gcp (GCR/GAR), basic (username/password), token (API keys)
type BuildRegistryCredentialsConfig struct {
	// Type of credentials: "aws", "gcp", "azure", "basic", "token"
	Type string `key:"type" json:"type"`
	// Credentials is a map of credential key-value pairs
	// For AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_SESSION_TOKEN (optional)
	// For GCP: GCP_ACCESS_TOKEN or GOOGLE_APPLICATION_CREDENTIALS
	// For Azure: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
	// For basic auth: USERNAME, PASSWORD
	// For token auth: NGC_API_KEY, GITHUB_TOKEN, DOCKERHUB_TOKEN
	Credentials map[string]string `key:"credentials" json:"credentials"`
}

type ImageRegistriesConfig struct {
	Docker DockerImageRegistryConfig `key:"docker" json:"docker"`
	S3     S3ImageRegistryConfig     `key:"s3" json:"s3"`
}

type DockerImageRegistryConfig struct {
	Username string `key:"username" json:"username"`
	Password string `key:"password" json:"password"`
}

type S3ImageRegistryConfig struct {
	BucketName     string `key:"bucketName" json:"bucket_name"`
	AccessKey      string `key:"accessKey" json:"access_key"`
	SecretKey      string `key:"secretKey" json:"secret_key"`
	Region         string `key:"region" json:"region"`
	Endpoint       string `key:"endpoint" json:"endpoint"`
	ForcePathStyle bool   `key:"forcePathStyle" json:"force_path_style"`
}

type RunnerConfig struct {
	BaseImageName     string                 `key:"baseImageName" json:"base_image_name"`
	BaseImageRegistry string                 `key:"baseImageRegistry" json:"base_image_registry"`
	Tags              map[string]string      `key:"tags" json:"tags"`
	PythonStandalone  PythonStandaloneConfig `key:"pythonStandalone" json:"python_standalone"`
}

type PythonStandaloneConfig struct {
	Versions              map[string]string `key:"versions" json:"versions"`
	InstallScriptTemplate string            `key:"installScriptTemplate" json:"install_script_template"`
}

type StorageConfig struct {
	WorkspaceStorage WorkspaceStorageConfig `key:"workspaceStorage" json:"workspace_storage"`
}

type WorkspaceStorageConfig struct {
	BaseMountPath               string `key:"baseMountPath" json:"base_mount_path"`
	DefaultStorageMode          string `key:"defaultStorageMode" json:"default_storage_mode"`
	DefaultBucketPrefix         string `key:"defaultBucketPrefix" json:"default_bucket_prefix"`
	DefaultAccessKey            string `key:"defaultAccessKey" json:"default_access_key"`
	DefaultSecretKey            string `key:"defaultSecretKey" json:"default_secret_key"`
	DefaultEndpointUrl          string `key:"defaultEndpointUrl" json:"default_endpoint_url"`
	DefaultPresignedEndpointUrl string `key:"defaultPresignedEndpointUrl" json:"default_presigned_endpoint_url"`
	DefaultRegion               string `key:"defaultRegion" json:"default_region"`

	// Storage mode configs
	Geese GeeseConfig `key:"geese" json:"geese"`
}

type GeeseConfig struct {
	Debug                  bool          `key:"debug" json:"debug"`                                // --debug
	FsyncOnClose           bool          `key:"fsyncOnClose" json:"fsync_on_close"`                // --fsync-on-close
	MountOptions           []string      `key:"mountOptions" json:"mount_options"`                 // --mount-options
	MemoryLimit            int64         `key:"memoryLimit" json:"memory_limit"`                   // --memory-limit
	MaxFlushers            int           `key:"maxFlushers" json:"max_flushers"`                   // --max-flushers
	MaxParallelParts       int           `key:"maxParallelParts" json:"max_parallel_parts"`        // --max-parallel-parts
	PartSizeMB             int64         `key:"partSizeMB" json:"part_size_mb"`                    // multipart upload part size for the first 1000 parts (default 64)
	HTTPTimeout            time.Duration `key:"httpTimeout" json:"http_timeout"`                   // --http-timeout
	ReadAheadKB            int           `key:"readAheadKB" json:"read_ahead_kb"`                  // --read-ahead-kb
	ReadAheadLargeKB       int           `key:"readAheadLargeKB" json:"read_ahead_large_kb"`       // --read-ahead-large-kb
	ReadAheadParallelKB    int           `key:"readAheadParallelKB" json:"read_ahead_parallel_kb"` // --read-ahead-parallel-kb
	FuseReadAheadKB        int           `key:"fuseReadAheadKB" json:"fuse_read_ahead_kb"`         // --fuse-read-ahead-kb
	DirMode                string        `key:"dirMode" json:"dir_mode"`                           // --dir-mode, e.g., "0777"
	FileMode               string        `key:"fileMode" json:"file_mode"`                         // --file-mode, e.g., "0666"
	ListType               int           `key:"listType" json:"list_type"`                         // --list-type
	AccessKey              string        `key:"accessKey" json:"access_key"`
	SecretKey              string        `key:"secretKey" json:"secret_key"`
	EndpointUrl            string        `key:"endpointURL" json:"endpoint_url"` // --endpoint
	BucketName             string        `key:"bucketName" json:"bucket_name"`
	Region                 string        `key:"region" json:"region"`
	DisableVolumeCaching   bool          `key:"disableVolumeCaching" json:"disable_volume_caching"`
	StagedWriteModeEnabled bool          `key:"stagedWriteModeEnabled" json:"staged_write_mode_enabled"`
	StagedWritePath        string        `key:"stagedWritePath" json:"staged_write_path"`
	StagedWriteDebounce    time.Duration `key:"stagedWriteDebounce" json:"staged_write_debounce"`
	CacheStreamingEnabled  bool          `key:"cacheStreamingEnabled" json:"cache_streaming_enabled"`
	CacheDirectIO          bool          `key:"cacheDirectIO" json:"cache_direct_io"`
	CacheThroughEnabled    bool          `key:"cacheThroughEnabled" json:"cache_through_enabled"`
}

// @go2proto
type MountPointConfig struct {
	BucketName     string `json:"s3_bucket"`
	AccessKey      string `json:"access_key"`
	SecretKey      string `json:"secret_key"`
	EndpointURL    string `json:"bucket_url"`
	Region         string `json:"region"`
	ReadOnly       bool   `json:"read_only"`
	ForcePathStyle bool   `json:"force_path_style"`
}

func (m MountPointConfig) WithoutCredentials() MountPointConfig {
	m.AccessKey = ""
	m.SecretKey = ""
	return m
}

func MountPointCredentialKey(mountPath, bucketName string) string {
	return mountPath + "\x00" + bucketName
}

func (m *MountPointConfig) ToProto() *pb.MountPointConfig {
	return &pb.MountPointConfig{
		BucketName:     m.BucketName,
		AccessKey:      m.AccessKey,
		SecretKey:      m.SecretKey,
		EndpointUrl:    m.EndpointURL,
		Region:         m.Region,
		ReadOnly:       m.ReadOnly,
		ForcePathStyle: m.ForcePathStyle,
	}
}

func NewMountPointConfigFromProto(in *pb.MountPointConfig) *MountPointConfig {
	return &MountPointConfig{
		BucketName:     in.BucketName,
		AccessKey:      in.AccessKey,
		SecretKey:      in.SecretKey,
		EndpointURL:    in.EndpointUrl,
		Region:         in.Region,
		ReadOnly:       in.ReadOnly,
		ForcePathStyle: in.ForcePathStyle,
	}
}

// JobResourceOverheadConfig is extra CPU/memory added to the worker pod's
// requests and limits on top of the capacity the scheduler hands out to
// containers. When jobResourcesEnforced is on, the pod limit is otherwise
// exactly the schedulable capacity, so a fully packed worker leaves nothing
// for the worker process itself (FUSE image/volume servers, log shipping,
// runtime) and container I/O gets throttled with the containers.
type JobResourceOverheadConfig struct {
	CPU    string `key:"cpu" json:"cpu"`
	Memory string `key:"memory" json:"memory"`
}

type WorkerConfig struct {
	Pools                        map[string]WorkerPoolConfig   `key:"pools" json:"pools"`
	HostNetwork                  bool                          `key:"hostNetwork" json:"host_network"`
	UseGatewayServiceHostname    bool                          `key:"useGatewayServiceHostname" json:"use_gateway_service_hostname"`
	UseHostResolvConf            bool                          `key:"useHostResolvConf" json:"use_host_resolv_conf"`
	ImageTag                     string                        `key:"imageTag" json:"image_tag"`
	ImageName                    string                        `key:"imageName" json:"image_name"`
	ImageRegistry                string                        `key:"imageRegistry" json:"image_registry"`
	ImagePullSecrets             []string                      `key:"imagePullSecrets" json:"image_pull_secrets"`
	Namespace                    string                        `key:"namespace" json:"namespace"`
	ServiceAccountName           string                        `key:"serviceAccountName" json:"service_account_name"`
	JobResourcesEnforced         bool                          `key:"jobResourcesEnforced" json:"job_resources_enforced"`
	JobResourceOverhead          JobResourceOverheadConfig     `key:"jobResourceOverhead" json:"job_resource_overhead"`
	ContainerResourceLimits      ContainerResourceLimitsConfig `key:"containerResourceLimits" json:"container_resource_limits"`
	DefaultWorkerCPURequest      int64                         `key:"defaultWorkerCPURequest" json:"default_worker_cpu_request"`
	DefaultWorkerMemoryRequest   int64                         `key:"defaultWorkerMemoryRequest" json:"default_worker_memory_request"`
	ImagePVCName                 string                        `key:"imagePVCName" json:"image_pvc_name"`
	CleanupWorkerInterval        time.Duration                 `key:"cleanupWorkerInterval" json:"cleanup_worker_interval"`
	CleanupPendingWorkerAgeLimit time.Duration                 `key:"cleanupPendingWorkerAgeLimit" json:"cleanup_pending_worker_age_limit"`
	TerminationGracePeriod       int64                         `key:"terminationGracePeriod"`
	CacheEnabled                 bool                          `key:"cacheEnabled" json:"cache_enabled"`
	CRIU                         CRIUConfig                    `key:"criu" json:"criu"`
	TmpSizeLimit                 string                        `key:"tmpSizeLimit" json:"tmp_size_limit"`
	ContainerLogLinesPerHour     int                           `key:"containerLogLinesPerHour" json:"container_log_lines_per_hour"`
	ContainerRuntime             string                        `key:"containerRuntime" json:"container_runtime"`
	// HeadroomWorkerMaxAge bounds how long pool headroom alone keeps an idle
	// worker up. Zero disables the bound.
	HeadroomWorkerMaxAge time.Duration `key:"headroomWorkerMaxAge" json:"headroom_worker_max_age"`
	// MaxAge bounds any non-persistent worker's lifetime, busy or idle: past it
	// the worker disables scheduling, lets running containers finish and exits.
	// Zero disables the bound.
	MaxAge time.Duration `key:"maxAge" json:"max_age"`

	Egress EgressConfig `key:"egress" json:"egress"`
}

// Listener port zero allocates a free worker-local port.
type EgressConfig struct {
	Enabled                    bool `key:"enabled" json:"enabled"`
	TLSPort                    int  `key:"tlsPort" json:"tls_port"`
	HTTPPort                   int  `key:"httpPort" json:"http_port"`
	OtherPort                  int  `key:"otherPort" json:"other_port"`
	MaxConnectionsPerContainer int  `key:"maxConnectionsPerContainer" json:"max_connections_per_container"`
}

type ContainerResourceLimitsConfig struct {
	CPUEnforced         bool `key:"cpuEnforced" json:"cpu_enforced"`
	CPUAffinityEnforced bool `key:"cpuAffinityEnforced" json:"cpu_affinity_enforced"`
	MemoryEnforced      bool `key:"memoryEnforced" json:"memory_enforced"`
}

type PoolMode string

type WorkerPoolManagementSource string
type WorkerPoolController string

var (
	PoolModeLocal    PoolMode = "local"
	PoolModeExternal PoolMode = "external"
	PoolModePrivate  PoolMode = "private"
	// Provider pools are workspace-contributed machines that serve managed
	// endpoints only; the workspace earns a share of the tokens sold on them.
	PoolModeProvider PoolMode = "provider"
)

const (
	WorkerPoolManagementSourceConfig WorkerPoolManagementSource = "config"
	WorkerPoolManagementSourceAPI    WorkerPoolManagementSource = "api"

	WorkerPoolControllerLocal WorkerPoolController = "local"
	WorkerPoolControllerAgent WorkerPoolController = "agent"
)

// AgentHosted reports whether pools of this mode run on agent-managed
// machines outside the cluster. Such workers hold no static credentials and
// use gateway-brokered access for images and caches.
func (m PoolMode) AgentHosted() bool {
	return m == PoolModePrivate || m == PoolModeProvider
}

type WorkerPoolConfig struct {
	GPUType string `key:"gpuType" json:"gpu_type"`
	// AlsoServes lists other GPU types whose requests may overflow into this
	// pool once the pools of that type have no room. It is the whole of
	// fallback configuration: a request for A10G runs on pools whose gpuType
	// is A10G, then on pools that list A10G here. A pool that requires a
	// selector may list its own gpuType, to take the requests for it that
	// name no pool the same way.
	AlsoServes                []string                          `key:"alsoServes" json:"also_serves,omitempty"`
	Runtime                   string                            `key:"runtime" json:"runtime"`                                 // Kubernetes RuntimeClass for pod (e.g., "nvidia")
	ContainerRuntime          string                            `key:"containerRuntime" json:"container_runtime"`              // Pool-specific container runtime: "runc", "gvisor", or "microvm"
	ContainerRuntimeConfig    RuntimeConfig                     `key:"containerRuntimeConfig" json:"container_runtime_config"` // Pool-specific container runtime configuration
	CPUAffinityEnforced       bool                              `key:"cpuAffinityEnforced" json:"cpu_affinity_enforced"`
	CPUShareMultiplier        CPUShareMultiplier                `key:"cpuShareMultiplier" json:"cpu_share_multiplier,omitempty"`
	GPUVirtualized            bool                              `key:"gpuVirtualized" json:"gpu_virtualized"`
	ContainerStartConcurrency int                               `key:"containerStartConcurrency" json:"container_start_concurrency"`
	NetworkPreallocation      *bool                             `key:"networkPreallocation" json:"network_preallocation"`
	NetworkSlotPoolSize       int                               `key:"networkSlotPoolSize" json:"network_slot_pool_size"`
	Mode                      PoolMode                          `key:"mode" json:"mode"`
	JobSpec                   WorkerPoolJobSpecConfig           `key:"jobSpec" json:"job_spec"`
	PoolSizing                WorkerPoolJobSpecPoolSizingConfig `key:"poolSizing" json:"pool_sizing"`
	RequiresPoolSelector      bool                              `key:"requiresPoolSelector" json:"requires_pool_selector"`
	Priority                  int32                             `key:"priority" json:"priority"`
	Preemptable               bool                              `key:"preemptable" json:"preemptable"`
	CRIUEnabled               bool                              `key:"criuEnabled" json:"criu_enabled"`
	TmpSizeLimit              string                            `key:"tmpSizeLimit" json:"tmp_size_limit"`
	ConfigGroup               string                            `key:"configGroup" json:"config_group"`
	StoragePath               string                            `key:"storagePath" json:"storage_path"` // Host root for workspace mounts, checkpoints, and durable disks.
	StorageMode               string                            `key:"storageMode" json:"storage_mode"`
	ImagesPath                string                            `key:"imagesPath" json:"images_path"`              // Host path backing the worker's /images volume; agent pools fall back to the installer state dir.
	DurableDisksPath          string                            `key:"durableDisksPath" json:"durable_disks_path"` // Host path backing durable disks; agent pools fall back to storagePath or the installer state dir.
	Cache                     WorkerPoolCacheConfig             `key:"cache" json:"cache"`
	HourlyCostCents           int64                             `key:"hourlyCostCents" json:"hourly_cost_cents"` // Cost of one machine in this pool, in cents per hour (e.g. 120 for $1.20/hr)
	// ManagedEndpoints lets platform endpoint replicas fill this pool's spare GPUs.
	ManagedEndpoints WorkerPoolManagedEndpointsConfig `key:"managedEndpoints" json:"managed_endpoints"`
	// Locality is the network domain used to scope KV caching and P/D pairing.
	// Defaults to the pool name.
	Locality string `key:"locality" json:"locality"`
	// StalledAfter is how long a Kubernetes pool's worker may stay Pending
	// before the pool counts as stalled. A stalled pool is not asked for more
	// workers, and requests stop waiting for the ones that have not started,
	// so they move on to the next pool in their route. Unset, the pool never
	// counts as stalled and its requests wait.
	StalledAfter Duration `key:"stalledAfter" json:"stalled_after,omitempty"`
	// Provisioning makes the pool buy its machines from providers. With
	// poolSizing.minFree* the pool keeps that much free, and with alsoServes
	// it takes overflow, like any other pool.
	Provisioning WorkerPoolProvisioningConfig `key:"provisioning" json:"provisioning,omitzero"`
}

// WorkerPoolProvisioningConfig is where and how a pool buys machines.
// Providers and MaxNodes are required.
//
// A pool that buys up to four spot L40S machines from Verda, with no more
// than four GPUs between them, keeps one GPU free and takes the overflow of
// the A10G pools. Below it, what all such pools together may spend, and the
// credentials of two providers:
//
//	worker:
//	  pools:
//	    spot-l40s:
//	      gpuType: L40S
//	      alsoServes: [A10G]
//	      provisioning:
//	        providers: [verda]
//	        spot: true
//	        maxNodes: 4
//	        maxGPUs: 4
//	        maxNodeHourlyCents: 150
//	        regions: [FIN-01]
//	        idleTimeout: 10m
//	        bootTimeout: 20m
//	      poolSizing:
//	        minFreeGpu: 1
//	providers:
//	  maxHourlyCents: 2000
//	  maxDailyCents: 20000
//	  verda:
//	    clientId:
//	    clientSecret:
//	  nebius:
//	    serviceAccountId:
//	    publicKeyId:
//	    privateKey:
//	    projects:
//	    - id:
type WorkerPoolProvisioningConfig struct {
	// Providers are the keys under providers that the machines are bought
	// from: each from whichever is expected to cost least.
	Providers []string `key:"providers" json:"providers"`
	// Spot buys machines the provider may take back, which makes the pool
	// preemptable. Without it the pool buys on-demand machines.
	Spot bool `key:"spot" json:"spot,omitempty"`
	// OnDemand has a spot pool buy on-demand machines too, when they are
	// expected to cost less or work that may not be interrupted waits. The
	// pool then takes such work.
	OnDemand bool `key:"onDemand" json:"on_demand,omitempty"`
	// MaxNodes is the most machines the pool holds, counting those that are
	// still booting.
	MaxNodes int `key:"maxNodes" json:"max_nodes"`
	// MaxGPUs is the most GPUs the pool holds across all its machines,
	// including machines still booting or being deleted. Zero means no cap.
	MaxGPUs int `key:"maxGPUs" json:"max_gpus,omitempty"`
	// MaxNodeHourlyCents is the most one machine may cost per hour. Zero
	// means any price.
	MaxNodeHourlyCents int64 `key:"maxNodeHourlyCents" json:"max_node_hourly_cents,omitempty"`
	// Regions are the provider's own names for the regions to buy in. Empty
	// means any region it offers.
	Regions []string `key:"regions" json:"regions,omitempty"`
	// IdleTimeout is how long a machine may have nothing to run before it is
	// released, unless it holds the pool's free capacity. Zero means ten
	// minutes.
	IdleTimeout Duration `key:"idleTimeout" json:"idle_timeout,omitempty"`
	// BootTimeout is how long a machine has to join the pool after it is
	// bought. One that has not is deleted, and so is one that joined and has
	// since been without a worker for this long. Zero means twenty minutes.
	BootTimeout Duration `key:"bootTimeout" json:"boot_timeout,omitempty"`
}

const (
	defaultProvisioningIdleTimeout = 10 * time.Minute
	defaultProvisioningBootTimeout = 20 * time.Minute
)

// IdleAfter is how long a machine may have nothing to run before it is
// released.
func (p WorkerPoolProvisioningConfig) IdleAfter() time.Duration {
	if p.IdleTimeout > 0 {
		return time.Duration(p.IdleTimeout)
	}
	return defaultProvisioningIdleTimeout
}

// BootWithin is how long a machine has to join the pool.
func (p WorkerPoolProvisioningConfig) BootWithin() time.Duration {
	if p.BootTimeout > 0 {
		return time.Duration(p.BootTimeout)
	}
	return defaultProvisioningBootTimeout
}

// written reports whether the config file or the API set anything in the
// block.
func (p WorkerPoolProvisioningConfig) written() bool {
	return len(p.Providers) > 0 || p.Spot || p.OnDemand || p.MaxNodes != 0 || p.MaxGPUs != 0 || p.MaxNodeHourlyCents != 0 ||
		len(p.Regions) > 0 || p.IdleTimeout != 0 || p.BootTimeout != 0
}

// Buys reports whether the pool buys its machines from a provider.
func (c WorkerPoolConfig) Buys() bool {
	return len(c.Provisioning.Providers) > 0
}

// AllowsManagedEndpointSpot checks the pool's explicit endpoint and Spot policies.
func (c WorkerPoolConfig) AllowsManagedEndpointSpot() bool {
	if !c.ManagedEndpoints.Enabled {
		return false
	}
	if c.Buys() {
		return c.Provisioning.Spot && !c.Provisioning.OnDemand
	}
	return c.Preemptable
}

// ApplyProvisioning fills in what a provisioning block implies. Machines that
// are bought join through the agent, so the pool needs no mode. Spot machines
// can be taken back, so a pool that buys nothing else takes only work that
// may be interrupted.
func (c *WorkerPoolConfig) ApplyProvisioning() {
	if !c.Buys() {
		return
	}
	if c.Mode == "" {
		c.Mode = PoolModeExternal
	}
	if c.Provisioning.Spot && !c.Provisioning.OnDemand {
		c.Preemptable = true
	}
}

// AgentHosted reports whether this concrete pool uses the agent control path.
func (c WorkerPoolConfig) AgentHosted() bool {
	return c.Mode.AgentHosted() || c.Mode == PoolModeExternal || c.Buys()
}

// Serves reports whether the pool lists the GPU type in alsoServes.
func (c WorkerPoolConfig) Serves(gpu string) bool {
	return slices.ContainsFunc(c.AlsoServes, func(served string) bool {
		return strings.EqualFold(served, gpu)
	})
}

// Validate reports settings that cannot mean what was intended. It is the
// one check of a pool's config, whether the pool comes from the config file
// or the pools API.
func (c WorkerPoolConfig) Validate() error {
	if err := c.CPUShareMultiplier.Validate(); err != nil {
		return err
	}
	if err := c.validateAlsoServes(); err != nil {
		return err
	}
	if err := c.validateProvisioning(); err != nil {
		return err
	}
	switch {
	case c.StalledAfter == 0:
	case c.Buys():
		return fmt.Errorf("stalledAfter applies to Kubernetes pools: a pool that buys machines gives each one provisioning.bootTimeout to join")
	case c.AgentHosted():
		return fmt.Errorf("stalledAfter applies to Kubernetes pools: a pool in %s mode has no worker Jobs that can stall", c.Mode)
	}
	return validateDuration("stalledAfter", c.StalledAfter, "2m")
}

// validateDuration rejects a duration that cannot be what was meant. A bare
// number in YAML is read as nanoseconds, so `120` would be 120ns.
func validateDuration(key string, d Duration, example string) error {
	switch {
	case d < 0:
		return fmt.Errorf("%s cannot be negative", key)
	case d > 0 && d < Duration(time.Second):
		return fmt.Errorf("%s is %s: write a duration of at least one second, such as %s", key, d, example)
	}
	return nil
}

// validateProvisioning reports a provisioning block that cannot buy what was
// intended.
func (c WorkerPoolConfig) validateProvisioning() error {
	p := c.Provisioning
	switch {
	case !p.written():
		return nil
	case len(p.Providers) == 0 || slices.ContainsFunc(p.Providers, func(name string) bool { return strings.TrimSpace(name) == "" }):
		return fmt.Errorf("provisioning needs a provider: provisioning.providers lists the keys under providers to buy machines from")
	case p.OnDemand && !p.Spot:
		return fmt.Errorf("provisioning.onDemand goes with provisioning.spot: without spot the pool buys on-demand machines already")
	case p.MaxNodes < 1:
		return fmt.Errorf("provisioning.maxNodes must be at least 1: it is the most machines the pool may hold")
	case p.MaxGPUs < 0:
		return fmt.Errorf("provisioning.maxGPUs cannot be negative")
	case p.MaxGPUs > 0 && strings.TrimSpace(c.GPUType) == "":
		return fmt.Errorf("provisioning.maxGPUs needs a gpuType: a CPU pool has no GPUs to cap")
	case p.MaxNodeHourlyCents < 0:
		return fmt.Errorf("provisioning.maxNodeHourlyCents cannot be negative")
	case c.Mode != "" && c.Mode != PoolModeExternal:
		return fmt.Errorf("a pool with a provisioning block buys machines that join through the agent: remove mode %q", c.Mode)
	}
	for _, region := range p.Regions {
		if strings.TrimSpace(region) == "" {
			return fmt.Errorf("provisioning.regions contains an empty region")
		}
	}
	if err := validateDuration("provisioning.idleTimeout", p.IdleTimeout, "10m"); err != nil {
		return err
	}
	return validateDuration("provisioning.bootTimeout", p.BootTimeout, "20m")
}

// validateAlsoServes reports alsoServes entries that cannot mean what was
// intended: a CPU pool serving GPU requests, a name no request asks for, the
// "any" wildcard, the GPU type of a pool that takes its requests anyway, or
// a GPU type listed twice.
func (c WorkerPoolConfig) validateAlsoServes() error {
	if len(c.AlsoServes) == 0 {
		return nil
	}
	if strings.TrimSpace(c.GPUType) == "" {
		return fmt.Errorf("alsoServes needs a gpuType: a CPU pool cannot run GPU requests")
	}
	seen := map[GpuType]bool{}
	for _, entry := range c.AlsoServes {
		written := strings.TrimSpace(entry)
		gpu := NormalizeGPUType(written)
		switch {
		case written == "":
			return fmt.Errorf("alsoServes contains an empty GPU type")
		case gpu == GPU_ANY:
			return fmt.Errorf("alsoServes cannot list %q: requests for any GPU already reach every GPU pool", entry)
		case !KnownGPUType(gpu):
			return fmt.Errorf("alsoServes lists %q, which is not a GPU type that requests ask for", entry)
		case !strings.EqualFold(string(gpu), written):
			return fmt.Errorf("alsoServes lists %q: requests ask for that GPU as %s", entry, gpu)
		case gpu == NormalizeGPUType(c.GPUType) && !c.RequiresPoolSelector:
			return fmt.Errorf("alsoServes lists %s, which is the pool's own gpuType: requests for it already run here", entry)
		case seen[gpu]:
			return fmt.Errorf("alsoServes lists %s twice", entry)
		}
		seen[gpu] = true
	}
	return nil
}

type WorkerPoolCacheConfig struct {
	Enabled *bool                     `key:"enabled" json:"enabled"`
	Disk    WorkerPoolCacheDiskConfig `key:"disk" json:"disk"`
}

type WorkerPoolCacheDiskConfig struct {
	Enabled      *bool   `key:"enabled" json:"enabled"`
	HostPath     string  `key:"hostPath" json:"host_path"`
	MountPath    string  `key:"mountPath" json:"mount_path"`
	MaxUsagePct  float64 `key:"maxUsagePct" json:"max_usage_pct"`
	MinFreeBytes int64   `key:"minFreeBytes" json:"min_free_bytes"`
}

// SchedulingConfig holds scheduling.failover, which alsoServes on each pool
// replaced. The section is still read for one release so a deployed config
// keeps working: its chains are converted to alsoServes at startup, with a
// warning that prints the lines to write instead.
type SchedulingConfig struct {
	Failover FailoverConfig `key:"failover" json:"failover"`
}

// FailoverConfig is the deprecated scheduling.failover section.
type FailoverConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`

	// Chains is keyed by GPU type (e.g. "A10G").
	Chains map[string]FailoverChain `key:"chains" json:"chains"`
}

// FailoverChain lists the pools that requests for one GPU type may overflow
// into. Each becomes an alsoServes entry on the pool it names.
type FailoverChain struct {
	Pools []string `key:"pools" json:"pools"`
}

// MicrosPerCent converts between the operator-facing unit (cents) and the
// micro-dollars used internally by reservations and offers.
const MicrosPerCent = 10_000

func MicrosToCents(micros int64) int64 {
	if micros <= 0 {
		return 0
	}
	return (micros + MicrosPerCent/2) / MicrosPerCent
}

// AlsoServes converts the chains to the alsoServes entries that replace them:
// for each pool a chain names, the GPU types it should list. It is empty when
// the section is disabled, as the chains had no effect then either.
//
// The section is not validated: it is on its way out, and a config that the
// previous release accepted must not stop this one starting. Names that are
// blank are skipped instead.
func (c FailoverConfig) AlsoServes() map[string][]string {
	served := map[string][]string{}
	if !c.Enabled {
		return served
	}
	for gpu, chain := range c.Chains {
		gpu = strings.ToUpper(strings.TrimSpace(gpu))
		if gpu == "" {
			continue
		}
		for _, pool := range chain.Pools {
			if strings.TrimSpace(pool) != "" {
				served[pool] = append(served[pool], gpu)
			}
		}
	}
	for pool, gpus := range served {
		slices.Sort(gpus)
		served[pool] = slices.Compact(gpus)
	}
	return served
}

type RuntimeConfig struct {
	// gVisor-specific configuration
	GVisorPlatform  string   `key:"gvisorPlatform" json:"gvisor_platform"`    // "kvm", "systrap", or "ptrace"
	GVisorRoot      string   `key:"gvisorRoot" json:"gvisor_root"`            // Root directory for gVisor state (default: "/run/gvisor")
	GVisorExtraArgs []string `key:"gvisorExtraArgs" json:"gvisor_extra_args"` // Additional runsc flags appended for this pool
}

func (c RuntimeConfig) WithDefaults(runtime string) RuntimeConfig {
	if runtime != ContainerRuntimeGvisor.String() {
		return c
	}
	if c.GVisorPlatform == "" {
		c.GVisorPlatform = "systrap"
	}
	if c.GVisorRoot == "" {
		c.GVisorRoot = "/run/gvisor"
	}
	if len(c.GVisorExtraArgs) == 0 {
		c.GVisorExtraArgs = []string{"--dcache=32768", "--overlay2=none", "--file-access=exclusive"}
	}
	return c
}

type WorkerPoolJobSpecConfig struct {
	NodeSelector map[string]string `key:"nodeSelector" json:"node_selector"`
	Env          []corev1.EnvVar   `key:"env" json:"env"`

	// Mimics corev1.Volume since that type doesn't currently serialize correctly
	Volumes []struct {
		Name   string `key:"name" json:"name"`
		Secret struct {
			SecretName string `key:"secretName" json:"secret_name"`
		} `key:"secret" json:"secret"`
	} `key:"volumes" json:"volumes"`

	VolumeMounts []corev1.VolumeMount `key:"volumeMounts" json:"volume_mounts"`
}

type WorkerPoolJobSpecPoolSizingConfig struct {
	DefaultWorkerCPU      string `key:"defaultWorkerCPU" json:"default_worker_cpu"`
	DefaultWorkerMemory   string `key:"defaultWorkerMemory" json:"default_worker_memory"`
	DefaultWorkerGpuType  string `key:"defaultWorkerGPUType" json:"default_worker_gpu_type"`
	DefaultWorkerGpuCount string `key:"defaultWorkerGpuCount" json:"default_worker_gpu_count"`
	MinFreeCPU            string `key:"minFreeCPU" json:"min_free_cpu"`
	MinFreeMemory         string `key:"minFreeMemory" json:"min_free_memory"`
	MinFreeGPU            string `key:"minFreeGPU" json:"min_free_gpu"`
	SharedMemoryLimitPct  string `key:"sharedMemoryLimitPct" json:"shared_memory_limit_pct"`
}

// ProviderConfig holds the credentials and settings of the providers Beam
// buys machines from.
type ProviderConfig struct {
	// Disabled prevents catalog discovery and new purchases while retaining
	// credentials for inspecting and deleting machines already bought. It
	// names providers by the keys below, in any case. A name that is no
	// provider's disables nothing, and is logged as an error at startup.
	Disabled  []string                `key:"disabled" json:"disabled"`
	Vast      VastProviderConfig      `key:"vast" json:"vast"`
	Shadeform ShadeformProviderConfig `key:"shadeform" json:"shadeform"`
	Hetzner   HetznerProviderConfig   `key:"hetzner" json:"hetzner"`
	Verda     VerdaProviderConfig     `key:"verda" json:"verda"`
	Nebius    NebiusProviderConfig    `key:"nebius" json:"nebius"`
	AWS       AWSProviderConfig       `key:"aws" json:"aws"`
	GCP       GCPProviderConfig       `key:"gcp" json:"gcp"`

	// MaxHourlyCents caps what the machines bought for pools cost per hour,
	// all pools together: a machine that would take the total over it is not
	// bought. Zero means no cap.
	MaxHourlyCents int64 `key:"maxHourlyCents" json:"max_hourly_cents"`
	// MaxDailyCents stops pools buying once this much has been spent on
	// their machines in the last 24 hours. Machines that are running keep
	// running. Zero means no cap.
	MaxDailyCents int64 `key:"maxDailyCents" json:"max_daily_cents"`

	Market ProviderMarketConfig `key:"market" json:"market"`
}

// ProviderMarketConfig is for the record of what the providers sell: every
// provider is asked for its offers at an interval, by one gateway at a time,
// and what it answers is kept for the gateways in Redis and as history in S2.
type ProviderMarketConfig struct {
	// Enabled turns the sampling on. Nil means on.
	Enabled *bool `key:"enabled" json:"enabled"`
	// Interval is how often the providers are asked. Zero means a minute.
	Interval time.Duration `key:"interval" json:"interval"`
	// DryRun samples and logs how much there is to write, by provider, and
	// writes no history. It is for sizing the streams before they are kept.
	DryRun bool `key:"dryRun" json:"dry_run"`
	// SnapshotRetention is how long every offer of every sample is kept.
	// Zero means 90 days.
	SnapshotRetention time.Duration `key:"snapshotRetention" json:"snapshot_retention"`
	// SeriesRetention is how long the samples a chart is drawn from are
	// kept at the interval they were taken at. Zero means 14 days. What they
	// come to by the hour is kept for good.
	SeriesRetention time.Duration `key:"seriesRetention" json:"series_retention"`
}

func (c ProviderMarketConfig) IsEnabled() bool {
	return c.Enabled == nil || *c.Enabled
}

func (c ProviderMarketConfig) IntervalOrDefault() time.Duration {
	if c.Interval <= 0 {
		return time.Minute
	}
	return c.Interval
}

func (c ProviderMarketConfig) SnapshotRetentionOrDefault() time.Duration {
	if c.SnapshotRetention <= 0 {
		return 90 * 24 * time.Hour
	}
	return c.SnapshotRetention
}

func (c ProviderMarketConfig) SeriesRetentionOrDefault() time.Duration {
	if c.SeriesRetention <= 0 {
		return 14 * 24 * time.Hour
	}
	return c.SeriesRetention
}

// VastProviderConfig is for Vast, from which pools rent interruptible
// virtual machines. The account needs an SSH key, added in Vast's console:
// Vast starts no virtual machine without one.
type VastProviderConfig struct {
	ApiKey  string `key:"apiKey" json:"api_key"`
	BaseURL string `key:"baseURL" json:"base_url"`
	// Image is the VM image a machine boots, one of Vast's
	// docker.io/vastai/kvm. Empty means Ubuntu 22.04 with the NVIDIA driver.
	Image string `key:"image" json:"image"`
	// DiskGB is the size of a machine's disk. Zero means 200.
	DiskGB int64 `key:"diskGB" json:"disk_gb"`
}

type ShadeformProviderConfig struct {
	ApiKey  string `key:"apiKey" json:"api_key"`
	BaseURL string `key:"baseURL" json:"base_url"`
}

// VerdaProviderConfig is for Verda, formerly DataCrunch. The client id and
// secret are the "Cloud API credentials" of a Verda project.
type VerdaProviderConfig struct {
	ClientID     string `key:"clientId" json:"client_id"`
	ClientSecret string `key:"clientSecret" json:"client_secret"`
	BaseURL      string `key:"baseURL" json:"base_url"`
	// Images are the OS images a machine may boot, best first. A machine
	// boots the first that its instance type supports. Empty means Ubuntu
	// 24.04 with the NVIDIA driver and Docker.
	Images []string `key:"images" json:"images"`
	// SSHKeyIDs are the ids of the Verda SSH keys that may log in.
	SSHKeyIDs []string `key:"sshKeyIds" json:"ssh_key_ids"`
	// DiskGB is the size of a machine's disk. Zero means 200.
	DiskGB int64 `key:"diskGB" json:"disk_gb"`
}

// NebiusProviderConfig is for Nebius, from which pools buy preemptible VMs.
// The service account needs the editor role. It signs in with an authorized
// key: publicKeyId is the ID Nebius gave the uploaded public key, and
// privateKey is the other half, in PEM form.
type NebiusProviderConfig struct {
	ServiceAccountID string `key:"serviceAccountId" json:"service_account_id"`
	PublicKeyID      string `key:"publicKeyId" json:"public_key_id"`
	PrivateKey       string `key:"privateKey" json:"private_key"`
	// Projects are where machines are bought. A Nebius project is in one
	// region, so there is one for each region to buy in.
	Projects []NebiusProjectConfig `key:"projects" json:"projects"`
	BaseURL  string                `key:"baseURL" json:"base_url"`
	TokenURL string                `key:"tokenURL" json:"token_url"`
	// ImageFamily is the family of the image a machine boots. Empty means
	// Ubuntu 24.04 with the NVIDIA driver.
	ImageFamily string `key:"imageFamily" json:"image_family"`
	// DiskGB is the size of a machine's disk. Zero means 200.
	DiskGB int64 `key:"diskGB" json:"disk_gb"`
}

type NebiusProjectConfig struct {
	ID string `key:"id" json:"id"`
	// SubnetID is the subnet machines are put in. It may be left out for a
	// project that has one subnet.
	SubnetID string `key:"subnetId" json:"subnet_id"`
}

// AWSProviderConfig is for AWS, from which pools buy EC2 Spot Instances. It
// is in use once it names a region.
type AWSProviderConfig struct {
	// AccessKeyID and SecretAccessKey are an IAM user's. Without them the
	// gateway uses the credentials of where it runs: its environment, or
	// the role of its pod or machine.
	AccessKeyID     string `key:"accessKeyId" json:"access_key_id"`
	SecretAccessKey string `key:"secretAccessKey" json:"secret_access_key"`
	// Regions are where machines are bought.
	Regions []AWSProviderRegionConfig `key:"regions" json:"regions"`
	// ImageName is the name of the Amazon image a machine boots, which may
	// end in *; the newest that fits is used. Empty means Amazon's Deep
	// Learning Base image with the NVIDIA driver, on Ubuntu 22.04.
	ImageName string `key:"imageName" json:"image_name"`
	// DiskGB is the size of a machine's disk. Zero means 200.
	DiskGB int64 `key:"diskGB" json:"disk_gb"`
	// KeyName is the EC2 key pair that may log in, if any.
	KeyName string `key:"keyName" json:"key_name"`
}

type AWSProviderRegionConfig struct {
	Name string `key:"name" json:"name"`
	// SubnetIDs are the subnets machines are put in, one for each
	// availability zone to buy in. Empty means the default subnets of the
	// region's default VPC.
	SubnetIDs []string `key:"subnetIds" json:"subnet_ids"`
	// SecurityGroupIDs are a machine's security groups. Empty means the
	// default group of its VPC.
	SecurityGroupIDs []string `key:"securityGroupIds" json:"security_group_ids"`
	// ImageID is the image a machine boots here, in place of the newest
	// that fits imageName.
	ImageID string `key:"imageId" json:"image_id"`
}

// GCPProviderConfig is for Google Cloud, from which pools buy Compute
// Engine Spot VMs. The service account needs the roles Compute Instance
// Admin and Compute Viewer.
type GCPProviderConfig struct {
	// ServiceAccountKey is the service account's JSON key file, as text.
	ServiceAccountKey string `key:"serviceAccountKey" json:"service_account_key"`
	// Project is where machines are bought. Empty means the service
	// account's own project.
	Project string `key:"project" json:"project"`
	// Regions are where machines are bought.
	Regions []GCPProviderRegionConfig `key:"regions" json:"regions"`
	BaseURL string                    `key:"baseURL" json:"base_url"`
	// Image is the image a machine boots, or the family of it, by its
	// path. Empty means Ubuntu 24.04 with the NVIDIA driver.
	Image string `key:"image" json:"image"`
	// DiskGB is the size of a machine's disk. Zero means 200.
	DiskGB int64 `key:"diskGB" json:"disk_gb"`
}

type GCPProviderRegionConfig struct {
	Name string `key:"name" json:"name"`
	// Subnetwork is the subnet machines are put in: its name, or its path
	// when it is another project's. Empty means the network named default.
	Subnetwork string `key:"subnetwork" json:"subnetwork"`
}

type HetznerProviderConfig struct {
	ApiToken             string                         `key:"apiToken" json:"api_token"`
	BaseURL              string                         `key:"baseURL" json:"base_url"`
	Image                string                         `key:"image" json:"image"`
	ImageByRegion        map[string]string              `key:"imageByRegion" json:"image_by_region"`
	SSHKeys              []string                       `key:"sshKeys" json:"ssh_keys"`
	SSHKeysByRegion      map[string][]string            `key:"sshKeysByRegion" json:"ssh_keys_by_region"`
	PrivateNetwork       HetznerPrivateNetworkConfig    `key:"privateNetwork" json:"private_network"`
	ServerTypePrices     map[string]float64             `key:"serverTypePrices" json:"server_type_prices"`
	ServerTypeCategories map[string]string              `key:"serverTypeCategories" json:"server_type_categories"`
	RegionMetadata       map[string]HetznerRegionConfig `key:"regionMetadata" json:"region_metadata"`
	DefaultRegions       []string                       `key:"defaultRegions" json:"default_regions"`
}

type HetznerPrivateNetworkConfig struct {
	ID            int64             `key:"id" json:"id"`
	Name          string            `key:"name" json:"name"`
	RegionIDs     map[string]int64  `key:"regionIds" json:"region_ids"`
	RegionNames   map[string]string `key:"regionNames" json:"region_names"`
	RequireSubnet *bool             `key:"requireSubnet" json:"require_subnet"`
}

type HetznerRegionConfig struct {
	DisplayName string  `key:"displayName" json:"display_name"`
	Latitude    float64 `key:"latitude" json:"latitude"`
	Longitude   float64 `key:"longitude" json:"longitude"`
}

type MetricsCollector string

var (
	MetricsCollectorPrometheus MetricsCollector = "prometheus"
	MetricsCollectorOpenMeter  MetricsCollector = "openmeter"
	MetricsCollectorNone       MetricsCollector = "none"
)

type MonitoringConfig struct {
	MetricsCollector         string                  `key:"metricsCollector" json:"metrics_collector"`
	Prometheus               PrometheusConfig        `key:"prometheus" json:"prometheus"`
	OpenMeter                OpenMeterConfig         `key:"openmeter" json:"openmeter"`
	Telemetry                TelemetryConfig         `key:"telemetry" json:"telemetry"`
	ContainerMetricsInterval time.Duration           `key:"containerMetricsInterval" json:"container_metrics_interval"`
	VictoriaMetrics          VictoriaMetricsConfig   `key:"victoriametrics" json:"victoriametrics"`
	ContainerCostHookConfig  ContainerCostHookConfig `key:"containerCostHook" json:"container_cost_hook"`
}

// Metering is the part of a monitoring config that records and prices
// container usage: the collector with its credentials and the cost hook. A
// control-plane managed worker boots without one and receives the control
// plane's at startup.
func (m MonitoringConfig) Metering() MonitoringConfig {
	return MonitoringConfig{MetricsCollector: m.MetricsCollector, Prometheus: m.Prometheus, OpenMeter: m.OpenMeter, ContainerCostHookConfig: m.ContainerCostHookConfig}
}

// Metered reports whether a collector is configured.
func (m MonitoringConfig) Metered() bool {
	return m.MetricsCollector != "" && m.MetricsCollector != string(MetricsCollectorNone)
}

// SetMetering replaces the metering part of m, keeping its local settings.
func (m *MonitoringConfig) SetMetering(metering MonitoringConfig) {
	m.MetricsCollector, m.Prometheus, m.OpenMeter, m.ContainerCostHookConfig = metering.MetricsCollector, metering.Prometheus, metering.OpenMeter, metering.ContainerCostHookConfig
}

const (
	ManagedComputeDefaultMinimumCreditCents int64   = 2500
	ManagedComputeDefaultBillableMarginPct  float64 = 0.10

	ManagedComputeDefaultBYOCMemoryHourlyMicrosPerGB int64 = 4_500
	ManagedComputeDefaultBYOCCPUHourlyMicrosPerVCPU  int64 = 9_500

	// ManagedComputeDefaultReservationBootTimeout is how long a reservation's
	// machine may stay pending at its provider where the config names no
	// time.
	ManagedComputeDefaultReservationBootTimeout = 20 * time.Minute
)

type ManagedComputeConfig struct {
	BillableMarginPct *float64                         `key:"billableMarginPct" json:"billable_margin_pct"`
	Billing           ManagedComputeBillingConfig      `key:"billing" json:"billing"`
	BYOC              ManagedComputeBYOCConfig         `key:"byoc" json:"byoc"`
	SSH               ManagedComputeSSHConfig          `key:"ssh" json:"ssh"`
	Reservations      ManagedComputeReservationsConfig `key:"reservations" json:"reservations"`
}

// ManagedComputeReservationsConfig says where the machines tenants reserve
// are bought.
type ManagedComputeReservationsConfig struct {
	// Providers are the keys under providers that sell reservations. Empty
	// means every provider that is configured. A provider that is configured
	// for pools to buy their own machines from, and is not named here, sells
	// nothing to a tenant.
	Providers []string `key:"providers" json:"providers"`
	// BootTimeout bounds how long a newly bought machine may remain pending
	// at its provider. It does not wait for agent registration. Zero means
	// ManagedComputeDefaultReservationBootTimeout.
	BootTimeout Duration `key:"bootTimeout" json:"boot_timeout"`
}

// BootTimeoutOrDefault is how long a newly bought machine may remain pending
// at its provider before the reservation is given up.
func (c ManagedComputeReservationsConfig) BootTimeoutOrDefault() time.Duration {
	if c.BootTimeout > 0 {
		return time.Duration(c.BootTimeout)
	}
	return ManagedComputeDefaultReservationBootTimeout
}

type ManagedComputeSSHConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`
}

func (c ManagedComputeConfig) BillableMarginPctOrDefault() float64 {
	if c.BillableMarginPct == nil {
		return ManagedComputeDefaultBillableMarginPct
	}
	if *c.BillableMarginPct < 0 {
		return 0
	}
	return *c.BillableMarginPct
}

type ManagedComputeBYOCConfig struct {
	Pricing ManagedComputeBYOCPricingConfig `key:"pricing" json:"pricing"`
	AWS     ManagedComputeBYOCAWSConfig     `key:"aws" json:"aws"`
}

type ManagedComputeBYOCPricingConfig struct {
	MemoryHourlyMicrosPerGB int64 `key:"memoryHourlyMicrosPerGB" json:"memory_hourly_micros_per_gb"`
	CPUHourlyMicrosPerVCPU  int64 `key:"cpuHourlyMicrosPerVCPU" json:"cpu_hourly_micros_per_vcpu"`
}

func (c ManagedComputeBYOCPricingConfig) MemoryHourlyMicrosPerGBOrDefault() int64 {
	if c.MemoryHourlyMicrosPerGB > 0 {
		return c.MemoryHourlyMicrosPerGB
	}
	return ManagedComputeDefaultBYOCMemoryHourlyMicrosPerGB
}

func (c ManagedComputeBYOCPricingConfig) CPUHourlyMicrosPerVCPUOrDefault() int64 {
	if c.CPUHourlyMicrosPerVCPU > 0 {
		return c.CPUHourlyMicrosPerVCPU
	}
	return ManagedComputeDefaultBYOCCPUHourlyMicrosPerVCPU
}

type ManagedComputeBYOCAWSConfig struct {
	TemplateURL             string `key:"templateUrl" json:"template_url"`
	ControlRolePrincipalARN string `key:"controlRolePrincipalArn" json:"control_role_principal_arn"`
}

type ManagedComputeBillingConfig struct {
	Mode               string                                 `key:"mode" json:"mode"`
	Endpoint           string                                 `key:"endpoint" json:"endpoint"`
	AuthToken          string                                 `key:"authToken" json:"auth_token"`
	PoolRoutes         []ManagedComputeBillingPoolRouteConfig `key:"poolRoutes" json:"pool_routes"`
	Required           bool                                   `key:"required" json:"required"`
	Timeout            time.Duration                          `key:"timeout" json:"timeout"`
	ReconcileInterval  time.Duration                          `key:"reconcileInterval" json:"reconcile_interval"`
	MinimumCreditCents int64                                  `key:"minimumCreditCents" json:"minimum_credit_cents"`
	// FailureGracePeriod is how long balance checks may keep failing (billing
	// unreachable, timing out or erroring) before managed reservations are
	// terminated. Pools keep running and accruing cost unchecked meanwhile.
	// Zero means the default; a negative value terminates on the first
	// failed check. Independent of CreditGracePeriod.
	FailureGracePeriod time.Duration `key:"failureGracePeriod" json:"failure_grace_period"`
	// CreditGracePeriod is how long a pool keeps running after its balance
	// first reads as exhausted, so an auto top-up or a manual purchase can
	// land before reserved machines are released. Zero means the default;
	// a negative value terminates immediately (the old behaviour).
	CreditGracePeriod time.Duration `key:"creditGracePeriod" json:"credit_grace_period"`
}

type ManagedComputeBillingPoolRouteConfig struct {
	PoolNamePrefix     string `key:"poolNamePrefix" json:"pool_name_prefix"`
	Endpoint           string `key:"endpoint" json:"endpoint"`
	AuthToken          string `key:"authToken" json:"auth_token"`
	MinimumCreditCents int64  `key:"minimumCreditCents" json:"minimum_credit_cents"`
}

func (r ManagedComputeBillingPoolRouteConfig) BillingConfig(base ManagedComputeBillingConfig) ManagedComputeBillingConfig {
	base.PoolRoutes = nil
	if endpoint := strings.TrimSpace(r.Endpoint); endpoint != "" {
		base.Endpoint = endpoint
		base.Mode = ""
	}
	if strings.TrimSpace(r.AuthToken) != "" {
		base.AuthToken = r.AuthToken
	}
	if r.MinimumCreditCents > 0 {
		base.MinimumCreditCents = r.MinimumCreditCents
	}
	return base
}

func (c ManagedComputeBillingConfig) MinimumCreditCentsOrDefault() int64 {
	if c.MinimumCreditCents > 0 {
		return c.MinimumCreditCents
	}
	return ManagedComputeDefaultMinimumCreditCents
}

func (c ManagedComputeBillingConfig) FailureGracePeriodOrDefault() time.Duration {
	if c.FailureGracePeriod < 0 {
		return 0
	}
	if c.FailureGracePeriod > 0 {
		return c.FailureGracePeriod
	}
	return 30 * time.Minute
}

func (c ManagedComputeBillingConfig) CreditGracePeriodOrDefault() time.Duration {
	if c.CreditGracePeriod < 0 {
		return 0
	}
	if c.CreditGracePeriod > 0 {
		return c.CreditGracePeriod
	}
	return 30 * time.Minute
}

func (c ManagedComputeBillingConfig) TimeoutOrDefault() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 5 * time.Second
}

func (c ManagedComputeBillingConfig) ReconcileIntervalOrDefault() time.Duration {
	if c.ReconcileInterval > 0 {
		return c.ReconcileInterval
	}
	return time.Minute
}

type VictoriaMetricsConfig struct {
	PushURL       string `key:"pushURL" json:"push_url"`
	AuthToken     string `key:"authToken" json:"auth_token"`
	PushSecs      int    `key:"pushSecs" json:"push_secs"`
	WriteURL      string `key:"writeURL" json:"write_url"`
	WriteUsername string `key:"writeUsername" json:"write_username"`
	WritePassword string `key:"writePassword" json:"write_password"`
}

type PrometheusConfig struct {
	AgentUrl      string `key:"agentUrl" json:"agent_url"`
	AgentUsername string `key:"agentUsername" json:"agent_username"`
	AgentPassword string `key:"agentPassword" json:"agent_password"`
	ScrapeWorkers bool   `key:"scrapeWorkers" json:"scrape_workers"`
	Port          int    `key:"port" json:"port"`
}

type TelemetryConfig struct {
	Enabled          bool          `key:"enabled" json:"enabled"`
	Endpoint         string        `key:"endpoint" json:"endpoint"`
	MeterInterval    time.Duration `key:"meterInterval" json:"meter_interval"`
	TraceInterval    time.Duration `key:"traceInterval" json:"trace_interval"`
	TraceSampleRatio float64       `key:"traceSampleRatio" json:"trace_sample_ratio"`
}

type OpenMeterConfig struct {
	ServerUrl string `key:"serverUrl" json:"server_url"`
	ApiKey    string `key:"apiKey" json:"api_key"`
}

type TailscaleConfig struct {
	ControlURL   string                  `key:"controlUrl" json:"control_url"`
	AuthKey      string                  `key:"authKey" json:"auth_key"`
	AgentAuthKey string                  `key:"agentAuthKey" json:"agent_auth_key"`
	HostName     string                  `key:"hostName" json:"host_name"`
	Enabled      bool                    `key:"enabled" json:"enabled"`
	Debug        bool                    `key:"debug" json:"debug"`
	AgentSSH     TailscaleAgentSSHConfig `key:"agentSsh" json:"agent_ssh"`

	// ServerlessAgentAuthKey carries the additional, operator-owned SSH tag.
	// It must never be issued to workspace-owned or reserved capacity.
	ServerlessAgentAuthKey string `key:"serverlessAgentAuthKey" json:"serverless_agent_auth_key"`
}

// TailscaleAgentSSHConfig runs Tailscale SSH on agent nodes in control-plane
// owned pools. Workspace-owned private/BYOC machines never get it.
type TailscaleAgentSSHConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`
}

type ObjectStoreConfig struct {
	BucketName     string `key:"bucketName" json:"bucket_name"`
	AccessKey      string `key:"accessKey" json:"access_key"`
	SecretKey      string `key:"secretKey" json:"secret_key"`
	EndpointURL    string `key:"endpointURL" json:"bucket_url"`
	Region         string `key:"region" json:"region"`
	ReadOnly       bool   `key:"readOnly" json:"read_only"`
	ForcePathStyle bool   `key:"forcePathStyle" json:"force_path_style"`
}

type CRIUConfigMode string

var (
	CRIUConfigModeNvidia CRIUConfigMode = "nvidia"
)

type CRIUConfig struct {
	Mode   CRIUConfigMode   `key:"mode" json:"mode"`
	Nvidia NvidiaCRIUConfig `key:"nvidia" json:"nvidia"`
}

type NvidiaCRIUConfig struct {
}

type AbstractionConfig struct {
	Bot BotConfig `key:"bot" json:"bot"`
	Pod PodConfig `key:"pod" json:"pod"`
	VM  VMConfig  `key:"vm" json:"vm"`
}

type VMConfig struct {
	// Optional pool used when a VM is created without an explicit selector.
	DefaultPool string `key:"defaultPool" json:"default_pool"`
	// Wildcard DNS/ingress pointing at the gateway; <handle>-<port>.<domain>.
	Domain  string `key:"domain" json:"domain"`
	BaseURL string `key:"baseURL" json:"base_url"`
	// Optional external browser sign-in endpoint; empty disables redirects.
	// Receives workspace, vm, port and a relative next path. The provider must
	// authorize the viewer and obtain a session through the management API.
	BrowserAuthURL string             `key:"browserAuthURL" json:"browser_auth_url"`
	Network        VMNetworkConfig    `key:"network" json:"network"`
	WebRTC         VMWebRTCConfig     `key:"webrtc" json:"webrtc"`
	GitHubRunners  GitHubRunnerConfig `key:"githubRunners" json:"github_runners"`
}

// GitHubRunnerConfig runs GitHub Actions jobs on ordinary CPU VMs through a
// GitHub App. An empty AppID disables it; with one, the App credentials,
// webhook secret, and runner image are required.
//
// The App needs Actions (write, to cancel runs) and Metadata (read) on
// repositories, Members (read) on organizations, and Workflow job events sent
// to the gateway's /api/v1/vm/github-runners/webhook. To register runners it
// needs Administration (write) on repositories, or Self-hosted runners
// (write) on organizations for those that OrganizationRunners covers. Its
// setup URL, with redirect on update, and its callback URL are both the
// dashboard's /vms page; leave "Request user authorization (OAuth) during
// installation" off.
type GitHubRunnerConfig struct {
	AppID   string `key:"appID" json:"app_id"`
	AppSlug string `key:"appSlug" json:"app_slug"` // as in github.com/apps/<slug>
	// OAuth credentials of the App, which prove that whoever connects an
	// account owns it.
	ClientID     string `key:"clientID" json:"client_id"`
	ClientSecret string `key:"clientSecret" json:"client_secret"`
	// PrivateKey is the PEM that GitHub generates for the App. Its newlines
	// may be written as \n.
	PrivateKey    string `key:"privateKey" json:"private_key"`
	WebhookSecret string `key:"webhookSecret" json:"webhook_secret"`
	// ImageID is printed by `beam vm image build docker/github-runner`.
	ImageID string `key:"imageID" json:"image_id"`
	// RunnerLimit is the most runners a workspace has at once. The jobs past
	// it stay queued on GitHub, and get theirs as runners finish, the jobs
	// that have waited longest first. Zero or less sets no limit.
	RunnerLimit int `key:"runnerLimit" json:"runner_limit"`
	// LabelPrefix replaces "beam-" in workflow size labels. Empty uses
	// "beam-"; values are trimmed and lowercased. Other prefixes starting
	// with "beam-" are rejected because production runners recognize them
	// too. Staging tests that share repositories with production must use
	// a prefix outside "beam-", so the production App ignores their jobs.
	LabelPrefix string `key:"labelPrefix" json:"label_prefix"`
	// OrganizationRunners registers the runners of an organization on the
	// organization, in a runner group that only their repository can use,
	// which takes Self-hosted runners (write) there and no Administration of
	// its repositories. Each organization changes over once it has granted
	// that: one that installed the App before the App asked for it has to
	// accept first, and registers runners on its repositories until then, as
	// a personal account always does. Administration can come off the App
	// when no account that is to keep its runners still depends on it.
	OrganizationRunners bool `key:"organizationRunners" json:"organization_runners"`
}

// VMWebRTCConfig bounds authenticated kiosk media on the shared TCP gateway.
// Zero values use defaults; timing values follow the normal duration config format.
type VMWebRTCConfig struct {
	MaxConnections           int           `key:"maxConnections" json:"max_connections"`
	MaxConnectionsPerSession int           `key:"maxConnectionsPerSession" json:"max_connections_per_session"`
	RegistrationsPerMinute   int           `key:"registrationsPerMinute" json:"registrations_per_minute"`
	HandshakeTimeout         time.Duration `key:"handshakeTimeout" json:"handshake_timeout"`
	SessionTTL               time.Duration `key:"sessionTTL" json:"session_ttl"`
	LeaseTTL                 time.Duration `key:"leaseTTL" json:"lease_ttl"`
	CheckInterval            time.Duration `key:"checkInterval" json:"check_interval"`
}

func (c *VMWebRTCConfig) ApplyDefaults() error {
	for _, item := range []struct {
		value    *int
		fallback int
	}{
		{&c.MaxConnections, 256}, {&c.MaxConnectionsPerSession, 8}, {&c.RegistrationsPerMinute, 64},
	} {
		if *item.value < 0 {
			return fmt.Errorf("VM WebRTC limits must be nonnegative")
		}
		if *item.value == 0 {
			*item.value = item.fallback
		}
	}
	for _, item := range []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&c.HandshakeTimeout, 5 * time.Second}, {&c.SessionTTL, 10 * time.Minute},
		{&c.LeaseTTL, 30 * time.Second}, {&c.CheckInterval, 10 * time.Second},
	} {
		if *item.value < 0 {
			return fmt.Errorf("VM WebRTC durations must be nonnegative")
		}
		if *item.value == 0 {
			*item.value = item.fallback
		}
		if *item.value < time.Millisecond {
			return fmt.Errorf("VM WebRTC durations must be at least one millisecond")
		}
	}
	if c.LeaseTTL <= c.CheckInterval+c.HandshakeTimeout || c.SessionTTL < c.HandshakeTimeout {
		return fmt.Errorf("VM WebRTC TTLs must cover handshake and lease renewal")
	}
	return nil
}

type VMNetworkConfig struct {
	// Empty disables public networking. The ingress must route this prefix to
	// the gateway while preserving the original destination with PROXY protocol.
	PublicIPv6Prefix string `key:"publicIPv6Prefix" json:"public_ipv6_prefix"`
	// Additional routed pools let shared ingress capacity grow without moving
	// existing VM addresses. Keep retired pools routed until their VMs are gone.
	PublicIPv6Prefixes []string `key:"publicIPv6Prefixes" json:"public_ipv6_prefixes"`
	// All published TCP ports must reach this listener with PROXY v2 TCP6
	// metadata carrying their original destination address and port.
	IngressPort int `key:"ingressPort" json:"ingress_port"`
	// These are the actual TCP ingress peers. Disable client IP preservation
	// on this listener so advertised client addresses cannot authorize a proxy.
	TrustedProxyCIDRs []string `key:"trustedProxyCIDRs" json:"trusted_proxy_cidrs"`
	// Maximum concurrent public TCP connections per gateway. Zero uses 16384.
	MaxConnections int `key:"maxConnections" json:"max_connections"`
	// Per-VM limit per gateway across public TCP ports, including HTTP keep-alives.
	// Zero uses 256, capped at one quarter of the gateway limit (minimum one).
	MaxConnectionsPerVM int `key:"maxConnectionsPerVM" json:"max_connections_per_vm"`
}

type BotConfig struct {
	SystemPrompt              string `key:"systemPrompt" json:"system_prompt"`
	StepIntervalS             uint   `key:"stepIntervalS" json:"step_interval_s"`
	SessionInactivityTimeoutS uint   `key:"sessionInactivityTimeoutS" json:"session_inactivity_timeout_s"`
}

type PodConfig struct {
	TCP PodTCPConfig `key:"tcp" json:"tcp"`
}

func (p *PodTCPConfig) GetExternalURL() string {
	baseUrl := "http"
	if p.Enabled {
		baseUrl += "s"
	}
	baseUrl += "://" + p.ExternalHost

	if p.ExternalPort != 80 && p.ExternalPort != 443 {
		baseUrl += fmt.Sprintf(":%d", p.ExternalPort)
	}

	return baseUrl
}

type PodTCPConfig struct {
	Enabled      bool   `key:"enabled" json:"enabled"`
	ExternalPort int    `key:"externalPort" json:"external_port"`
	ExternalHost string `key:"externalHost" json:"external_host"`
	Port         int    `key:"port" json:"port"`
	CertFile     string `key:"certFile" json:"cert_file"`
	KeyFile      string `key:"keyFile" json:"key_file"`
	// ServiceProxyTarget is host:port of this listener as workers reach it.
	// When set, containers that reference *.ExternalHost are pinned to a
	// worker-side proxy to it, which opens TLS for clients that don't, instead
	// of resolving ExternalHost over DNS. Agent-hosted workers ask the gateway
	// for its public listener instead.
	ServiceProxyTarget string `key:"serviceProxyTarget" json:"service_proxy_target"`
	// ServiceProxyVerify makes the proxy verify the target's certificate, as it
	// does whenever CertFile is set.
	ServiceProxyVerify bool `key:"serviceProxyVerify" json:"service_proxy_verify"`
}

// Duration is a time.Duration that is written as text, such as "10m", in
// JSON as it is in the config file. encoding/json writes a time.Duration as
// a number of nanoseconds, which nobody can type into an API call.
type Duration time.Duration

func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("%q is not a duration: write one like 90s, 10m or 1h30m", text)
	}
	*d = Duration(parsed)
	return nil
}

// UnmarshalJSON reads text, or a number of nanoseconds, which is also what a
// bare number means in the config file.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var nanoseconds int64
	if json.Unmarshal(data, &nanoseconds) == nil {
		*d = Duration(nanoseconds)
		return nil
	}
	var text string
	if json.Unmarshal(data, &text) != nil {
		return fmt.Errorf("%s: a duration is written as text, such as \"10m\"", data)
	}
	return d.UnmarshalText([]byte(text))
}
