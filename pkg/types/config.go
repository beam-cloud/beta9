package types

import (
	"fmt"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	cedana "github.com/cedana/cedana/pkg/config"
	corev1 "k8s.io/api/core/v1"
)

type AppConfig struct {
	ClusterName       string                    `key:"clusterName" json:"cluster_name"`
	DebugMode         bool                      `key:"debugMode" json:"debug_mode"`
	PrettyLogs        bool                      `key:"prettyLogs" json:"pretty_logs"`
	Database          DatabaseConfig            `key:"database" json:"database"`
	GatewayService    GatewayServiceConfig      `key:"gateway" json:"gateway_service"`
	FileService       FileServiceConfig         `key:"fileService" json:"file_service"`
	ImageService      ImageServiceConfig        `key:"imageService" json:"image_service"`
	Storage           StorageConfig             `key:"storage" json:"storage"`
	Worker            WorkerConfig              `key:"worker" json:"worker"`
	Providers         ProviderConfig            `key:"providers" json:"providers"`
	Tailscale         TailscaleConfig           `key:"tailscale" json:"tailscale"`
	Proxy             ProxyConfig               `key:"proxy" json:"proxy"`
	Monitoring        MonitoringConfig          `key:"monitoring" json:"monitoring"`
	BlobCacheMetadata BlobCacheMetadataConfig   `key:"blobcacheMetadata" json:"blobcache_metadata"`
	BlobCache         blobcache.BlobCacheConfig `key:"blobcache" json:"blobcache"`
	Abstractions      AbstractionConfig         `key:"abstractions" json:"abstractions"`
}

type DatabaseConfig struct {
	Redis    RedisConfig    `key:"redis" json:"redis"`
	Postgres PostgresConfig `key:"postgres" json:"postgres"`
}

type RedisMode string

var (
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

type BlobCacheMetadataConfig struct {
	ValkeyConfig ValkeyConfig `key:"valkey" json:"valkey"`
}

type ValkeyConfig struct {
	Enabled         bool                   `key:"enabled" json:"enabled"`
	Password        string                 `key:"password" json:"password"`
	TLS             bool                   `key:"tls" json:"tls"`
	PersistenceSize string                 `key:"persistenceSize" json:"persistenceSize"`
	ReplicaCount    int                    `key:"replicaCount" json:"replicaCount"`
	PoolNodeCount   int                    `key:"poolNodeCount" json:"poolNodeCount"`
	ExistingPrimary *ValkeyExistingPrimary `key:"existingPrimary" json:"existingPrimary,omitempty"`
	Sentinel        ValkeySentinelConfig   `key:"sentinel" json:"sentinel"`
}

type ValkeyExistingPrimary struct {
	Host string `key:"host" json:"host"`
	Port int    `key:"port" json:"port"`
}

type ValkeySentinelConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`
	Quorum  int  `key:"quorum" json:"quorum"`
}

type PostgresConfig struct {
	Host      string `key:"host" json:"host"`
	Port      int    `key:"port" json:"port"`
	Name      string `key:"name" json:"name"`
	Username  string `key:"username" json:"username"`
	Password  string `key:"password" json:"password"`
	TimeZone  string `key:"timezone" json:"timezone"`
	EnableTLS bool   `key:"enableTLS" json:"enable_tls"`
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
	Memory      uint64 `key:"memory" json:"memory"`
	MaxReplicas uint64 `key:"maxReplicas" json:"max_replicas"`
	MaxGpuCount uint32 `key:"maxGpuCount" json:"max_gpu_count"`
}

type ContainerCostHookConfig struct {
	Endpoint string `key:"endpoint" json:"endpoint"`
	Token    string `key:"token" json:"token"`
}

type GatewayServiceConfig struct {
	Host            string        `key:"host" json:"host"`
	InvokeURLType   string        `key:"invokeURLType" json:"invoke_url_type"`
	GRPC            GRPCConfig    `key:"grpc" json:"grpc"`
	HTTP            HTTPConfig    `key:"http" json:"http"`
	ShutdownTimeout time.Duration `key:"shutdownTimeout" json:"shutdown_timeout"`
	StubLimits      StubLimits    `key:"stubLimits" json:"stub_limits"`
}

type FileServiceConfig struct {
	EndpointURL string `key:"endpointUrl" json:"endpoint_url"`
	BucketName  string `key:"bucketName" json:"bucket_name"`
	AccessKey   string `key:"accessKey" json:"access_key"`
	SecretKey   string `key:"secretKey" json:"secret_key"`
	Region      string `key:"region" json:"region"`

	// Determines if the SDK should use this service
	// Requires that EndpointURL and BucketName are set
	Enabled bool `key:"enabled" json:"enabled"`
}

type ImageServiceConfig struct {
	LocalCacheEnabled              bool                  `key:"localCacheEnabled" json:"local_cache_enabled"`
	RegistryStore                  string                `key:"registryStore" json:"registry_store"`
	RegistryCredentialProviderName string                `key:"registryCredentialProvider" json:"registry_credential_provider_name"`
	Registries                     ImageRegistriesConfig `key:"registries" json:"registries"`
	PythonVersion                  string                `key:"pythonVersion" json:"python_version"`
	EnableTLS                      bool                  `key:"enableTLS" json:"enable_tls"`
	BuildContainerCpu              int64                 `key:"buildContainerCpu" json:"build_container_cpu"`
	BuildContainerMemory           int64                 `key:"buildContainerMemory" json:"build_container_memory"`
	BuildContainerPoolSelector     string                `key:"buildContainerPoolSelector" json:"build_container_pool_selector"`
	Runner                         RunnerConfig          `key:"runner" json:"runner"`
	ArchiveNanosecondsPerByte      int64                 `key:"archiveNanosecondsPerByte" json:"archive_nanoseconds_per_byte"`
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
	Mode           string           `key:"mode" json:"mode"`
	FilesystemName string           `key:"fsName" json:"filesystem_name"`
	FilesystemPath string           `key:"fsPath" json:"filesystem_path"`
	ObjectPath     string           `key:"objectPath" json:"object_path"`
	JuiceFS        JuiceFSConfig    `key:"juicefs" json:"juicefs"`
	CunoFS         CunoFSConfig     `key:"cunofs" json:"cunofs"`
	MountPoint     MountPointConfig `key:"mountpoint" json:"mountpoint"`
}

type JuiceFSConfig struct {
	RedisURI     string `key:"redisURI" json:"redis_uri"`
	AWSS3Bucket  string `key:"awsS3Bucket" json:"aws_s3_bucket"`
	AWSAccessKey string `key:"awsAccessKey" json:"aws_access_key"`
	AWSSecretKey string `key:"awsSecretKey" json:"aws_secret_key"`
	CacheSize    int64  `key:"cacheSize" json:"cache_size"`
	BlockSize    int64  `key:"blockSize" json:"block_size"`
	Prefetch     int64  `key:"prefetch" json:"prefetch"`
	BufferSize   int64  `key:"bufferSize" json:"buffer_size"`
}

type CunoFSConfig struct {
	LicenseKey    string `key:"licenseKey" json:"license_key"`
	S3AccessKey   string `key:"s3AccessKey" json:"s3_access_key"`
	S3SecretKey   string `key:"s3SecretKey" json:"s3_secret_key"`
	S3EndpointUrl string `key:"s3EndpointURL" json:"s3_endpoint_url"`
	S3BucketName  string `key:"s3BucketName" json:"s3_bucket_name"`
}

// @go2proto
type MountPointConfig struct {
	BucketName  string `json:"s3_bucket"`
	AccessKey   string `json:"access_key"`
	SecretKey   string `json:"secret_key"`
	EndpointURL string `json:"bucket_url"`
	Region      string `json:"region"`
	ReadOnly    bool   `json:"read_only"`
}

func (m *MountPointConfig) ToProto() *pb.MountPointConfig {
	return &pb.MountPointConfig{
		BucketName:  m.BucketName,
		AccessKey:   m.AccessKey,
		SecretKey:   m.SecretKey,
		EndpointUrl: m.EndpointURL,
		Region:      m.Region,
		ReadOnly:    m.ReadOnly,
	}
}

func NewMountPointConfigFromProto(in *pb.MountPointConfig) *MountPointConfig {
	return &MountPointConfig{
		BucketName:  in.BucketName,
		AccessKey:   in.AccessKey,
		SecretKey:   in.SecretKey,
		EndpointURL: in.EndpointUrl,
		Region:      in.Region,
		ReadOnly:    in.ReadOnly,
	}
}

type WorkerConfig struct {
	Pools                        map[string]WorkerPoolConfig `key:"pools" json:"pools"`
	HostNetwork                  bool                        `key:"hostNetwork" json:"host_network"`
	UseGatewayServiceHostname    bool                        `key:"useGatewayServiceHostname" json:"use_gateway_service_hostname"`
	UseHostResolvConf            bool                        `key:"useHostResolvConf" json:"use_host_resolv_conf"`
	ImageTag                     string                      `key:"imageTag" json:"image_tag"`
	ImageName                    string                      `key:"imageName" json:"image_name"`
	ImageRegistry                string                      `key:"imageRegistry" json:"image_registry"`
	ImagePullSecrets             []string                    `key:"imagePullSecrets" json:"image_pull_secrets"`
	Namespace                    string                      `key:"namespace" json:"namespace"`
	ServiceAccountName           string                      `key:"serviceAccountName" json:"service_account_name"`
	JobResourcesEnforced         bool                        `key:"jobResourcesEnforced" json:"job_resources_enforced"`
	RunCResourcesEnforced        bool                        `key:"runcResourcesEnforced" json:"runc_resources_enforced"`
	DefaultWorkerCPURequest      int64                       `key:"defaultWorkerCPURequest" json:"default_worker_cpu_request"`
	DefaultWorkerMemoryRequest   int64                       `key:"defaultWorkerMemoryRequest" json:"default_worker_memory_request"`
	ImagePVCName                 string                      `key:"imagePVCName" json:"image_pvc_name"`
	CleanupWorkerInterval        time.Duration               `key:"cleanupWorkerInterval" json:"cleanup_worker_interval"`
	CleanupPendingWorkerAgeLimit time.Duration               `key:"cleanupPendingWorkerAgeLimit" json:"cleanup_pending_worker_age_limit"`
	TerminationGracePeriod       int64                       `key:"terminationGracePeriod"`
	BlobCacheEnabled             bool                        `key:"blobCacheEnabled" json:"blob_cache_enabled"`
	CRIU                         CRIUConfig                  `key:"criu" json:"criu"`
	TmpSizeLimit                 string                      `key:"tmpSizeLimit" json:"tmp_size_limit"`
	ContainerLogLinesPerHour     int                         `key:"containerLogLinesPerHour" json:"container_log_lines_per_hour"`
	Failover                     FailoverConfig              `key:"failover" json:"failover"`
}

type FailoverConfig struct {
	Enabled                bool  `key:"enabled" json:"enabled"`
	MaxPendingWorkers      int64 `key:"maxPendingWorkers" json:"max_pending_workers"`
	MaxSchedulingLatencyMs int64 `key:"maxSchedulingLatencyMs" json:"max_scheduling_latency_ms"`
	MinMachinesAvailable   int64 `key:"minMachinesAvailable" json:"min_machines_available"`
}

type PoolMode string

var (
	PoolModeLocal    PoolMode = "local"
	PoolModeExternal PoolMode = "external"
)

type WorkerPoolConfig struct {
	GPUType              string                            `key:"gpuType" json:"gpu_type"`
	Runtime              string                            `key:"runtime" json:"runtime"`
	Mode                 PoolMode                          `key:"mode" json:"mode"`
	Provider             *MachineProvider                  `key:"provider" json:"provider"`
	JobSpec              WorkerPoolJobSpecConfig           `key:"jobSpec" json:"job_spec"`
	PoolSizing           WorkerPoolJobSpecPoolSizingConfig `key:"poolSizing" json:"pool_sizing"`
	DefaultMachineCost   float64                           `key:"defaultMachineCost" json:"default_machine_cost"`
	RequiresPoolSelector bool                              `key:"requiresPoolSelector" json:"requires_pool_selector"`
	Priority             int32                             `key:"priority" json:"priority"`
	Preemptable          bool                              `key:"preemptable" json:"preemptable"`
	UserData             string                            `key:"userData" json:"user_data"`
	CRIUEnabled          bool                              `key:"criuEnabled" json:"criu_enabled"`
	TmpSizeLimit         string                            `key:"tmpSizeLimit" json:"tmp_size_limit"`
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

type MachineProvider string

var (
	ProviderEC2        MachineProvider = "ec2"
	ProviderOCI        MachineProvider = "oci"
	ProviderLambdaLabs MachineProvider = "lambda"
	ProviderCrusoe     MachineProvider = "crusoe"
	ProviderHydra      MachineProvider = "hydra"
	ProviderGeneric    MachineProvider = "generic"
)

type ProviderConfig struct {
	EC2        EC2ProviderConfig        `key:"ec2" json:"ec2"`
	OCI        OCIProviderConfig        `key:"oci" json:"oci"`
	LambdaLabs LambdaLabsProviderConfig `key:"lambda" json:"lambda"`
	Crusoe     CrusoeProviderConfig     `key:"crusoe" json:"crusoe"`
	Hydra      HydraProviderConfig      `key:"hydra" json:"hydra"`
	Generic    GenericProviderConfig    `key:"generic" json:"generic"`
}

type ProviderAgentConfig struct {
	ElasticSearch ElasticSearchConfig `key:"elasticSearch" json:"elastic_search"`
	VictoriaLogs  VictoriaLogsConfig  `key:"victoriaLogs" json:"victoria_logs"`
}

type ElasticSearchConfig struct {
	Host       string `key:"host" json:"host"`
	Port       string `key:"port" json:"port"`
	HttpUser   string `key:"httpUser" json:"http_user"`
	HttpPasswd string `key:"httpPasswd" json:"http_passwd"`
}

type VictoriaLogsConfig struct {
	Host     string `key:"host" json:"host"`
	Port     string `key:"port" json:"port"`
	Username string `key:"username" json:"username"`
	Password string `key:"password" json:"password"`
}

type EC2ProviderConfig struct {
	AWSAccessKey string              `key:"awsAccessKey" json:"aws_access_key"`
	AWSSecretKey string              `key:"awsSecretKey" json:"aws_secret_key"`
	AWSRegion    string              `key:"awsRegion" json:"aws_region"`
	AMI          string              `key:"ami" json:"ami"`
	SubnetId     *string             `key:"subnetId" json:"subnet_id"`
	Agent        ProviderAgentConfig `key:"agent" json:"agent"`
}

type OCIProviderConfig struct {
	Tenancy            string              `key:"tenancy" json:"tenancy"`
	UserId             string              `key:"userId" json:"user_id"`
	Region             string              `key:"region" json:"region"`
	FingerPrint        string              `key:"fingerprint" json:"fingerprint"`
	PrivateKey         string              `key:"privateKey" json:"private_key"`
	PrivateKeyPassword string              `key:"privateKeyPassword" json:"private_key_password"`
	CompartmentId      string              `key:"compartmentId" json:"compartment_id"`
	SubnetId           string              `key:"subnetId" json:"subnet_id"`
	AvailabilityDomain string              `key:"availabilityDomain" json:"availability_domain"`
	ImageId            string              `key:"imageId" json:"image_id"`
	Agent              ProviderAgentConfig `key:"agent" json:"agent"`
}

type LambdaLabsProviderConfig struct {
	ApiKey string              `key:"apiKey" json:"apiKey"`
	Agent  ProviderAgentConfig `key:"agent" json:"agent"`
}

type CrusoeProviderConfig struct {
	Agent ProviderAgentConfig `key:"agent" json:"agent"`
}

type HydraProviderConfig struct {
	Agent ProviderAgentConfig `key:"agent" json:"agent"`
}

type GenericProviderConfig struct {
	Agent ProviderAgentConfig `key:"agent" json:"agent"`
}

type MetricsCollector string

var (
	MetricsCollectorPrometheus MetricsCollector = "prometheus"
	MetricsCollectorOpenMeter  MetricsCollector = "openmeter"
)

type MonitoringConfig struct {
	MetricsCollector         string                  `key:"metricsCollector" json:"metrics_collector"`
	Prometheus               PrometheusConfig        `key:"prometheus" json:"prometheus"`
	OpenMeter                OpenMeterConfig         `key:"openmeter" json:"openmeter"`
	FluentBit                FluentBitConfig         `key:"fluentbit" json:"fluentbit"`
	Telemetry                TelemetryConfig         `key:"telemetry" json:"telemetry"`
	ContainerMetricsInterval time.Duration           `key:"containerMetricsInterval" json:"container_metrics_interval"`
	VictoriaMetrics          VictoriaMetricsConfig   `key:"victoriametrics" json:"victoriametrics"`
	ContainerCostHookConfig  ContainerCostHookConfig `key:"containerCostHook" json:"container_cost_hook"`
}

type VictoriaMetricsConfig struct {
	PushURL   string `key:"pushURL" json:"push_url"`
	AuthToken string `key:"authToken" json:"auth_token"`
	PushSecs  int    `key:"pushSecs" json:"push_secs"`
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
	ControlURL string `key:"controlUrl" json:"control_url"`
	User       string `key:"user" json:"user"`
	AuthKey    string `key:"authKey" json:"auth_key"`
	HostName   string `key:"hostName" json:"host_name"`
	Enabled    bool   `key:"enabled" json:"enabled"`
	Debug      bool   `key:"debug" json:"debug"`
}

type ProxyConfig struct {
	HTTPPort int               `key:"httpPort" json:"http_port"`
	Services []InternalService `key:"services" json:"services"`
}

type InternalService struct {
	Name        string `key:"name" json:"name"`
	LocalPort   int    `key:"localPort" json:"local_port"`
	Destination string `key:"destination" json:"destination"`
}

type FluentBitConfig struct {
	Events FluentBitEventConfig `key:"events" json:"events"`
}

type FluentBitEventMapping struct {
	Name string `key:"name" json:"name"`
	Tag  string `key:"tag" json:"tag"`
}

type ObjectStoreConfig struct {
	BucketName  string `key:"bucketName" json:"bucket_name"`
	AccessKey   string `key:"accessKey" json:"access_key"`
	SecretKey   string `key:"secretKey" json:"secret_key"`
	EndpointURL string `key:"endpointURL" json:"bucket_url"`
	Region      string `key:"region" json:"region"`
	ReadOnly    bool   `key:"readOnly" json:"read_only"`
}

type FluentBitEventConfig struct {
	Endpoint        string                  `key:"endpoint" json:"endpoint"`
	MaxConns        int                     `key:"maxConns" json:"max_conns"`
	MaxIdleConns    int                     `key:"maxIdleConns" json:"max_idle_conns"`
	IdleConnTimeout time.Duration           `key:"idleConnTimeout" json:"idle_conn_timeout"`
	DialTimeout     time.Duration           `key:"dialTimeout" json:"dial_timeout"`
	KeepAlive       time.Duration           `key:"keepAlive" json:"keep_alive"`
	Mapping         []FluentBitEventMapping `key:"mapping" json:"mapping"`
}

type CRIUConfigMode string

var (
	CRIUConfigModeCedana CRIUConfigMode = "cedana"
	CRIUConfigModeNvidia CRIUConfigMode = "nvidia"
)

type CRIUConfig struct {
	Mode    CRIUConfigMode          `key:"mode" json:"mode"`
	Storage CheckpointStorageConfig `key:"storage" json:"storage"`
	Cedana  cedana.Config           `key:"cedana" json:"cedana"`
	Nvidia  NvidiaCRIUConfig        `key:"nvidia" json:"nvidia"`
}

type NvidiaCRIUConfig struct {
}

type CheckpointStorageConfig struct {
	MountPath   string            `key:"mountPath" json:"mount_path"`
	Mode        string            `key:"mode" json:"mode"`
	ObjectStore ObjectStoreConfig `key:"objectStore" json:"object_store"`
}

type CheckpointStorageMode string

var (
	CheckpointStorageModeLocal CheckpointStorageMode = "local"
	CheckpointStorageModeS3    CheckpointStorageMode = "s3"
)

type AbstractionConfig struct {
	Bot BotConfig `key:"bot" json:"bot"`
}

type BotConfig struct {
	SystemPrompt              string `key:"systemPrompt" json:"system_prompt"`
	StepIntervalS             uint   `key:"stepIntervalS" json:"step_interval_s"`
	SessionInactivityTimeoutS uint   `key:"sessionInactivityTimeoutS" json:"session_inactivity_timeout_s"`
}
