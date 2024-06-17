package types

import (
	"time"

	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	corev1 "k8s.io/api/core/v1"
)

type AppConfig struct {
	ClusterName    string               `key:"clusterName" json:"cluster_name"`
	DebugMode      bool                 `key:"debugMode" json:"debug_mode"`
	Database       DatabaseConfig       `key:"database" json:"database"`
	GatewayService GatewayServiceConfig `key:"gateway" json:"gateway_service"`
	ImageService   ImageServiceConfig   `key:"imageservice" json:"image_service"`
	Storage        StorageConfig        `key:"storage" json:"storage"`
	Worker         WorkerConfig         `key:"worker" json:"worker"`
	Providers      ProviderConfig       `key:"providers" json:"providers"`
	Tailscale      TailscaleConfig      `key:"tailscale" json:"tailscale"`
	Proxy          ProxyConfig          `key:"proxy" json:"proxy"`
	Monitoring     MonitoringConfig     `key:"monitoring" json:"monitoring"`
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
	Port           int `key:"port" json:"port"`
	MaxRecvMsgSize int `key:"maxRecvMsgSize" json:"max_recv_msg_size"`
	MaxSendMsgSize int `key:"maxSendMsgSize" json:"max_send_msg_size"`
}

type HTTPConfig struct {
	EnablePrettyLogs bool       `key:"enablePrettyLogs" json:"enable_pretty_logs"`
	CORS             CORSConfig `key:"cors" json:"cors"`
	Port             int        `key:"port" json:"port"`
}

type CORSConfig struct {
	AllowedOrigins []string `key:"allowOrigins" json:"allow_origins"`
	AllowedMethods []string `key:"allowMethods" json:"allow_methods"`
	AllowedHeaders []string `key:"allowHeaders" json:"allow_headers"`
}

type GatewayServiceConfig struct {
	Host            string        `key:"host" json:"host"`
	ExternalURL     string        `key:"externalURL" json:"external_url"`
	GRPC            GRPCConfig    `key:"grpc" json:"grpc"`
	HTTP            HTTPConfig    `key:"http" json:"http"`
	ShutdownTimeout time.Duration `key:"shutdownTimeout" json:"shutdown_timeout"`
}

type ImageServiceConfig struct {
	CacheURL                       string                    `key:"cacheURL" json:"cache_url"`
	CacheConfig                    blobcache.BlobCacheConfig `key:"cacheConfig" json:"cache_config"`
	CachedEnabled                  bool                      `key:"cachedEnabled" json:"cache_enabled"`
	RegistryStore                  string                    `key:"registryStore" json:"registry_store"`
	RegistryCredentialProviderName string                    `key:"registryCredentialProvider" json:"registry_credential_provider_name"`
	Registries                     ImageRegistriesConfig     `key:"registries" json:"registries"`
	LocalCacheEnabled              bool                      `key:"localCacheEnabled" json:"local_cache_enabled"`
	EnableTLS                      bool                      `key:"enableTLS" json:"enable_tls"`
	BuildContainerCpu              int64                     `key:"buildContainerCpu" json:"build_container_cpu"`
	BuildContainerMemory           int64                     `key:"buildContainerMemory" json:"build_container_memory"`
	BuildContainerPoolSelector     string                    `key:"buildContainerPoolSelector" json:"build_container_pool_selector"`
	Runner                         RunnerConfig              `key:"runner" json:"runner"`
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
	BucketName string `key:"bucketName" json:"bucket_name"`
	AccessKey  string `key:"accessKey" json:"access_key"`
	SecretKey  string `key:"secretKey" json:"secret_key"`
	Region     string `key:"region" json:"region"`
	Endpoint   string `key:"endpoint" json:"endpoint"`
}

type RunnerConfig struct {
	BaseImageName     string            `key:"baseImageName" json:"base_image_name"`
	BaseImageRegistry string            `key:"baseImageRegistry" json:"base_image_registry"`
	BaseImageTag      string            `key:"baseImageTag" json:"base_image_tag"`
	Tags              map[string]string `key:"tags" json:"tags"`
}

type StorageConfig struct {
	Mode           string           `key:"mode" json:"mode"`
	FilesystemName string           `key:"fsName" json:"filesystem_name"`
	FilesystemPath string           `key:"fsPath" json:"filesystem_path"`
	ObjectPath     string           `key:"objectPath" json:"object_path"`
	JuiceFS        JuiceFSConfig    `key:"juicefs" json:"juicefs"`
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

type MountPointConfig struct {
	AWSS3Bucket  string `key:"awsS3Bucket" json:"aws_s3_bucket"`
	AWSAccessKey string `key:"awsAccessKey" json:"aws_access_key"`
	AWSSecretKey string `key:"awsSecretKey" json:"aws_secret_key"`
}

type WorkerConfig struct {
	Pools                      map[string]WorkerPoolConfig `key:"pools" json:"pools"`
	HostNetwork                bool                        `key:"hostNetwork" json:"host_network"`
	ImageTag                   string                      `key:"imageTag" json:"image_tag"`
	ImageName                  string                      `key:"imageName" json:"image_name"`
	ImageRegistry              string                      `key:"imageRegistry" json:"image_registry"`
	ImagePullSecrets           []string                    `key:"imagePullSecrets" json:"image_pull_secrets"`
	Namespace                  string                      `key:"namespace" json:"namespace"`
	ServiceAccountName         string                      `key:"serviceAccountName" json:"service_account_name"`
	ResourcesEnforced          bool                        `key:"resourcesEnforced" json:"resources_enforced"`
	DefaultWorkerCPURequest    int64                       `key:"defaultWorkerCPURequest" json:"default_worker_cpu_request"`
	DefaultWorkerMemoryRequest int64                       `key:"defaultWorkerMemoryRequest" json:"default_worker_memory_request"`
	ImagePVCName               string                      `key:"imagePVCName" json:"image_pvc_name"`
	AddWorkerTimeout           time.Duration               `key:"addWorkerTimeout" json:"add_worker_timeout"`
	TerminationGracePeriod     int64                       `key:"terminationGracePeriod"`
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
	RequiresPoolSelector bool                              `key:"requiresPoolSelector" json:"requires_pool_selector"`
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
}

type MachineProvider string

var (
	ProviderEC2        MachineProvider = "ec2"
	ProviderOCI        MachineProvider = "oci"
	ProviderLambdaLabs MachineProvider = "lambda"
)

type ProviderConfig struct {
	EC2Config        EC2ProviderConfig        `key:"ec2" json:"ec2"`
	OCIConfig        OCIProviderConfig        `key:"oci" json:"oci"`
	LambdaLabsConfig LambdaLabsProviderConfig `key:"lambda" json:"lambda"`
}

type ProviderAgentConfig struct {
	ElasticSearch ElasticSearchConfig `key:"elasticSearch" json:"elastic_search"`
}

type ElasticSearchConfig struct {
	Host       string `key:"host" json:"host"`
	Port       string `key:"port" json:"port"`
	HttpUser   string `key:"httpUser" json:"http_user"`
	HttpPasswd string `key:"httpPasswd" json:"http_passwd"`
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

type MetricsCollector string

var (
	MetricsCollectorPrometheus MetricsCollector = "prometheus"
	MetricsCollectorOpenMeter  MetricsCollector = "openmeter"
)

type MonitoringConfig struct {
	MetricsCollector string           `key:"metricsCollector" json:"metrics_collector"`
	Prometheus       PrometheusConfig `key:"prometheus" json:"prometheus"`
	OpenMeter        OpenMeterConfig  `key:"openmeter" json:"openmeter"`
	FluentBit        FluentBitConfig  `key:"fluentbit" json:"fluentbit"`
}

type PrometheusConfig struct {
	ScrapeWorkers bool `key:"scrapeWorkers" json:"scrape_workers"`
	Port          int  `key:"port" json:"port"`
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

type FluentBitEventConfig struct {
	Endpoint        string        `key:"endpoint" json:"endpoint"`
	MaxConns        int           `key:"maxConns" json:"max_conns"`
	MaxIdleConns    int           `key:"maxIdleConns" json:"max_idle_conns"`
	IdleConnTimeout time.Duration `key:"idleConnTimeout" json:"idle_conn_timeout"`
	DialTimeout     time.Duration `key:"dialTimeout" json:"dial_timeout"`
	KeepAlive       time.Duration `key:"keepAlive" json:"keep_alive"`
}
