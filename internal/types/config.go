package types

import (
	"time"

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
	Addrs           []string      `key:"addrs" json:"addrs"`
	Mode            RedisMode     `key:"mode" json:"mode"`
	ClientName      string        `key:"clientName" json:"client_name"`
	EnableTLS       bool          `key:"enableTLS" json:"enable_tls"`
	MinIdleConns    int           `key:"minIdleConns" json:"min_idle_conns"`
	MaxIdleConns    int           `key:"maxIdleConns" json:"max_idle_conns"`
	ConnMaxIdleTime time.Duration `key:"connMaxIdleTime" json:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `key:"connMaxLifetime" json:"conn_max_lifetime"`
	DialTimeout     time.Duration `key:"dialTimeout" json:"dial_timeout"`
	ReadTimeout     time.Duration `key:"readTimeout" json:"read_timeout"`
	WriteTimeout    time.Duration `key:"writeTimeout" json:"write_timeout"`
	MaxRedirects    int           `key:"maxRedirects" json:"max_redirects"`
	MaxRetries      int           `key:"maxRetries" json:"max_retries"`
	PoolSize        int           `key:"poolSize" json:"pool_size"`
	Username        string        `key:"username" json:"username"`
	Password        string        `key:"password" json:"password"`
	RouteByLatency  bool          `key:"routeByLatency" json:"route_by_latency"`
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

type GatewayServiceConfig struct {
	Host           string `key:"host" json:"host"`
	HTTPPort       int    `key:"httpPort" json:"http_port"`
	GRPCPort       int    `key:"grpcPort" json:"grpc_port"`
	MaxRecvMsgSize int    `key:"maxRecvMsgSize" json:"max_recv_msg_size"`
	MaxSendMsgSize int    `key:"maxSendMsgSize" json:"max_send_msg_size"`
}

type ImageServiceConfig struct {
	CacheURL                       string                `key:"cacheURL" json:"cache_url"`
	RegistryStore                  string                `key:"registryStore" json:"registry_store"`
	RegistryCredentialProviderName string                `key:"registryCredentialProvider" json:"registry_credential_provider_name"`
	Registries                     ImageRegistriesConfig `key:"registries" json:"registries"`
	EnableTLS                      bool                  `key:"enableTLS" json:"enable_tls"`
	Runner                         RunnerConfig          `key:"runner" json:"runner"`
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
	AWSS3Bucket  string `key:"awsS3Bucket" json:"aws_s3_bucket"`
	AWSAccessKey string `key:"awsAccessKey" json:"aws_access_key"`
	AWSSecretKey string `key:"awsSecretKey" json:"aws_secret_key"`
	AWSRegion    string `key:"awsRegion" json:"aws_region"`
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
	PoolModeLocal  PoolMode = "local"
	PoolModeMetal  PoolMode = "metal"
	PoolModeRemote PoolMode = "remote"
)

type WorkerPoolConfig struct {
	GPUType    string                            `key:"gpuType" json:"gpu_type"`
	Runtime    string                            `key:"runtime" json:"runtime"`
	Mode       PoolMode                          `key:"mode" json:"mode"`
	Provider   *MachineProvider                  `key:"provider" json:"provider"`
	JobSpec    WorkerPoolJobSpecConfig           `key:"jobSpec" json:"job_spec"`
	PoolSizing WorkerPoolJobSpecPoolSizingConfig `key:"poolSizing" json:"pool_sizing"`
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
	ProviderVastAI     MachineProvider = "vastai"
	ProviderLambdaLabs MachineProvider = "lambda"
)

type ProviderConfig struct {
	EC2Config EC2ProviderConfig `key:"ec2" json:"ec2"`
	OCIConfig OCIProviderConfig `key:"oci" json:"oci"`
}

type EC2ProviderConfig struct {
	AWSAccessKey string  `key:"awsAccessKey" json:"aws_access_key"`
	AWSSecretKey string  `key:"awsSecretKey" json:"aws_secret_key"`
	AWSRegion    string  `key:"awsRegion" json:"aws_region"`
	AMI          string  `key:"ami" json:"ami"`
	SubnetId     *string `key:"subnetId" json:"subnet_id"`
}

type OCIProviderConfig struct {
	Tenancy            string `key:"tenancy" json:"tenancy"`
	UserId             string `key:"userId" json:"user_id"`
	Region             string `key:"region" json:"region"`
	FingerPrint        string `key:"fingerprint" json:"fingerprint"`
	PrivateKey         string `key:"privateKey" json:"private_key"`
	PrivateKeyPassword string `key:"privateKeyPassword" json:"private_key_password"`
	CompartmentId      string `key:"compartmentId" json:"compartment_id"`
	SubnetId           string `key:"subnetId" json:"subnet_id"`
	AvailabilityDomain string `key:"availabilityDomain" json:"availability_domain"`
	ImageId            string `key:"imageId" json:"image_id"`
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
	Services []InternalService `key:"services" json:"services"`
}

type InternalService struct {
	Name        string `key:"name" json:"name"`
	LocalPort   int    `key:"localPort" json:"local_port"`
	Destination string `key:"destination" json:"destination"`
}

type FluentBitConfig struct {
	Events FluentBitEventConfig `key:"events"`
}

type FluentBitEventConfig struct {
	Endpoint        string        `key:"endpoint"`
	MaxConns        int           `key:"maxConns"`
	MaxIdleConns    int           `key:"maxIdleConns"`
	IdleConnTimeout time.Duration `key:"idleConnTimeout"`
	DialTimeout     time.Duration `key:"dialTimeout"`
	KeepAlive       time.Duration `key:"keepAlive"`
}
