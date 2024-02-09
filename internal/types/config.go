package types

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

type AppConfig struct {
	DebugMode      bool                 `key:"debugMode"`
	Database       DatabaseConfig       `key:"database"`
	GatewayService GatewayServiceConfig `key:"gateway"`
	ImageService   ImageServiceConfig   `key:"imageservice"`
	Monitoring     MonitoringConfig     `key:"monitoring"`
	Storage        StorageConfig        `key:"storage"`
	Worker         WorkerConfig         `key:"worker"`
}

type DatabaseConfig struct {
	Redis    RedisConfig    `key:"redis"`
	Postgres PostgresConfig `key:"postgres"`
}

type RedisMode string

var (
	RedisModeSingle  RedisMode = "single"
	RedisModeCluster RedisMode = "cluster"
)

type RedisConfig struct {
	Addrs           []string      `key:"addrs"`
	Mode            RedisMode     `key:"mode"`
	ClientName      string        `key:"clientName"`
	EnableTLS       bool          `key:"enableTLS"`
	MinIdleConns    int           `key:"minIdleConns"`
	MaxIdleConns    int           `key:"maxIdleConns"`
	ConnMaxIdleTime time.Duration `key:"connMaxIdleTime"`
	ConnMaxLifetime time.Duration `key:"connMaxLifetime"`
	DialTimeout     time.Duration `key:"dialTimeout"`
	ReadTimeout     time.Duration `key:"readTimeout"`
	WriteTimeout    time.Duration `key:"writeTimeout"`
	MaxRedirects    int           `key:"maxRedirects"`
	MaxRetries      int           `key:"maxRetries"`
	PoolSize        int           `key:"poolSize"`
	Username        string        `key:"username"`
	Password        string        `key:"password"`
	RouteByLatency  bool          `key:"routeByLatency"`
}

type PostgresConfig struct {
	Host      string `key:"host"`
	Port      int    `key:"port"`
	Name      string `key:"name"`
	Username  string `key:"username"`
	Password  string `key:"password"`
	TimeZone  string `key:"timezone"`
	EnableTLS bool   `key:"enableTLS"`
}

type GatewayServiceConfig struct {
	Host               string `key:"host"`
	HTTPPort           int    `key:"httpPort"`
	GRPCPort           int    `key:"grpcPort"`
	GRPCMaxRecvMsgSize int    `key:"grpcMaxRecvMsgSizeInMB"`
	GRPCMaxSendMsgSize int    `key:"grpcMaxSendMsgSizeInMB"`
}

type ImageServiceConfig struct {
	CacheURL                       string                `key:"cacheURL"`
	RegistryStore                  string                `key:"registryStore"`
	RegistryCredentialProviderName string                `key:"registryCredentialProvider"`
	Registries                     ImageRegistriesConfig `key:"registries"`
	EnableTLS                      bool                  `key:"enableTLS"`
	Runner                         RunnerConfig          `key:"runner"`
}

type ImageRegistriesConfig struct {
	Docker DockerImageRegistryConfig `key:"docker"`
	S3     S3ImageRegistryConfig     `key:"s3"`
}

type DockerImageRegistryConfig struct {
	Username string `key:"username"`
	Password string `key:"password"`
}

type S3ImageRegistryConfig struct {
	AccessKeyID     string `key:"accessKeyID"`
	SecretAccessKey string `key:"secretAccessKey"`
	Bucket          string `key:"bucket"`
	Region          string `key:"region"`
}

type RunnerConfig struct {
	BaseImageName     string            `key:"baseImageName"`
	BaseImageRegistry string            `key:"baseImageRegistry"`
	BaseImageTag      string            `key:"baseImageTag"`
	Tags              map[string]string `key:"tags"`
}

type StorageConfig struct {
	Mode           string        `key:"mode"`
	FilesystemName string        `key:"fsName"`
	FilesystemPath string        `key:"fsPath"`
	ObjectPath     string        `key:"objectPath"`
	JuiceFS        JuiceFSConfig `key:"juicefs"`
}

type JuiceFSConfig struct {
	RedisURI           string `key:"redisURI"`
	AWSS3Bucket        string `key:"awsS3Bucket"`
	AWSAccessKeyID     string `key:"awsAccessKeyID"`
	AWSSecretAccessKey string `key:"awsSecretAccessKey"`
}

type WorkerConfig struct {
	Pools              map[string]WorkerPoolConfig `key:"pools"`
	HostNetwork        bool                        `key:"hostNetwork"`
	ImageTag           string                      `key:"imageTag"`
	ImageName          string                      `key:"imageName"`
	ImageRegistry      string                      `key:"imageRegistry"`
	ImagePullSecrets   []string                    `key:"imagePullSecrets"`
	Namespace          string                      `key:"namespace"`
	ServiceAccountName string                      `key:"serviceAccountName"`

	ResourcesEnforced          bool   `key:"resourcesEnforced"`
	DefaultWorkerCPURequest    int64  `key:"defaultWorkerCPURequest"`
	DefaultWorkerMemoryRequest int64  `key:"defaultWorkerMemoryRequest"`
	TerminationGracePeriod     int64  `key:"terminationGracePeriod"`
	ImagePVCName               string `key:"imagePVCName"`
}

type WorkerPoolConfig struct {
	GPUType    string                            `key:"gpuType"`
	Runtime    string                            `key:"runtime"`
	JobSpec    WorkerPoolJobSpecConfig           `key:"jobSpec"`
	PoolSizing WorkerPoolJobSpecPoolSizingConfig `key:"poolSizing"`
}

type WorkerPoolJobSpecConfig struct {
	NodeSelector map[string]string `key:"nodeSelector"`
	Env          []corev1.EnvVar   `key:"env"`

	// Mimics corev1.Volume since that type doesn't currently serialize correctly
	Volumes []struct {
		Name   string `key:"name"`
		Secret struct {
			SecretName string `key:"secretName"`
		} `key:"secret"`
	} `key:"volumes"`

	VolumeMounts []corev1.VolumeMount `key:"volumeMounts"`
}

type WorkerPoolJobSpecPoolSizingConfig struct {
	DefaultWorkerCPU     string `key:"defaultWorkerCPU"`
	DefaultWorkerMemory  string `key:"defaultWorkerMemory"`
	DefaultWorkerGPUType string `key:"defaultWorkerGPUType"`
	MinFreeCPU           string `key:"minFreeCPU"`
	MinFreeMemory        string `key:"minFreeMemory"`
	MinFreeGPU           string `key:"minFreeGPU"`
}

type MonitoringConfig struct {
	Prometheus PrometheusConfig `key:"prometheus"`
	FluentBit  FluentBitConfig  `key:"fluentbit"`
}

type PrometheusConfig struct {
	ScrapeWorkers bool `key:"scrapeWorkers"`
	Port          int  `key:"port"`
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
