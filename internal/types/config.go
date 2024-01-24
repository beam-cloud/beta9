package types

import "time"

type AppConfig struct {
	DebugMode      bool                 `key:"debugMode"`
	Database       DatabaseConfig       `key:"database"`
	GatewayService GatewayServiceConfig `key:"gateway"`
	ImageService   ImageServiceConfig   `key:"imageservice"`
	Metrics        MetricsConfig        `key:"metrics"`
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
	Host           string `key:"host"`
	Port           int    `key:"port"`
	MaxRecvMsgSize int    `key:"max_recv_msg_size_in_mb"`
	MaxSendMsgSize int    `key:"max_send_msg_size_in_mb"`
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

	ResourcesEnforced          bool  `key:"resourcesEnforced"`
	DefaultWorkerCPURequest    int64 `key:"defaultWorkerCPURequest"`
	DefaultWorkerMemoryRequest int64 `key:"defaultWorkerMemoryRequest"`
}

type WorkerPoolConfig struct {
	GPUType    string                            `key:"gpuType"`
	Runtime    string                            `key:"runtime"`
	JobSpec    WorkerPoolJobSpecConfig           `key:"jobSpec"`
	PoolSizing WorkerPoolJobSpecPoolSizingConfig `key:"poolSizing"`
}

type WorkerPoolJobSpecConfig struct {
	NodeSelector map[string]string `key:"nodeSelector"`
}

type WorkerPoolJobSpecPoolSizingConfig struct {
	DefaultWorkerCPU     string `key:"defaultWorkerCPU"`
	DefaultWorkerMemory  string `key:"defaultWorkerMemory"`
	DefaultWorkerGPUType string `key:"defaultWorkerGPUType"`
	MinFreeCPU           string `key:"minFreeCPU"`
	MinFreeMemory        string `key:"minFreeMemory"`
	MinFreeGPU           string `key:"minFreeGPU"`
}

type MetricsConfig struct {
	Kinesis KinesisConfig `key:"kinesis"`
}

type KinesisConfig struct {
	StreamName      string `key:"streamName"`
	Region          string `key:"region"`
	AccessKeyID     string `key:"accessKeyID"`
	SecretAccessKey string `key:"secretAccessKey"`
	SessionKey      string `key:"sessionKey"`
	Endpoint        string `key:"endpoint"`
}
