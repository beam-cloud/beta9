package cache

import (
	"os"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
)

const (
	HostPrefix string = "cache-host"
	Version    string = "dev"
)

const (
	defaultHostStorageCapacityThresholdPct float64 = 0.95
	defaultHostKeepAliveIntervalS          int     = 10
	defaultHostKeepAliveTimeoutS           int     = 60
)

type Config struct {
	Enabled  bool           `key:"enabled" json:"enabled"`
	Disk     DiskConfig     `key:"disk" json:"disk"`
	Memory   MemoryConfig   `key:"memory" json:"memory"`
	Embedded EmbeddedConfig `key:"embedded" json:"embedded"`
	Server   ServerConfig   `key:"server" json:"server"`
	Client   ClientConfig   `key:"client" json:"client"`
	Global   GlobalConfig   `key:"global" json:"global"`
	Metrics  MetricsConfig  `key:"metrics" json:"metrics"`
}

type DiskConfig struct {
	Enabled     bool    `key:"enabled" json:"enabled"`
	HostPath    string  `key:"hostPath" json:"host_path"`
	MountPath   string  `key:"mountPath" json:"mount_path"`
	MaxUsagePct float64 `key:"maxUsagePct" json:"max_usage_pct"`
}

type MemoryConfig struct {
	Enabled     bool  `key:"enabled" json:"enabled"`
	MaxCachePct int64 `key:"maxCachePct" json:"max_cache_pct"`
}

type MetricsConfig struct {
	PushIntervalS int    `key:"pushIntervalS" json:"push_interval_s"`
	URL           string `key:"url" json:"url"`
	Username      string `key:"username" json:"username"`
	Password      string `key:"password" json:"password"`
}

type EmbeddedMode string

const (
	EmbeddedModeActiveActive  EmbeddedMode = "active-active"
	EmbeddedModeSinglePrimary EmbeddedMode = "single-primary"
)

type EmbeddedConfig struct {
	Mode                EmbeddedMode `key:"mode" json:"mode"`
	ReplicasPerNode     int          `key:"replicasPerNode" json:"replicas_per_node"`
	LeaseTTLSeconds     int          `key:"leaseTTLSeconds" json:"lease_ttl_seconds"`
	LeaseRefreshSeconds int          `key:"leaseRefreshSeconds" json:"lease_refresh_seconds"`
	LeaseRetrySeconds   int          `key:"leaseRetrySeconds" json:"lease_retry_seconds"`
}

type GlobalConfig struct {
	DefaultLocality                 string  `key:"defaultLocality" json:"default_locality"`
	CoordinatorHost                 string  `key:"coordinatorHost" json:"coordinator_host"`
	ServerPort                      uint    `key:"serverPort" json:"server_port"`
	DiscoveryIntervalS              int     `key:"discoveryIntervalS" json:"discovery_interval_s"`
	DiscoveryJitterS                int     `key:"discoveryJitterS" json:"discovery_jitter_s"`
	MaxDiscoveryConcurrency         int     `key:"maxDiscoveryConcurrency" json:"max_discovery_concurrency"`
	HostMonitorIntervalS            int     `key:"hostMonitorIntervalS" json:"host_monitor_interval_s"`
	RoundTripThresholdMilliseconds  uint    `key:"rttThresholdMilliseconds" json:"rtt_threshold_ms"`
	HostStorageCapacityThresholdPct float64 `key:"hostStorageCapacityThresholdPct" json:"host_storage_capacity_threshold_pct"`
	GRPCDialTimeoutS                int     `key:"grpcDialTimeoutS" json:"grpc_dial_timeout_s"`
	GRPCMessageSizeBytes            int     `key:"grpcMessageSizeBytes" json:"grpc_message_size_bytes"`
	GRPCInitialWindowSize           int     `key:"grpcInitialWindowSize" json:"grpc_initial_window_size"`
	GRPCInitialConnWindowSize       int     `key:"grpcInitialConnWindowSize" json:"grpc_initial_conn_window_size"`
	GRPCWriteBufferSize             int     `key:"grpcWriteBufferSize" json:"grpc_write_buffer_size"`
	GRPCReadBufferSize              int     `key:"grpcReadBufferSize" json:"grpc_read_buffer_size"`
	GRPCMaxConcurrentStreams        int     `key:"grpcMaxConcurrentStreams" json:"grpc_max_concurrent_streams"`
	GRPCNumStreamWorkers            int     `key:"grpcNumStreamWorkers" json:"grpc_num_stream_workers"`
	DebugMode                       bool    `key:"debugMode" json:"debug_mode"`
	PrettyLogs                      bool    `key:"prettyLogs" json:"pretty_logs"`
}

func (c *GlobalConfig) GetLocality() string {
	locality := os.Getenv("CACHE_LOCALITY")
	if locality != "" {
		return locality
	}

	return c.DefaultLocality
}

type ServerMode string

const (
	ServerModeCoordinator ServerMode = "coordinator"
	ServerModeNode        ServerMode = "node"
)

type ServerConfig struct {
	Mode                  ServerMode     `key:"mode" json:"mode"`
	DiskCacheDir          string         `key:"diskCacheDir" json:"disk_cache_dir"`
	DiskCacheMaxUsagePct  float64        `key:"diskCacheMaxUsagePct" json:"disk_cache_max_usage_pct"`
	EnableMemoryCache     bool           `key:"enableMemoryCache" json:"enable_memory_cache"`
	Token                 string         `key:"token" json:"token"`
	PrettyLogs            bool           `key:"prettyLogs" json:"pretty_logs"`
	ObjectTtlS            int            `key:"objectTtlS" json:"object_ttl_s"`
	MaxCachePct           int64          `key:"maxCachePct" json:"max_cache_pct"`
	PageSizeBytes         int64          `key:"pageSizeBytes" json:"page_size_bytes"`
	Metadata              MetadataConfig `key:"metadata" json:"metadata"`
	Sources               []SourceConfig `key:"sources" json:"sources"`
	S3DownloadConcurrency int64          `key:"s3DownloadConcurrency" json:"s3_download_concurrency"`
	S3DownloadChunkSize   int64          `key:"s3DownloadChunkSize" json:"s3_download_chunk_size"`

	// Allows a coordinator to override a slave server's config for a specific locality/region
	Regions map[string]RegionConfig `key:"regions" json:"regions"`
}

func (c *ServerConfig) ToProto() *proto.CacheServerConfig {
	protoConfig := &proto.CacheServerConfig{
		Mode:                  string(c.Mode),
		DiskCacheDir:          c.DiskCacheDir,
		DiskCacheMaxUsagePct:  float32(c.DiskCacheMaxUsagePct),
		MaxCachePct:           c.MaxCachePct,
		PageSizeBytes:         c.PageSizeBytes,
		ObjectTtlS:            int64(c.ObjectTtlS),
		PrettyLogs:            c.PrettyLogs,
		Token:                 c.Token,
		Sources:               make([]*proto.CacheSourceConfig, 0),
		S3DownloadConcurrency: c.S3DownloadConcurrency,
		S3DownloadChunkSize:   c.S3DownloadChunkSize,
	}

	for _, source := range c.Sources {
		protoSource := &proto.CacheSourceConfig{
			Mode:           string(source.Mode),
			FilesystemName: source.FilesystemName,
			FilesystemPath: source.FilesystemPath,
		}

		switch source.Mode {
		case SourceModeMountPoint:
			protoSource.Mountpoint = &proto.CacheMountPointConfig{
				BucketName:     source.MountPoint.BucketName,
				AccessKey:      source.MountPoint.AccessKey,
				SecretKey:      source.MountPoint.SecretKey,
				Region:         source.MountPoint.Region,
				EndpointUrl:    source.MountPoint.EndpointURL,
				ForcePathStyle: source.MountPoint.ForcePathStyle,
			}

		case SourceModeJuiceFS:
			protoSource.Juicefs = &proto.CacheJuiceFSConfig{
				RedisUri:  source.JuiceFS.RedisURI,
				Bucket:    source.JuiceFS.Bucket,
				AccessKey: source.JuiceFS.AccessKey,
				SecretKey: source.JuiceFS.SecretKey,
			}

		}

		protoConfig.Sources = append(protoConfig.Sources, protoSource)
	}

	return protoConfig
}

func ServerConfigFromProto(protoConfig *proto.CacheServerConfig) ServerConfig {
	if protoConfig == nil {
		return ServerConfig{}
	}

	cfg := ServerConfig{
		Mode:                  ServerMode(protoConfig.Mode),
		DiskCacheDir:          protoConfig.DiskCacheDir,
		DiskCacheMaxUsagePct:  float64(protoConfig.DiskCacheMaxUsagePct),
		MaxCachePct:           protoConfig.MaxCachePct,
		PageSizeBytes:         protoConfig.PageSizeBytes,
		ObjectTtlS:            int(protoConfig.ObjectTtlS),
		PrettyLogs:            protoConfig.PrettyLogs,
		Token:                 protoConfig.Token,
		Sources:               make([]SourceConfig, len(protoConfig.Sources)),
		S3DownloadConcurrency: protoConfig.S3DownloadConcurrency,
		S3DownloadChunkSize:   protoConfig.S3DownloadChunkSize,
	}

	for i, protoSource := range protoConfig.Sources {
		if protoSource == nil {
			continue
		}

		localSource := SourceConfig{
			Mode:           protoSource.Mode,
			FilesystemName: protoSource.FilesystemName,
			FilesystemPath: protoSource.FilesystemPath,
		}

		switch protoSource.Mode {
		case SourceModeMountPoint:
			if protoSource.Mountpoint != nil {
				localSource.MountPoint = MountPointConfig{
					BucketName:     protoSource.Mountpoint.BucketName,
					AccessKey:      protoSource.Mountpoint.AccessKey,
					SecretKey:      protoSource.Mountpoint.SecretKey,
					Region:         protoSource.Mountpoint.Region,
					EndpointURL:    protoSource.Mountpoint.EndpointUrl,
					ForcePathStyle: protoSource.Mountpoint.ForcePathStyle,
				}
			}

		case SourceModeJuiceFS:
			if protoSource.Juicefs != nil {
				localSource.JuiceFS = JuiceFSConfig{
					RedisURI:  protoSource.Juicefs.RedisUri,
					Bucket:    protoSource.Juicefs.Bucket,
					AccessKey: protoSource.Juicefs.AccessKey,
					SecretKey: protoSource.Juicefs.SecretKey,
				}
			}
		}

		cfg.Sources[i] = localSource
	}

	return cfg
}

type RegionConfig struct {
	ServerConfig ServerConfig `key:"server" json:"server"`
}

type ClientConfig struct {
	Token                 string   `key:"token" json:"token"`
	MinRetryLengthBytes   int64    `key:"minRetryLengthBytes" json:"min_retry_length_bytes"`
	MaxGetContentAttempts int      `key:"maxGetContentAttempts" json:"max_get_content_attempts"`
	NTopHosts             int      `key:"nTopHosts" json:"n_top_hosts"`
	CacheFS               FSConfig `key:"cachefs" json:"cachefs"`
}

type MetadataMode string

const (
	MetadataModeDefault MetadataMode = "default"
	MetadataModeLocal   MetadataMode = "local"
)

type ValkeyConfig struct {
	PrimaryName     string                `key:"primaryName" json:"primary_name"`
	Password        string                `key:"password" json:"password"`
	TLS             bool                  `key:"tls" json:"tls"`
	Host            string                `key:"host" json:"host"`
	Port            int                   `key:"port" json:"port"`
	ExistingPrimary ValkeyExistingPrimary `key:"existingPrimary" json:"existingPrimary"`
}

type ValkeyExistingPrimary struct {
	Host string `key:"host" json:"host"`
	Port int    `key:"port" json:"port"`
}

type MetadataConfig struct {
	Mode         MetadataMode `key:"mode" json:"mode"`
	ValkeyConfig ValkeyConfig `key:"valkey" json:"valkey"`

	// Default config
	RedisAddr               string    `key:"redisAddr" json:"redis_addr"`
	RedisPasswd             string    `key:"redisPasswd" json:"redis_passwd"`
	RedisTLSEnabled         bool      `key:"redisTLSEnabled" json:"redis_tls_enabled"`
	RedisInsecureSkipVerify bool      `key:"redisInsecureSkipVerify" json:"redis_insecure_skip_verify"`
	RedisMode               RedisMode `key:"redisMode" json:"redis_mode"`
	RedisMasterName         string    `key:"redisMasterName" json:"redis_master_name"`
}

type RedisMode string

var (
	RedisModeSingle   RedisMode = "single"
	RedisModeCluster  RedisMode = "cluster"
	RedisModeSentinel RedisMode = "sentinel"
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
	MasterName         string        `key:"masterName" json:"master_name"`
	SentinelPassword   string        `key:"sentinelPassword" json:"sentinel_password"`
}

type FSConfig struct {
	Enabled            bool     `key:"enabled" json:"enabled"`
	MountPoint         string   `key:"mountPoint" json:"mount_point"`
	MaxBackgroundTasks int      `key:"maxBackgroundTasks" json:"max_background_tasks"`
	MaxWriteKB         int      `key:"maxWriteKB" json:"max_write_kb"`
	MaxReadAheadKB     int      `key:"maxReadAheadKB" json:"max_read_ahead_kb"`
	DirectMount        bool     `key:"directMount" json:"direct_mount"`
	DirectIO           bool     `key:"directIO" json:"direct_io"`
	Options            []string `key:"options" json:"options"`
}

type FSMetadata struct {
	PID       string `redis:"pid" json:"pid"`
	ID        string `redis:"id" json:"id"`
	Name      string `redis:"name" json:"name"`
	Path      string `redis:"path" json:"path"`
	Hash      string `redis:"hash" json:"hash"`
	Ino       uint64 `redis:"ino" json:"ino"`
	Size      uint64 `redis:"size" json:"size"`
	Blocks    uint64 `redis:"blocks" json:"blocks"`
	Atime     uint64 `redis:"atime" json:"atime"`
	Mtime     uint64 `redis:"mtime" json:"mtime"`
	Ctime     uint64 `redis:"ctime" json:"ctime"`
	Atimensec uint32 `redis:"atimensec" json:"atimensec"`
	Mtimensec uint32 `redis:"mtimensec" json:"mtimensec"`
	Ctimensec uint32 `redis:"ctimensec" json:"ctimensec"`
	Mode      uint32 `redis:"mode" json:"mode"`
	Nlink     uint32 `redis:"nlink" json:"nlink"`
	Rdev      uint32 `redis:"rdev" json:"rdev"`
	Blksize   uint32 `redis:"blksize" json:"blksize"`
	Padding   uint32 `redis:"padding" json:"padding"`
	Uid       uint32 `redis:"uid" json:"uid"`
	Gid       uint32 `redis:"gid" json:"gid"`
	Gen       uint64 `redis:"gen" json:"gen"`
}

func (m *FSMetadata) ToProto() *proto.CacheFSMetadata {
	return &proto.CacheFSMetadata{
		Id:        m.ID,
		Pid:       m.PID,
		Name:      m.Name,
		Path:      m.Path,
		Hash:      m.Hash,
		Ino:       m.Ino,
		Size:      m.Size,
		Blocks:    m.Blocks,
		Atime:     m.Atime,
		Mtime:     m.Mtime,
		Ctime:     m.Ctime,
		Atimensec: m.Atimensec,
		Mtimensec: m.Mtimensec,
		Ctimensec: m.Ctimensec,
		Mode:      m.Mode,
		Nlink:     m.Nlink,
		Rdev:      m.Rdev,
		Blksize:   m.Blksize,
		Padding:   m.Padding,
		Uid:       m.Uid,
		Gid:       m.Gid,
		Gen:       m.Gen,
	}
}

type SourceConfig struct {
	Mode           string           `key:"mode" json:"mode"`
	FilesystemName string           `key:"fsName" json:"filesystem_name"`
	FilesystemPath string           `key:"fsPath" json:"filesystem_path"`
	JuiceFS        JuiceFSConfig    `key:"juicefs" json:"juicefs"`
	MountPoint     MountPointConfig `key:"mountpoint" json:"mountpoint"`
}

type JuiceFSConfig struct {
	RedisURI   string `key:"redisURI" json:"redis_uri"`
	Bucket     string `key:"bucket" json:"bucket"`
	AccessKey  string `key:"accessKey" json:"access_key"`
	SecretKey  string `key:"secretKey" json:"secret_key"`
	CacheSize  int64  `key:"cacheSize" json:"cache_size"`
	BlockSize  int64  `key:"blockSize" json:"block_size"`
	Prefetch   int64  `key:"prefetch" json:"prefetch"`
	BufferSize int64  `key:"bufferSize" json:"buffer_size"`
}

type MountPointConfig struct {
	BucketName     string `key:"bucketName" json:"bucket_name"`
	AccessKey      string `key:"accessKey" json:"access_key"`
	SecretKey      string `key:"secretKey" json:"secret_key"`
	Region         string `key:"region" json:"region"`
	EndpointURL    string `key:"endpointUrl" json:"endpoint_url"`
	ForcePathStyle bool   `key:"forcePathStyle" json:"force_path_style"`
}

type Host struct {
	RTT              time.Duration `redis:"rtt" json:"rtt"`
	HostId           string        `redis:"host_id" json:"host_id"`
	Addr             string        `redis:"addr" json:"addr"`
	PrivateAddr      string        `redis:"private_addr" json:"private_addr"`
	CapacityUsagePct float64       `redis:"capacity_usage_pct" json:"capacity_usage_pct"`
}

// Bytes is needed for the rendezvous hasher
func (h *Host) Bytes() []byte {
	return []byte(h.HostId)
}

func (h *Host) ToProto() *proto.CacheHost {
	return &proto.CacheHost{
		HostId:           h.HostId,
		Addr:             h.Addr,
		PrivateIpAddr:    h.PrivateAddr,
		CapacityUsagePct: float32(h.CapacityUsagePct),
	}
}

type ClientRequest struct {
	rt        ClientRequestType
	hash      string
	key       string // This key is used for rendezvous hashing / deterministic routing (it's usually the same as the hash)
	hostIndex int
}

type ClientRequestType int

const (
	ClientRequestTypeStorage ClientRequestType = iota
	ClientRequestTypeRetrieval
)

// CacheFS types
type FileSystemOpts struct {
	MountPoint string
	Verbose    bool
	Metadata   *Metadata
}

type FileSystem interface {
	Mount(opts FileSystemOpts) (func() error, <-chan error, error)
	Unmount() error
	Format() error
}

type FileSystemStorage interface {
	Metadata()
	Get(string)
	ReadFile(interface{} /* This could be any sort of FS node type, depending on the implementation */, []byte, int64)
}
