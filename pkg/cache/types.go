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
	defaultHostKeepAliveTimeoutS           int     = 60
)

type Config struct {
	Enabled         bool                  `key:"enabled" json:"enabled"`
	Disk            DiskConfig            `key:"disk" json:"disk"`
	Memory          MemoryConfig          `key:"memory" json:"memory"`
	Coordinator     CoordinatorConfig     `key:"coordinator" json:"coordinator"`
	RequiredContent RequiredContentConfig `key:"requiredContent" json:"required_content"`
	Server          ServerConfig          `key:"server" json:"server"`
	Client          ClientConfig          `key:"client" json:"client"`
	Global          GlobalConfig          `key:"global" json:"global"`
	Metrics         MetricsConfig         `key:"metrics" json:"metrics"`
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

type CoordinatorConfig struct {
	Token                    string `key:"token" json:"token"`
	RegistrationTTLSeconds   int    `key:"registrationTTLSeconds" json:"registration_ttl_seconds"`
	HeartbeatIntervalSeconds int    `key:"heartbeatIntervalSeconds" json:"heartbeat_interval_seconds"`
	HostWatchIntervalSeconds int    `key:"hostWatchIntervalSeconds" json:"host_watch_interval_seconds"`
}

type RequiredContentConfig struct {
	Enabled               bool          `key:"enabled" json:"enabled"`
	StubTTL               time.Duration `key:"stubTTL" json:"stub_ttl"`
	VolumeMinBytes        int64         `key:"volumeMinBytes" json:"volume_min_bytes"`
	ReconcileInterval     time.Duration `key:"reconcileInterval" json:"reconcile_interval"`
	ReconcileConcurrency  int           `key:"reconcileConcurrency" json:"reconcile_concurrency"`
	BatchSize             int           `key:"batchSize" json:"batch_size"`
	MaxBytesPerCycle      int64         `key:"maxBytesPerCycle" json:"max_bytes_per_cycle"`
	OriginFallbackEnabled bool          `key:"originFallbackEnabled" json:"origin_fallback_enabled"`
}

type GlobalConfig struct {
	DefaultLocality                 string  `key:"defaultLocality" json:"default_locality"`
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
	GRPCPayloadCodecV2              bool    `key:"grpcPayloadCodecV2" json:"grpc_payload_codec_v2"`
	GRPCPayloadCodecMinBytes        int     `key:"grpcPayloadCodecMinBytes" json:"grpc_payload_codec_min_bytes"`
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

type ServerConfig struct {
	DiskCacheDir          string                    `key:"diskCacheDir" json:"disk_cache_dir"`
	DiskCacheMaxUsagePct  float64                   `key:"diskCacheMaxUsagePct" json:"disk_cache_max_usage_pct"`
	ObjectTtlS            int                       `key:"objectTtlS" json:"object_ttl_s"`
	MaxCachePct           int64                     `key:"maxCachePct" json:"max_cache_pct"`
	PageSizeBytes         int64                     `key:"pageSizeBytes" json:"page_size_bytes"`
	PageFileBuckets       int                       `key:"pageFileBuckets" json:"page_file_buckets"`
	ReadTransport         ServerReadTransportConfig `key:"readTransport" json:"read_transport"`
	Metadata              MetadataConfig            `key:"metadata" json:"metadata"`
	Sources               []SourceConfig            `key:"sources" json:"sources"`
	S3DownloadConcurrency int64                     `key:"s3DownloadConcurrency" json:"s3_download_concurrency"`
	S3DownloadChunkSize   int64                     `key:"s3DownloadChunkSize" json:"s3_download_chunk_size"`
}

type ServerReadTransportConfig struct {
	Enabled  bool `key:"enabled" json:"enabled"`
	Sendfile bool `key:"sendfile" json:"sendfile"`
}

type ClientConfig struct {
	Token                 string                    `key:"token" json:"token"`
	MinRetryLengthBytes   int64                     `key:"minRetryLengthBytes" json:"min_retry_length_bytes"`
	MaxGetContentAttempts int                       `key:"maxGetContentAttempts" json:"max_get_content_attempts"`
	NTopHosts             int                       `key:"nTopHosts" json:"n_top_hosts"`
	CacheFS               FSConfig                  `key:"cachefs" json:"cachefs"`
	PreferLocalCacheHost  bool                      `key:"preferLocalCacheHost" json:"prefer_local_cache_host"`
	PageFDCacheSize       int                       `key:"pageFDCacheSize" json:"page_fd_cache_size"`
	ReadTransport         ClientReadTransportConfig `key:"readTransport" json:"read_transport"`
	Prefetch              ReadPrefetchConfig        `key:"prefetch" json:"prefetch"`
}

type ClientReadTransportConfig struct {
	Enabled               bool `key:"enabled" json:"enabled"`
	MaxActiveConnsPerHost int  `key:"maxActiveConnsPerHost" json:"max_active_conns_per_host"`
	MaxIdleConnsPerHost   int  `key:"maxIdleConnsPerHost" json:"max_idle_conns_per_host"`
}

type ReadPrefetchConfig struct {
	Enabled         bool  `key:"enabled" json:"enabled"`
	AheadBytes      int64 `key:"aheadBytes" json:"ahead_bytes"`
	Workers         int   `key:"workers" json:"workers"`
	PartLengthBytes int64 `key:"partLengthBytes" json:"part_length_bytes"`
	MaxPartsPerRead int   `key:"maxPartsPerRead" json:"max_parts_per_read"`
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

func (m *FSMetadata) ToWorkerCacheProto() *proto.WorkerCacheFSMetadata {
	if m == nil {
		return nil
	}
	return &proto.WorkerCacheFSMetadata{
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

func FSMetadataFromWorkerCacheProto(metadata *proto.WorkerCacheFSMetadata) *FSMetadata {
	if metadata == nil {
		return nil
	}
	return &FSMetadata{
		ID:        metadata.Id,
		PID:       metadata.Pid,
		Name:      metadata.Name,
		Path:      metadata.Path,
		Hash:      metadata.Hash,
		Ino:       metadata.Ino,
		Size:      metadata.Size,
		Blocks:    metadata.Blocks,
		Atime:     metadata.Atime,
		Mtime:     metadata.Mtime,
		Ctime:     metadata.Ctime,
		Atimensec: metadata.Atimensec,
		Mtimensec: metadata.Mtimensec,
		Ctimensec: metadata.Ctimensec,
		Mode:      metadata.Mode,
		Nlink:     metadata.Nlink,
		Rdev:      metadata.Rdev,
		Blksize:   metadata.Blksize,
		Padding:   metadata.Padding,
		Uid:       metadata.Uid,
		Gid:       metadata.Gid,
		Gen:       metadata.Gen,
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
	RegistrationID   string        `redis:"registration_id" json:"registration_id"`
	PoolName         string        `redis:"pool_name" json:"pool_name"`
	Locality         string        `redis:"locality" json:"locality"`
	NodeID           string        `redis:"node_id" json:"node_id"`
	CachePathID      string        `redis:"cache_path_id" json:"cache_path_id"`
	Addr             string        `redis:"addr" json:"addr"`
	PrivateAddr      string        `redis:"private_addr" json:"private_addr"`
	CapacityUsagePct float64       `redis:"capacity_usage_pct" json:"capacity_usage_pct"`
}

func (h *Host) HasEndpoint() bool {
	return h != nil && (h.Addr != "" || h.PrivateAddr != "")
}

func (h *Host) LogicalOnly() *Host {
	if h == nil {
		return nil
	}
	logical := *h
	logical.RegistrationID = ""
	logical.Addr = ""
	logical.PrivateAddr = ""
	logical.RTT = 0
	logical.CapacityUsagePct = 0
	return &logical
}

// Bytes is needed for the rendezvous hasher
func (h *Host) Bytes() []byte {
	// HRW placement is intentionally based only on the stable logical cache
	// host. Registration, endpoint, and pool metadata describe the active
	// process lease for that host and must not perturb placement during worker
	// churn.
	return []byte(h.HostId)
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
