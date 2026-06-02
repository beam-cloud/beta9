package cache

import (
	"context"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultRequiredContentStubTTL          = 7 * 24 * time.Hour
	DefaultRequiredContentVolumeMinBytes   = 32 * 1024 * 1024
	DefaultRequiredContentInterval         = 30 * time.Second
	DefaultRequiredContentConcurrency      = 2
	DefaultRequiredContentBatchSize        = 256
	DefaultRequiredContentMaxBytesPerCycle = 32 * 1024 * 1024 * 1024
)

type RequiredContentKind string

const (
	RequiredContentKindClipOCI      RequiredContentKind = "clip_oci_layer"
	RequiredContentKindClipV1       RequiredContentKind = "clip_v1_content"
	RequiredContentKindVolume       RequiredContentKind = "volume_content"
	RequiredContentKindImageArchive RequiredContentKind = "image_archive"
)

type RequiredContentReconciliationStatus string

const (
	RequiredContentStatusPending       RequiredContentReconciliationStatus = "pending"
	RequiredContentStatusMaterializing RequiredContentReconciliationStatus = "materializing"
	RequiredContentStatusPresent       RequiredContentReconciliationStatus = "present"
	RequiredContentStatusSkipped       RequiredContentReconciliationStatus = "skipped"
	RequiredContentStatusSourceMissing RequiredContentReconciliationStatus = "source_missing"
	RequiredContentStatusError         RequiredContentReconciliationStatus = "error"
)

type RequiredContentSourceType string

const (
	RequiredContentSourceCacheReplica RequiredContentSourceType = "cache_replica"
	RequiredContentSourceS3           RequiredContentSourceType = "s3"
	RequiredContentSourceOCIRegistry  RequiredContentSourceType = "oci_registry"
	RequiredContentSourceUnknown      RequiredContentSourceType = "unknown"
)

type RequiredContentSource struct {
	Type           RequiredContentSourceType `json:"type"`
	Descriptor     string                    `json:"descriptor,omitempty"`
	Registry       string                    `json:"registry,omitempty"`
	Repository     string                    `json:"repository,omitempty"`
	Reference      string                    `json:"reference,omitempty"`
	LayerDigest    string                    `json:"layer_digest,omitempty"`
	BucketName     string                    `json:"bucket_name,omitempty"`
	Region         string                    `json:"region,omitempty"`
	EndpointURL    string                    `json:"endpoint_url,omitempty"`
	ObjectPath     string                    `json:"object_path,omitempty"`
	ForcePathStyle bool                      `json:"force_path_style,omitempty"`
}

func OCIRequiredContentOriginPath(source RequiredContentSource) string {
	if source.Registry == "" || source.Repository == "" || source.LayerDigest == "" {
		return ""
	}
	u := url.URL{
		Scheme: "oci",
		Host:   source.Registry,
		Path:   "/" + strings.TrimPrefix(source.Repository, "/"),
	}
	query := u.Query()
	query.Set("layer_digest", source.LayerDigest)
	if source.Reference != "" {
		query.Set("reference", source.Reference)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

func ParseOCIRequiredContentOriginPath(rawPath string) (RequiredContentSource, bool) {
	u, err := url.Parse(rawPath)
	if err != nil || u.Scheme != "oci" || u.Host == "" {
		return RequiredContentSource{}, false
	}
	query := u.Query()
	source := RequiredContentSource{
		Type:        RequiredContentSourceOCIRegistry,
		Registry:    u.Host,
		Repository:  strings.TrimPrefix(u.Path, "/"),
		Reference:   query.Get("reference"),
		LayerDigest: query.Get("layer_digest"),
	}
	if source.Repository == "" || source.LayerDigest == "" {
		return RequiredContentSource{}, false
	}
	return source, true
}

type RequiredContentOriginInstruction struct {
	Path           string
	BucketName     string
	Region         string
	EndpointURL    string
	AccessKey      string
	SecretKey      string
	CachePath      string
	ForcePathStyle bool
	ExpectedHash   string
}

type RequiredContentOriginResolver interface {
	ResolveRequiredContentOrigin(ctx context.Context, item RequiredContentItem) (RequiredContentOriginInstruction, bool, error)
}

type RequiredContentStubLocality struct {
	Locality    string    `json:"locality"`
	WorkspaceID string    `json:"workspace_id"`
	StubID      string    `json:"stub_id"`
	LastSeen    time.Time `json:"last_seen"`
}

type RequiredContentItem struct {
	Locality     string                              `json:"locality"`
	WorkspaceID  string                              `json:"workspace_id"`
	StubID       string                              `json:"stub_id"`
	Kind         RequiredContentKind                 `json:"kind"`
	Hash         string                              `json:"hash"`
	RoutingKey   string                              `json:"routing_key"`
	SizeBytes    int64                               `json:"size_bytes"`
	ExpectedHash string                              `json:"expected_hash"`
	Source       RequiredContentSource               `json:"source"`
	FirstSeen    time.Time                           `json:"first_seen"`
	LastSeen     time.Time                           `json:"last_seen"`
	AccessCount  int64                               `json:"access_count"`
	Status       RequiredContentReconciliationStatus `json:"status"`
	LastStatusAt time.Time                           `json:"last_status_at"`
	LastError    string                              `json:"last_error,omitempty"`
}

func (i RequiredContentItem) Normalized() RequiredContentItem {
	if i.RoutingKey == "" {
		i.RoutingKey = i.Hash
	}
	if i.ExpectedHash == "" {
		i.ExpectedHash = i.Hash
	}
	if i.Status == "" {
		i.Status = RequiredContentStatusPending
	}
	if i.Source.Type == "" {
		i.Source.Type = RequiredContentSourceUnknown
	}
	return i
}

type RequiredContentReconciliationLock interface {
	Release(ctx context.Context) error
	Refresh(ctx context.Context, ttl time.Duration) error
}

type RequiredContentRepository interface {
	MarkStubLocalityAccessed(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error
	UpsertRequiredContent(ctx context.Context, item RequiredContentItem, ttl time.Duration) error
	ListRecentStubLocalities(ctx context.Context, locality string, since time.Time, limit int) ([]RequiredContentStubLocality, error)
	ListRequiredContentForStub(ctx context.Context, locality, workspaceID, stubID string, limit int) ([]RequiredContentItem, error)
	SetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string, status RequiredContentReconciliationStatus, errorMsg string, ttl time.Duration) error
	AcquireRequiredContentReconciliationLock(ctx context.Context, locality, logicalHostID, hash string, ttl time.Duration) (RequiredContentReconciliationLock, bool, error)
}

type RequiredContentBatchRepository interface {
	UpsertRequiredContentBatch(ctx context.Context, items []RequiredContentItem, ttl time.Duration) error
}

func NormalizeRequiredContentConfig(config RequiredContentConfig) RequiredContentConfig {
	if config.StubTTL <= 0 {
		config.StubTTL = DefaultRequiredContentStubTTL
	}
	if config.VolumeMinBytes <= 0 {
		config.VolumeMinBytes = DefaultRequiredContentVolumeMinBytes
	}
	if config.ReconcileInterval <= 0 {
		config.ReconcileInterval = DefaultRequiredContentInterval
	}
	if config.ReconcileConcurrency <= 0 {
		config.ReconcileConcurrency = DefaultRequiredContentConcurrency
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultRequiredContentBatchSize
	}
	if config.MaxBytesPerCycle <= 0 {
		config.MaxBytesPerCycle = DefaultRequiredContentMaxBytesPerCycle
	}
	return config
}
