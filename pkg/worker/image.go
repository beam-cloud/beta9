package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/registry"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/beam-cloud/clip/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/cas/dir"
	"github.com/opencontainers/umoci/oci/casext"
	"github.com/opencontainers/umoci/oci/layer"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	imageBundlePath                   string = "/dev/shm/images"
	imageTmpDir                       string = types.AgentTmpPath
	metricsSourceLabel                       = "image_client"
	pullLazyBackoff                          = 1000 * time.Millisecond
	embeddedImageCacheLockWaitTimeout        = 2 * time.Second
	embeddedImageCacheWaitInterval           = 250 * time.Millisecond
	imageArchiveLockRetryInterval            = 100 * time.Millisecond
	maxSyncV1ArchiveDataRestoreBytes         = 512 * 1024 * 1024
	imageLayerPrepareConcurrency             = 8
	imageLayerProgressInterval               = 3 * time.Second
)

var (
	baseImageCachePath string = types.AgentImageCachePath
	baseImageMountPath string = types.AgentImageMountPattern
)

func getImageCachePath() string {
	path := baseImageCachePath

	_ = ensureImageDirectory(path, 0755)

	return path
}

func getImageMountPath(workerId string) string {
	path := fmt.Sprintf(baseImageMountPath, workerId)

	_ = ensureImageDirectory(path, 0755)

	return path
}

func ensureImageDirectory(dir string, perm os.FileMode) error {
	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create image directory <%s>: %w", dir, err)
	}

	return nil
}

type PathInfo struct {
	Path           string
	cachedSize     float64
	lastModifiedAt time.Time
}

func (p *PathInfo) GetSize() float64 {
	info, err := os.Stat(p.Path)
	if err != nil {
		return p.calculateSize()
	}

	modTime := info.ModTime()

	// Use cached size if directory hasn't been modified since our last calculation
	if !p.lastModifiedAt.IsZero() && !modTime.After(p.lastModifiedAt) {
		return p.cachedSize
	}

	return p.calculateSize()
}

func (p *PathInfo) calculateSize() float64 {
	var size int64

	filepath.Walk(p.Path, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	p.cachedSize = float64(size) / 1024 / 1024

	// Update last modified time
	if info, err := os.Stat(p.Path); err == nil {
		p.lastModifiedAt = info.ModTime()
	}

	return p.cachedSize
}

func NewPathInfo(path string) *PathInfo {
	p := &PathInfo{
		Path: path,
	}
	p.calculateSize()
	return p
}

type ImageClient struct {
	registry           *reg.ImageRegistry
	cacheClient        *cache.Client
	imageCachePath     string
	imageMountPath     string
	imageBundlePath    string
	mountedFuseServers *common.SafeMap[*fuse.Server]
	mountLocks         map[string]*sync.Mutex
	mountLocksMu       sync.Mutex
	skopeoClient       common.SkopeoClient
	config             types.AppConfig
	workerId           string
	workerPoolName     string
	workerRepoClient   pb.WorkerRepositoryServiceClient
	logger             *ContainerLogger
	eventRepo          repo.EventRepository
	contentReporter    *cacheContentReporter
	originCredsMu      sync.Mutex
	originCredsCache   map[string]*originCredentials
	// archiveContentMetadata resolves the cached image archive object (hash/size)
	// for a cachefs path. It is a field so tests can inject a fake; in production
	// it delegates to the cache client.
	archiveContentMetadata func(ctx context.Context, cachePath string) (*cache.FSMetadata, error)
	// Cache source image references for v2 images (imageId -> sourceImageRef)
	v2ImageRefs       *common.SafeMap[string]
	v2ArchiveMetadata *common.SafeMap[*clipCommon.ClipArchiveMetadata]
	clipRuntimeMu     sync.RWMutex
	clipActive        map[string]*types.ContainerRequest
	clipRuntimePIDs   map[int]clipPIDReference
	clipPIDCache      map[int]clipPIDReference
	clipReadEvents    chan clipCommon.ReadTraceEvent
	clipAggregates    map[string]*clipReadAggregate
}

func NewImageClient(config types.AppConfig, workerId, workerPoolName string, workerRepoClient pb.WorkerRepositoryServiceClient, fileCacheManager *FileCacheManager) (*ImageClient, error) {
	registry, err := reg.NewImageRegistry(config, config.ImageService.Registries.S3)
	if err != nil {
		return nil, err
	}

	imageCachePath := getImageCachePath()
	imageMountPath := getImageMountPath(workerId)
	if err := ensureImageDirectory(imageCachePath, 0755); err != nil {
		return nil, err
	}
	if err := ensureImageDirectory(imageMountPath, 0755); err != nil {
		return nil, err
	}

	c := &ImageClient{
		config:             config,
		registry:           registry,
		cacheClient:        fileCacheManager.GetClient(),
		imageBundlePath:    imageBundlePath,
		imageCachePath:     imageCachePath,
		imageMountPath:     imageMountPath,
		workerId:           workerId,
		workerPoolName:     workerPoolName,
		workerRepoClient:   workerRepoClient,
		skopeoClient:       common.NewSkopeoClient(config),
		mountedFuseServers: common.NewSafeMap[*fuse.Server](),
		mountLocks:         make(map[string]*sync.Mutex),
		v2ImageRefs:        common.NewSafeMap[string](),
		v2ArchiveMetadata:  common.NewSafeMap[*clipCommon.ClipArchiveMetadata](),
		clipActive:         make(map[string]*types.ContainerRequest),
		clipRuntimePIDs:    make(map[int]clipPIDReference),
		clipPIDCache:       make(map[int]clipPIDReference),
		clipReadEvents:     make(chan clipCommon.ReadTraceEvent, clipReadEventQueueSize),
		clipAggregates:     make(map[string]*clipReadAggregate),
		originCredsCache:   make(map[string]*originCredentials),
		logger: &ContainerLogger{
			logLinesPerHour: config.Worker.ContainerLogLinesPerHour,
		},
	}
	if c.cacheClient != nil {
		c.archiveContentMetadata = c.cacheClient.CacheFSMetadata
	}
	go c.runClipReadEventReporter()

	if config.DebugMode {
		clip.SetLogLevel("debug")
	} else {
		clip.SetLogLevel("info")
	}

	err = os.MkdirAll(c.imageBundlePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *ImageClient) imageMountPoint(imageId string) string {
	return filepath.Join(c.imageMountPath, imageId)
}

func (c *ImageClient) mountedImageReady(imageId string) bool {
	if c.mountedFuseServers == nil {
		return false
	}
	_, mounted := c.mountedFuseServers.Get(imageId)
	return mounted
}

func (c *ImageClient) lockImageMount(imageId string) func() {
	c.mountLocksMu.Lock()
	if c.mountLocks == nil {
		c.mountLocks = make(map[string]*sync.Mutex)
	}

	lock, ok := c.mountLocks[imageId]
	if !ok {
		lock = &sync.Mutex{}
		c.mountLocks[imageId] = lock
	}
	c.mountLocksMu.Unlock()

	lock.Lock()
	return lock.Unlock
}

func ociStorageInfo(meta *clipCommon.ClipArchiveMetadata) (*clipCommon.OCIStorageInfo, bool) {
	if meta == nil || meta.StorageInfo == nil {
		return nil, false
	}

	if ociInfo, ok := meta.StorageInfo.(clipCommon.OCIStorageInfo); ok {
		return &ociInfo, true
	}

	if ociInfo, ok := meta.StorageInfo.(*clipCommon.OCIStorageInfo); ok {
		return ociInfo, true
	}

	return nil, false
}

func (c *ImageClient) PullLazy(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) (time.Duration, error) {
	startTime := time.Now()

	// Refresh the recent-stub window on every container start so reconciliation
	// keeps this stub's content warm for the configured TTL after its last
	// container. This is cheap and independent of one-time content generation.
	if c.contentReporter != nil && request != nil {
		c.contentReporter.touchRecentStub(cacheRequestWorkspaceID(request), cacheRequestStubID(request))
	}

	if elapsed, ok := c.mountedImageHit(startTime, request, "clip_mounted_fuse_hit"); ok {
		return elapsed, nil
	}

	localLockStart := time.Now()
	unlockMount := c.lockImageMount(request.ImageId)
	c.recordImageLifecycle(request, types.ContainerLifecycleID("image.local_mount_lock"), localLockStart, time.Since(localLockStart), true, nil)
	defer unlockMount()

	if elapsed, ok := c.mountedImageHit(startTime, request, "clip_mounted_fuse_hit_after_local_lock"); ok {
		return elapsed, nil
	}

	archive, err := c.prepareLazyImageArchive(ctx, request)
	if err != nil {
		return time.Since(startTime), err
	}

	mountOptions := c.lazyMountOptions(ctx, request, archive)
	if archive.usesOCIStorage() {
		mountOptions.Context = ctx
		mountOptions.PrepareConcurrency = imageLayerPrepareConcurrency
		mountOptions.PrepareProgress = imageLayerPrepareProgressLogger(outputLogger)
		if err := clip.PrepareArchiveContent(mountOptions); err != nil {
			return time.Since(startTime), err
		}
		mountOptions.PrepareConcurrency = 0
		mountOptions.PrepareProgress = nil
	}

	if elapsed, ok := c.mountedImageHit(startTime, request, "clip_mounted_fuse_hit"); ok {
		return elapsed, nil
	}

	remoteLockStart := time.Now()
	releaseImageLock, err := c.acquireRemoteImageMountLock(request.ImageId)
	c.recordImageLifecycle(request, types.ContainerLifecycleID("image.remote_mount_lock"), remoteLockStart, time.Since(remoteLockStart), err == nil, nil)
	if err != nil {
		return time.Since(startTime), err
	}
	defer releaseImageLock()

	if elapsed, ok := c.mountedImageHit(startTime, request, "clip_mounted_fuse_hit_after_lock"); ok {
		return elapsed, nil
	}

	mountStart := time.Now()
	if err := c.mountLazyImageArchive(request, mountOptions); err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleID("image.mount_archive"), mountStart, time.Since(mountStart), false, map[string]string{
			"storage_mode": archive.storageMode,
		})
		return time.Since(startTime), err
	}
	c.recordImageLifecycle(request, types.ContainerLifecycleID("image.mount_archive"), mountStart, time.Since(mountStart), true, map[string]string{
		"storage_mode": archive.storageMode,
	})

	return time.Since(startTime), nil
}

func imageLayerPrepareProgressLogger(outputLogger *slog.Logger) func(storage.PrepareProgress) {
	if outputLogger == nil {
		return nil
	}

	startedAt := time.Now()
	var mu sync.Mutex
	var lastLog time.Time
	return func(progress storage.PrepareProgress) {
		mu.Lock()
		defer mu.Unlock()

		now := time.Now()
		if progress.Completed == 0 {
			outputLogger.Info(fmt.Sprintf("Preparing %d image layers (%d concurrent)...\n", progress.Total, imageLayerPrepareConcurrency))
			lastLog = now
			return
		}
		if progress.Completed < progress.Total && now.Sub(lastLog) < imageLayerProgressInterval {
			return
		}

		elapsed := now.Sub(startedAt).Round(100 * time.Millisecond)
		if progress.Completed == progress.Total {
			outputLogger.Info(fmt.Sprintf("Prepared %d image layers (%s) in %s\n", progress.Total, formatImageBytes(progress.Bytes), elapsed))
		} else {
			outputLogger.Info(fmt.Sprintf("Preparing image layers: %d/%d ready (%s, %s)\n", progress.Completed, progress.Total, formatImageBytes(progress.Bytes), elapsed))
		}
		lastLog = now
	}
}

type lazyImageArchive struct {
	path           string
	sourceRegistry *types.S3ImageRegistryConfig
	storageMode    string
	metadata       *clipCommon.ClipArchiveMetadata
}

func (a lazyImageArchive) usesOCIStorage() bool {
	return isOCIStorageMode(a.storageMode)
}

func (c *ImageClient) mountedImageHit(startTime time.Time, request *types.ContainerRequest, phase string) (time.Duration, bool) {
	if !c.mountedImageReady(request.ImageId) {
		return 0, false
	}

	elapsed := time.Since(startTime)
	attrs := map[string]string{
		"clip_version":     fmt.Sprintf("%d", c.config.ImageService.ClipVersion),
		"mounted_fuse_hit": "true",
	}
	metrics.RecordWorkerStartupPhase(phase, elapsed, request, attrs)
	c.recordImageLifecycle(request, types.ContainerLifecycleID("image."+phase), startTime, elapsed, true, attrs)
	return elapsed, true
}

func (c *ImageClient) recordImageLifecycle(request *types.ContainerRequest, id types.ContainerLifecycleID, startedAt time.Time, duration time.Duration, success bool, attrs map[string]string) {
	if c.eventRepo == nil || request == nil {
		return
	}

	lifecycle := containerLifecycleFromDuration(id, request, startedAt, duration, success, attrs)
	lifecycle.WorkerID = c.workerId
	c.eventRepo.PushContainerLifecycleEvent(lifecycle)
}

func (c *ImageClient) prepareLazyImageArchive(ctx context.Context, request *types.ContainerRequest) (lazyImageArchive, error) {
	archivePath := c.localArchivePath(request.ImageId)
	archiveAlreadyOnDisk := fileExists(archivePath)

	phaseStart := time.Now()
	sourceRegistry, err := c.pullImageFromRegistry(ctx, archivePath, request)
	registryAttrs := map[string]string{
		"archive_on_disk": fmt.Sprintf("%t", archiveAlreadyOnDisk),
		"clip_version":    fmt.Sprintf("%d", c.config.ImageService.ClipVersion),
		"success":         fmt.Sprintf("%t", err == nil),
	}
	metrics.RecordWorkerStartupPhase("image_registry_pull", time.Since(phaseStart), request, registryAttrs)
	c.recordImageLifecycle(request, types.ContainerLifecycleID("image.registry_pull"), phaseStart, time.Since(phaseStart), err == nil, registryAttrs)
	if err != nil {
		return lazyImageArchive{}, err
	}

	phaseStart = time.Now()
	meta, err := c.processPulledArchive(archivePath, request.ImageId)
	metadataAttrs := map[string]string{
		"clip_version": fmt.Sprintf("%d", c.config.ImageService.ClipVersion),
		"success":      fmt.Sprintf("%t", err == nil),
	}
	metrics.RecordWorkerStartupPhase("clip_metadata_extract", time.Since(phaseStart), request, metadataAttrs)
	c.recordImageLifecycle(request, types.ContainerLifecycleID("image.clip_metadata_extract"), phaseStart, time.Since(phaseStart), err == nil, metadataAttrs)
	if err != nil {
		return lazyImageArchive{}, err
	}

	archive := lazyImageArchive{
		path:           archivePath,
		sourceRegistry: sourceRegistry,
		storageMode:    archiveStorageMode(meta),
		metadata:       meta,
	}
	if archive.usesOCIStorage() {
		log.Info().Str("image_id", request.ImageId).Str("storage_type", archive.storageMode).Msg("detected CLIP OCI image")
	} else {
		if archive.sourceRegistry == nil || archive.sourceRegistry.BucketName == "" {
			archive.sourceRegistry = c.imageArchiveSourceRegistry(ctx, request)
		}
		if localArchivePath, ok := c.ensureV1ArchiveDataCache(ctx, request, archive.sourceRegistry); ok {
			archive.path = localArchivePath
			archive.sourceRegistry = nil
			archive.storageMode = string(clipCommon.StorageModeLocal)
		}
	}
	if report, ok := c.imageRequiredContent(ctx, request, meta); ok {
		c.publishRequiredContent(request, report)
	}

	return archive, nil
}

// reportRequiredContent enumerates the content a stub's image requires and feeds
// it to the cache reconciliation reporter. It runs at image-load time (off the
// read hot path) and is a no-op when reconciliation is disabled.
func (c *ImageClient) reportRequiredContent(ctx context.Context, request *types.ContainerRequest, meta *clipCommon.ClipArchiveMetadata) {
	if request == nil || meta == nil {
		return
	}

	report, ok := c.imageRequiredContent(ctx, request, meta)
	if !ok {
		// Nothing to report; do not claim the one-time generation so a later
		// load that does yield content can still publish it.
		return
	}

	c.publishRequiredContent(request, report)
}

func (c *ImageClient) publishRequiredContent(request *types.ContainerRequest, report requiredContentReport) {
	if c.contentReporter == nil || request == nil {
		return
	}

	// Required content is immutable per stub, so publish it only the first time
	// the stub loads, not on every container start.
	stubID := cacheRequestStubID(request)
	if !c.contentReporter.shouldGenerateRequiredContent(stubID) {
		return
	}

	workspaceID := cacheRequestWorkspaceID(request)
	c.contentReporter.reportBatches(workspaceID, stubID, []requiredContentReport{report})
	c.contentReporter.flush()

	log.Debug().
		Str("workspace_id", workspaceID).
		Str("stub_id", stubID).
		Str("image_id", request.ImageId).
		Str("kind", string(report.kind)).
		Int("item_count", len(report.items)).
		Msg("reported image required content")
}

func cacheRequestWorkspaceID(request *types.ContainerRequest) string {
	if request == nil {
		return ""
	}
	if request.WorkspaceId != "" {
		return request.WorkspaceId
	}
	return request.Workspace.ExternalId
}

func cacheRequestStubID(request *types.ContainerRequest) string {
	if request == nil {
		return ""
	}
	if request.StubId != "" {
		return request.StubId
	}
	return request.Stub.ExternalId
}

// imageRequiredContent returns the required-content batch for a stub's image:
// per-layer decompressed hashes for CLIP v2, or the whole archive as a single
// content object for CLIP v1 (reconciling the archive as one file avoids
// re-materializing the thousands of per-file entries in the v1 index).
func (c *ImageClient) imageRequiredContent(ctx context.Context, request *types.ContainerRequest, meta *clipCommon.ClipArchiveMetadata) (requiredContentReport, bool) {
	if ociInfo, ok := ociStorageInfo(meta); ok && len(ociInfo.DecompressedHashByLayer) > 0 {
		items := ociRequiredContentItems(request.ImageId, ociInfo)
		if len(items) == 0 {
			return requiredContentReport{}, false
		}
		return requiredContentReport{kind: types.CacheContentKindClipV2, items: items}, true
	}

	item, ok := c.clipV1ArchiveRequiredContent(ctx, request)
	if !ok {
		return requiredContentReport{}, false
	}
	return requiredContentReport{kind: types.CacheContentKindClipV1, items: []types.CacheRequiredContentItem{item}}, true
}

// ociRequiredContentItems enumerates CLIP v2 decompressed layer hashes. The
// non-secret source descriptor is the full OCI layer reference so the HRW owner
// can fetch and decompress the layer from the source registry like the read path.
func ociRequiredContentItems(imageID string, ociInfo *clipCommon.OCIStorageInfo) []types.CacheRequiredContentItem {
	items := make([]types.CacheRequiredContentItem, 0, len(ociInfo.DecompressedHashByLayer))
	for layerDigest, hash := range ociInfo.DecompressedHashByLayer {
		if !isSHA256HexDigest(hash) {
			continue
		}
		source := ociLayerReference(ociInfo, layerDigest)
		if source == "" {
			continue
		}
		items = append(items, types.CacheRequiredContentItem{
			Hash:         hash,
			RoutingKey:   hash,
			ExpectedHash: hash,
			ImageID:      imageID,
			Source:       source,
			Kind:         types.CacheContentKindClipV2,
		})
	}
	return items
}

// clipV1ArchiveRequiredContent describes the cached CLIP v1 data archive as a
// single content object. Remote CLIP v1 can load metadata from a .rclip archive,
// but reconciliation must track the full .clip data archive so recent stubs can
// be re-materialized from source when a cache host disappears.
func (c *ImageClient) clipV1ArchiveRequiredContent(ctx context.Context, request *types.ContainerRequest) (types.CacheRequiredContentItem, bool) {
	if c.archiveContentMetadata == nil || request == nil {
		return types.CacheRequiredContentItem{}, false
	}

	cachePath := c.clipV1ArchiveCachePath(request.ImageId)
	metadata, err := c.archiveContentMetadata(ctx, cachePath)
	if err != nil || metadata == nil || metadata.Hash == "" || metadata.Size == 0 {
		return types.CacheRequiredContentItem{}, false
	}

	return types.CacheRequiredContentItem{
		Hash:         metadata.Hash,
		RoutingKey:   cachePath,
		ExpectedHash: metadata.Hash,
		SizeBytes:    int64(metadata.Size),
		ImageID:      request.ImageId,
		// Origin source descriptor: the data archive's key in the image
		// registry. This intentionally stays .clip even when the mounted
		// metadata archive is .rclip.
		Source: c.clipV1ArchiveDataSourceKey(request.ImageId),
		Kind:   types.CacheContentKindClipV1,
	}, true
}

// imageArchiveSourceKey returns the image registry object key for a stub's
// archive (e.g. "{imageId}.rclip"), matching the key used by the embedded-cache
// pull that originally stored it.
func (c *ImageClient) imageArchiveSourceKey(imageId string) string {
	return fmt.Sprintf("%s.%s", imageId, c.registry.ImageFileExtension)
}

// ociLayerReference builds a fully-qualified, non-secret OCI layer digest
// reference (registry/repository@sha256:...) used as the required-content source
// descriptor so a cache host can fetch and decompress the layer from origin.
func ociLayerReference(ociInfo *clipCommon.OCIStorageInfo, layerDigest string) string {
	if ociInfo == nil || ociInfo.Repository == "" {
		return ""
	}
	registry := ociInfo.RegistryURL
	registry = strings.TrimPrefix(registry, "https://")
	registry = strings.TrimPrefix(registry, "http://")
	registry = strings.Trim(registry, "/")
	if registry == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s@%s", registry, ociInfo.Repository, layerDigest)
}

func isSHA256HexDigest(hash string) bool {
	if len(hash) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func (c *ImageClient) localArchivePath(imageId string) string {
	return fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
}

func (c *ImageClient) clipV1ArchiveCachePath(imageId string) string {
	return fmt.Sprintf("%s/%s.%s", types.AgentImagesPath, imageId, reg.LocalImageFileExtension)
}

func (c *ImageClient) clipV1ArchiveDataCachePath(imageId string) string {
	return fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, reg.LocalImageFileExtension)
}

func (c *ImageClient) clipV1ArchiveDataSourceKey(imageId string) string {
	return fmt.Sprintf("%s.%s", imageId, reg.LocalImageFileExtension)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func archiveStorageMode(meta *clipCommon.ClipArchiveMetadata) string {
	if meta == nil || meta.StorageInfo == nil {
		return ""
	}

	if t, ok := meta.StorageInfo.(interface{ Type() string }); ok {
		return strings.ToLower(t.Type())
	}

	return ""
}

func isOCIStorageMode(mode string) bool {
	mode = strings.ToLower(mode)
	return mode == "oci" || mode == strings.ToLower(string(clipCommon.StorageModeOCI))
}

func (c *ImageClient) lazyMountOptions(ctx context.Context, request *types.ContainerRequest, archive lazyImageArchive) clip.MountOptions {
	cacheKind := "legacy-file-runtime"
	if archive.usesOCIStorage() {
		cacheKind = "oci-layer-runtime"
	}
	contentCache := newImageContentCache(c.cacheClient, request.ImageId, cacheKind, c.imageContentCacheObserver(request))
	mountOptions := clip.MountOptions{
		ArchivePath:           archive.path,
		Metadata:              archive.metadata,
		MountPoint:            c.imageMountPoint(request.ImageId),
		CachePath:             c.contentCachePath(request, archive),
		ContentCache:          contentCache,
		ContentCacheAvailable: contentCache != nil,
		ReadTraceObserver:     c.observeClipRead,
	}

	if archive.usesOCIStorage() {
		mountOptions.RegistryCredProvider = c.getCredentialProviderForImage(ctx, request.ImageId, request)
		mountOptions.UseCheckpoints = true
		return mountOptions
	}

	// Only override the archive's storage info when a usable S3 source is
	// known; an empty bucket would poison every lazy data read.
	if archive.sourceRegistry != nil && archive.sourceRegistry.BucketName != "" {
		mountOptions.Credentials = storage.ClipStorageCredentials{
			S3: &storage.S3ClipStorageCredentials{
				AccessKey: archive.sourceRegistry.AccessKey,
				SecretKey: archive.sourceRegistry.SecretKey,
			},
		}
		mountOptions.StorageInfo = &clipCommon.S3StorageInfo{
			Bucket:         archive.sourceRegistry.BucketName,
			Region:         archive.sourceRegistry.Region,
			Endpoint:       archive.sourceRegistry.Endpoint,
			Key:            fmt.Sprintf("%s.%s", request.ImageId, reg.LocalImageFileExtension),
			ForcePathStyle: archive.sourceRegistry.ForcePathStyle,
		}
	}

	return mountOptions
}

func (c *ImageClient) contentCachePath(request *types.ContainerRequest, archive lazyImageArchive) string {
	if archive.usesOCIStorage() {
		return c.imageCachePath
	}

	if c.config.ImageService.RegistryStore == registry.S3ImageRegistryStore {
		return c.clipV1ArchiveDataCachePath(request.ImageId)
	}

	if c.config.ImageService.LocalCacheEnabled || strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix) {
		return fmt.Sprintf("%s/%s.cache", c.imageCachePath, request.ImageId)
	}

	return ""
}

func (c *ImageClient) ensureV1ArchiveDataCache(ctx context.Context, request *types.ContainerRequest, sourceRegistry *types.S3ImageRegistryConfig) (string, bool) {
	if request == nil || c.config.ImageService.RegistryStore != registry.S3ImageRegistryStore {
		return "", false
	}

	imageID := request.ImageId
	targetPath := c.clipV1ArchiveDataCachePath(imageID)
	if c.localImageArchiveReady(targetPath, imageID) {
		return targetPath, true
	}

	cachePath := c.clipV1ArchiveCachePath(imageID)
	if sourceRegistry == nil || sourceRegistry.BucketName == "" {
		sourceRegistry = c.imageArchiveSourceRegistry(ctx, request)
	}

	if deferRestore, size := c.deferV1ArchiveDataRestore(ctx, imageID, cachePath, sourceRegistry); deferRestore {
		attrs := map[string]string{
			"reason":          "large_archive_with_remote_source",
			"size_bytes":      fmt.Sprintf("%d", size),
			"threshold_bytes": fmt.Sprintf("%d", maxSyncV1ArchiveDataRestoreBytes),
		}
		c.recordImageLifecycle(request, types.ContainerLifecycleImageV1DataCacheDeferred, time.Now(), 0, true, attrs)
		c.warmV1ArchiveDataCacheAsync(imageID, targetPath, cachePath, *sourceRegistry)
		return "", false
	}

	restoreStart := time.Now()
	path, ok := c.materializeV1ArchiveDataCache(ctx, request, imageID, targetPath, cachePath, sourceRegistry)
	c.recordImageLifecycle(request, types.ContainerLifecycleImageV1DataCacheRestore, restoreStart, time.Since(restoreStart), ok, nil)
	return path, ok
}

func (c *ImageClient) materializeV1ArchiveDataCache(ctx context.Context, request *types.ContainerRequest, imageID, targetPath, cachePath string, sourceRegistry *types.S3ImageRegistryConfig) (string, bool) {
	unlock, err := lockImageArchiveFile(ctx, targetPath)
	if err != nil {
		log.Debug().Err(err).Str("image_id", imageID).Str("path", targetPath).Msg("v1 image data archive lock failed")
		return "", false
	}
	defer unlock()

	if c.localImageArchiveReady(targetPath, imageID) {
		return targetPath, true
	}

	if c.cacheClient != nil {
		if ok, err := c.copyImageArchiveFromContentCachePath(ctx, targetPath, imageID, cachePath); err != nil {
			log.Debug().Err(err).Str("image_id", imageID).Str("cache_path", cachePath).Msg("v1 image data archive content cache restore failed")
		} else if ok {
			return targetPath, true
		}
	}

	if sourceRegistry == nil || sourceRegistry.BucketName == "" {
		if request != nil && c.downloadV1ArchiveDataFromBrokeredURL(ctx, request, targetPath, cachePath) {
			return targetPath, true
		}
		return "", false
	}
	if c.cacheClient == nil {
		return "", false
	}

	key := c.clipV1ArchiveDataSourceKey(imageID)
	routingKey := cachePath
	hash, err := c.cacheClient.StoreContentFromS3Source(cache.S3ContentSource{
		Path:           key,
		CachePath:      cachePath,
		BucketName:     sourceRegistry.BucketName,
		Region:         sourceRegistry.Region,
		EndpointURL:    sourceRegistry.Endpoint,
		AccessKey:      sourceRegistry.AccessKey,
		SecretKey:      sourceRegistry.SecretKey,
		ForcePathStyle: sourceRegistry.ForcePathStyle,
	}, cache.StoreContentOptions{RoutingKey: routingKey, Lock: true})
	if err != nil {
		if errors.Is(err, cache.ErrUnableToAcquireLock) {
			if ok, waitErr := c.waitForImageArchiveContentCachePath(ctx, targetPath, imageID, cachePath); ok {
				return targetPath, true
			} else if waitErr != nil {
				log.Debug().Err(waitErr).Str("image_id", imageID).Str("cache_path", cachePath).Msg("v1 image data archive content cache lock wait failed")
			}
		} else {
			log.Debug().Err(err).Str("image_id", imageID).Str("cache_path", cachePath).Str("source", key).Msg("v1 image data archive content cache store failed")
		}
		return "", false
	}

	size, err := c.imageArchiveSize(ctx, imageID, key, sourceRegistry)
	if err != nil {
		log.Debug().Err(err).Str("image_id", imageID).Str("source", key).Msg("v1 image data archive size lookup failed")
		return "", false
	}
	if err := c.writeImageArchiveFromContentCache(ctx, targetPath, imageID, hash, size, routingKey); err != nil {
		log.Debug().Err(err).Str("image_id", imageID).Str("cache_path", cachePath).Msg("v1 image data archive content cache write failed")
		return "", false
	}
	return targetPath, true
}

func (c *ImageClient) deferV1ArchiveDataRestore(ctx context.Context, imageID, cachePath string, sourceRegistry *types.S3ImageRegistryConfig) (bool, int64) {
	if sourceRegistry == nil || sourceRegistry.BucketName == "" {
		return false, 0
	}

	if c.cacheClient != nil {
		if metadata, err := c.cacheClient.CacheFSMetadata(ctx, cachePath); err == nil && metadata != nil && metadata.Size > 0 {
			size := int64(metadata.Size)
			return size > maxSyncV1ArchiveDataRestoreBytes, size
		}
	}

	size, err := c.imageArchiveSize(ctx, imageID, c.clipV1ArchiveDataSourceKey(imageID), sourceRegistry)
	if err != nil {
		return false, 0
	}
	return size > maxSyncV1ArchiveDataRestoreBytes, size
}

func (c *ImageClient) warmV1ArchiveDataCacheAsync(imageID, targetPath, cachePath string, sourceRegistry types.S3ImageRegistryConfig) {
	if c.cacheClient == nil || imageID == "" || targetPath == "" || sourceRegistry.BucketName == "" {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), imageArchiveDownloadTimeout)
		defer cancel()

		if c.localImageArchiveReady(targetPath, imageID) {
			return
		}
		if _, ok := c.materializeV1ArchiveDataCache(ctx, nil, imageID, targetPath, cachePath, &sourceRegistry); !ok {
			log.Debug().Str("image_id", imageID).Str("cache_path", cachePath).Msg("background v1 image data archive warm skipped")
		}
	}()
}

// downloadV1ArchiveDataFromBrokeredURL restores the CLIP v1 data archive
// through a gateway-presigned URL when no S3 source is available. Agent-hosted
// pools hold no image registry credentials, so the archive lands on local disk
// and is then seeded into the embedded cache off the hot path.
func (c *ImageClient) downloadV1ArchiveDataFromBrokeredURL(ctx context.Context, request *types.ContainerRequest, targetPath, cachePath string) bool {
	if !c.brokeredImageAccessRequest(request) {
		return false
	}
	creds := c.originCredentials(ctx, request, request.ImageId, "")
	if creds == nil || creds.imageArchiveDataURL == "" {
		return false
	}

	tempPath := targetPath + ".url.tmp"
	defer os.Remove(tempPath)
	if err := downloadImageArchiveURL(ctx, creds.imageArchiveDataURL, tempPath); err != nil {
		log.Debug().Err(err).Str("image_id", request.ImageId).Msg("v1 image data archive brokered url fetch failed")
		return false
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		log.Debug().Err(err).Str("image_id", request.ImageId).Msg("v1 image data archive rename failed")
		return false
	}

	go c.seedV1ArchiveDataInEmbeddedCache(targetPath, cachePath, request.ImageId)
	log.Info().Str("image_id", request.ImageId).Msg("restored v1 image data archive from brokered URL")
	return true
}

func (c *ImageClient) seedV1ArchiveDataInEmbeddedCache(localPath, cachePath, imageId string) {
	if c.cacheClient == nil {
		return
	}
	if _, err := c.cacheClient.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      localPath,
		CachePath: cachePath,
	}, cache.StoreContentOptions{RoutingKey: cachePath, Lock: true}); err != nil {
		log.Debug().Err(err).Str("image_id", imageId).Str("cache_path", cachePath).Msg("v1 image data archive embedded cache seed failed")
	}
}

func (c *ImageClient) localImageArchiveReady(archivePath, imageID string) bool {
	info, err := os.Stat(archivePath)
	if err != nil {
		return false
	}
	if info.IsDir() || info.Size() <= 0 {
		_ = os.RemoveAll(archivePath)
		return false
	}
	if err := c.validateRestoredImageArchive(archivePath, imageID, info.Size()); err != nil {
		log.Debug().Err(err).Str("image_id", imageID).Str("path", archivePath).Msg("discarding invalid local image archive")
		_ = os.Remove(archivePath)
		return false
	}
	return true
}

func (c *ImageClient) acquireRemoteImageMountLock(imageId string) (func(), error) {
	lockResponse, err := handleGRPCResponse(c.workerRepoClient.SetImagePullLock(context.Background(), &pb.SetImagePullLockRequest{
		WorkerId: c.workerId,
		ImageId:  imageId,
	}))
	if err != nil {
		return nil, err
	}

	return func() {
		_, err := handleGRPCResponse(c.workerRepoClient.RemoveImagePullLock(context.Background(), &pb.RemoveImagePullLockRequest{
			WorkerId: c.workerId,
			ImageId:  imageId,
			Token:    lockResponse.Token,
		}))
		if err != nil {
			log.Warn().Err(err).Str("image_id", imageId).Msg("failed to release image pull lock")
		}
	}, nil
}

func (c *ImageClient) mountLazyImageArchive(request *types.ContainerRequest, mountOptions clip.MountOptions) error {
	phaseStart := time.Now()
	startServer, _, server, err := clip.MountArchive(mountOptions)
	metrics.RecordWorkerStartupPhase("clip_mount_archive_init", time.Since(phaseStart), request, map[string]string{
		"clip_version": fmt.Sprintf("%d", c.config.ImageService.ClipVersion),
		"success":      fmt.Sprintf("%t", err == nil),
	})
	if err != nil {
		return err
	}

	phaseStart = time.Now()
	err = startServer()
	metrics.RecordWorkerStartupPhase("clip_mount_archive_start", time.Since(phaseStart), request, map[string]string{
		"clip_version": fmt.Sprintf("%d", c.config.ImageService.ClipVersion),
		"success":      fmt.Sprintf("%t", err == nil),
	})
	if err != nil {
		if server != nil {
			_ = server.Unmount()
		}
		return err
	}

	c.mountedFuseServers.Set(request.ImageId, server)
	return nil
}

// processPulledArchive extracts metadata and moves v2 OCI archives to canonical location
func (c *ImageClient) processPulledArchive(downloadPath, imageId string) (*clipCommon.ClipArchiveMetadata, error) {
	archiver := clip.NewClipArchiver()
	meta, err := archiver.ExtractMetadata(downloadPath)
	if err != nil {
		return nil, err
	}

	// Check if this is an OCI v2 image
	isOCI := false
	if meta != nil {
		if ociInfo, ok := ociStorageInfo(meta); ok {
			isOCI = ociInfo.Type() == string(clipCommon.StorageModeOCI) || strings.ToLower(ociInfo.Type()) == "oci"
		} else if t, ok := meta.StorageInfo.(interface{ Type() string }); ok {
			isOCI = t.Type() == string(clipCommon.StorageModeOCI) || strings.ToLower(t.Type()) == "oci"
		}
	} else {
		return nil, fmt.Errorf("metadata not available")
	}

	if !isOCI {
		return meta, nil
	}

	// Cache OCI metadata for later use
	c.cacheOCIMetadata(imageId, meta)

	return meta, nil
}

// cacheOCIMetadata extracts and caches OCI image metadata
func (c *ImageClient) cacheOCIMetadata(imageId string, meta *clipCommon.ClipArchiveMetadata) {
	if meta == nil {
		return
	}

	if c.v2ArchiveMetadata != nil {
		c.v2ArchiveMetadata.Set(imageId, meta)
	}

	ociInfo, ok := ociStorageInfo(meta)
	if !ok {
		return
	}

	if ociInfo.RegistryURL == "" {
		ociInfo.RegistryURL = "https://docker.io"
	}

	if ociInfo.Repository != "" && ociInfo.Reference != "" {
		registryHost := strings.TrimPrefix(ociInfo.RegistryURL, "https://")
		registryHost = strings.TrimPrefix(registryHost, "http://")
		sourceRef := fmt.Sprintf("%s/%s:%s", registryHost, ociInfo.Repository, ociInfo.Reference)
		c.v2ImageRefs.Set(imageId, sourceRef)
		log.Info().Str("image_id", imageId).Str("source_ref", sourceRef).Msg("cached image reference from metadata")
	}
}

// GetSourceImageRef retrieves the cached source image reference for a v2 image
func (c *ImageClient) GetSourceImageRef(imageId string) (string, bool) {
	return c.v2ImageRefs.Get(imageId)
}

// GetCLIPImageMetadata extracts CLIP image metadata from the archive
func (c *ImageClient) GetCLIPImageMetadata(imageId string) (*clipCommon.ImageMetadata, bool) {
	if c.v2ArchiveMetadata != nil {
		if meta, ok := c.v2ArchiveMetadata.Get(imageId); ok {
			if ociInfo, ok := ociStorageInfo(meta); ok && ociInfo.ImageMetadata != nil {
				return ociInfo.ImageMetadata, true
			}
		}
	}

	// Determine the archive path for this image
	archivePath := fmt.Sprintf("%s/%s.%s", types.AgentImagesPath, imageId, reg.LocalImageFileExtension)

	// Check if the archive exists
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		// Try cache path as fallback
		if c.registry != nil && c.registry.ImageFileExtension != "" {
			archivePath = fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
		} else {
			archivePath = fmt.Sprintf("%s/%s.clip", c.imageCachePath, imageId)
		}

		if _, err := os.Stat(archivePath); os.IsNotExist(err) {
			return nil, false
		}
	}

	// Extract metadata from the CLIP archive
	archiver := clip.NewClipArchiver()
	meta, err := archiver.ExtractMetadata(archivePath)
	if err != nil {
		log.Warn().Err(err).Str("image_id", imageId).Msg("failed to extract metadata from clip archive")
		return nil, false
	}

	// Check if this is an OCI archive with metadata
	c.cacheOCIMetadata(imageId, meta)
	if ociInfo, ok := ociStorageInfo(meta); ok && ociInfo.ImageMetadata != nil {
		return ociInfo.ImageMetadata, true
	}

	return nil, false
}

// getCredentialProviderForImage determines the appropriate credentials for an
// image. Agent-hosted pools are gateway-only: request-embedded credentials are
// stripped before scheduling and ignored here as a second guardrail.
func (c *ImageClient) getCredentialProviderForImage(ctx context.Context, imageId string, request *types.ContainerRequest) clipCommon.RegistryCredentialProvider {
	sourceRef, hasRef := c.v2ImageRefs.Get(imageId)
	if !hasRef {
		return nil
	}

	registry := registryFromImageRef(sourceRef)
	if registry == "" {
		return nil
	}

	if c.brokeredImageAccessRequest(request) {
		if provider := c.gatewayCredentialProviderForImage(ctx, imageId, registry, request); provider != nil {
			return provider
		}
		log.Warn().
			Str("image_id", imageId).
			Str("registry", registry).
			Msg("agent worker has no gateway-vended registry credentials; using anonymous auth and avoiding ambient keychain")
		return privateWorkerAnonymousRegistryProvider{}
	}

	if request.ImageCredentials != "" {
		return c.parseAndCreateProvider(ctx, request.ImageCredentials, registry, imageId, "runtime secret")
	}

	if provider := c.gatewayCredentialProviderForImage(ctx, imageId, registry, request); provider != nil {
		return provider
	}

	// Build registry credentials for images we built and pushed.
	// This must come before source image credentials because when we build with a custom base image,
	// the final image is in the build registry, not the source image registry
	// We check both the registry domain AND the build repository name to avoid false positives
	buildRegistry := c.getBuildRegistry()
	buildRepoName := c.config.ImageService.BuildRepositoryName
	if buildRegistry != "" && buildRepoName != "" &&
		strings.Contains(sourceRef, buildRegistry) &&
		strings.Contains(sourceRef, buildRepoName) &&
		request.BuildRegistryCredentials != "" {
		return c.parseAndCreateProvider(ctx, request.BuildRegistryCredentials, registry, imageId, "build registry")
	}

	if request.BuildOptions.SourceImageCreds != "" {
		return c.parseAndCreateProvider(ctx, request.BuildOptions.SourceImageCreds, registry, imageId, "source image")
	}

	return nil
}

// parseAndCreateProvider parses credentials and creates a CLIP credential provider
func (c *ImageClient) parseAndCreateProvider(ctx context.Context, credStr string, registry string, imageId string, source string) clipCommon.RegistryCredentialProvider {
	creds, err := reg.ParseCredentialsFromJSON(credStr)
	if err != nil || len(creds) == 0 {
		parts := strings.SplitN(credStr, ":", 2)
		if len(parts) == 2 {
			creds = map[string]string{
				"USERNAME": parts[0],
				"PASSWORD": parts[1],
			}
		}
	}

	if len(creds) == 0 {
		return nil
	}

	provider := reg.CredentialsToProvider(ctx, registry, creds)
	if provider != nil {
		log.Info().Str("image_id", imageId).Str("registry", registry).Str("source", source).Msg("using credentials")
	}

	return provider
}

// cacheV2SourceImageRef caches the source image reference for external images
func (c *ImageClient) cacheV2SourceImageRef(request *types.ContainerRequest) {
	if c.config.ImageService.ClipVersion == uint32(types.ClipVersion2) && request.BuildOptions.SourceImage != nil {
		c.v2ImageRefs.Set(request.ImageId, *request.BuildOptions.SourceImage)
		log.Info().Str("image_id", request.ImageId).Str("source_image", *request.BuildOptions.SourceImage).Msg("cached image reference")
	}
}

func (c *ImageClient) Cleanup() error {
	mountedFuseServers := map[string]*fuse.Server{}
	c.mountedFuseServers.Range(func(imageId string, server *fuse.Server) bool {
		mountedFuseServers[imageId] = server
		return true
	})

	for imageId, server := range mountedFuseServers {
		mountPoint := c.imageMountPoint(imageId)
		log.Info().Str("image_id", imageId).Str("mount_point", mountPoint).Msg("un-mounting image")
		server.Unmount()
		if err := forceUnmountImageMount(mountPoint); err != nil {
			log.Warn().Str("image_id", imageId).Str("mount_point", mountPoint).Err(err).Msg("failed to force unmount image mount")
		}
		c.mountedFuseServers.Delete(imageId)
	}

	log.Info().Str("path", c.imageCachePath).Msg("cleaning up cachefs image cache")
	if c.config.Cache.Client.CacheFS.Enabled && c.cacheClient != nil {
		err := c.cacheClient.Cleanup()
		if err != nil {
			return err
		}
	}

	return nil
}

func cleanupImageMountPath(mountPath string) error {
	entries, err := os.ReadDir(mountPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(mountPath, entry.Name())
		if err := forceUnmountImageMount(path); err != nil {
			log.Warn().Str("mount_point", path).Err(err).Msg("failed to force unmount image mount during cleanup")
		}
	}

	return os.RemoveAll(mountPath)
}

func forceUnmountImageMount(mountPoint string) error {
	if mountPoint == "" {
		return nil
	}

	if _, err := os.Stat(mountPoint); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	commands := [][]string{
		{"fusermount", "-uz", mountPoint},
		{"umount", "-l", mountPoint},
		{"umount", mountPoint},
	}

	var lastErr error
	for _, command := range commands {
		cmd := exec.Command(command[0], command[1:]...)
		output, err := cmd.CombinedOutput()
		if err == nil || isBenignUnmountError(string(output), err) {
			return nil
		}
		lastErr = fmt.Errorf("%s: %w: %s", strings.Join(command, " "), err, strings.TrimSpace(string(output)))
	}

	return lastErr
}

func isBenignUnmountError(output string, err error) bool {
	if err == nil {
		return true
	}

	msg := strings.ToLower(output + " " + err.Error())
	return strings.Contains(msg, "not mounted") ||
		strings.Contains(msg, "not mount") ||
		strings.Contains(msg, "no mount point") ||
		strings.Contains(msg, "invalid argument")
}

func (c *ImageClient) pullImageFromRegistry(ctx context.Context, archivePath string, request *types.ContainerRequest) (*types.S3ImageRegistryConfig, error) {
	imageId := request.ImageId
	sourceRegistry := c.config.ImageService.Registries.S3
	if c.brokeredImageAccessRequest(request) {
		// Agent-hosted workers never hold valid static S3 credentials; any config
		// values are placeholder defaults and must not reach the clip mount.
		sourceRegistry = types.S3ImageRegistryConfig{}
	}

	unlock, err := lockImageArchiveFile(ctx, archivePath)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// Check if file exists now that we have the lock (another worker may have downloaded it)
	if _, err := os.Stat(archivePath); err == nil {
		return &sourceRegistry, nil
	}

	if ok, cacheSourceRegistry, err := c.pullImageArchiveFromEmbeddedCache(ctx, archivePath, request); ok {
		if cacheSourceRegistry != nil {
			return cacheSourceRegistry, nil
		}
		return &sourceRegistry, nil
	} else if err != nil {
		logEmbeddedImageCacheFallback(err, imageId, request)
	}

	// Download to temp file, then atomically rename
	tempPath := archivePath + ".tmp"
	defer os.Remove(tempPath)

	if c.brokeredImageAccessRequest(request) {
		if pulled, brokeredRegistry, err := c.pullImageArchiveFromBrokeredOrigin(ctx, tempPath, request); err == nil && pulled {
			if err := os.Rename(tempPath, archivePath); err != nil {
				return nil, err
			}
			go c.publishImageArchiveToEmbeddedCache(archivePath, imageId)
			return brokeredRegistry, nil
		} else if err != nil {
			log.Warn().Err(err).Str("image_id", imageId).Msg("brokered image archive origin unavailable for agent worker")
			return nil, err
		}
		return nil, fmt.Errorf("gateway-brokered image archive origin is unavailable for agent worker image %s", imageId)
	}

	err = c.registry.Pull(ctx, tempPath, imageId)
	if err != nil && c.config.ImageService.RegistryStore == registry.LocalImageRegistryStore && !c.brokeredImageAccessRequest(request) {
		if s3Registry, e2 := registry.NewImageRegistry(c.config, c.config.ImageService.Registries.S3); e2 == nil {
			_ = c.registry.CopyImageFromRegistry(ctx, imageId, s3Registry)
			if err2 := c.registry.Pull(ctx, tempPath, imageId); err2 == nil {
				err = nil
			} else {
				err = err2
			}
		}
	}

	if err != nil {
		logImageRegistryPullFailure(err, imageId, request)
		return nil, err
	}

	if err := os.Rename(tempPath, archivePath); err != nil {
		return nil, err
	}
	go c.publishImageArchiveToEmbeddedCache(archivePath, imageId)

	return &sourceRegistry, nil
}

func logEmbeddedImageCacheFallback(err error, imageId string, request *types.ContainerRequest) {
	if request != nil && request.IsBuildRequest() {
		log.Debug().
			Err(err).
			Str("image_id", imageId).
			Msg("embedded image archive cache unavailable for build image, continuing with build request path")
		return
	}

	log.Warn().
		Err(err).
		Str("image_id", imageId).
		Msg("embedded image archive cache unavailable, falling back to registry")
}

func logImageRegistryPullFailure(err error, imageId string, request *types.ContainerRequest) {
	if request != nil && request.IsBuildRequest() {
		log.Debug().
			Err(err).
			Str("image_id", imageId).
			Msg("build image archive unavailable in registry, continuing with build request path")
		return
	}

	log.Error().
		Err(err).
		Str("image_id", imageId).
		Msg("failed to pull image from registry")
}

func (c *ImageClient) brokeredImageAccessRequest(request *types.ContainerRequest) bool {
	if c == nil {
		return false
	}
	if request != nil && request.PoolSelector != "" {
		if pool, ok := c.config.Worker.Pools[request.PoolSelector]; ok {
			return pool.AgentHosted()
		}
	}
	if c.workerPoolName == "" {
		return false
	}
	pool, ok := c.config.Worker.Pools[c.workerPoolName]
	return ok && pool.AgentHosted()
}

// pullImageArchiveFromBrokeredOrigin pulls the image archive using
// gateway-brokered origin access: a presigned URL when vended, or S3
// credentials from older gateways. It reports whether the archive was pulled;
// the returned source registry is nil for URL pulls since the worker holds no
// S3 credentials in that mode.
func (c *ImageClient) pullImageArchiveFromBrokeredOrigin(ctx context.Context, archivePath string, request *types.ContainerRequest) (bool, *types.S3ImageRegistryConfig, error) {
	creds := c.originCredentials(ctx, request, request.ImageId, "")
	if creds == nil {
		return false, nil, nil
	}
	if creds.imageArchiveURL != "" {
		if err := downloadImageArchiveURL(ctx, creds.imageArchiveURL, archivePath); err != nil {
			return false, nil, err
		}
		log.Info().Str("image_id", request.ImageId).Msg("pulled image archive from brokered URL")
		return true, nil, nil
	}

	if creds.imageArchiveStorage == nil || creds.imageArchiveObjectKey == "" {
		return false, nil, nil
	}

	sourceRegistry := imageArchiveRegistryConfig(creds.imageArchiveStorage)
	if sourceRegistry.BucketName == "" {
		return false, nil, nil
	}

	store, err := reg.NewS3Store(sourceRegistry)
	if err != nil {
		return false, nil, err
	}
	if err := store.Get(ctx, creds.imageArchiveObjectKey, archivePath); err != nil {
		return false, nil, err
	}

	log.Info().
		Str("image_id", request.ImageId).
		Str("object_key", creds.imageArchiveObjectKey).
		Msg("pulled image archive from brokered origin")
	return true, &sourceRegistry, nil
}

const (
	// imageArchiveDownloadTimeout bounds a whole-archive download so a stalled
	// origin cannot hold the image pull flock indefinitely.
	imageArchiveDownloadTimeout       = 30 * time.Minute
	imageArchiveDownloadHeaderTimeout = 30 * time.Second
)

var imageArchiveHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ResponseHeaderTimeout: imageArchiveDownloadHeaderTimeout,
	},
}

func downloadImageArchiveURL(ctx context.Context, url, path string) error {
	ctx, cancel := context.WithTimeout(ctx, imageArchiveDownloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := imageArchiveHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("image archive download failed: %s", resp.Status)
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	return err
}

// publishImageArchiveToEmbeddedCache best-effort seeds the pulled archive into
// the embedded content cache so other workers in the locality can restore it
// without re-fetching from origin. Callers run it off the container start path.
func (c *ImageClient) publishImageArchiveToEmbeddedCache(archivePath, imageId string) {
	if c.cacheClient == nil || archivePath == "" || imageId == "" {
		return
	}

	cachePath := c.imageArchiveCachePath(imageId)
	hash, err := c.cacheClient.StoreContentFromLocalFile(cache.LocalContentSource{
		Path:      archivePath,
		CachePath: cachePath,
	}, cache.StoreContentOptions{RoutingKey: cachePath, Lock: true})
	if err != nil {
		log.Debug().Err(err).Str("image_id", imageId).Str("cache_path", cachePath).Msg("image archive embedded cache seed failed")
		return
	}

	log.Info().Str("image_id", imageId).Str("cache_path", cachePath).Str("hash", hash).Msg("seeded image archive in embedded cache")
}

func (c *ImageClient) pullImageArchiveFromEmbeddedCache(ctx context.Context, archivePath string, request *types.ContainerRequest) (bool, *types.S3ImageRegistryConfig, error) {
	imageId := request.ImageId
	if c.cacheClient == nil {
		return false, nil, nil
	}

	metadataStart := time.Now()
	if ok, err := c.copyImageArchiveFromContentCacheMetadata(ctx, archivePath, imageId); ok {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheMetadata, metadataStart, time.Since(metadataStart), true, nil)
		log.Info().Str("image_id", imageId).Msg("restored image archive from embedded cache")
		return true, c.imageArchiveSourceRegistry(ctx, request), nil
	} else if err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheMetadata, metadataStart, time.Since(metadataStart), false, nil)
		log.Warn().Err(err).Str("image_id", imageId).Msg("embedded image archive content cache metadata unavailable")
	} else {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheMetadata, metadataStart, time.Since(metadataStart), true, map[string]string{"hit": "false"})
	}

	cachePath := c.imageArchiveCachePath(imageId)
	routingKey := cachePath
	key := fmt.Sprintf("%s.%s", imageId, c.registry.ImageFileExtension)

	var (
		hash string
		err  error
	)
	storeStart := time.Now()
	var sourceRegistry *types.S3ImageRegistryConfig
	if c.config.ImageService.RegistryStore == registry.S3ImageRegistryStore {
		sourceRegistry = c.imageArchiveSourceRegistry(ctx, request)
		if sourceRegistry == nil || sourceRegistry.BucketName == "" {
			return false, nil, nil
		}
		hash, err = c.cacheClient.StoreContentFromS3Source(cache.S3ContentSource{
			Path:           key,
			CachePath:      cachePath,
			BucketName:     sourceRegistry.BucketName,
			Region:         sourceRegistry.Region,
			EndpointURL:    sourceRegistry.Endpoint,
			AccessKey:      sourceRegistry.AccessKey,
			SecretKey:      sourceRegistry.SecretKey,
			ForcePathStyle: sourceRegistry.ForcePathStyle,
		}, cache.StoreContentOptions{RoutingKey: routingKey, Lock: true})
	} else {
		sourcePath := filepath.Join(types.AgentImagesPath, key)
		info, statErr := os.Stat(sourcePath)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return false, nil, nil
			}
			return false, nil, statErr
		}
		if info.IsDir() {
			return false, nil, fmt.Errorf("image archive source is a directory: %s", sourcePath)
		}

		hash, err = c.cacheClient.StoreContentFromLocalFile(cache.LocalContentSource{
			Path:      sourcePath,
			CachePath: cachePath,
		}, cache.StoreContentOptions{RoutingKey: routingKey, Lock: true})
	}
	if err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheStore, storeStart, time.Since(storeStart), false, nil)
		if errors.Is(err, cache.ErrUnableToAcquireLock) {
			waitStart := time.Now()
			if ok, waitErr := c.waitForImageArchiveContentCache(ctx, archivePath, imageId); ok {
				c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheWait, waitStart, time.Since(waitStart), true, map[string]string{
					"reason": "store_lock_contended",
				})
				return true, sourceRegistry, nil
			} else if waitErr != nil {
				c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheWait, waitStart, time.Since(waitStart), false, map[string]string{
					"reason": "store_lock_contended",
				})
				return false, nil, waitErr
			} else {
				c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheWait, waitStart, time.Since(waitStart), false, map[string]string{
					"reason": "store_lock_contended",
				})
				return false, nil, nil
			}
		}
		return false, nil, err
	}
	c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheStore, storeStart, time.Since(storeStart), true, map[string]string{
		"registry_store": c.config.ImageService.RegistryStore,
	})

	restoreStart := time.Now()
	size, err := c.imageArchiveSize(ctx, imageId, key, sourceRegistry)
	if err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheRestore, restoreStart, time.Since(restoreStart), false, nil)
		return false, nil, err
	}
	if err := c.writeImageArchiveFromContentCache(ctx, archivePath, imageId, hash, size, routingKey); err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheRestore, restoreStart, time.Since(restoreStart), false, map[string]string{"size_bytes": fmt.Sprintf("%d", size)})
		return false, nil, err
	}
	c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheRestore, restoreStart, time.Since(restoreStart), true, map[string]string{"size_bytes": fmt.Sprintf("%d", size)})

	return true, sourceRegistry, nil
}

func (c *ImageClient) imageArchiveSourceRegistry(ctx context.Context, request *types.ContainerRequest) *types.S3ImageRegistryConfig {
	// Agent-hosted workers never hold valid static S3 credentials: their config
	// is generated by the agent, and any registry values present are placeholder
	// defaults from the worker image's embedded config. Only gateway-brokered
	// credentials are usable.
	if c.brokeredImageAccessRequest(request) {
		creds := c.originCredentials(ctx, request, request.ImageId, "")
		if creds == nil || creds.imageArchiveStorage == nil {
			return nil
		}
		sourceRegistry := imageArchiveRegistryConfig(creds.imageArchiveStorage)
		if sourceRegistry.BucketName == "" {
			return nil
		}
		return &sourceRegistry
	}

	sourceRegistry := c.config.ImageService.Registries.S3
	if sourceRegistry.BucketName == "" {
		return nil
	}
	return &sourceRegistry
}

func (c *ImageClient) imageArchiveSize(ctx context.Context, imageID, key string, sourceRegistry *types.S3ImageRegistryConfig) (int64, error) {
	if sourceRegistry == nil {
		return c.registry.Size(ctx, imageID)
	}
	store, err := reg.NewS3Store(*sourceRegistry)
	if err != nil {
		return 0, err
	}
	return store.Size(ctx, key)
}

func (c *ImageClient) waitForImageArchiveContentCache(ctx context.Context, archivePath, imageId string) (bool, error) {
	return c.waitForImageArchiveContentCachePath(ctx, archivePath, imageId, c.imageArchiveCachePath(imageId))
}

func (c *ImageClient) waitForImageArchiveContentCachePath(ctx context.Context, archivePath, imageId, cachePath string) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, embeddedImageCacheLockWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(embeddedImageCacheWaitInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		ok, err := c.copyImageArchiveFromContentCachePath(waitCtx, archivePath, imageId, cachePath)
		if ok {
			return true, nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-waitCtx.Done():
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if lastErr != nil {
				return false, lastErr
			}
			return false, nil
		case <-ticker.C:
		}
	}
}

func (c *ImageClient) copyImageArchiveFromContentCacheMetadata(ctx context.Context, archivePath, imageId string) (bool, error) {
	return c.copyImageArchiveFromContentCachePath(ctx, archivePath, imageId, c.imageArchiveCachePath(imageId))
}

func (c *ImageClient) copyImageArchiveFromContentCachePath(ctx context.Context, archivePath, imageId, cachePath string) (bool, error) {
	metadata, err := c.cacheClient.CacheFSMetadata(ctx, cachePath)
	if err != nil {
		if isEmbeddedImageCacheMiss(err) {
			return false, nil
		}
		return false, err
	}
	if metadata == nil || metadata.Hash == "" {
		return false, fmt.Errorf("cachefs metadata missing hash for image archive path %s", cachePath)
	}
	if metadata.Size > uint64(^uint(0)>>1) {
		return false, fmt.Errorf("image archive too large for local restore: %d bytes", metadata.Size)
	}

	exists, err := c.cacheClient.IsCachedReachableContext(ctx, metadata.Hash, cachePath)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	if err := c.writeImageArchiveFromContentCache(ctx, archivePath, imageId, metadata.Hash, int64(metadata.Size), cachePath); err != nil {
		if isEmbeddedImageCacheMiss(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isEmbeddedImageCacheMiss(err error) bool {
	var notFound *cache.ErrNodeNotFound
	return errors.Is(err, cache.ErrContentNotFound) || errors.As(err, &notFound) || strings.Contains(err.Error(), "cachefs node not found")
}

func (c *ImageClient) imageArchiveCachePath(imageId string) string {
	return fmt.Sprintf("%s/%s.%s", types.AgentImagesPath, imageId, c.registry.ImageFileExtension)
}

func (c *ImageClient) writeImageArchiveFromContentCache(ctx context.Context, archivePath, imageId, hash string, size int64, routingKey string) error {
	if routingKey == "" {
		routingKey = hash
	}

	tmp, err := os.CreateTemp(filepath.Dir(archivePath), filepath.Base(archivePath)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	f := tmp
	defer os.Remove(tmpPath)

	hasher := sha256.New()
	offset := int64(0)
	bufSize := int64(4 * 1024 * 1024)
	buf := make([]byte, bufSize)
	for offset < size {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return err
		}

		length := min(bufSize, size-offset)
		n, err := c.cacheClient.ReadContentInto(ctx, hash, offset, buf[:length], cache.ClientOptions{RoutingKey: routingKey})
		if err != nil {
			_ = f.Close()
			return err
		}
		if n != length {
			_ = f.Close()
			return fmt.Errorf("short embedded image archive cache read: expected %d bytes, got %d", length, n)
		}

		content := buf[:n]
		if _, err := f.Write(content); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := hasher.Write(content); err != nil {
			_ = f.Close()
			return err
		}
		offset += length
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if actualHash != hash {
		return fmt.Errorf("image archive cache hash mismatch: expected %s, got %s", hash, actualHash)
	}
	if err := c.validateRestoredImageArchive(tmpPath, imageId, size); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, archivePath); err != nil {
		return err
	}

	log.Info().Str("image_id", imageId).Str("hash", hash).Str("routing_key", routingKey).Int64("size", size).Msg("loaded image archive from content cache")
	return nil
}

func (c *ImageClient) validateRestoredImageArchive(archivePath, imageId string, size int64) error {
	archiver := clip.NewClipArchiver()
	meta, err := archiver.ExtractMetadata(archivePath)
	if err != nil {
		return fmt.Errorf("restored image archive metadata invalid: image_id=%s: %w", imageId, err)
	}

	ociInfo, ok := ociStorageInfo(meta)
	if !ok || strings.ToLower(ociInfo.Type()) != string(clipCommon.StorageModeOCI) {
		return nil
	}

	const maxExpectedV2ArchiveSize = int64(128 * 1024 * 1024)
	if size > maxExpectedV2ArchiveSize {
		return fmt.Errorf("restored v2 image archive is unexpectedly large: image_id=%s size=%d", imageId, size)
	}
	if ociInfo.ImageMetadata == nil {
		return fmt.Errorf("restored v2 image archive is missing embedded image metadata: image_id=%s", imageId)
	}
	if len(ociInfo.Layers) == 0 || len(ociInfo.DecompressedHashByLayer) == 0 {
		return fmt.Errorf("restored v2 image archive has no layer cache metadata: image_id=%s layers=%d hashes=%d", imageId, len(ociInfo.Layers), len(ociInfo.DecompressedHashByLayer))
	}

	c.cacheOCIMetadata(imageId, meta)
	return nil
}

func openImageLockFile(lockPath string) (*os.File, error) {
	var lastErr error

	for attempt := 0; attempt < 3; attempt++ {
		if err := ensureImageDirectory(filepath.Dir(lockPath), 0755); err != nil {
			return nil, err
		}

		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
		if err == nil {
			return lockFile, nil
		}

		lastErr = err
		if !os.IsNotExist(err) {
			return nil, err
		}

		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}

	return nil, lastErr
}

func lockImageArchiveFile(ctx context.Context, archivePath string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	lockFile, err := openImageLockFile(archivePath + ".lock")
	if err != nil {
		return nil, err
	}

	for {
		if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			return func() {
				_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
				_ = lockFile.Close()
			}, nil
		} else if !imageArchiveLockBusy(err) {
			_ = lockFile.Close()
			return nil, err
		}

		select {
		case <-ctx.Done():
			_ = lockFile.Close()
			return nil, ctx.Err()
		case <-time.After(imageArchiveLockRetryInterval):
		}
	}
}

func imageArchiveLockBusy(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}

func (c *ImageClient) inspectAndVerifyImage(ctx context.Context, request *types.ContainerRequest) error {
	imageMetadata, err := c.skopeoClient.Inspect(ctx, *request.BuildOptions.SourceImage, request.BuildOptions.SourceImageCreds, nil)
	if err != nil {
		return err
	}

	if imageMetadata.Architecture != runtime.GOARCH {
		return &types.ExitCodeError{
			ExitCode: types.ContainerExitCodeIncorrectImageArch,
		}
	}

	if imageMetadata.Os != runtime.GOOS {
		return &types.ExitCodeError{
			ExitCode: types.ContainerExitCodeIncorrectImageOs,
		}
	}

	return nil
}

// getBuildRegistry returns the registry to use for final and intermediate build images
func (c *ImageClient) getBuildRegistry() string {
	if c.config.ImageService.BuildRegistry != "" {
		return c.config.ImageService.BuildRegistry
	}

	if c.config.ImageService.Runner.BaseImageRegistry != "" {
		return c.config.ImageService.Runner.BaseImageRegistry
	}

	return "localhost"
}

// setupBuildahDirs creates paths for buildah operations. The graphroot is where
// buildah keeps reusable Dockerfile layers, so use a cache-backed directory when
// one is available and keep runroot/tmpdir ephemeral.
func (c *ImageClient) setupBuildahDirs() (graphroot, runroot, tmpdir string, cleanupGraphroot bool) {
	cacheRoot := buildahLayerCacheRoot()
	if cacheRoot != "" {
		graphroot = filepath.Join(cacheRoot, "storage")
		if err := ensureBuildahGraphroot(graphroot); err == nil {
			runroot = mustMkdirTempBuildahDir("buildah-run-")
			tmpdir = mustMkdirTempBuildahDir("buildah-tmp-")
			return graphroot, runroot, tmpdir, false
		} else {
			log.Warn().Err(err).Str("path", graphroot).Msg("buildah layer cache unavailable")
		}
	}

	graphroot = mustMkdirTempBuildahDir("buildah-storage-")
	runroot = mustMkdirTempBuildahDir("buildah-run-")
	tmpdir = mustMkdirTempBuildahDir("buildah-tmp-")
	return graphroot, runroot, tmpdir, true
}

func buildahLayerCacheRoot() string {
	if dir := strings.TrimSpace(os.Getenv(types.AgentBuildCacheDirEnv)); dir != "" {
		return filepath.Join(dir, "buildah")
	}

	cacheRoot := filepath.Join(types.AgentCachePath, "buildah")
	if err := os.MkdirAll(cacheRoot, 0o700); err == nil {
		return cacheRoot
	}

	return ""
}

func ensureBuildahGraphroot(graphroot string) error {
	overlayDir := filepath.Join(graphroot, "overlay")
	if err := os.MkdirAll(overlayDir, 0o700); err != nil {
		return err
	}

	// Buildah probes this marker under the overlay graphroot. Some mounted
	// cache filesystems do not support that write, so validate it before using
	// the directory as persistent layer cache.
	marker := filepath.Join(overlayDir, ".has-mount-program")
	if err := os.WriteFile(marker, []byte("false"), 0o600); err != nil {
		return err
	}
	return os.Remove(marker)
}

func buildahStorageDriver(graphroot string) string {
	const overlayFSMagic = 0x794c7630

	var stat syscall.Statfs_t
	if err := syscall.Statfs(graphroot, &stat); err == nil && uint64(stat.Type) == overlayFSMagic {
		log.Warn().Str("path", graphroot).Msg("buildah graphroot is overlayfs, using vfs storage driver")
		return "vfs"
	}

	return "overlay"
}

func mustMkdirTempBuildahDir(pattern string) string {
	for _, parent := range []string{"/dev/shm", ""} {
		dir, err := os.MkdirTemp(parent, pattern)
		if err == nil {
			return dir
		}
	}
	fallback := filepath.Join(os.TempDir(), pattern+fmt.Sprintf("%d", time.Now().UnixNano()))
	_ = os.MkdirAll(fallback, 0o700)
	return fallback
}

// writeStorageConf creates a containers/storage configuration file
func (c *ImageClient) writeStorageConf(graphroot, runroot, driver string) (string, error) {
	var conf string
	if driver == "overlay" {
		conf = fmt.Sprintf(`[storage]
driver = "overlay"
graphroot = "%s"
runroot = "%s"

[storage.options.overlay]
mountopt = "nodev"
force_mask = "0000"
`, graphroot, runroot)
	} else {
		// vfs driver config
		conf = fmt.Sprintf(`[storage]
driver = "vfs"
graphroot = "%s"
runroot = "%s"
`, graphroot, runroot)
	}

	f, err := os.CreateTemp(runroot, "storage-*.conf")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := f.WriteString(conf); err != nil {
		return "", err
	}

	return f.Name(), nil
}

// buildahEnv returns environment variables for buildah
func (c *ImageClient) buildahEnv(runroot, tmpdir, storageConf string) []string {
	env := append(os.Environ(),
		"TMPDIR="+tmpdir,
		"XDG_RUNTIME_DIR="+runroot,
		"CONTAINERS_STORAGE_CONF="+storageConf,
		"BUILDAH_LAYERS=true",
		"GOMAXPROCS=0",
		"PIGZ=-p"+fmt.Sprintf("%d", runtime.NumCPU()),
	)
	return env
}

// getBuildahAuthArgs returns buildah authentication arguments from user-provided credentials
// Expects credentials in username:password format (either directly or in JSON)
// For ECR/GCR, users should pass pre-generated tokens, not raw AWS/GCP credentials
func (c *ImageClient) getBuildahAuthArgs(ctx context.Context, imageRef string, creds string) []string {
	if creds == "" {
		return nil
	}

	// Parse credentials - supports both JSON and username:password formats
	parsedCreds, err := reg.ParseCredentialsFromJSON(creds)
	if err != nil {
		log.Warn().Err(err).Str("image_ref", imageRef).Msg("failed to parse credentials for buildah")
		return nil
	}

	// Check for basic username/password auth (covers most registries including pre-generated ECR/GCR tokens)
	if username, ok := parsedCreds["USERNAME"]; ok {
		if password, ok := parsedCreds["PASSWORD"]; ok {
			return []string{"--creds", fmt.Sprintf("%s:%s", username, password)}
		}
	}

	// Check for Docker Hub credentials
	if username, ok := parsedCreds["DOCKERHUB_USERNAME"]; ok {
		if password, ok := parsedCreds["DOCKERHUB_PASSWORD"]; ok {
			return []string{"--creds", fmt.Sprintf("%s:%s", username, password)}
		}
	}

	// No valid username:password found
	log.Warn().Str("image_ref", imageRef).Msg("credentials provided but no valid username:password found")
	return nil
}

// formatImageBytes renders a byte count for user-facing build output.
func formatImageBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%d KiB", n/1024)
	}
}

const (
	imageIndexAggregateBytesBucket int64 = 1 << 30
	imageIndexProgressInterval           = 10 * time.Second
)

type imageIndexProgressReporter struct {
	logger           *slog.Logger
	started          time.Time
	processedByLayer map[string]int64
	completedByLayer map[string]bool
	processedBytes   int64
	completedLayers  int
	cachedLayers     int
	totalLayers      int
	lastLayerBucket  int
	lastByteBucket   int64
	lastReported     time.Time
}

func newImageIndexProgressReporter(logger *slog.Logger) *imageIndexProgressReporter {
	started := time.Now()
	return &imageIndexProgressReporter{
		logger:           logger,
		started:          started,
		lastReported:     started,
		processedByLayer: make(map[string]int64),
		completedByLayer: make(map[string]bool),
	}
}

func (r *imageIndexProgressReporter) report(progress clip.OCIIndexProgress) {
	if progress.TotalLayers > r.totalLayers {
		r.totalLayers = progress.TotalLayers
	}

	key := progress.LayerDigest
	if key == "" {
		key = fmt.Sprintf("%d", progress.LayerIndex)
	}
	processed := progress.BytesProcessed
	if progress.Stage == "completed" && progress.BytesTotal > processed {
		processed = progress.BytesTotal
	}
	if processed > r.processedByLayer[key] {
		r.processedBytes += processed - r.processedByLayer[key]
		r.processedByLayer[key] = processed
	}

	if progress.Stage == "completed" && !r.completedByLayer[key] {
		r.completedByLayer[key] = true
		if progress.CompletedLayers > r.completedLayers {
			r.completedLayers = progress.CompletedLayers
		}
		if len(r.completedByLayer) > r.completedLayers {
			r.completedLayers = len(r.completedByLayer)
		}
		if progress.Source == clip.LayerSourceIndexCache || progress.Source == clip.LayerSourceContentCache {
			r.cachedLayers++
		}
	}

	layerBucket := 0
	if r.totalLayers > 0 {
		layerBucket = r.completedLayers * 10 / r.totalLayers
	}
	byteBucket := r.processedBytes / imageIndexAggregateBytesBucket
	if layerBucket <= r.lastLayerBucket && byteBucket <= r.lastByteBucket {
		return
	}
	if r.completedLayers >= r.totalLayers && r.totalLayers > 0 {
		return
	}
	if time.Since(r.lastReported) < imageIndexProgressInterval {
		return
	}
	r.lastLayerBucket = layerBucket
	r.lastByteBucket = byteBucket
	r.lastReported = time.Now()
	r.logger.Info(fmt.Sprintf("Image indexing: %d/%d layers complete, %s processed\n",
		r.completedLayers, r.totalLayers, formatImageBytes(r.processedBytes)))
}

func (r *imageIndexProgressReporter) finish() {
	cached := ""
	if r.cachedLayers > 0 {
		cached = fmt.Sprintf(", %d cached", r.cachedLayers)
	}
	r.logger.Info(fmt.Sprintf("Image indexed in %.1fs: %d layers, %s processed%s\n",
		time.Since(r.started).Seconds(), r.totalLayers, formatImageBytes(r.processedBytes), cached))
}

type activeOutputWriter struct {
	logger       *slog.Logger
	lastOutputNS *atomic.Int64
}

func newActiveOutputWriter(logger *slog.Logger) *activeOutputWriter {
	w := &activeOutputWriter{logger: logger, lastOutputNS: &atomic.Int64{}}
	w.lastOutputNS.Store(time.Now().UnixNano())
	return w
}

func (w *activeOutputWriter) Write(p []byte) (int, error) {
	w.lastOutputNS.Store(time.Now().UnixNano())
	w.logger.Info(string(p))
	return len(p), nil
}

const buildOutputHeartbeatInterval = 15 * time.Second
const imageCommandCancelGracePeriod = 2 * time.Second

func newImageCommand(ctx context.Context, command string, args []string, env []string, stdout, stderr io.Writer) *exec.Cmd {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = imageCommandCancelGracePeriod
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return terminateImageProcessGroup(cmd.Process.Pid)
	}
	return cmd
}

func newBuildahCommand(ctx context.Context, args []string, env []string, stdout, stderr io.Writer) *exec.Cmd {
	return newImageCommand(ctx, "buildah", args, env, stdout, stderr)
}

func terminateImageProcessGroup(pid int) error {
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}

	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	time.Sleep(imageCommandCancelGracePeriod)
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	return nil
}

// startSilentOutputHeartbeat emits a user-facing heartbeat only when the
// wrapped command has produced no output for a while. This keeps noisy phases
// like pip downloads readable while making silent buildah commit/push phases
// understandable.
func startSilentOutputHeartbeat(ctx context.Context, outputLogger *slog.Logger, started time.Time, writer *activeOutputWriter, message string) func() {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(buildOutputHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				lastOutput := time.Unix(0, writer.lastOutputNS.Load())
				if time.Since(lastOutput) < buildOutputHeartbeatInterval {
					continue
				}
				outputLogger.Info(fmt.Sprintf("%s (%ds elapsed)\n", message, int(time.Since(started).Seconds())))
			}
		}
	}()
	return cancel
}

func (c *ImageClient) createOCIImageWithProgress(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, imageRef, localLayoutPath, outputPath string, checkpointMiB int64) error {
	outputLogger.Info(fmt.Sprintf("Indexing image (%d concurrent layers)...\n", imageLayerPrepareConcurrency))
	progressChan := make(chan clip.OCIIndexProgress, 100)
	reporter := newImageIndexProgressReporter(outputLogger)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for progress := range progressChan {
			log.Debug().
				Str("container_id", request.ContainerId).
				Str("stage", progress.Stage).
				Str("source", progress.Source).
				Int("layer", progress.LayerIndex).
				Int("total", progress.TotalLayers).
				Int64("bytes", progress.BytesProcessed).
				Int64("bytes_total", progress.BytesTotal).
				Int64("compressed_bytes", progress.CompressedBytesProcessed).
				Int64("compressed_bytes_total", progress.CompressedBytesTotal).
				Msg("image index progress")

			reporter.report(progress)
		}
	}()

	// Create index-only clip archive from the OCI image
	err := clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
		ImageRef:         imageRef,
		LocalLayoutPath:  localLayoutPath,
		OutputPath:       outputPath,
		CheckpointMiB:    checkpointMiB,
		ProgressChan:     progressChan,
		CredProvider:     c.getCredentialProviderForImage(ctx, request.ImageId, request),
		ContentCache:     newImageContentCache(c.cacheClient, request.ImageId, "oci-layer-build"),
		ContentCacheDir:  filepath.Dir(outputPath),
		LayerIndexCache:  newImageLayerIndexCache(c.cacheClient),
		IndexConcurrency: imageLayerPrepareConcurrency,
	})

	// Close channel and wait for all progress messages to be logged
	close(progressChan)
	wg.Wait()

	if err != nil {
		return err
	}

	reporter.finish()
	return nil
}

func ociLayoutPushArgs(layoutPath, imageTag, credentials string, insecure bool) []string {
	args := []string{
		"copy",
		"--image-parallel-copies", fmt.Sprintf("%d", imageLayerPrepareConcurrency),
		"--preserve-digests",
		"--retry-times", "5",
		"--retry-delay", "1s",
	}
	if insecure {
		args = append(args, "--dest-tls-verify=false")
	}
	if credentials != "" {
		args = append(args, "--dest-creds", credentials)
	}
	return append(args, "oci:"+layoutPath+":latest", "docker://"+imageTag)
}

func (c *ImageClient) pushOCILayout(ctx context.Context, outputLogger *slog.Logger, layoutPath, imageTag, credentials string) error {
	outputLogger.Info(fmt.Sprintf("Publishing image (%d concurrent layers)...\n", imageLayerPrepareConcurrency))
	started := time.Now()
	heartbeat := newActiveOutputWriter(outputLogger)
	stopHeartbeat := startSilentOutputHeartbeat(ctx, outputLogger, started, heartbeat, "Still publishing image...")
	defer stopHeartbeat()

	var commandOutput strings.Builder
	cmd := newImageCommand(
		ctx,
		"skopeo",
		ociLayoutPushArgs(layoutPath, imageTag, credentials, c.config.ImageService.BuildRegistryInsecure),
		common.AddSkopeoEnvVars(os.Environ()),
		&commandOutput,
		&commandOutput,
	)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to publish OCI image: %w: %s", err, strings.TrimSpace(commandOutput.String()))
	}

	outputLogger.Info(fmt.Sprintf("Image published in %.1fs\n", time.Since(started).Seconds()))
	return nil
}

func (c *ImageClient) BuildAndArchiveImage(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest) error {
	startTime := time.Now()

	// Use /dev/shm (tmpfs/RAM) for buildah storage to eliminate disk I/O bottlenecks
	buildPath, err := os.MkdirTemp("/dev/shm", "buildah-")
	if err != nil {
		// Fallback to /tmp if /dev/shm is not available
		buildPath, err = os.MkdirTemp("", "")
		if err != nil {
			return err
		}
	}
	defer os.RemoveAll(buildPath)

	// Set up paths for buildah operations - use /dev/shm (ram disk) for all storage we can
	graphroot, runroot, tmpdir, cleanupGraphroot := c.setupBuildahDirs()
	if cleanupGraphroot {
		defer os.RemoveAll(graphroot)
	}
	defer os.RemoveAll(runroot)
	defer os.RemoveAll(tmpdir)

	storageDriver := buildahStorageDriver(graphroot)
	storageConf, err := c.writeStorageConf(graphroot, runroot, storageDriver)
	if err != nil {
		log.Warn().Err(err).Str("storage_driver", storageDriver).Msg("failed to write buildah storage config, falling back to vfs")
		storageDriver = "vfs"
		storageConf, err = c.writeStorageConf(graphroot, runroot, storageDriver)
		if err != nil {
			log.Warn().Err(err).Msg("failed to write vfs storage config")
		}
	}

	defer func() {
		if storageConf != "" {
			os.Remove(storageConf)
		}
	}()

	buildCtxPath, err := c.getBuildContext(ctx, buildPath, request)
	if err != nil {
		return err
	}

	imagePath := filepath.Join(buildPath, "image")
	ociPath := filepath.Join(buildPath, "oci")
	tmpBundlePath := NewPathInfo(filepath.Join(c.imageBundlePath, request.ImageId))

	defer os.RemoveAll(tmpBundlePath.Path)
	os.MkdirAll(imagePath, 0755)
	os.MkdirAll(ociPath, 0755)

	// Pre-pull base image with insecure option if necessary
	insecure := false
	sourceImage := ""
	if request.BuildOptions.SourceImage != nil {
		sourceImage = *request.BuildOptions.SourceImage
	}
	dockerfile := *request.BuildOptions.Dockerfile
	if sourceImage != "" {
		insecure = c.config.ImageService.BuildRegistryInsecure

		cachedBaseRef, cachedBase, cacheErr := c.cachedBaseImageOCIRef(ctx, outputLogger, request, sourceImage, buildPath)
		if cacheErr != nil {
			log.Debug().Err(cacheErr).Str("source_image", sourceImage).Msg("base image distributed cache unavailable")
		}
		if cachedBase {
			dockerfile = strings.ReplaceAll(dockerfile, sourceImage, cachedBaseRef)
			outputLogger.Info("Using cached base image layers\n")
		} else {
			// buildah pull the base image so bud doesn't attempt HTTPS
			pullArgs := []string{"--root", graphroot, "--runroot", runroot, "--storage-driver=" + storageDriver, "pull"}
			if insecure {
				pullArgs = append(pullArgs, "--tls-verify=false")
			}

			// Add credentials if provided
			if authArgs := c.getBuildahAuthArgs(ctx, sourceImage, request.BuildOptions.SourceImageCreds); len(authArgs) > 0 {
				pullArgs = append(pullArgs, authArgs...)
			}

			pullArgs = append(pullArgs, "docker://"+sourceImage)
			cmd := newBuildahCommand(
				ctx,
				pullArgs,
				c.buildahEnv(runroot, tmpdir, storageConf),
				&common.ExecWriter{Logger: outputLogger},
				&common.ExecWriter{Logger: outputLogger},
			)
			if err := cmd.Run(); err != nil {
				return err
			}
		}
	}

	tempDockerFile := filepath.Join(buildPath, "Dockerfile")
	f, err := os.Create(tempDockerFile)
	if err != nil {
		return err
	}

	fmt.Fprint(f, dockerfile)
	f.Close()

	budArgs := []string{"--root", graphroot, "--runroot", runroot, "--storage-driver=" + storageDriver, "bud"}
	if insecure {
		budArgs = append(budArgs, "--tls-verify=false")
	}

	budArgs = append(budArgs, "--layers")           // Enable layer caching for faster rebuilds
	budArgs = append(budArgs, "--format", "docker") // Use docker format to avoid conversion at push time
	budArgs = append(budArgs, "--jobs", "8")        // Use parallel jobs for faster layer processing

	// Add credentials for multi-stage builds and private base images
	if authArgs := c.getBuildahAuthArgs(ctx, sourceImage, request.BuildOptions.SourceImageCreds); len(authArgs) > 0 {
		budArgs = append(budArgs, authArgs...)
	}

	// Add build secrets as build arguments
	if len(request.BuildOptions.BuildSecrets) > 0 {
		for _, secret := range request.BuildOptions.BuildSecrets {
			if secret != "" {
				budArgs = append(budArgs, "--build-arg", secret)
			}
		}
	}

	// Clip v2: export once, then publish and index the same local layer blobs.
	if c.config.ImageService.ClipVersion == uint32(types.ClipVersion2) {
		archiveName := fmt.Sprintf("%s.%s.tmp", request.ImageId, c.registry.ImageFileExtension)
		archivePath := filepath.Join(tmpdir, archiveName)

		buildRegistry := c.getBuildRegistry()
		imageTag := fmt.Sprintf("%s/%s:%s", buildRegistry, c.config.ImageService.BuildRepositoryName, request.ImageId)

		// Build w/ buildah
		budArgs = append(budArgs, "-f", tempDockerFile, "-t", imageTag, buildCtxPath)
		buildOutput := newActiveOutputWriter(outputLogger)
		cmd := newBuildahCommand(ctx, budArgs, c.buildahEnv(runroot, tmpdir, storageConf), buildOutput, buildOutput)
		buildStart := time.Now()
		stopHeartbeat := startSilentOutputHeartbeat(ctx, outputLogger, buildStart, buildOutput, "Still building image...")
		err = cmd.Run()
		stopHeartbeat()
		if err != nil {
			return err
		}
		outputLogger.Info(fmt.Sprintf("Image built in %.1fs\n", time.Since(buildStart).Seconds()))

		if err := os.RemoveAll(ociPath); err != nil {
			return err
		}
		outputLogger.Info("Exporting image layers once for publication and indexing...\n")
		exportArgs := []string{
			"--root", graphroot,
			"--runroot", runroot,
			"--storage-driver=" + storageDriver,
			"push",
			"--compression-format", "gzip",
			"--compression-level", "1",
			imageTag,
			"oci:" + ociPath + ":latest",
		}
		var exportOutput strings.Builder
		exportStart := time.Now()
		exportHeartbeat := newActiveOutputWriter(outputLogger)
		stopHeartbeat = startSilentOutputHeartbeat(ctx, outputLogger, exportStart, exportHeartbeat, "Still exporting image layers...")
		cmd = newBuildahCommand(ctx, exportArgs, c.buildahEnv(runroot, tmpdir, storageConf), &exportOutput, &exportOutput)
		err = cmd.Run()
		stopHeartbeat()
		if err != nil {
			return fmt.Errorf("failed to export OCI image: %w: %s", err, strings.TrimSpace(exportOutput.String()))
		}
		outputLogger.Info(fmt.Sprintf("Image layers exported in %.1fs\n", time.Since(exportStart).Seconds()))

		buildRegistryCredentials := request.BuildRegistryCredentials
		if buildRegistryCredentials == "" {
			buildRegistryCredentials = c.gatewayRegistryCredentials(ctx, buildRegistry, request)
		}
		if buildRegistry == "localhost" || strings.HasPrefix(buildRegistry, "127.0.0.1") {
			buildRegistryCredentials = ""
		}

		c.v2ImageRefs.Set(request.ImageId, imageTag)
		log.Info().Str("image_id", request.ImageId).Str("image_tag", imageTag).Msg("cached image reference")

		group, groupCtx := errgroup.WithContext(ctx)
		group.Go(func() error {
			return c.pushOCILayout(groupCtx, outputLogger, ociPath, imageTag, buildRegistryCredentials)
		})
		group.Go(func() error {
			return c.createOCIImageWithProgress(groupCtx, outputLogger, request, imageTag, ociPath, archivePath, 2)
		})
		if err = group.Wait(); err != nil {
			return err
		}

		// Upload the clip archive to object storage
		if err = c.registry.Push(ctx, archivePath, request.ImageId); err != nil {
			return err
		}

		return nil
	}

	// Clip v1: Build, push to OCI layout, then process locally
	budArgs = append(budArgs, "-f", tempDockerFile, "-t", request.ImageId+":latest", buildCtxPath)
	cmd := newBuildahCommand(
		ctx,
		budArgs,
		c.buildahEnv(runroot, tmpdir, storageConf),
		&common.ExecWriter{Logger: outputLogger},
		&common.ExecWriter{Logger: outputLogger},
	)
	err = cmd.Run()
	if err != nil {
		return err
	}

	// Push to local OCI layout (v1 clip path)
	v1PushArgs := []string{"--root", graphroot, "--runroot", runroot, "--storage-driver=" + storageDriver, "push", request.ImageId + ":latest", "oci:" + ociPath + ":latest"}
	cmd = newBuildahCommand(
		ctx,
		v1PushArgs,
		c.buildahEnv(runroot, tmpdir, storageConf),
		&common.ExecWriter{Logger: outputLogger},
		&common.ExecWriter{Logger: outputLogger},
	)
	err = cmd.Run()
	if err != nil {
		return err
	}

	ociImageInfo, err := os.Stat(ociPath)
	if err == nil {
		ociImageMB := float64(ociImageInfo.Size()) / 1024 / 1024
		metrics.RecordImageBuildSpeed(ociImageMB, time.Since(startTime))
	} else {
		log.Warn().Err(err).Str("path", ociPath).Msg("unable to inspect image size")
	}

	engine, err := dir.Open(ociPath)
	if err != nil {
		return err
	}
	defer engine.Close()

	unpackOptions := umociUnpackOptions()
	engineExt := casext.NewEngine(engine)
	defer engineExt.Close()

	err = umoci.Unpack(engineExt, "latest", tmpBundlePath.Path, unpackOptions)
	if err != nil {
		return err
	}

	err = c.Archive(ctx, tmpBundlePath, request.ImageId, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *ImageClient) PullAndArchiveImage(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest) error {
	baseImage, err := image.ExtractImageNameAndTag(*request.BuildOptions.SourceImage)
	if err != nil {
		return err
	}

	c.cacheV2SourceImageRef(request)

	outputLogger.Info("Inspecting image name and verifying architecture...\n")
	if err := c.inspectAndVerifyImage(ctx, request); err != nil {
		return err
	}

	baseTmpBundlePath := filepath.Join(c.imageBundlePath, baseImage.Repo)
	os.MkdirAll(baseTmpBundlePath, 0755)

	copyDir := filepath.Join(imageTmpDir, baseImage.Repo)
	os.MkdirAll(copyDir, 0755)

	// Local OCI-layout reference name. Digest-pinned source images
	// (registry/repo@sha256:...) have no tag, which would yield an empty,
	// invalid reference for both the skopeo destination and the umoci unpack
	// ("refusing to resolve invalid reference"). Derive a unique, tag-safe ref
	// from the digest rather than a shared "latest": the OCI layout is shared
	// per-repo, so distinct digests of the same repo must not collide on one ref
	// (which would unpack the wrong image when builds overlap). The image is
	// still pulled by its full (digest) reference; this name only labels it
	// locally.
	ociRef := baseImage.Tag
	if ociRef == "" {
		ociRef = localOCILayoutRef(baseImage.Digest)
	}
	dest := fmt.Sprintf("oci:%s:%s", baseImage.Repo, ociRef)

	imageBytes, err := c.skopeoClient.InspectSizeInBytes(ctx, *request.BuildOptions.SourceImage, request.BuildOptions.SourceImageCreds)
	if err != nil {
		log.Warn().Err(err).Msg("unable to inspect image size")
	}
	imageSizeMB := float64(imageBytes) / 1024 / 1024

	outputLogger.Info(fmt.Sprintf("Copying image (size: %.2f MB)...\n", imageSizeMB))
	startTime := time.Now()
	err = c.skopeoClient.Copy(ctx, *request.BuildOptions.SourceImage, dest, request.BuildOptions.SourceImageCreds, outputLogger)
	if err != nil {
		return err
	}
	metrics.RecordImageCopySpeed(imageSizeMB, time.Since(startTime))

	// Clip v2: Create index-only clip archive from the source image (no unpack needed)
	if c.config.ImageService.ClipVersion == uint32(types.ClipVersion2) {
		archiveName := fmt.Sprintf("%s.%s.tmp", request.ImageId, c.registry.ImageFileExtension)
		archivePath := filepath.Join("/dev/shm", archiveName)

		// Create index-only clip from the source docker image reference with progress reporting
		if err = c.createOCIImageWithProgress(ctx, outputLogger, request, *request.BuildOptions.SourceImage, copyDir, archivePath, 2); err != nil {
			return err
		}

		// Upload the clip archive to the image registry
		if err = c.registry.Push(ctx, archivePath, request.ImageId); err != nil {
			return err
		}

		return nil
	}

	outputLogger.Info("Unpacking image...\n")
	tmpBundlePath := NewPathInfo(filepath.Join(baseTmpBundlePath, request.ImageId))
	err = c.unpack(ctx, baseImage.Repo, ociRef, tmpBundlePath)
	if err != nil {
		return fmt.Errorf("unable to unpack image: %v", err)
	}
	defer os.RemoveAll(baseTmpBundlePath)
	defer os.RemoveAll(copyDir)

	outputLogger.Info("Archiving base image...\n")
	err = c.Archive(ctx, tmpBundlePath, request.ImageId, nil)
	if err != nil {
		return err
	}

	return nil
}

// localOCILayoutRef converts an image digest (e.g. "sha256:abc...") into a
// tag-safe, content-unique reference for the local OCI layout. Because the
// layout is shared per repository, using the digest keeps distinct images
// distinct (and identical content idempotent), avoiding a shared "latest" ref
// that could unpack the wrong image when same-repo builds overlap. Falls back to
// "latest" only when no digest is available.
func localOCILayoutRef(digest string) string {
	if digest == "" {
		return "latest"
	}
	// OCI tags must match [a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}.
	ref := strings.NewReplacer(":", "-", "/", "-", "+", "-", "@", "-").Replace(digest)
	if len(ref) > 128 {
		ref = ref[:128]
	}
	return ref
}

func (c *ImageClient) unpack(ctx context.Context, baseImageName string, baseImageTag string, bundlePath *PathInfo) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	startTime := time.Now()
	unpackOptions := umociUnpackOptions()

	// Get a reference to the CAS.
	baseImagePath := filepath.Join(imageTmpDir, baseImageName)
	engine, err := dir.Open(baseImagePath)
	if err != nil {
		return errors.Wrap(err, "open CAS")
	}
	defer engine.Close()

	engineExt := casext.NewEngine(engine)
	defer engineExt.Close()

	tmpBundlePath := filepath.Join(bundlePath.Path + "_")
	err = umoci.Unpack(engineExt, baseImageTag, tmpBundlePath, unpackOptions)
	if err == nil {
		return os.Rename(tmpBundlePath, bundlePath.Path)
	}

	metrics.RecordImageUnpackSpeed(bundlePath.GetSize(), time.Since(startTime))
	return err
}

// Generate and upload archived version of the image for distribution
func (c *ImageClient) Archive(ctx context.Context, bundlePath *PathInfo, imageId string, progressChan chan int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s.tmp", imageId, c.registry.ImageFileExtension)
	archivePath := filepath.Join("/dev/shm", archiveName)

	defer func() {
		os.RemoveAll(archivePath)
	}()

	var err error = nil
	switch c.config.ImageService.RegistryStore {
	case registry.S3ImageRegistryStore:
		err = clip.CreateAndUploadArchive(ctx, clip.CreateOptions{
			InputPath:  bundlePath.Path,
			OutputPath: archivePath,
			Credentials: storage.ClipStorageCredentials{
				S3: &storage.S3ClipStorageCredentials{
					AccessKey: c.config.ImageService.Registries.S3.AccessKey,
					SecretKey: c.config.ImageService.Registries.S3.SecretKey,
				},
			},
			ProgressChan: progressChan,
			// v1 content is cached and reconciled as a single archive object on
			// first load (the embedded image-archive cache), so we intentionally
			// do not warm every individual file into the distributed cache here.
		}, &clipCommon.S3StorageInfo{
			Bucket:         c.config.ImageService.Registries.S3.BucketName,
			Region:         c.config.ImageService.Registries.S3.Region,
			Endpoint:       c.config.ImageService.Registries.S3.Endpoint,
			Key:            fmt.Sprintf("%s.clip", imageId),
			ForcePathStyle: c.config.ImageService.Registries.S3.ForcePathStyle,
		})
	case registry.LocalImageRegistryStore:
		// No per-file ContentCache: v1 is cached/reconciled as a single archive
		// object on first load, not file-by-file during the build.
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath.Path,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Error().Err(err).Msg("unable to create archive")
		return err
	}
	elapsed := time.Since(startTime)
	log.Info().Str("container_id", imageId).Dur("seconds", time.Duration(elapsed.Seconds())).Float64("size", bundlePath.GetSize()).Msg("container archive took")
	metrics.RecordImageArchiveSpeed(bundlePath.GetSize(), elapsed)

	// Push the archive to a registry
	startTime = time.Now()
	err = c.registry.Push(ctx, archivePath, imageId)
	if err != nil {
		log.Error().Str("image_id", imageId).Err(err).Msg("failed to push image")
		return err
	}

	elapsed = time.Since(startTime)
	log.Info().Str("image_id", imageId).Dur("seconds", time.Duration(elapsed.Seconds())).Float64("size", bundlePath.GetSize()).Msg("image push took")
	metrics.RecordImagePushSpeed(bundlePath.GetSize(), elapsed)
	return nil
}

func umociUnpackOptions() layer.UnpackOptions {
	var unpackOptions layer.UnpackOptions
	var meta umoci.Meta
	meta.Version = umoci.MetaVersion
	unpackOptions.KeepDirlinks = true
	unpackOptions.MapOptions = meta.MapOptions
	return unpackOptions
}

func (c *ImageClient) getBuildContext(ctx context.Context, buildPath string, request *types.ContainerRequest) (string, error) {
	if request.BuildOptions.BuildCtxObject == nil {
		return ".", nil
	}

	buildCtxPath := filepath.Join(types.DefaultExtractedObjectPath, request.Workspace.Name, *request.BuildOptions.BuildCtxObject)
	objectPath := path.Join(types.DefaultObjectPath, request.Workspace.Name, *request.BuildOptions.BuildCtxObject)

	if request.StorageAvailable() {
		buildCtxPath = path.Join(buildPath, "build-ctx")

		if !workspaceStorageDownloadAvailable(request.Workspace.Storage) {
			return "", fmt.Errorf("workspace storage credentials are required to download build context %q directly", *request.BuildOptions.BuildCtxObject)
		}

		objectPath = path.Join(buildPath, "build-ctx.zip")
		if err := downloadWorkspaceBuildContext(ctx, request, *request.BuildOptions.BuildCtxObject, objectPath); err != nil {
			return "", err
		}
	}

	err := common.ExtractObjectFile(ctx, objectPath, buildCtxPath)
	if err != nil {
		return "", err
	}

	return buildCtxPath, nil
}

func downloadWorkspaceBuildContext(ctx context.Context, request *types.ContainerRequest, objectID, destPath string) error {
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return fmt.Errorf("create workspace storage client: %w", err)
	}

	reader, err := storageClient.DownloadWithReader(ctx, path.Join(types.DefaultObjectPrefix, objectID))
	if err != nil {
		return fmt.Errorf("download workspace build context: %w", err)
	}
	defer reader.Close()

	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create local build context archive: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("write local build context archive: %w", err)
	}

	return nil
}
