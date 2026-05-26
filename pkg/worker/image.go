package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	"github.com/beam-cloud/beta9/pkg/cache"
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
)

const (
	imageBundlePath                string = "/dev/shm/images"
	imageTmpDir                    string = "/tmp"
	metricsSourceLabel                    = "image_client"
	pullLazyBackoff                       = 1000 * time.Millisecond
	embeddedImageCacheWaitTimeout         = 2 * time.Minute
	embeddedImageCacheWaitInterval        = 250 * time.Millisecond
)

var (
	baseImageCachePath string = "/images/cache"
	baseImageMountPath string = "/images/mnt/%s"
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
	workerRepoClient   pb.WorkerRepositoryServiceClient
	logger             *ContainerLogger
	eventRepo          repo.EventRepository
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

func NewImageClient(config types.AppConfig, workerId string, workerRepoClient pb.WorkerRepositoryServiceClient, fileCacheManager *FileCacheManager) (*ImageClient, error) {
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
		logger: &ContainerLogger{
			logLinesPerHour: config.Worker.ContainerLogLinesPerHour,
		},
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
	if !mounted {
		return false
	}

	mountPoint := c.imageMountPoint(imageId)
	if _, err := os.Stat(mountPoint); err == nil {
		return true
	}

	c.mountedFuseServers.Delete(imageId)
	return false
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

func (c *ImageClient) PullLazy(ctx context.Context, request *types.ContainerRequest) (time.Duration, error) {
	startTime := time.Now()
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

type lazyImageArchive struct {
	path           string
	sourceRegistry *types.S3ImageRegistryConfig
	storageMode    string
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
	}
	if archive.usesOCIStorage() {
		log.Info().Str("image_id", request.ImageId).Str("storage_type", archive.storageMode).Msg("detected CLIP OCI image")
	}

	return archive, nil
}

func (c *ImageClient) localArchivePath(imageId string) string {
	return fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
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
	contentCache := newImageContentCache(c.cacheClient, request.ImageId, cacheKind)
	mountOptions := clip.MountOptions{
		ArchivePath:           archive.path,
		MountPoint:            c.imageMountPoint(request.ImageId),
		CachePath:             c.contentCachePath(request, archive),
		ContentCache:          contentCache,
		ContentCacheAvailable: contentCache != nil,
		ReadTraceObserver:     c.observeClipRead,
	}

	if archive.usesOCIStorage() {
		mountOptions.RegistryCredProvider = c.getCredentialProviderForImage(ctx, request.ImageId, request)
		return mountOptions
	}

	if archive.sourceRegistry != nil {
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

	if c.config.ImageService.LocalCacheEnabled || strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix) {
		return fmt.Sprintf("%s/%s.cache", c.imageCachePath, request.ImageId)
	}

	return ""
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
	archivePath := fmt.Sprintf("/images/%s.%s", imageId, reg.LocalImageFileExtension)

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

// getCredentialProviderForImage determines the appropriate credentials for an image
// Priority: runtime credentials > build registry credentials > source image credentials > ambient auth
func (c *ImageClient) getCredentialProviderForImage(ctx context.Context, imageId string, request *types.ContainerRequest) clipCommon.RegistryCredentialProvider {
	sourceRef, hasRef := c.v2ImageRefs.Get(imageId)
	if !hasRef {
		return nil
	}

	registry := reg.ParseRegistry(sourceRef)
	if registry == "" {
		return nil
	}

	// Priority 1: Runtime credentials (from secret)
	if request.ImageCredentials != "" {
		return c.parseAndCreateProvider(ctx, request.ImageCredentials, registry, imageId, "runtime secret")
	}

	// Priority 2: Build registry credentials (for images we built and pushed)
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

	// Priority 3: Source image credentials (for external images pulled directly without building)
	if request.BuildOptions.SourceImageCreds != "" {
		return c.parseAndCreateProvider(ctx, request.BuildOptions.SourceImageCreds, registry, imageId, "source image")
	}

	// Priority 4: Ambient auth (IAM role, docker config, etc.)
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

	lockPath := archivePath + ".lock"
	lockFile, err := openImageLockFile(lockPath)
	if err != nil {
		return nil, err
	}
	defer lockFile.Close()

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return nil, err
	}

	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	// Check if file exists now that we have the lock (another worker may have downloaded it)
	if _, err := os.Stat(archivePath); err == nil {
		return &sourceRegistry, nil
	}

	if ok, err := c.pullImageArchiveFromEmbeddedCache(ctx, archivePath, request); ok {
		return &sourceRegistry, nil
	} else if err != nil {
		log.Warn().Err(err).Str("image_id", imageId).Msg("embedded image archive cache unavailable, falling back to registry")
	}

	// Download to temp file, then atomically rename
	tempPath := archivePath + ".tmp"
	defer os.Remove(tempPath)

	if err = c.registry.Pull(ctx, tempPath, imageId); err != nil {
		if c.config.ImageService.RegistryStore == registry.LocalImageRegistryStore {
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
			log.Error().Err(err).Str("image_id", imageId).Msg("failed to pull image from registry")
			return nil, err
		}
	}

	if err := os.Rename(tempPath, archivePath); err != nil {
		return nil, err
	}

	return &sourceRegistry, nil
}

func (c *ImageClient) pullImageArchiveFromEmbeddedCache(ctx context.Context, archivePath string, request *types.ContainerRequest) (bool, error) {
	imageId := request.ImageId
	if c.cacheClient == nil {
		return false, nil
	}

	metadataStart := time.Now()
	if ok, err := c.copyImageArchiveFromContentCacheMetadata(ctx, archivePath, imageId); ok {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheMetadata, metadataStart, time.Since(metadataStart), true, nil)
		return true, nil
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
	if c.config.ImageService.RegistryStore == registry.S3ImageRegistryStore {
		sourceRegistry := c.registry.Registry()
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
		sourcePath := filepath.Join("/images", key)
		info, statErr := os.Stat(sourcePath)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return false, nil
			}
			return false, statErr
		}
		if info.IsDir() {
			return false, fmt.Errorf("image archive source is a directory: %s", sourcePath)
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
				return true, nil
			} else if waitErr != nil {
				c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheWait, waitStart, time.Since(waitStart), false, map[string]string{
					"reason": "store_lock_contended",
				})
				return false, waitErr
			}
		}
		return false, err
	}
	c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheStore, storeStart, time.Since(storeStart), true, map[string]string{
		"registry_store": c.config.ImageService.RegistryStore,
	})

	restoreStart := time.Now()
	size, err := c.registry.Size(ctx, imageId)
	if err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheRestore, restoreStart, time.Since(restoreStart), false, nil)
		return false, err
	}
	if err := c.writeImageArchiveFromContentCache(ctx, archivePath, imageId, hash, size, routingKey); err != nil {
		c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheRestore, restoreStart, time.Since(restoreStart), false, map[string]string{"size_bytes": fmt.Sprintf("%d", size)})
		return false, err
	}
	c.recordImageLifecycle(request, types.ContainerLifecycleImageEmbeddedCacheRestore, restoreStart, time.Since(restoreStart), true, map[string]string{"size_bytes": fmt.Sprintf("%d", size)})

	return true, nil
}

func (c *ImageClient) waitForImageArchiveContentCache(ctx context.Context, archivePath, imageId string) (bool, error) {
	waitCtx, cancel := context.WithTimeout(ctx, embeddedImageCacheWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(embeddedImageCacheWaitInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		ok, err := c.copyImageArchiveFromContentCacheMetadata(waitCtx, archivePath, imageId)
		if ok {
			return true, nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-waitCtx.Done():
			if lastErr != nil {
				return false, lastErr
			}
			return false, waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (c *ImageClient) copyImageArchiveFromContentCacheMetadata(ctx context.Context, archivePath, imageId string) (bool, error) {
	cachePath := c.imageArchiveCachePath(imageId)
	metadata, err := c.cacheClient.CacheFSMetadata(ctx, cachePath)
	if err != nil {
		var notFound *cache.ErrNodeNotFound
		if errors.As(err, &notFound) {
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

	if err := c.writeImageArchiveFromContentCache(ctx, archivePath, imageId, metadata.Hash, int64(metadata.Size), cachePath); err != nil {
		return false, err
	}
	return true, nil
}

func (c *ImageClient) imageArchiveCachePath(imageId string) string {
	return fmt.Sprintf("/images/%s.%s", imageId, c.registry.ImageFileExtension)
}

func (c *ImageClient) writeImageArchiveFromContentCache(ctx context.Context, archivePath, imageId, hash string, size int64, routingKey string) error {
	tmpPath := archivePath + ".tmp"
	defer os.Remove(tmpPath)
	if routingKey == "" {
		routingKey = hash
	}

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

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
	if c.config.ImageService.ClipVersion != uint32(types.ClipVersion2) {
		return nil
	}

	const maxExpectedV2ArchiveSize = int64(128 * 1024 * 1024)
	if size > maxExpectedV2ArchiveSize {
		return fmt.Errorf("restored v2 image archive is unexpectedly large: image_id=%s size=%d", imageId, size)
	}

	archiver := clip.NewClipArchiver()
	meta, err := archiver.ExtractMetadata(archivePath)
	if err != nil {
		return fmt.Errorf("restored v2 image archive metadata invalid: image_id=%s: %w", imageId, err)
	}

	ociInfo, ok := ociStorageInfo(meta)
	if !ok || strings.ToLower(ociInfo.Type()) != string(clipCommon.StorageModeOCI) {
		return fmt.Errorf("restored v2 image archive is not an OCI metadata archive: image_id=%s", imageId)
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

// setupBuildahDirs creates and returns optimal paths for buildah operations using /dev/shm
// All paths use tmpfs (/dev/shm) for fast I/O and to avoid slow disk bottlenecks
func (c *ImageClient) setupBuildahDirs() (graphroot, runroot, tmpdir string) {
	// Use /dev/shm for all paths - tmpfs is fast and overlay can work here for builds
	graphroot = filepath.Join("/dev/shm", "buildah-storage")
	runroot = filepath.Join("/dev/shm", "buildah-run")
	tmpdir = filepath.Join("/dev/shm", "buildah-tmp")

	// Create directories with proper permissions
	_ = os.MkdirAll(graphroot, 0o700)
	_ = os.MkdirAll(runroot, 0o700)
	_ = os.MkdirAll(tmpdir, 0o700)

	return
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

// getBuildRegistryAuthArgs returns buildah authentication arguments for pushing to build registry
func (c *ImageClient) getBuildRegistryAuthArgs(buildRegistry string, buildRegistryCredentials string) []string {
	// For localhost, no auth needed
	if buildRegistry == "localhost" || strings.HasPrefix(buildRegistry, "127.0.0.1") {
		return nil
	}

	// Use explicit credentials from BuildOptions if provided (generated fresh in scheduler)
	if buildRegistryCredentials != "" {
		log.Info().Str("registry", buildRegistry).Msg("using build registry credentials from request")
		return []string{"--creds", buildRegistryCredentials}
	}

	// Fall back to ambient credentials (IAM role, service account, docker config)
	log.Info().Str("registry", buildRegistry).Msg("using ambient credentials for build registry")
	return nil
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

func (c *ImageClient) createOCIImageWithProgress(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, imageRef, outputPath string, checkpointMiB int64) error {
	outputLogger.Info("Indexing image...\n")
	progressChan := make(chan clip.OCIIndexProgress, 100)

	var wg sync.WaitGroup
	wg.Add(1)

	// Process progress updates in goroutine
	go func() {
		defer wg.Done()
		for progress := range progressChan {
			percent := float64(progress.LayerIndex) / float64(progress.TotalLayers) * 100

			switch progress.Stage {
			case "starting":
				log.Info().
					Str("container_id", request.ContainerId).
					Int("layer", progress.LayerIndex).
					Int("total", progress.TotalLayers).
					Msgf("Indexing layer %d/%d (%.0f%%)", progress.LayerIndex, progress.TotalLayers, percent)

				outputLogger.Info(fmt.Sprintf("Indexing layer %d/%d (%.0f%%)\n",
					progress.LayerIndex, progress.TotalLayers, percent))

			case "completed":
				log.Info().
					Str("container_id", request.ContainerId).
					Int("layer", progress.LayerIndex).
					Int("total", progress.TotalLayers).
					Int("files", progress.FilesIndexed).
					Msgf("Completed layer %d/%d (%.0f%%, %d files indexed)", progress.LayerIndex, progress.TotalLayers, percent, progress.FilesIndexed)

				outputLogger.Info(fmt.Sprintf("Completed layer %d/%d (%.0f%%, %d files indexed)\n",
					progress.LayerIndex, progress.TotalLayers, percent, progress.FilesIndexed))

			default:
				log.Info().
					Str("container_id", request.ContainerId).
					Str("stage", progress.Stage).
					Int("layer", progress.LayerIndex).
					Int("total", progress.TotalLayers).
					Msgf("Index progress [%s]: layer %d/%d", progress.Stage, progress.LayerIndex, progress.TotalLayers)

				outputLogger.Info(fmt.Sprintf("Index progress [%s]: layer %d/%d\n",
					progress.Stage, progress.LayerIndex, progress.TotalLayers))
			}
		}
	}()

	// Create index-only clip archive from the OCI image
	err := clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
		ImageRef:        imageRef,
		OutputPath:      outputPath,
		CheckpointMiB:   checkpointMiB,
		ProgressChan:    progressChan,
		CredProvider:    c.getCredentialProviderForImage(ctx, request.ImageId, request),
		ContentCache:    newImageContentCache(c.cacheClient, request.ImageId, "oci-layer-build"),
		ContentCacheDir: filepath.Dir(outputPath),
	})

	// Close channel and wait for all progress messages to be logged
	close(progressChan)
	wg.Wait()

	if err != nil {
		return err
	}

	outputLogger.Info("Image indexing completed successfully\n")
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
	graphroot, runroot, tmpdir := c.setupBuildahDirs()
	defer os.RemoveAll(graphroot)
	defer os.RemoveAll(runroot)
	defer os.RemoveAll(tmpdir)

	storageDriver := "overlay"
	storageConf, err := c.writeStorageConf(graphroot, runroot, storageDriver)
	if err != nil {
		log.Warn().Err(err).Msg("failed to write overlay storage config, falling back to vfs")
		storageDriver = "vfs"
		// Write vfs config for fallback case
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

	buildCtxPath, err := c.getBuildContext(buildPath, request)
	if err != nil {
		return err
	}

	tempDockerFile := filepath.Join(buildPath, "Dockerfile")
	f, err := os.Create(tempDockerFile)
	if err != nil {
		return err
	}

	fmt.Fprintf(f, *request.BuildOptions.Dockerfile)
	f.Close()

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

	if sourceImage != "" {
		insecure = c.config.ImageService.BuildRegistryInsecure

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
		cmd := exec.CommandContext(ctx, "buildah", pullArgs...)
		cmd.Env = c.buildahEnv(runroot, tmpdir, storageConf)
		cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
		cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
		if err := cmd.Run(); err != nil {
			return err
		}
	}

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

	// Clip v2: Build and push directly to registry, skip OCI layout
	if c.config.ImageService.ClipVersion == uint32(types.ClipVersion2) {
		archiveName := fmt.Sprintf("%s.%s.tmp", request.ImageId, c.registry.ImageFileExtension)
		archivePath := filepath.Join(tmpdir, archiveName)

		buildRegistry := c.getBuildRegistry()
		imageTag := fmt.Sprintf("%s/%s:%s", buildRegistry, c.config.ImageService.BuildRepositoryName, request.ImageId)

		// Build w/ buildah
		budArgs = append(budArgs, "-f", tempDockerFile, "-t", imageTag, buildCtxPath)
		cmd := exec.CommandContext(ctx, "buildah", budArgs...)
		cmd.Env = c.buildahEnv(runroot, tmpdir, storageConf)
		cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
		cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
		if err = cmd.Run(); err != nil {
			return err
		}

		outputLogger.Info(fmt.Sprintf("Pushing image to registry: %s\n", imageTag))

		pushArgs := []string{"--root", graphroot, "--runroot", runroot, "--storage-driver=" + storageDriver, "push"}

		if c.config.ImageService.BuildRegistryInsecure {
			pushArgs = append(pushArgs, "--tls-verify=false")
		}

		pushArgs = append(pushArgs, "--compression-format", "gzip", "--compression-level", "1")
		pushArgs = append(pushArgs, "--retry", "5")
		pushArgs = append(pushArgs, "--retry-delay", "1s")

		if authArgs := c.getBuildRegistryAuthArgs(buildRegistry, request.BuildRegistryCredentials); len(authArgs) > 0 {
			pushArgs = append(pushArgs, authArgs...)
		}

		pushArgs = append(pushArgs, imageTag, "docker://"+imageTag)

		cmd = exec.CommandContext(ctx, "buildah", pushArgs...)
		cmd.Env = c.buildahEnv(runroot, tmpdir, storageConf)
		cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
		cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
		if err = cmd.Run(); err != nil {
			return err
		}

		c.v2ImageRefs.Set(request.ImageId, imageTag)
		log.Info().Str("image_id", request.ImageId).Str("image_tag", imageTag).Msg("cached image reference")

		// Create the image index (CLIP archive)
		if err = c.createOCIImageWithProgress(ctx, outputLogger, request, imageTag, archivePath, 2); err != nil {
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
	cmd := exec.CommandContext(ctx, "buildah", budArgs...)
	cmd.Env = c.buildahEnv(runroot, tmpdir, storageConf)
	cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
	cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
	err = cmd.Run()
	if err != nil {
		return err
	}

	// Push to local OCI layout (v1 clip path)
	v1PushArgs := []string{"--root", graphroot, "--runroot", runroot, "--storage-driver=" + storageDriver, "push", request.ImageId + ":latest", "oci:" + ociPath + ":latest"}
	cmd = exec.CommandContext(ctx, "buildah", v1PushArgs...)
	cmd.Env = c.buildahEnv(runroot, tmpdir, storageConf)
	cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
	cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
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

	dest := fmt.Sprintf("oci:%s:%s", baseImage.Repo, baseImage.Tag)

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
		if err = c.createOCIImageWithProgress(ctx, outputLogger, request, *request.BuildOptions.SourceImage, archivePath, 2); err != nil {
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
	err = c.unpack(ctx, baseImage.Repo, baseImage.Tag, tmpBundlePath)
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
			ContentCache: newImageContentCache(c.cacheClient, imageId, "legacy-file-build"),
		}, &clipCommon.S3StorageInfo{
			Bucket:         c.config.ImageService.Registries.S3.BucketName,
			Region:         c.config.ImageService.Registries.S3.Region,
			Endpoint:       c.config.ImageService.Registries.S3.Endpoint,
			Key:            fmt.Sprintf("%s.clip", imageId),
			ForcePathStyle: c.config.ImageService.Registries.S3.ForcePathStyle,
		})
	case registry.LocalImageRegistryStore:
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:    bundlePath.Path,
			OutputPath:   archivePath,
			ContentCache: newImageContentCache(c.cacheClient, imageId, "legacy-file-build"),
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

func (c *ImageClient) getBuildContext(buildPath string, request *types.ContainerRequest) (string, error) {
	if request.BuildOptions.BuildCtxObject == nil {
		return ".", nil
	}

	buildCtxPath := filepath.Join(types.DefaultExtractedObjectPath, request.Workspace.Name, *request.BuildOptions.BuildCtxObject)
	objectPath := path.Join(types.DefaultObjectPath, request.Workspace.Name, *request.BuildOptions.BuildCtxObject)

	if request.StorageAvailable() {
		// Overwrite the path if workspace storage is available
		objectPath = path.Join(c.config.Storage.WorkspaceStorage.BaseMountPath, request.Workspace.Name, "objects", *request.BuildOptions.BuildCtxObject)
		buildCtxPath = path.Join(buildPath, "build-ctx")
	}

	err := common.ExtractObjectFile(context.TODO(), objectPath, buildCtxPath)
	if err != nil {
		return "", err
	}

	return buildCtxPath, nil
}
