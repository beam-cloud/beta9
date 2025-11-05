package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/registry"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/beam-cloud/clip/pkg/storage"
	"github.com/cenkalti/backoff/v4"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/cas/dir"
	"github.com/opencontainers/umoci/oci/casext"
	"github.com/opencontainers/umoci/oci/layer"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	imageBundlePath    string = "/dev/shm/images"
	imageTmpDir        string = "/tmp"
	metricsSourceLabel        = "image_client"
	pullLazyBackoff           = 1000 * time.Millisecond
)

var (
	baseImageCachePath string = "/images/cache"
	baseImageMountPath string = "/images/mnt/%s"
)

func getImageCachePath() string {
	path := baseImageCachePath

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0755)
	}

	return path
}

func getImageMountPath(workerId string) string {
	path := fmt.Sprintf(baseImageMountPath, workerId)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0755)
	}

	return path
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
	cacheClient        *blobcache.BlobCacheClient
	imageCachePath     string
	imageMountPath     string
	imageBundlePath    string
	mountedFuseServers *common.SafeMap[*fuse.Server]
	skopeoClient       common.SkopeoClient
	config             types.AppConfig
	workerId           string
	workerRepoClient   pb.WorkerRepositoryServiceClient
	logger             *ContainerLogger
	// Cache source image references for v2 images (imageId -> sourceImageRef)
	v2ImageRefs *common.SafeMap[string]
}

func NewImageClient(config types.AppConfig, workerId string, workerRepoClient pb.WorkerRepositoryServiceClient, fileCacheManager *FileCacheManager) (*ImageClient, error) {
	registry, err := reg.NewImageRegistry(config, config.ImageService.Registries.S3)
	if err != nil {
		return nil, err
	}

	c := &ImageClient{
		config:             config,
		registry:           registry,
		cacheClient:        fileCacheManager.GetClient(),
		imageBundlePath:    imageBundlePath,
		imageCachePath:     getImageCachePath(),
		imageMountPath:     getImageMountPath(workerId),
		workerId:           workerId,
		workerRepoClient:   workerRepoClient,
		skopeoClient:       common.NewSkopeoClient(config),
		mountedFuseServers: common.NewSafeMap[*fuse.Server](),
		v2ImageRefs:        common.NewSafeMap[string](),
		logger: &ContainerLogger{
			logLinesPerHour: config.Worker.ContainerLogLinesPerHour,
		},
	}

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

func (c *ImageClient) PullLazy(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) (time.Duration, error) {
	imageId := request.ImageId
	isBuildContainer := strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix)

	localCachePath := c.imageCachePath
	if !c.config.ImageService.LocalCacheEnabled && !isBuildContainer {
		localCachePath = ""
	}

	// If we have a valid cache client, attempt to cache entirety of the image
	// in memory (in a nearby region). If a remote cache is available, this supercedes
	// the local cache - which is basically just downloading the image to disk
	startTime := time.Now()

	if c.cacheClient != nil && !isBuildContainer {
		sourcePath := fmt.Sprintf("/images/%s.clip", imageId)

		// Create constant backoff
		b := backoff.NewConstantBackOff(pullLazyBackoff)

		operation := func() error {
			baseBlobFsContentPath := fmt.Sprintf("%s/%s", baseFileCachePath, sourcePath)
			if _, err := os.Stat(baseBlobFsContentPath); err == nil && c.cacheClient.IsPathCachedNearby(ctx, sourcePath) {
				localCachePath = baseBlobFsContentPath
				return nil
			}

			if !c.cacheClient.HostsAvailable() {
				return nil
			}

			pullStartTime := time.Now()

			_, err := c.cacheClient.StoreContentFromFUSE(struct {
				Path string
			}{
				Path: sourcePath,
			}, struct {
				RoutingKey string
				Lock       bool
			}{
				RoutingKey: sourcePath,
				Lock:       true,
			})
			if err != nil {
				if err == blobcache.ErrUnableToAcquireLock {
					log.Error().Str("image_id", imageId).Msg("unable to acquire lock on image, retrying...")
					return err
				}

				outputLogger.Error(fmt.Sprintf("Failed to cache image in worker's region <%s>: %v\n", imageId, err))
				return backoff.Permanent(err)
			}

			localCachePath = baseBlobFsContentPath
			outputLogger.Info(fmt.Sprintf("Image <%s> cached in worker region\n", imageId))
			metrics.RecordImagePullTime(time.Since(pullStartTime))
			return nil
		}

		// Run with context
		err := backoff.RetryNotify(operation, backoff.WithContext(b, ctx),
			func(err error, d time.Duration) {
				log.Info().Str("image_id", imageId).Err(err).Msg("retrying cache attempt")
			})
		if err != nil {
			outputLogger.Info(fmt.Sprintf("Giving up on caching image <%s>: %v\n", imageId, err))
		}
	}

	elapsed := time.Since(startTime)
	// Always fetch the remote archive into the cache directory first
	downloadPath := fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
	_ = os.MkdirAll(filepath.Dir(downloadPath), 0755)

	sourceRegistry, err := c.pullImageFromRegistry(ctx, downloadPath, imageId)
	if err != nil {
		return elapsed, err
	}

	// Extract metadata and determine the mount archive path
	mountArchivePath, meta := c.processPulledArchive(downloadPath, imageId)

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           mountArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", c.imageMountPath, imageId),
		CachePath:             localCachePath,
		ContentCache:          c.cacheClient,
		ContentCacheAvailable: c.cacheClient != nil,
	}

	// Do not persist or rely on an initial spec file for v2; base runc config will be used instead
	// Default to legacy S3 storage if we cannot detect OCI
	storageType := ""
	if meta != nil && meta.StorageInfo != nil {
		if t, ok := meta.StorageInfo.(interface{ Type() string }); ok {
			storageType = t.Type()
		}
	}

	if strings.ToLower(storageType) == string(clipCommon.StorageModeOCI) {
		// v2: ClipFS reads embedded OCI storage info from archive
		mountOptions.StorageInfo = nil
		mountOptions.RegistryCredProvider = c.getCredentialProviderForImage(ctx, imageId, request)
	} else {
		// v1 (legacy S3 data-carrying)
		mountOptions.Credentials = storage.ClipStorageCredentials{
			S3: &storage.S3ClipStorageCredentials{
				AccessKey: sourceRegistry.AccessKey,
				SecretKey: sourceRegistry.SecretKey,
			},
		}

		mountOptions.StorageInfo = &clipCommon.S3StorageInfo{
			Bucket:         sourceRegistry.BucketName,
			Region:         sourceRegistry.Region,
			Endpoint:       sourceRegistry.Endpoint,
			Key:            fmt.Sprintf("%s.%s", imageId, reg.LocalImageFileExtension),
			ForcePathStyle: sourceRegistry.ForcePathStyle,
		}
	}

	// Check if a fuse server exists for this imageId
	_, mounted := c.mountedFuseServers.Get(imageId)
	if mounted {
		return elapsed, nil
	}

	// Get lock on image mount
	lockResponse, err := handleGRPCResponse(c.workerRepoClient.SetImagePullLock(context.Background(), &pb.SetImagePullLockRequest{
		WorkerId: c.workerId,
		ImageId:  imageId,
	}))
	if err != nil {
		return elapsed, err
	}
	defer handleGRPCResponse(c.workerRepoClient.RemoveImagePullLock(context.Background(), &pb.RemoveImagePullLockRequest{
		WorkerId: c.workerId,
		ImageId:  imageId,
		Token:    lockResponse.Token,
	}))

	startServer, _, server, err := clip.MountArchive(*mountOptions)
	if err != nil {
		return elapsed, err
	}

	err = startServer()
	if err != nil {
		return elapsed, err
	}

	c.mountedFuseServers.Set(imageId, server)
	return elapsed, nil
}

// processPulledArchive extracts metadata and moves v2 OCI archives to canonical location
func (c *ImageClient) processPulledArchive(downloadPath, imageId string) (string, *clipCommon.ClipArchiveMetadata) {
	// Extract metadata from the archive
	archiver := clip.NewClipArchiver()
	meta, _ := archiver.ExtractMetadata(downloadPath)

	// Check if this is an OCI v2 image
	isOCI := false
	if meta != nil {
		if t, ok := meta.StorageInfo.(interface{ Type() string }); ok {
			isOCI = t.Type() == string(clipCommon.StorageModeOCI) || strings.ToLower(t.Type()) == "oci"
		}
	}

	// For non-OCI images, use download path as-is
	if !isOCI {
		return downloadPath, meta
	}

	// For OCI images, move to canonical location
	canonicalPath := fmt.Sprintf("/images/%s.%s", imageId, reg.LocalImageFileExtension)
	_ = os.MkdirAll(filepath.Dir(canonicalPath), 0755)

	if downloadPath != canonicalPath {
		// Move file to canonical location (with fallback to copy+remove)
		if err := os.Rename(downloadPath, canonicalPath); err != nil {
			// Fallback: copy then remove
			if in, e1 := os.Open(downloadPath); e1 == nil {
				defer in.Close()
				if out, e2 := os.Create(canonicalPath); e2 == nil {
					if _, e3 := io.Copy(out, in); e3 == nil {
						out.Close()
						_ = os.Remove(downloadPath)
					} else {
						out.Close()
					}
				}
			}
		}
	}

	// Cache OCI metadata for later use
	c.cacheOCIMetadata(imageId, meta)

	return canonicalPath, meta
}

// cacheOCIMetadata extracts and caches OCI image metadata
func (c *ImageClient) cacheOCIMetadata(imageId string, meta *clipCommon.ClipArchiveMetadata) {
	if meta == nil {
		return
	}

	ociInfo, ok := meta.StorageInfo.(clipCommon.OCIStorageInfo)
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
	if meta != nil && meta.StorageInfo != nil {
		if ociInfo, ok := meta.StorageInfo.(clipCommon.OCIStorageInfo); ok && ociInfo.ImageMetadata != nil {
			return ociInfo.ImageMetadata, true
		}
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
	c.mountedFuseServers.Range(func(imageId string, server *fuse.Server) bool {
		log.Info().Str("image_id", imageId).Msg("un-mounting image")
		server.Unmount()
		return true // Continue iteration
	})

	log.Info().Str("path", c.imageCachePath).Msg("cleaning up blobfs image cache")
	if c.config.BlobCache.Client.BlobFs.Enabled && c.cacheClient != nil {
		err := c.cacheClient.Cleanup()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ImageClient) pullImageFromRegistry(ctx context.Context, archivePath string, imageId string) (*types.S3ImageRegistryConfig, error) {
	sourceRegistry := c.config.ImageService.Registries.S3

	if _, err := os.Stat(archivePath); err != nil {
		// First try pulling from the configured store
		if err = c.registry.Pull(ctx, archivePath, imageId); err != nil {
			// If local store failed (object missing), attempt to copy from S3 then retry
			if c.config.ImageService.RegistryStore == registry.LocalImageRegistryStore {
				if s3Registry, e2 := registry.NewImageRegistry(c.config, c.config.ImageService.Registries.S3); e2 == nil {
					_ = c.registry.CopyImageFromRegistry(ctx, imageId, s3Registry)
					// Retry pull after copy
					if err2 := c.registry.Pull(ctx, archivePath, imageId); err2 == nil {
						return &sourceRegistry, nil
					} else {
						err = err2
					}
				}
			}

			log.Error().Err(err).Str("image_id", imageId).Msg("failed to pull image from registry")
			return nil, err
		}

		return &sourceRegistry, nil
	}

	return &sourceRegistry, nil
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

// buildahEnv returns environment variables for buildah to use fast storage paths
func (c *ImageClient) buildahEnv(runroot, tmpdir, storageConf string) []string {
	env := append(os.Environ(),
		"TMPDIR="+tmpdir,
		"XDG_RUNTIME_DIR="+runroot,
		"CONTAINERS_STORAGE_CONF="+storageConf,
		"BUILDAH_LAYERS=true",
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
		ImageRef:      imageRef,
		OutputPath:    outputPath,
		CheckpointMiB: checkpointMiB,
		ProgressChan:  progressChan,
		CredProvider:  c.getCredentialProviderForImage(ctx, request.ImageId, request),
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

		// Get build registry and construct tag
		buildRegistry := c.getBuildRegistry()
		imageTag := fmt.Sprintf("%s/%s:%s", buildRegistry, c.config.ImageService.BuildRepositoryName, request.ImageId)

		// Build with final tag directly
		budArgs = append(budArgs, "-f", tempDockerFile, "-t", imageTag, buildCtxPath)
		cmd := exec.CommandContext(ctx, "buildah", budArgs...)
		cmd.Env = c.buildahEnv(runroot, tmpdir, storageConf)
		cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
		cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
		if err = cmd.Run(); err != nil {
			return err
		}

		log.Warn().Str("image_id", request.ImageId).Msg("image build complete, pushing to registry")

		// Use skopeo copy instead of buildah push for better internal blob upload performance
		// Skopeo has superior parallelism and chunking for registry operations
		outputLogger.Info(fmt.Sprintf("Pushing image to registry: %s\n", imageTag))

		skopeoArgs := []string{"copy"}
		skopeoArgs = append(skopeoArgs, "--dest-compress=false")

		// Disable TLS verification if needed
		if c.config.ImageService.BuildRegistryInsecure {
			skopeoArgs = append(skopeoArgs, "--src-tls-verify=false", "--dest-tls-verify=false")
		}

		// Add destination credentials
		if authArgs := c.getBuildRegistryAuthArgs(buildRegistry, request.BuildRegistryCredentials); len(authArgs) > 0 {
			// Convert --creds to --dest-creds for skopeo
			for i, arg := range authArgs {
				if arg == "--creds" && i+1 < len(authArgs) {
					skopeoArgs = append(skopeoArgs, "--dest-creds", authArgs[i+1])
					break
				}
			}
		}

		// Source: containers-storage (buildah's storage)
		// Use default store - storage driver is configured via CONTAINERS_STORAGE_CONF env var
		srcRef := fmt.Sprintf("containers-storage:%s", imageTag)
		destRef := fmt.Sprintf("docker://%s", imageTag)
		skopeoArgs = append(skopeoArgs, srcRef, destRef)

		cmd = exec.CommandContext(ctx, "skopeo", skopeoArgs...)
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
		}, &clipCommon.S3StorageInfo{
			Bucket:         c.config.ImageService.Registries.S3.BucketName,
			Region:         c.config.ImageService.Registries.S3.Region,
			Endpoint:       c.config.ImageService.Registries.S3.Endpoint,
			Key:            fmt.Sprintf("%s.clip", imageId),
			ForcePathStyle: c.config.ImageService.Registries.S3.ForcePathStyle,
		})
	case registry.LocalImageRegistryStore:
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
