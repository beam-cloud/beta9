package worker

import (
	"context"
	"encoding/json"
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
	"github.com/beam-cloud/beta9/pkg/oci"
	"github.com/beam-cloud/beta9/pkg/registry"
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
	registry           *registry.ImageRegistry
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
	registry, err := registry.NewImageRegistry(config, config.ImageService.Registries.S3)
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
	// Ensure parent dir exists
	_ = os.MkdirAll(filepath.Dir(downloadPath), 0755)

	sourceRegistry, err := c.pullImageFromRegistry(ctx, downloadPath, imageId)
	if err != nil {
		return elapsed, err
	}

	// Detect storage type (v1 S3 data-carrying vs v2 OCI index-only) from the archive metadata
	archiver := clip.NewClipArchiver()
	meta, _ := archiver.ExtractMetadata(downloadPath)

	// Decide final mount archive path:
	// - v2 (oci): place a single canonical index file at /images/<id>.clip and use that for mount,
	//             preserving only one on-disk artifact for v2
	// - v1 (s3/local): mount the downloaded file as-is from the cache to avoid mixing artifacts
	mountArchivePath := downloadPath
	if meta != nil {
		if t, ok := meta.StorageInfo.(interface{ Type() string }); ok && (t.Type() == string(clipCommon.StorageModeOCI) || strings.ToLower(t.Type()) == "oci") {
			canonicalIndexPath := fmt.Sprintf("/images/%s.%s", imageId, registry.LocalImageFileExtension)
			_ = os.MkdirAll(filepath.Dir(canonicalIndexPath), 0755)
			if downloadPath != canonicalIndexPath {
				// Move the downloaded file to the canonical clip index path
				if err := os.Rename(downloadPath, canonicalIndexPath); err != nil {
					// Fallback: copy then remove
					if in, e1 := os.Open(downloadPath); e1 == nil {
						defer in.Close()
						if out, e2 := os.Create(canonicalIndexPath); e2 == nil {
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
			mountArchivePath = canonicalIndexPath

			// Extract and cache the OCI image reference for later use (for deriving spec in non-build containers)
			if ociInfo, ok := meta.StorageInfo.(clipCommon.OCIStorageInfo); ok {
				if ociInfo.RegistryURL == "" {
					ociInfo.RegistryURL = "https://docker.io"
				}
				// Build the full image reference from OCI storage info
				if ociInfo.Repository != "" && ociInfo.Reference != "" {
					// Convert registry URL to registry host (strip https://)
					registryHost := strings.TrimPrefix(ociInfo.RegistryURL, "https://")
					registryHost = strings.TrimPrefix(registryHost, "http://")
					sourceRef := fmt.Sprintf("%s/%s:%s", registryHost, ociInfo.Repository, ociInfo.Reference)
					c.v2ImageRefs.Set(imageId, sourceRef)
					log.Info().Str("image_id", imageId).Str("source_image", sourceRef).Msg("cached source image reference from clip metadata")
				}
			}
		}
	}

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           mountArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", c.imageMountPath, imageId),
		CachePath:             localCachePath,
		ContentCache:          c.cacheClient,
		ContentCacheAvailable: c.cacheClient != nil,
		UseCheckpoints:        true,
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
		// v2 (OCI index-only): ClipFS will read embedded OCI storage info; no S3 info needed
		// Ensure we don't pass a stale S3 StorageInfo
		mountOptions.StorageInfo = nil
		
		// Use credentials from request (passed by scheduler)
		if request.ImageCredentials != "" {
			var credData map[string]interface{}
			if err := json.Unmarshal([]byte(request.ImageCredentials), &credData); err == nil {
				registry, _ := credData["registry"].(string)
				credsMap, _ := credData["credentials"].(map[string]interface{})
				
				if registry != "" && credsMap != nil {
					// Convert to string map and build credential keys list
					credMap := make(map[string]string)
					credKeys := []string{}
					for k, v := range credsMap {
						if strVal, ok := v.(string); ok {
							credMap[k] = strVal
							credKeys = append(credKeys, k)
							// Set in environment for CreateProviderFromEnv to read
							os.Setenv(k, strVal)
						}
					}
					
					// Create CLIP provider from credentials
					if provider, err := oci.CreateProviderFromEnv(ctx, registry, credKeys); err == nil && provider != nil {
						mountOptions.RegistryCredProvider = provider
						log.Info().
							Str("image_id", imageId).
							Str("registry", registry).
							Msg("using credential provider from request for OCI image mount")
					} else if err != nil {
						log.Warn().
							Err(err).
							Str("image_id", imageId).
							Msg("failed to create credential provider, using default")
					}
				}
			} else {
				log.Warn().
					Err(err).
					Str("image_id", imageId).
					Msg("failed to parse ImageCredentials JSON, using default provider")
			}
		}
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
			Key:            fmt.Sprintf("%s.%s", imageId, registry.LocalImageFileExtension),
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

// GetSourceImageRef retrieves the cached source image reference for a v2 image
func (c *ImageClient) GetSourceImageRef(imageId string) (string, bool) {
	return c.v2ImageRefs.Get(imageId)
}

// cacheV2SourceImageRef caches the source image reference for a v2 image build
func (c *ImageClient) cacheV2SourceImageRef(request *types.ContainerRequest) {
	if c.config.ImageService.ClipVersion == uint32(types.ClipVersion2) && request.BuildOptions.SourceImage != nil {
		c.v2ImageRefs.Set(request.ImageId, *request.BuildOptions.SourceImage)
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

// createOCIImageWithProgress creates an OCI index with progress reporting
func (c *ImageClient) createOCIImageWithProgress(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, imageRef, outputPath string, checkpointMiB int64, authConfig string) error {
	outputLogger.Info("Starting OCI image indexing...\n")
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
					Msgf("OCI index progress [%s]: layer %d/%d", progress.Stage, progress.LayerIndex, progress.TotalLayers)

				outputLogger.Info(fmt.Sprintf("OCI index progress [%s]: layer %d/%d\n",
					progress.Stage, progress.LayerIndex, progress.TotalLayers))
			}
		}
	}()

	// Create index-only clip archive from the OCI image
	err := clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
		ImageRef:      imageRef,
		OutputPath:    outputPath,
		CheckpointMiB: checkpointMiB,
		AuthConfig:    authConfig,
		ProgressChan:  progressChan,
	})

	// Close channel and wait for all progress messages to be logged
	close(progressChan)
	wg.Wait()

	if err != nil {
		return err
	}

	outputLogger.Info("OCI image indexing completed successfully\n")
	return nil
}

func (c *ImageClient) BuildAndArchiveImage(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest) error {
	startTime := time.Now()

	// Cache the source image reference for v2 images so we can retrieve it later for non-build containers
	c.cacheV2SourceImageRef(request)

	buildPath, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(buildPath)

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
		if base, perr := image.ExtractImageNameAndTag(sourceImage); perr == nil {
			// Treat runner/build registry as insecure if configured
			if c.config.ImageService.BuildRegistryInsecure &&
				(base.Registry == c.config.ImageService.BuildRegistry || base.Registry == c.config.ImageService.Runner.BaseImageRegistry) {
				insecure = true
			}

			// Also consider localhost registries insecure by default
			if strings.Contains(base.Registry, "localhost") || strings.HasPrefix(base.Registry, "127.0.0.1") {
				insecure = c.config.ImageService.BuildRegistryInsecure || true
			}
		}
		// buildah pull the base image so bud doesn't attempt HTTPS
		pullArgs := []string{"--root", imagePath, "pull"}
		if insecure {
			pullArgs = append(pullArgs, "--tls-verify=false")
		}

		pullArgs = append(pullArgs, "docker://"+sourceImage)
		cmd := exec.CommandContext(ctx, "buildah", pullArgs...)
		cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
		cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
		if err := cmd.Run(); err != nil {
			return err
		}
	}

	budArgs := []string{"--root", imagePath, "bud"}
	if insecure {
		budArgs = append(budArgs, "--tls-verify=false")
	}

	budArgs = append(budArgs, "-f", tempDockerFile, "-t", request.ImageId+":latest", buildCtxPath)
	cmd := exec.CommandContext(ctx, "buildah", budArgs...)
	cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
	cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
	err = cmd.Run()
	if err != nil {
		return err
	}

	cmd = exec.CommandContext(ctx, "buildah", "--root", imagePath, "push", request.ImageId+":latest", "oci:"+ociPath+":latest")
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

	// Clip v2: Skip unpack and create index-only clip from OCI layout.
	// The image is pushed to the build registry so the clip indexer can stream from it.
	if c.config.ImageService.ClipVersion == uint32(types.ClipVersion2) {
		archiveName := fmt.Sprintf("%s.%s.tmp", request.ImageId, c.registry.ImageFileExtension)
		archivePath := filepath.Join("/tmp", archiveName)

		// Determine the build registry to push the image to
		dockerRegistry := c.config.ImageService.BuildRegistry
		if dockerRegistry == "" {
			dockerRegistry = c.config.ImageService.Runner.BaseImageRegistry
		}
		if dockerRegistry == "" {
			dockerRegistry = "localhost"
		}
		localTag := fmt.Sprintf("%s/%s:latest", dockerRegistry, request.ImageId)

		// Tag the built image
		if err = exec.CommandContext(ctx, "buildah", "--root", imagePath, "tag", request.ImageId+":latest", localTag).Run(); err != nil {
			return err
		}

		// Push to registry (required for clip indexer to access the image)
		pushArgs := []string{"--root", imagePath, "push", localTag, "docker://" + localTag}
		if c.config.ImageService.BuildRegistryInsecure {
			pushArgs = []string{"--root", imagePath, "push", "--tls-verify=false", localTag, "docker://" + localTag}
		}

		cmd = exec.CommandContext(ctx, "buildah", pushArgs...)
		cmd.Stdout = &common.ExecWriter{Logger: outputLogger}
		cmd.Stderr = &common.ExecWriter{Logger: outputLogger}
		if err = cmd.Run(); err != nil {
			return err
		}

		// Create index-only clip archive with progress reporting
		if err = c.createOCIImageWithProgress(ctx, outputLogger, request, localTag, archivePath, 2, ""); err != nil {
			return err
		}

		// Upload the clip archive to the image registry
		if err = c.registry.Push(ctx, archivePath, request.ImageId); err != nil {
			return err
		}

		return nil
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

	// Cache the source image reference for v2 images
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
		archivePath := filepath.Join("/tmp", archiveName)

		// Create index-only clip from the source docker image reference with progress reporting
		if err = c.createOCIImageWithProgress(ctx, outputLogger, request, *request.BuildOptions.SourceImage, archivePath, 2, ""); err != nil {
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
	archivePath := filepath.Join("/tmp", archiveName)

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

