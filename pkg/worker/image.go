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
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
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

var requiredContainerDirectories []string = []string{"/workspace", "/volumes"}

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
		logger: &ContainerLogger{
			logLinesPerHour: config.Worker.ContainerLogLinesPerHour,
		},
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

	localCachePath := fmt.Sprintf("%s/%s.cache", c.imageCachePath, imageId)
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
			// If the embedded OCI storage info is missing registry URL, default to docker.io
			if ociInfo, ok := meta.StorageInfo.(clipCommon.OCIStorageInfo); ok {
				if ociInfo.RegistryURL == "" {
					ociInfo.RegistryURL = "https://docker.io"
					// Note: metadata is not persisted back to file; ClipFS should handle missing URL by deriving from refs
				}
				_ = ociInfo
			}
		}
	}

    var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           mountArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", c.imageMountPath, imageId),
		Verbose:               false,
		CachePath:             localCachePath,
		ContentCache:          c.cacheClient,
		ContentCacheAvailable: c.cacheClient != nil,
	}

    // Ensure a minimal OCI runtime spec exists in the mount for build containers (v2 path)
    if request.IsBuildRequest() {
        configPath := filepath.Join(mountOptions.MountPoint, "config.json")
        if _, statErr := os.Stat(configPath); statErr != nil {
            // Write a minimal config.json so lifecycle can proceed
            _ = os.MkdirAll(mountOptions.MountPoint, 0755)
            minimal := `{"ociVersion":"1.0.2","process":{"terminal":false,"args":["/bin/sh"],"cwd":"/mnt/code"},"root":{"path":"."}}`
            _ = os.WriteFile(configPath, []byte(minimal), 0644)
        }
    }

	// Default to legacy S3 storage if we cannot detect OCI
	storageType := ""
	if meta != nil && meta.StorageInfo != nil {
		if t, ok := meta.StorageInfo.(interface{ Type() string }); ok {
			storageType = t.Type()
		}
	}

	if storageType == string(clipCommon.StorageModeOCI) || strings.ToLower(storageType) == "oci" {
		// v2 (OCI index-only): ClipFS will read embedded OCI storage info; no S3 info needed
		// Ensure we don't pass a stale S3 StorageInfo
		mountOptions.StorageInfo = nil
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

func (c *ImageClient) BuildAndArchiveImage(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest) error {
	outputLogger.Info("Building image from Dockerfile\n")
	startTime := time.Now()

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
	insecureBud := false
	sourceImage := ""
	if request.BuildOptions.SourceImage != nil {
		sourceImage = *request.BuildOptions.SourceImage
	}
	if sourceImage != "" {
		if base, perr := image.ExtractImageNameAndTag(sourceImage); perr == nil {
			// Treat runner/build registry as insecure if configured
			if c.config.ImageService.BuildRegistryInsecure &&
				(base.Registry == c.config.ImageService.BuildRegistry || base.Registry == c.config.ImageService.Runner.BaseImageRegistry) {
				insecureBud = true
			}
			// Also consider localhost registries insecure by default
			if strings.Contains(base.Registry, "localhost") || strings.HasPrefix(base.Registry, "127.0.0.1") {
				insecureBud = c.config.ImageService.BuildRegistryInsecure || true
			}
		}
		// buildah pull the base image so bud doesn't attempt HTTPS
		pullArgs := []string{"--root", imagePath, "pull"}
		if insecureBud {
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
	if insecureBud {
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

	// If configured for v2, skip unpack and create index-only clip from OCI
	if c.config.ImageService.ClipVersion == 2 {
		archiveName := fmt.Sprintf("%s.%s.tmp", request.ImageId, c.registry.ImageFileExtension)
		archivePath := filepath.Join("/tmp", archiveName)

        // Determine registry ref for final image, but avoid pushing here; push only after successful build
        dockerRegistry := c.config.ImageService.BuildRegistry
        if dockerRegistry == "" {
            dockerRegistry = c.config.ImageService.Runner.BaseImageRegistry
            if dockerRegistry == "" {
                dockerRegistry = "localhost"
            }
        }
        localTag := fmt.Sprintf("%s/%s:latest", dockerRegistry, request.ImageId)
        if err = exec.CommandContext(ctx, "buildah", "--root", imagePath, "tag", request.ImageId+":latest", localTag).Run(); err != nil {
            return err
        }

        // Index directly from the local OCI layout we pushed to ociPath (no remote pull required)
        err = clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
            ImageRef:      "oci:" + ociPath + ":latest",
			OutputPath:    archivePath,
			CheckpointMiB: 2,
			AuthConfig:    "", // rely on docker creds or anonymous for local insecure
		})
		if err != nil {
			return err
		}

        // Push the resulting .clip via existing registry abstraction
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

	for _, dir := range requiredContainerDirectories {
		fullPath := filepath.Join(tmpBundlePath.Path, "rootfs", dir)
		err := os.MkdirAll(fullPath, 0755)
		if err != nil {
			return err
		}
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

    if c.config.ImageService.ClipVersion == 2 {
		// v2: create index-only clip from the local OCI layout we just copied
		archiveName := fmt.Sprintf("%s.%s.tmp", request.ImageId, c.registry.ImageFileExtension)
		archivePath := filepath.Join("/tmp", archiveName)

        // Index directly from the local OCI layout we just created with skopeo
        err = clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
            ImageRef:      dest,
			OutputPath:    archivePath,
			CheckpointMiB: 2,
			AuthConfig:    "",
		})
		if err != nil {
			return err
		}

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
		for _, dir := range requiredContainerDirectories {
			fullPath := filepath.Join(tmpBundlePath, "rootfs", dir)
			err := os.MkdirAll(fullPath, 0755)
			if err != nil {
				errors.Wrap(err, fmt.Sprintf("creating /%s directory", dir))
				return err
			}
		}

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
