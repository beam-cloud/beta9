package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	common "github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
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
	imageBundlePath string = "/dev/shm/images"
	imageTmpDir     string = "/tmp"
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

type ImageClient struct {
	registry           *common.ImageRegistry
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
	registry, err := common.NewImageRegistry(config.ImageService)
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

func (c *ImageClient) PullLazy(request *types.ContainerRequest) error {
	imageId := request.ImageId
	isBuildContainer := strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix)

	c.logger.Log(request.ContainerId, request.StubId, "Loading image: %s", imageId)

	localCachePath := fmt.Sprintf("%s/%s.cache", c.imageCachePath, imageId)
	if !c.config.ImageService.LocalCacheEnabled && !isBuildContainer {
		localCachePath = ""
	}

	// If we have a valid cache client, attempt to cache entirety of the image
	// in memory (in a nearby region). If a remote cache is available, this supercedes
	// the local cache - which is basically just downloading the image to disk
	startTime := time.Now()

	if c.cacheClient != nil && !isBuildContainer {
		sourcePath := fmt.Sprintf("images/%s.clip", imageId)
		sourceOffset := int64(0)

		// If the image archive is already cached in memory (in blobcache), then we can use that as the local cache path
		baseBlobFsContentPath := fmt.Sprintf("%s/%s", baseFileCachePath, sourcePath)
		if _, err := os.Stat(baseBlobFsContentPath); err == nil {
			localCachePath = baseBlobFsContentPath
		} else {
			c.logger.Log(request.ContainerId, request.StubId, "image <%s> not found in cache, caching nearby", imageId)

			// Otherwise, lets cache it in a nearby blobcache host
			_, err := c.cacheClient.StoreContentFromSource(sourcePath, sourceOffset)
			if err == nil {
				localCachePath = baseBlobFsContentPath
			} else {
				c.logger.Log(request.ContainerId, request.StubId, "unable to cache image nearby <%s>: %v\n", imageId, err)
			}
		}
	}

	elapsed := time.Since(startTime)
	c.logger.Log(request.ContainerId, request.StubId, "Loaded image <%s>, took: %s", imageId, elapsed)

	remoteArchivePath := fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
	if _, err := os.Stat(remoteArchivePath); err != nil {
		err = c.registry.Pull(context.TODO(), remoteArchivePath, imageId)
		if err != nil {
			return err
		}
	}

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           remoteArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", c.imageMountPath, imageId),
		Verbose:               false,
		CachePath:             localCachePath,
		ContentCache:          c.cacheClient,
		ContentCacheAvailable: c.cacheClient != nil,
		Credentials: storage.ClipStorageCredentials{
			S3: &storage.S3ClipStorageCredentials{
				AccessKey: c.config.ImageService.Registries.S3.AccessKey,
				SecretKey: c.config.ImageService.Registries.S3.SecretKey,
			},
		},
	}

	// Check if a fuse server exists for this imageId
	_, mounted := c.mountedFuseServers.Get(imageId)
	if mounted {
		return nil
	}

	// Get lock on image mount
	lockResponse, err := handleGRPCResponse(c.workerRepoClient.SetImagePullLock(context.Background(), &pb.SetImagePullLockRequest{
		WorkerId: c.workerId,
		ImageId:  imageId,
	}))
	if err != nil {
		return err
	}
	defer handleGRPCResponse(c.workerRepoClient.RemoveImagePullLock(context.Background(), &pb.RemoveImagePullLockRequest{
		WorkerId: c.workerId,
		ImageId:  imageId,
		Token:    lockResponse.Token,
	}))

	startServer, _, server, err := clip.MountArchive(*mountOptions)
	if err != nil {
		return err
	}

	err = startServer()
	if err != nil {
		return err
	}

	c.mountedFuseServers.Set(imageId, server)
	return nil
}

func (c *ImageClient) Cleanup() error {
	c.mountedFuseServers.Range(func(imageId string, server *fuse.Server) bool {
		log.Info().Str("image_id", imageId).Msg("un-mounting image")
		server.Unmount()
		return true // Continue iteration
	})

	log.Info().Str("path", c.imageCachePath).Msg("cleaning up blobfs image cache")
	if c.config.BlobCache.BlobFs.Enabled && c.cacheClient != nil {
		err := c.cacheClient.Cleanup()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ImageClient) inspectAndVerifyImage(ctx context.Context, request *types.ContainerRequest) error {
	imageMetadata, err := c.skopeoClient.Inspect(ctx, *request.BuildOptions.SourceImage, request.BuildOptions.SourceImageCreds)
	if err != nil {
		return err
	}

	if imageMetadata.Architecture != runtime.GOARCH {
		return &types.ExitCodeError{
			ExitCode: types.WorkerContainerExitCodeIncorrectImageArch,
		}
	}

	if imageMetadata.Os != runtime.GOOS {
		return &types.ExitCodeError{
			ExitCode: types.WorkerContainerExitCodeIncorrectImageOs,
		}
	}

	return nil
}

// Will be replaced when structured logging is merged
type ExecWriter struct {
	outputLogger *slog.Logger
}

func (c *ExecWriter) Write(p []byte) (n int, err error) {
	c.outputLogger.Info(string(p))
	return len(p), nil
}

func (c *ImageClient) BuildAndArchiveImage(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest) error {
	outputLogger.Info("Building image from Dockerfile\n")

	buildCtxPath, err := getBuildContext(request)
	if err != nil {
		return err
	}

	buildPath, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(buildPath)
	tempDockerFile := filepath.Join(buildPath, "Dockerfile")
	f, err := os.Create(tempDockerFile)
	if err != nil {
		return err
	}
	fmt.Fprintf(f, *request.BuildOptions.Dockerfile)
	f.Close()

	imagePath := filepath.Join(buildPath, "image")
	ociPath := filepath.Join(buildPath, "oci")
	tmpBundlePath := filepath.Join(c.imageBundlePath, request.ImageId)
	defer os.RemoveAll(tmpBundlePath)
	os.MkdirAll(imagePath, 0755)
	os.MkdirAll(ociPath, 0755)

	cmd := exec.CommandContext(ctx, "buildah", "--root", imagePath, "bud", "-f", tempDockerFile, "-t", request.ImageId+":latest", buildCtxPath)
	cmd.Stdout = &ExecWriter{outputLogger: outputLogger}
	cmd.Stderr = &ExecWriter{outputLogger: outputLogger}
	err = cmd.Run()
	if err != nil {
		return err
	}

	cmd = exec.CommandContext(ctx, "buildah", "--root", imagePath, "push", request.ImageId+":latest", "oci:"+ociPath+":latest")
	cmd.Stdout = &ExecWriter{outputLogger: outputLogger}
	cmd.Stderr = &ExecWriter{outputLogger: outputLogger}
	err = cmd.Run()
	if err != nil {
		return err
	}

	engine, err := dir.Open(ociPath)
	if err != nil {
		return err
	}
	defer engine.Close()

	unpackOptions := umociUnpackOptions()

	engineExt := casext.NewEngine(engine)
	defer engineExt.Close()

	err = umoci.Unpack(engineExt, "latest", tmpBundlePath, unpackOptions)
	if err != nil {
		return err
	}

	for _, dir := range requiredContainerDirectories {
		fullPath := filepath.Join(tmpBundlePath, "rootfs", dir)
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

	outputLogger.Info("Copying image...\n")
	err = c.skopeoClient.Copy(ctx, *request.BuildOptions.SourceImage, dest, request.BuildOptions.SourceImageCreds)
	if err != nil {
		return err
	}

	outputLogger.Info("Unpacking image...\n")
	tmpBundlePath := filepath.Join(baseTmpBundlePath, request.ImageId)
	err = c.unpack(ctx, baseImage.Repo, baseImage.Tag, tmpBundlePath)
	if err != nil {
		return fmt.Errorf("unable to unpack image: %v", err)
	}

	defer os.RemoveAll(baseTmpBundlePath)
	defer os.RemoveAll(copyDir)

	outputLogger.Info("Archiving base image...\n")
	return c.Archive(ctx, tmpBundlePath, request.ImageId, nil)
}

func (c *ImageClient) unpack(ctx context.Context, baseImageName string, baseImageTag string, bundlePath string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

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

	tmpBundlePath := filepath.Join(bundlePath + "_")
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

		return os.Rename(tmpBundlePath, bundlePath)
	}

	return err
}

// Generate and upload archived version of the image for distribution
func (c *ImageClient) Archive(ctx context.Context, bundlePath string, imageId string, progressChan chan int) error {
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
	case common.S3ImageRegistryStore:
		err = clip.CreateAndUploadArchive(ctx, clip.CreateOptions{
			InputPath:  bundlePath,
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
	case common.LocalImageRegistryStore:
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Error().Err(err).Msg("unable to create archive")
		return err
	}
	log.Info().Str("container_id", imageId).Dur("duration", time.Since(startTime)).Msg("container archive took")

	// Push the archive to a registry
	startTime = time.Now()
	err = c.registry.Push(ctx, archivePath, imageId)
	if err != nil {
		log.Error().Str("image_id", imageId).Err(err).Msg("failed to push image")
		return err
	}

	log.Info().Str("image_id", imageId).Dur("duration", time.Since(startTime)).Msg("image push took")
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

func getBuildContext(request *types.ContainerRequest) (string, error) {
	buildCtxPath := "."
	if request.BuildOptions.BuildCtxObject != nil {
		err := common.ExtractObjectFile(context.TODO(), *request.BuildOptions.BuildCtxObject, request.Workspace.Name)
		if err != nil {
			return "", err
		}
		buildCtxPath = filepath.Join(types.DefaultExtractedObjectPath, request.Workspace.Name, *request.BuildOptions.BuildCtxObject)
	}
	return buildCtxPath, nil
}
