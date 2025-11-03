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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
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
		// v2 (OCI index-only): ClipFS will read embedded OCI storage info; no S3 info needed
		mountOptions.StorageInfo = nil

		// Attach credential provider for runtime layer loading
		var credProvider clipCommon.RegistryCredentialProvider

		if request.ImageCredentials != "" {
			// Runtime container: credentials already in JSON format from secret
			credProvider = c.createCredentialProvider(ctx, request.ImageCredentials, imageId)
		} else if request.BuildOptions.SourceImageCreds != "" {
			// Build container: parse and use source image credentials
			sourceRef, hasRef := c.v2ImageRefs.Get(imageId)
			if hasRef {
				registry := reg.ParseRegistry(sourceRef)
				if registry != "" {
					// Parse credentials (handles both JSON and username:password formats)
					creds, err := reg.ParseCredentialsFromJSON(request.BuildOptions.SourceImageCreds)
					if err != nil {
						log.Warn().
							Err(err).
							Str("image_id", imageId).
							Str("container_id", request.ContainerId).
							Str("registry", registry).
							Msg("failed to parse build credentials for mount")
					} else if len(creds) > 0 {
						credProvider = reg.CredentialsToProvider(ctx, registry, creds)
					}
				}
			}
		}

		if credProvider != nil {
			mountOptions.RegistryCredProvider = credProvider
			log.Info().Str("image_id", imageId).Str("provider", credProvider.Name()).Msg("attached credential provider")
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

// processPulledArchive handles the pulled archive, extracts metadata, and determines the mount path
// For v2 OCI images: moves to canonical location and caches metadata
// For v1 S3 images: uses the download path as-is
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

	// Default registry to Docker Hub if not specified
	if ociInfo.RegistryURL == "" {
		ociInfo.RegistryURL = "https://docker.io"
	}

	// Build and cache the full image reference
	if ociInfo.Repository != "" && ociInfo.Reference != "" {
		registryHost := strings.TrimPrefix(ociInfo.RegistryURL, "https://")
		registryHost = strings.TrimPrefix(registryHost, "http://")
		sourceRef := fmt.Sprintf("%s/%s:%s", registryHost, ociInfo.Repository, ociInfo.Reference)
		c.v2ImageRefs.Set(imageId, sourceRef)
		log.Info().Str("image_id", imageId).Str("source_image", sourceRef).Msg("cached source image reference from clip metadata")
	}

	// Log metadata availability
	if ociInfo.ImageMetadata != nil {
		log.Info().Str("image_id", imageId).Msg("image metadata available in clip archive")
	}
}

// GetSourceImageRef retrieves the cached source image reference for a v2 image
func (c *ImageClient) GetSourceImageRef(imageId string) (string, bool) {
	return c.v2ImageRefs.Get(imageId)
}

// GetCLIPImageMetadata extracts CLIP image metadata from the archive for a v2 image
// Returns the CLIP metadata directly from the archive (source of truth)
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

// createCredentialProvider creates a CLIP credential provider from JSON credentials
// Credentials are expected in JSON format: {"registry":"...","type":"...","credentials":{...}}
// This format is used for both build and runtime containers
func (c *ImageClient) createCredentialProvider(ctx context.Context, credentialsStr, imageId string) clipCommon.RegistryCredentialProvider {
	if credentialsStr == "" {
		return nil
	}

	// Parse JSON credentials using the registry package helper
	registry, _, creds, err := reg.UnmarshalCredentials(credentialsStr)
	if err != nil {
		log.Warn().
			Err(err).
			Str("image_id", imageId).
			Msg("failed to parse image credentials JSON")
		return nil
	}

	if registry == "" || len(creds) == 0 {
		log.Warn().Str("image_id", imageId).Msg("missing registry or credentials in JSON")
		return nil
	}

	// Create provider using the registry package helper
	provider := reg.CredentialsToProvider(ctx, registry, creds)
	if provider == nil {
		log.Warn().Str("image_id", imageId).Str("registry", registry).Msg("failed to create credential provider")
		return nil
	}

	return provider
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
// getBuildRegistry returns the registry to use for pushing intermediate build images
func (c *ImageClient) getBuildRegistry() string {
	if c.config.ImageService.BuildRegistry != "" {
		return c.config.ImageService.BuildRegistry
	}
	if c.config.ImageService.Runner.BaseImageRegistry != "" {
		return c.config.ImageService.Runner.BaseImageRegistry
	}
	return "localhost"
}

// getBuildRegistryAuthArgs returns buildah authentication arguments for the build registry
// This uses Beta9's internal registry credentials from config, NOT the user's source image credentials
func (c *ImageClient) getBuildRegistryAuthArgs(ctx context.Context, buildRegistry string) []string {
	// For localhost, no auth needed
	if buildRegistry == "localhost" || strings.HasPrefix(buildRegistry, "127.0.0.1") {
		return nil
	}

	// Use Docker Hub credentials from config
	if strings.Contains(buildRegistry, "docker.io") || buildRegistry == "docker.io" {
		username := c.config.ImageService.Registries.Docker.Username
		password := c.config.ImageService.Registries.Docker.Password
		if username != "" && password != "" {
			return []string{"--creds", fmt.Sprintf("%s:%s", username, password)}
		}
		log.Warn().Str("registry", buildRegistry).Msg("Docker Hub build registry configured but no credentials in config.ImageService.Registries.Docker")
		return nil
	}

	// For ECR, GCR, and in-cluster registries: let buildah use ambient credentials
	// - ECR: IAM role attached to pod/instance (buildah will automatically use AWS SDK)
	// - GCR: Service account with Workload Identity (buildah will use gcloud credentials)
	// - In-cluster registry: Service account tokens mounted by Kubernetes
	// - Private registry: credentials in ~/.docker/config.json (set by ImagePullSecrets)
	// 
	// No explicit config needed - just let it "just work" with ambient credentials
	log.Info().
		Str("registry", buildRegistry).
		Msg("using ambient credentials for build registry (IAM role, service account, or docker config)")
	return nil
}

// isInsecureRegistry returns true if TLS verification should be disabled
// Uses the BuildRegistryInsecure config flag - no auto-detection or hardcoded addresses
func (c *ImageClient) isInsecureRegistry() bool {
	return c.config.ImageService.BuildRegistryInsecure
}

// isECRRegistry returns true if the registry is AWS ECR
func (c *ImageClient) isECRRegistry(registry string) bool {
	return strings.Contains(strings.ToLower(registry), ".ecr.") && strings.Contains(strings.ToLower(registry), ".amazonaws.com")
}

// ensureECRRepository creates an ECR repository if it doesn't exist
// repository should be in format: workspace_id/image_id
func (c *ImageClient) ensureECRRepository(ctx context.Context, registry string, repository string, outputLogger *slog.Logger) error {
	// Parse region from registry URL
	// Format: account-id.dkr.ecr.region.amazonaws.com
	parts := strings.Split(registry, ".")
	if len(parts) < 4 {
		return fmt.Errorf("invalid ECR registry format: %s", registry)
	}
	
	region := ""
	for i, part := range parts {
		if part == "ecr" && i+1 < len(parts) {
			region = parts[i+1]
			break
		}
	}
	
	if region == "" {
		return fmt.Errorf("could not parse region from ECR registry: %s", registry)
	}

	outputLogger.Info(fmt.Sprintf("Ensuring ECR repository exists: %s in region %s\n", repository, region))
	log.Info().Str("registry", registry).Str("repository", repository).Str("region", region).Msg("ensuring ECR repository exists")

	// Get AWS credentials from config or use ambient credentials (IAM role)
	cfg, err := common.GetAWSConfig("", "", region, "")
	if err != nil {
		return fmt.Errorf("failed to get AWS config: %w", err)
	}

	// Create ECR client
	ecrClient := ecr.NewFromConfig(cfg)

	// Try to describe the repository first
	_, err = ecrClient.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{repository},
	})

	if err == nil {
		// Repository already exists
		outputLogger.Info(fmt.Sprintf("ECR repository %s already exists\n", repository))
		log.Info().Str("repository", repository).Msg("ECR repository already exists")
		return nil
	}

	// Check if error is "repository not found"
	if !strings.Contains(err.Error(), "RepositoryNotFoundException") {
		// Some other error occurred
		log.Warn().Err(err).Str("repository", repository).Msg("failed to describe ECR repository")
		// Don't fail - we'll try to create it anyway
	}

	// Create the repository
	_, err = ecrClient.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repository),
		ImageScanningConfiguration: &ecrtypes.ImageScanningConfiguration{
			ScanOnPush: false,
		},
	})

	if err != nil {
		// Check if error is "repository already exists" - this is fine
		if strings.Contains(err.Error(), "RepositoryAlreadyExistsException") {
			outputLogger.Info(fmt.Sprintf("ECR repository %s already exists\n", repository))
			log.Info().Str("repository", repository).Msg("ECR repository already exists")
			return nil
		}
		return fmt.Errorf("failed to create ECR repository: %w", err)
	}

	outputLogger.Info(fmt.Sprintf("Created ECR repository: %s\n", repository))
	log.Info().Str("repository", repository).Msg("created ECR repository")
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

func (c *ImageClient) createOCIImageWithProgress(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, imageRef, outputPath string, checkpointMiB int64, authConfig string) error {
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

	// Create credential provider if credentials are available
	// For build containers, use SourceImageCreds; for runtime containers, use ImageCredentials
	var credProvider clipCommon.RegistryCredentialProvider
	if request.ImageCredentials != "" {
		// Runtime container: credentials are already in JSON format from secret
		credProvider = c.createCredentialProvider(ctx, request.ImageCredentials, request.ImageId)
	} else if request.BuildOptions.SourceImageCreds != "" {
		// Build container: credentials may be in legacy username:password format or JSON
		// Parse and convert to proper format for CLIP
		registry := reg.ParseRegistry(imageRef)
		if registry != "" {
			// Parse credentials (handles both JSON and username:password formats)
			creds, err := reg.ParseCredentialsFromJSON(request.BuildOptions.SourceImageCreds)
			if err != nil {
				log.Warn().
					Err(err).
					Str("image_id", request.ImageId).
					Str("registry", registry).
					Msg("failed to parse build credentials, will try without auth")
			} else if len(creds) > 0 {
				// Create provider from parsed credentials
				credProvider = reg.CredentialsToProvider(ctx, registry, creds)
				log.Info().
					Str("image_id", request.ImageId).
					Str("registry", registry).
					Msg("created credential provider from build options for OCI indexing")
			}
		}
	}

	// Create index-only clip archive from the OCI image
	err := clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
		ImageRef:      imageRef,
		OutputPath:    outputPath,
		CheckpointMiB: checkpointMiB,
		ProgressChan:  progressChan,
		CredProvider:  credProvider,
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
		// Use configured insecure mode for all registry operations
		insecure = c.isInsecureRegistry()
		
		// buildah pull the base image so bud doesn't attempt HTTPS
		pullArgs := []string{"--root", imagePath, "pull"}
		if insecure {
			pullArgs = append(pullArgs, "--tls-verify=false")
		}
		
		// Add credentials if provided
		if authArgs := c.getBuildahAuthArgs(ctx, sourceImage, request.BuildOptions.SourceImageCreds); len(authArgs) > 0 {
			pullArgs = append(pullArgs, authArgs...)
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

		// Get build registry and construct tag
		// The build registry is where we push intermediate images so CLIP indexer can stream from them
		buildRegistry := c.getBuildRegistry()
		if buildRegistry == "localhost" {
			log.Warn().Str("image_id", request.ImageId).Msg("using localhost as build registry - CLIP indexer may not be able to access this")
		}
		
		// Construct repository path with workspace namespace for ECR/cloud registries
		// For localhost: localhost/image_id:latest
		// For ECR: registry.ecr.region.amazonaws.com/workspace_id/image_id:latest
		var repositoryPath string
		if c.isECRRegistry(buildRegistry) {
			// ECR format: namespace by workspace ID
			repositoryPath = fmt.Sprintf("%s/%s", request.WorkspaceId, request.ImageId)
		} else {
			// Local or other registries: just use image ID
			repositoryPath = request.ImageId
		}
		localTag := fmt.Sprintf("%s/%s:latest", buildRegistry, repositoryPath)

		// Tag the built image
		if err = exec.CommandContext(ctx, "buildah", "--root", imagePath, "tag", request.ImageId+":latest", localTag).Run(); err != nil {
			return err
		}

		// Create ECR repository if needed (before pushing)
		if c.isECRRegistry(buildRegistry) {
			if err := c.ensureECRRepository(ctx, buildRegistry, repositoryPath, outputLogger); err != nil {
				log.Error().Err(err).Str("registry", buildRegistry).Str("repository", repositoryPath).Msg("failed to ensure ECR repository exists")
				// Don't fail the build - repository might already exist and we just don't have describe permissions
			}
		}

		// Build push arguments with authentication
		// IMPORTANT: Use build registry credentials (internal), NOT user's source image credentials
		pushArgs := []string{"--root", imagePath, "push"}
		if c.isInsecureRegistry() {
			pushArgs = append(pushArgs, "--tls-verify=false")
		}
		// Add authentication for internal build registry
		if authArgs := c.getBuildRegistryAuthArgs(ctx, buildRegistry); len(authArgs) > 0 {
			pushArgs = append(pushArgs, authArgs...)
		}
		pushArgs = append(pushArgs, localTag, "docker://"+localTag)

		// Push to registry (required for clip indexer to access the image)
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
