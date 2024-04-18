package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/internal/abstractions/image"
	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	types "github.com/beam-cloud/beta9/internal/types"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/beam-cloud/clip/pkg/storage"
	"github.com/moby/sys/mountinfo"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/cas/dir"
	"github.com/opencontainers/umoci/oci/casext"
	"github.com/opencontainers/umoci/oci/layer"
	"github.com/pkg/errors"

	runc "github.com/beam-cloud/go-runc"
)

const (
	imagePullCommand string = "skopeo"
	imageBundlePath  string = "/dev/shm/images"
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
	registry        *common.ImageRegistry
	cacheClient     *CacheClient
	imageCachePath  string
	imageMountPath  string
	imageBundlePath string
	pullCommand     string
	pDeathSignal    syscall.Signal
	commandTimeout  int
	debug           bool
	creds           string
	config          types.ImageServiceConfig
	workerId        string
	workerRepo      repository.WorkerRepository
}

func NewImageClient(config types.ImageServiceConfig, workerId string, workerRepo repository.WorkerRepository) (*ImageClient, error) {
	var provider CredentialProvider // Configure image registry credentials

	switch config.RegistryCredentialProviderName {
	case "aws":
		provider = &AWSCredentialProvider{
			Region:    config.Registries.S3.AWSRegion,
			AccessKey: config.Registries.S3.AWSAccessKey,
			SecretKey: config.Registries.S3.AWSSecretKey,
		}
	case "docker":
		provider = &DockerCredentialProvider{
			Username: config.Registries.Docker.Username,
			Password: config.Registries.Docker.Password,
		}
	default:
		return nil, fmt.Errorf("invalid credential provider name: %s", config.RegistryCredentialProviderName)
	}

	registry, err := common.NewImageRegistry(config)
	if err != nil {
		return nil, err
	}

	var cacheClient *CacheClient = nil
	if config.CacheURL != "" {
		cacheClient, err = NewCacheClient(config.CacheURL, "")
		if err != nil {
			return nil, err
		}
	}

	c := &ImageClient{
		config:          config,
		registry:        registry,
		cacheClient:     cacheClient,
		imageBundlePath: imageBundlePath,
		imageCachePath:  getImageCachePath(),
		imageMountPath:  getImageMountPath(workerId),
		pullCommand:     imagePullCommand,
		commandTimeout:  -1,
		debug:           false,
		creds:           "",
		workerId:        workerId,
		workerRepo:      workerRepo,
	}

	err = os.MkdirAll(c.imageBundlePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// TODO: refactor credentials logic for base image registries
	// Right now, the aws credential provider is not actually being used
	// because the base image is stored in a public registry
	// We will probably need to adjust the config to make more sense here
	_, err = provider.GetAuthString()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *ImageClient) PullLazy(imageId string) error {
	localCachePath := fmt.Sprintf("%s/%s.cache", c.imageCachePath, imageId)
	if !c.config.LocalCacheEnabled {
		localCachePath = ""
	}

	remoteArchivePath := fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
	var err error = nil

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
				AccessKey: c.config.Registries.S3.AWSAccessKey,
				SecretKey: c.config.Registries.S3.AWSSecretKey,
			},
		},
	}

	// Check if mount point is already in use
	if mounted, _ := mountinfo.Mounted(mountOptions.MountPoint); mounted {
		return nil
	}

	// Get lock on image mount
	err = c.workerRepo.SetImagePullLock(c.workerId, imageId)
	if err != nil {
		return err
	}
	defer c.workerRepo.RemoveImagePullLock(c.workerId, imageId)

	startServer, _, err := clip.MountArchive(*mountOptions)
	if err != nil {
		return err
	}

	err = startServer()
	if err != nil {
		return err
	}

	return nil
}

func (c *ImageClient) PullAndArchiveImage(ctx context.Context, sourceImage string, imageId string, creds *string) error {
	baseImage, err := extractImageNameAndTag(sourceImage)
	if err != nil {
		return err
	}

	dest := fmt.Sprintf("oci:%s:%s", baseImage.ImageName, baseImage.ImageTag)
	args := []string{"copy", fmt.Sprintf("docker://%s", sourceImage), dest}

	args = append(args, c.args(creds)...)
	cmd := exec.CommandContext(ctx, c.pullCommand, args...)
	cmd.Env = os.Environ()
	cmd.Dir = c.imageBundlePath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	ec, err := c.startCommand(cmd)
	if err != nil {
		return err
	}

	status, err := runc.Monitor.Wait(cmd, ec)
	if err == nil && status != 0 {
		log.Printf("unable to copy base image: %v -> %v", sourceImage, dest)
	}

	tmpBundlePath := filepath.Join(c.imageBundlePath, imageId)
	err = c.unpack(baseImage.ImageName, baseImage.ImageTag, tmpBundlePath)
	if err != nil {
		return fmt.Errorf("unable to unpack image: %v", err)
	}

	defer func() {
		os.RemoveAll(tmpBundlePath)
	}()

	return c.Archive(ctx, tmpBundlePath, imageId)
}

func (c *ImageClient) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if c.pDeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}

func (c *ImageClient) args(creds *string) (out []string) {
	if creds != nil && *creds != "" {
		out = append(out, "--src-creds", *creds)
	} else if creds != nil && *creds == "" {
		out = append(out, "--src-no-creds")
	} else if c.creds != "" {
		out = append(out, "--src-creds", c.creds)
	}

	if c.commandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", c.commandTimeout))
	}

	if !c.config.EnableTLS {
		out = append(out, []string{"--src-tls-verify=false", "--dest-tls-verify=false"}...)
	}

	if c.debug {
		out = append(out, "--debug")
	}

	return out
}

func (c *ImageClient) unpack(baseImageName string, baseImageTag string, bundlePath string) error {
	var unpackOptions layer.UnpackOptions
	var meta umoci.Meta
	meta.Version = umoci.MetaVersion

	unpackOptions.KeepDirlinks = true
	unpackOptions.MapOptions = meta.MapOptions

	// Get a reference to the CAS.
	baseImagePath := fmt.Sprintf("%s/%s", c.imageBundlePath, baseImageName)
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
func (c *ImageClient) Archive(ctx context.Context, bundlePath string, imageId string) error {
	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s.tmp", imageId, c.registry.ImageFileExtension)
	archivePath := filepath.Join("/tmp", archiveName)

	defer func() {
		os.RemoveAll(archivePath)
	}()

	var err error = nil
	switch c.config.RegistryStore {
	case "s3":
		err = clip.CreateAndUploadArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
			Credentials: storage.ClipStorageCredentials{
				S3: &storage.S3ClipStorageCredentials{
					AccessKey: c.config.Registries.S3.AWSAccessKey,
					SecretKey: c.config.Registries.S3.AWSSecretKey,
				},
			},
		}, &clipCommon.S3StorageInfo{
			Bucket: c.config.Registries.S3.AWSS3Bucket,
			Region: c.config.Registries.S3.AWSRegion,
			Key:    fmt.Sprintf("%s.clip", imageId),
		})
	case "local":
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Printf("Unable to create archive: %v\n", err)
		return err
	}
	log.Printf("Container <%v> archive took %v\n", imageId, time.Since(startTime))

	// Push the archive to a registry
	startTime = time.Now()
	err = c.registry.Push(ctx, archivePath, imageId)
	if err != nil {
		log.Printf("Failed to push image <%v>: %v\n", imageId, err)
		return err
	}

	log.Printf("Image <%v> push took %v\n", imageId, time.Since(startTime))
	return nil
}

var imageNamePattern = regexp.MustCompile(`^(?:(.*?)\/)?(?:([^\/:]+)\/)?([^\/:]+)(?::([^\/:]+))?$`)

func extractImageNameAndTag(sourceImage string) (image.BaseImage, error) {
	matches := imageNamePattern.FindStringSubmatch(sourceImage)
	if matches == nil {
		return image.BaseImage{}, errors.New("invalid image URI format")
	}

	registry, name, tag := matches[1], matches[3], matches[4]

	if registry == "" {
		registry = "docker.io"
	}

	if tag == "" {
		tag = "latest"
	}

	return image.BaseImage{
		SourceRegistry: registry,
		ImageName:      name,
		ImageTag:       tag,
	}, nil
}
