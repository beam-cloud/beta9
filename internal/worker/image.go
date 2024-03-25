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
	imagePullCommand       string = "skopeo"
	imageCachePath         string = "/dev/shm/images"
	imageMountPath         string = "/dev/shm/images/mount"
	imageAvailableFilename string = "IMAGE_AVAILABLE"
	imageMountLockFilename string = "IMAGE_MOUNT_LOCK"
)

var requiredContainerDirectories []string = []string{"/workspace", "/volumes", "/snapshot"}

type ImageClient struct {
	registry       *common.ImageRegistry
	cacheClient    *CacheClient
	ImagePath      string
	PullCommand    string
	PdeathSignal   syscall.Signal
	CommandTimeout int
	Debug          bool
	Creds          string
	config         types.ImageServiceConfig
	workerId       string
	workerRepo     repository.WorkerRepository
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

	err = os.MkdirAll(imageCachePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(imageMountPath, os.ModePerm)
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

	return &ImageClient{
		config:         config,
		registry:       registry,
		cacheClient:    cacheClient,
		ImagePath:      imageCachePath,
		PullCommand:    imagePullCommand,
		CommandTimeout: -1,
		Debug:          false,
		Creds:          "",
		workerId:       workerId,
		workerRepo:     workerRepo,
	}, nil
}

func (c *ImageClient) PullLazy(imageId string) error {
	localCachePath := fmt.Sprintf("%s/%s.cache", imagePath, imageId)
	remoteArchivePath := fmt.Sprintf("%s/%s.%s", imagePath, imageId, c.registry.ImageFileExtension)
	var err error = nil

	if _, err := os.Stat(remoteArchivePath); err != nil {
		err = c.registry.Pull(context.TODO(), remoteArchivePath, imageId)
		if err != nil {
			return err
		}
	}

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           remoteArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", imageMountPath, imageId),
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

func (i *ImageClient) PullAndArchiveImage(ctx context.Context, sourceImage string, imageId string, creds *string) error {
	baseImage, err := extractImageNameAndTag(sourceImage)
	if err != nil {
		return err
	}

	dest := fmt.Sprintf("oci:%s:%s", baseImage.ImageName, baseImage.ImageTag)
	args := []string{"copy", fmt.Sprintf("docker://%s", sourceImage), dest}

	args = append(args, i.args(creds)...)
	cmd := exec.CommandContext(ctx, i.PullCommand, args...)
	cmd.Env = os.Environ()
	cmd.Dir = i.ImagePath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	ec, err := i.startCommand(cmd)
	if err != nil {
		return err
	}

	status, err := runc.Monitor.Wait(cmd, ec)
	if err == nil && status != 0 {
		log.Printf("unable to copy base image: %v -> %v", sourceImage, dest)
	}

	bundlePath := filepath.Join(imagePath, imageId)
	err = i.unpack(baseImage.ImageName, baseImage.ImageTag, bundlePath)
	if err != nil {
		return fmt.Errorf("unable to unpack image: %v", err)
	}

	defer func() {
		os.RemoveAll(bundlePath)
	}()

	return i.Archive(ctx, bundlePath, imageId)
}

func (i *ImageClient) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if i.PdeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}

func (i *ImageClient) args(creds *string) (out []string) {
	if creds != nil && *creds != "" {
		out = append(out, "--src-creds", *creds)
	} else if creds != nil && *creds == "" {
		out = append(out, "--src-no-creds")
	} else if i.Creds != "" {
		out = append(out, "--src-creds", i.Creds)
	}

	if i.CommandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", i.CommandTimeout))
	}

	if !i.config.EnableTLS {
		out = append(out, []string{"--src-tls-verify=false", "--dest-tls-verify=false"}...)
	}

	if i.Debug {
		out = append(out, "--debug")
	}

	return out
}

func (i *ImageClient) unpack(baseImageName string, baseImageTag string, bundlePath string) error {
	var unpackOptions layer.UnpackOptions
	var meta umoci.Meta
	meta.Version = umoci.MetaVersion

	unpackOptions.KeepDirlinks = true
	unpackOptions.MapOptions = meta.MapOptions

	// Get a reference to the CAS.
	baseImagePath := fmt.Sprintf("%s/%s", imageCachePath, baseImageName)
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
func (i *ImageClient) Archive(ctx context.Context, bundlePath string, imageId string) error {
	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s.tmp", imageId, i.registry.ImageFileExtension)
	archivePath := filepath.Join("/tmp", archiveName)

	defer func() {
		os.RemoveAll(archivePath)
	}()

	var err error = nil
	switch i.config.RegistryStore {
	case "s3":
		err = clip.CreateAndUploadArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
			Credentials: storage.ClipStorageCredentials{
				S3: &storage.S3ClipStorageCredentials{
					AccessKey: i.config.Registries.S3.AWSAccessKey,
					SecretKey: i.config.Registries.S3.AWSSecretKey,
				},
			},
		}, &clipCommon.S3StorageInfo{
			Bucket: i.config.Registries.S3.AWSS3Bucket,
			Region: i.config.Registries.S3.AWSRegion,
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
	err = i.registry.Push(ctx, archivePath, imageId)
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
