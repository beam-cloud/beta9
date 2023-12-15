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

	"github.com/beam-cloud/beam/internal/abstractions/image"
	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/cas/dir"
	"github.com/opencontainers/umoci/oci/casext"
	"github.com/opencontainers/umoci/oci/layer"
	"github.com/pkg/errors"

	runc "github.com/slai-labs/go-runc"
)

const (
	imagePullCommand          string = "skopeo"
	awsCredentialProviderName string = "aws"
	imageCachePath            string = "/dev/shm/images"
	imageAvailableFilename    string = "IMAGE_AVAILABLE"
)

var requiredContainerDirectories []string = []string{"/workspace", "/volumes", "/snapshot", "/outputs", "/packages"}

type ImageClient struct {
	registry       *common.ImageRegistry
	cacheClient    *CacheClient
	ImagePath      string
	PullCommand    string
	PdeathSignal   syscall.Signal
	CommandTimeout int
	Debug          bool
	Creds          string
}

func NewImageClient() (*ImageClient, error) {
	// Configure image registry credentials
	var provider CredentialProvider

	providerName := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_CREDENTIAL_PROVIDER", "aws")
	if providerName == awsCredentialProviderName {
		provider = &AWSCredentialProvider{}
	} else {
		provider = &DockerCredentialProvider{}
	}

	storeName := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_REGISTRY_STORE", "s3")
	registry, err := common.NewImageRegistry(storeName)
	if err != nil {
		return nil, err
	}

	cacheUrl, cacheUrlSet := os.LookupEnv("BEAM_CACHE_URL")
	var cacheClient *CacheClient = nil
	if cacheUrlSet && cacheUrl != "" {
		cacheClient, err = NewCacheClient(cacheUrl, "")
		if err != nil {
			return nil, err
		}
	}

	baseImagePath := filepath.Join(imageCachePath)
	os.MkdirAll(baseImagePath, os.ModePerm)

	creds, err := provider.GetAuthString()
	if err != nil {
		return nil, err
	}

	return &ImageClient{
		registry:       registry,
		cacheClient:    cacheClient,
		ImagePath:      baseImagePath,
		PullCommand:    imagePullCommand,
		CommandTimeout: -1,
		Debug:          false,
		Creds:          creds,
	}, nil
}

func (c *ImageClient) PullLazy(imageId string) error {
	localCachePath := fmt.Sprintf("%s/%s.cache", imagePath, imageId)
	remoteArchivePath := fmt.Sprintf("%s/%s.%s", imagePath, imageId, c.registry.ImageFileExtension)

	var err error = nil
	if _, err := os.Stat(remoteArchivePath); err != nil {
		return err
	}

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           remoteArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", imagePath, imageId),
		Verbose:               false,
		CachePath:             localCachePath,
		ContentCache:          c.cacheClient,
		ContentCacheAvailable: c.cacheClient != nil,
	}

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

func (i *ImageClient) PullAndArchive(context context.Context, sourceImage *string, creds *string) error {
	dest := fmt.Sprintf("oci:%s:%s", *sourceImage, "hi")
	args := []string{"copy", *sourceImage, dest}

	baseImage, err := i.extractImageNameAndTag((*sourceImage))
	if err != nil {
		return err
	}

	args = append(args, i.args(creds)...)
	cmd := exec.CommandContext(context, i.PullCommand, args...)
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
		err = fmt.Errorf("unable to pull base image: %s", *sourceImage)
	}

	err = i.unpack(baseImage.ImageName, baseImage.ImageName, imagePath, fmt.Sprintf("%s:%s", baseImage.ImageName, baseImage.ImageName))
	return err
}

func (i *ImageClient) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if i.PdeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}

func (i *ImageClient) extractImageNameAndTag(sourceImage string) (image.BaseImage, error) {
	re := regexp.MustCompile(`^(([^/]+/[^/]+)/)?([^:]+):?(.*)$`)
	matches := re.FindStringSubmatch(sourceImage)

	if matches == nil {
		return image.BaseImage{}, errors.New("invalid image URI format")
	}

	// Use default source registry if not specified
	sourceRegistry := "docker.io"
	if matches[2] != "" {
		sourceRegistry = matches[2]
	}

	imageName := matches[3]
	imageTag := "latest"

	if matches[4] != "" {
		imageTag = matches[4]
	}

	return image.BaseImage{
		SourceRegistry: sourceRegistry,
		ImageName:      imageName,
		ImageTag:       imageTag,
	}, nil
}

func (i *ImageClient) args(creds *string) (out []string) {
	if creds != nil && *creds != "" {
		out = append(out, "--src-creds", *creds)
	} else if creds != nil && *creds == "" {
		out = append(out, "--src-no-creds")
	} else {
		out = append(out, "--src-creds", i.Creds)
	}

	if i.CommandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", i.CommandTimeout))
	}

	if i.Debug {
		out = append(out, "--debug")
	}

	return out
}

func (i *ImageClient) unpack(baseImageName string, baseImageTag string, cacheDir string, bundleId string) error {
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

	tmpBundlePath := filepath.Join(cacheDir, "_"+bundleId)
	bundlePath := filepath.Join(cacheDir, bundleId)
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
func (i *ImageClient) Archive(ctx context.Context, bundlePath string, imageTag string, outputChan chan common.OutputMsg) error {
	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s", imageTag, i.registry.ImageFileExtension)
	archivePath := filepath.Join(filepath.Dir(bundlePath), archiveName)

	var err error = nil
	archiveStore := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_REGISTRY_STORE", "s3")
	switch archiveStore {
	case "s3":
		err = clip.CreateAndUploadArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		}, &clipCommon.S3StorageInfo{
			Bucket: common.Secrets().Get("BEAM_IMAGESERVICE_IMAGE_REGISTRY_S3_BUCKET"),
			Region: common.Secrets().Get("BEAM_IMAGESERVICE_IMAGE_REGISTRY_S3_REGION"),
			Key:    fmt.Sprintf("%s.clip", imageTag),
		})
	case "local":
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Printf("unable to create archive: %v\n", err)
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to archive image."}
		return err
	}
	log.Printf("container <%v> archive took %v", imageTag, time.Since(startTime))

	// Push the archive to a registry
	startTime = time.Now()
	err = i.registry.Push(ctx, archivePath, imageTag)
	if err != nil {
		log.Printf("failed to push image for image <%v>: %v", imageTag, err)
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to push image."}
		return err
	}

	log.Printf("container <%v> push took %v", imageTag, time.Since(startTime))
	log.Printf("container <%v> build completed successfully", imageTag)
	return nil
}
