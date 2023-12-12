package worker

import (
	"fmt"
	"os"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/clip/pkg/clip"
)

const awsCredentialProviderName = "aws"

type ImageClient struct {
	registry       *common.ImageRegistry
	legacyRegistry *common.ImageRegistry
	cacheClient    *CacheClient
	puller         *ImagePuller
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

	puller, err := NewImagePuller(provider)
	if err != nil {
		return nil, err
	}

	storeName := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_REGISTRY_STORE", "s3")
	registry, err := common.NewImageRegistry(storeName)
	if err != nil {
		return nil, err
	}

	legacyRegistry, err := common.NewImageRegistry(common.S3LegacyImageRegistryStoreName)
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

	return &ImageClient{
		registry:       registry,
		legacyRegistry: legacyRegistry,
		cacheClient:    cacheClient,
		puller:         puller,
	}, nil
}

const imageAvailableFilename = "IMAGE_AVAILABLE"

func (c *ImageClient) PullLazy(imageTag string) error {
	localCachePath := fmt.Sprintf("%s/%s.cache", imagePath, imageTag)
	remoteArchivePath := fmt.Sprintf("%s/%s.%s", imagePath, imageTag, c.registry.ImageFileExtension)

	var err error = nil
	if _, err := os.Stat(remoteArchivePath); err != nil {
		return err
	}

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           remoteArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", imagePath, imageTag),
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
