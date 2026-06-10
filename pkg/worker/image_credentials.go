package worker

import (
	"context"
	"strings"
	"time"

	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/rs/zerolog/log"
)

func (c *ImageClient) gatewayCredentialProviderForImage(ctx context.Context, imageID, registry string, request *types.ContainerRequest) clipCommon.RegistryCredentialProvider {
	creds := c.originCredentials(ctx, request, imageID, registry)
	if creds == nil || creds.registryCredentials == "" {
		return nil
	}
	return c.parseAndCreateProvider(ctx, creds.registryCredentials, registry, imageID, "gateway-vended")
}

func (c *ImageClient) gatewayRegistryCredentials(ctx context.Context, registry string, request *types.ContainerRequest) string {
	if registry == "" {
		return ""
	}
	creds := c.originCredentials(ctx, request, request.ImageId, registry)
	if creds == nil {
		return ""
	}
	return creds.registryCredentials
}

func (c *ImageClient) originCredentials(ctx context.Context, request *types.ContainerRequest, imageID, registry string) *originCredentials {
	if c.workerRepoClient == nil || request == nil {
		return nil
	}

	workspaceID := cacheRequestWorkspaceID(request)
	if workspaceID == "" {
		return nil
	}

	stubID := cacheRequestStubID(request)
	key := strings.Join([]string{workspaceID, stubID, imageID, registry}, "\x00")
	c.originCredsMu.Lock()
	if cached, ok := c.originCredsCache[key]; ok && time.Since(cached.fetchedAt) < originCredentialsTTL {
		c.originCredsMu.Unlock()
		return cached
	}
	c.originCredsMu.Unlock()

	resp, err := handleGRPCResponse(c.workerRepoClient.GetCacheOriginCredentials(ctx, &pb.GetCacheOriginCredentialsRequest{
		WorkspaceId: workspaceID,
		StubId:      stubID,
		Registry:    registry,
		ImageId:     imageID,
	}))
	if err != nil {
		log.Debug().
			Err(err).
			Str("workspace_id", workspaceID).
			Str("stub_id", stubID).
			Str("image_id", imageID).
			Str("registry", registry).
			Msg("failed to fetch image origin credentials")
		return nil
	}

	creds := &originCredentials{
		registryCredentials:   resp.RegistryCredentials,
		workspaceStorage:      resp.WorkspaceStorage,
		imageArchiveStorage:   resp.ImageArchiveStorage,
		imageArchiveObjectKey: resp.ImageArchiveObjectKey,
		imageArchiveURL:       resp.ImageArchiveUrl,
		imageArchiveDataURL:   resp.ImageArchiveDataUrl,
		fetchedAt:             time.Now(),
	}
	c.originCredsMu.Lock()
	c.originCredsCache[key] = creds
	c.originCredsMu.Unlock()
	return creds
}

func registryFromImageRef(imageRef string) string {
	registry := reg.ParseRegistry(imageRef)
	if registry == "" {
		return ""
	}
	return registry
}

func imageArchiveRegistryConfig(creds *pb.CacheWorkspaceStorageCredentials) types.S3ImageRegistryConfig {
	if creds == nil {
		return types.S3ImageRegistryConfig{}
	}
	return types.S3ImageRegistryConfig{
		BucketName:     creds.BucketName,
		Region:         creds.Region,
		AccessKey:      creds.AccessKey,
		SecretKey:      creds.SecretKey,
		Endpoint:       creds.EndpointUrl,
		ForcePathStyle: creds.ForcePathStyle,
	}
}
