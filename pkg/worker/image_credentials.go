package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/rs/zerolog/log"
)

func (c *ImageClient) buildahAuthFileArgs(ctx context.Context, request *types.ContainerRequest, imageRef, runroot string) ([]string, error) {
	args := c.buildahAuthArgs(ctx, request, imageRef)
	if len(args) == 0 {
		return nil, nil
	}
	registry := reg.ParseRegistry(imageRef)
	if registry == "" {
		return nil, fmt.Errorf("registry credentials require a source image")
	}
	username, password, _ := strings.Cut(args[1], ":")
	config := struct {
		Auths map[string]authn.AuthConfig `json:"auths"`
	}{Auths: map[string]authn.AuthConfig{registry: {Username: username, Password: password}}}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	// The private runroot is outside the build context and RUN mounts.
	path := filepath.Join(runroot, "auth.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return nil, err
	}
	return []string{"--authfile", path}, nil
}

// gatewayCredentialProviderForImage vends registry credentials from the
// gateway on first use. Registry access only happens on a content cache miss,
// so the round trip stays off the mount path. When the gateway has none,
// fallback answers instead.
func (c *ImageClient) gatewayCredentialProviderForImage(imageID, registry string, request *types.ContainerRequest, fallback clipCommon.RegistryCredentialProvider) clipCommon.RegistryCredentialProvider {
	return &gatewayRegistryCredentialProvider{
		client:         c,
		request:        request,
		imageID:        imageID,
		registry:       registry,
		fallback:       fallback,
		preventAmbient: c.brokeredImageAccessRequest(request),
	}
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
	key := strings.Join([]string{request.ContainerId, workspaceID, stubID, imageID, registry}, "\x00")
	c.originCredsMu.Lock()
	if cached, ok := c.originCredsCache[key]; ok && time.Since(cached.fetchedAt) < originCredentialsTTL {
		c.originCredsMu.Unlock()
		return cached
	}
	c.originCredsMu.Unlock()

	resp, err := handleGRPCResponse(c.workerRepoClient.GetCacheOriginCredentials(ctx, &pb.GetCacheOriginCredentialsRequest{
		ContainerId: request.ContainerId,
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
	if c.originCredsCache == nil {
		c.originCredsCache = make(map[string]*originCredentials)
	}
	c.originCredsCache[key] = creds
	c.originCredsMu.Unlock()
	return creds
}

type gatewayRegistryCredentialProvider struct {
	client         *ImageClient
	request        *types.ContainerRequest
	imageID        string
	registry       string
	mu             sync.Mutex
	provider       clipCommon.RegistryCredentialProvider
	fallback       clipCommon.RegistryCredentialProvider
	credentialsAt  time.Time
	preventAmbient bool
}

func (p *gatewayRegistryCredentialProvider) GetCredentials(ctx context.Context, registry, scope string) (*authn.AuthConfig, error) {
	creds := p.client.originCredentials(ctx, p.request, p.imageID, p.registry)

	p.mu.Lock()
	defer p.mu.Unlock()
	if creds != nil && creds.fetchedAt.After(p.credentialsAt) {
		// A newer answer from the gateway replaces whatever was cached,
		// including an answer with no credentials: they were revoked or have
		// expired, so the fallback must take over rather than the stale
		// provider being reused. A failed fetch (creds == nil) keeps the
		// cached provider, since nothing newer is known.
		p.provider = nil
		if creds.registryCredentials != "" {
			p.provider = p.client.parseAndCreateProvider(ctx, creds.registryCredentials, p.registry, p.imageID, "gateway-vended")
		}
		p.credentialsAt = creds.fetchedAt
	}
	if p.provider == nil {
		if p.fallback == nil {
			return nil, clipCommon.ErrNoCredentials
		}
		return p.fallback.GetCredentials(ctx, registry, scope)
	}
	authConfig, err := p.provider.GetCredentials(ctx, registry, scope)
	if p.preventAmbient && (err != nil || authConfig == nil) {
		return &authn.AuthConfig{}, nil
	}
	return authConfig, err
}

func (*gatewayRegistryCredentialProvider) Name() string {
	return "gateway-vended"
}

func registryFromImageRef(imageRef string) string {
	registry := reg.ParseRegistry(imageRef)
	if registry == "" {
		return ""
	}
	return registry
}

type privateWorkerAnonymousRegistryProvider struct{}

func (privateWorkerAnonymousRegistryProvider) GetCredentials(context.Context, string, string) (*authn.AuthConfig, error) {
	// Return an explicit empty auth config instead of ErrNoCredentials. CLIP
	// treats ErrNoCredentials as permission to use the default keychain, but
	// private workers must not use ambient node credentials.
	return &authn.AuthConfig{}, nil
}

func (privateWorkerAnonymousRegistryProvider) Name() string {
	return "private-worker-anonymous"
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

// brokerImageReference preserves the repository and content address while
// directing configured build-registry traffic through the gateway.
func (c *ImageClient) brokerImageReference(ref string) string {
	upstream := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(c.config.ImageService.UpstreamBuildRegistry, "https://"), "http://"), "/")
	if upstream != "" && strings.HasPrefix(ref, upstream+"/") {
		return c.getBuildRegistry() + strings.TrimPrefix(ref, upstream)
	}
	return ref
}
func (c *ImageClient) brokerBuildRequest(ctx context.Context, request *types.ContainerRequest) *types.ContainerRequest {
	request = request.Clone()
	if request.BuildOptions.SourceImage != nil {
		source := c.brokerImageReference(*request.BuildOptions.SourceImage)
		request.BuildOptions.SourceImage = &source
		request.BuildOptions.SourceImageCreds = c.gatewayRegistryCredentials(ctx, reg.ParseRegistry(source), request)
	}
	return request
}
