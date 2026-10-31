package repository_services

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type workerImageGrantReader interface {
	Get(context.Context, string, string) (*repository.WorkerImageGrant, error)
}

func (s *WorkerRepositoryService) assignedImageGrant(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) (*repository.WorkerImageGrant, error) {
	info, ok := auth.AuthInfoFromContext(ctx)
	if !ok || info == nil || info.Token == nil || !types.IsWorkerTokenType(info.Token.TokenType) {
		return nil, nil
	}
	if req.ContainerId == "" || info.Token.WorkerID == "" || s.imageGrants == nil {
		return nil, fmt.Errorf("container image assignment is required")
	}
	state, err := containerStateForWorker(s.containerRepo, req.ContainerId, info.Token.WorkerID)
	if err != nil {
		return nil, err
	}
	if state.WorkspaceId != req.WorkspaceId || state.StubId != req.StubId {
		return nil, fmt.Errorf("image workspace/stub does not match assignment")
	}
	grant, err := s.imageGrants.Get(ctx, info.Token.WorkerID, req.ContainerId)
	if err != nil || grant == nil || grant.WorkspaceID != state.WorkspaceId || grant.StubID != state.StubId {
		return nil, fmt.Errorf("image grant does not match assignment")
	}
	if req.ImageId != "" && !imageGrantReads(grant, req.ImageId, s.appConfig.ImageService.EffectiveBuildRegistry(), s.appConfig.ImageService.BuildRepositoryName) {
		return nil, fmt.Errorf("image is not assigned to worker")
	}
	return grant, nil
}

func imageGrantReads(grant *repository.WorkerImageGrant, imageID, registry, repo string) bool {
	if imageID == grant.ImageID || imageID == grant.OutputImageID {
		return true
	}
	prefix := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(registry, "https://"), "http://"), "/") + "/" + strings.Trim(repo, "/") + ":"
	if strings.HasPrefix(grant.SourceImage, prefix) && strings.TrimPrefix(grant.SourceImage, prefix) == imageID {
		return true
	}
	prefix = strings.TrimSuffix(prefix, ":") + "@"
	return validRegistryDigest(imageID) && grant.SourceImage == prefix+imageID
}

func (s *WorkerRepositoryService) workerRegistryCredentials(ctx context.Context, req *pb.GetCacheOriginCredentialsRequest) string {
	if s.isBuildRegistry(req.Registry) {
		if info, ok := auth.AuthInfoFromContext(ctx); ok && info != nil && info.Token != nil {
			return req.ContainerId + ":" + info.Token.Key
		}
		return "cache:" + s.cacheCoordinatorToken
	}
	if info, ok := auth.AuthInfoFromContext(ctx); ok && info != nil && info.Token != nil && s.imageGrants != nil {
		if grant, err := s.imageGrants.Get(ctx, info.Token.WorkerID, req.ContainerId); err == nil {
			if req.ImageId == grant.ImageID && grant.ImageCredentials != "" {
				return grant.ImageCredentials
			}
			if registryFromReference(grant.SourceImage) == req.Registry {
				return grant.SourceCredentials
			}
		}
	}
	return s.imageRegistryCredentials(ctx, req.WorkspaceId, req.ImageId)
}

func registryFromReference(ref string) string {
	ref = strings.TrimPrefix(strings.TrimPrefix(ref, "https://"), "http://")
	host, _, _ := strings.Cut(ref, "/")
	return host
}

func (s *WorkerRepositoryService) isBuildRegistry(registry string) bool {
	if registry == "" {
		return false
	}
	if registryHostsEqual(registry, s.appConfig.ImageService.EffectiveBuildRegistry()) {
		return true
	}
	if registryHostsEqual(registry, s.appConfig.GatewayService.HTTP.GetExternalURL()) {
		return true
	}
	cfg := s.appConfig
	cfg.Worker.UseGatewayServiceHostname = true
	local := cfg.GatewayService.Host
	if local != "" && registryHostsEqual(registry, fmt.Sprintf("%s:%d", local, cfg.GatewayService.HTTP.Port)) {
		return true
	}
	return false
}
