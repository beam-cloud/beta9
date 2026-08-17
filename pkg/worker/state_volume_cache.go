package worker

import (
	"context"
	"fmt"
	"sort"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// stateBlockRequiredContentReports authenticates and expands the complete
// ancestry reachable from a committed consistency group. The caller builds it
// before CommitStateSnapshot, then enqueues it only after the atomic commit
// succeeds, so cache locality never advertises a partial generation.
func (s *Worker) stateBlockRequiredContentReports(
	ctx context.Context,
	request *types.ContainerRequest,
	generations []*pb.VolumeGeneration,
	published map[string]BlockV1Manifest,
	cas BlockV1CAS,
) ([]requiredContentReport, error) {
	if s == nil || s.cacheManager == nil || s.cacheManager.ContentReporter() == nil {
		return nil, fmt.Errorf("state volume cache reporter/reconciler is unavailable")
	}
	if request == nil || request.Workspace.ExternalId == "" || request.Stub.ExternalId == "" || s.backendRepoClient == nil || cas == nil {
		return nil, fmt.Errorf("state block cache reporting context is incomplete")
	}

	records := make(map[string]*pb.VolumeGeneration, len(generations))
	heads := make([]*pb.VolumeGeneration, 0, len(generations))
	seenScopes := make(map[string]struct{}, len(generations))
	for _, generation := range generations {
		if generation == nil || generation.ExternalId == "" || generation.VolumeId == "" || generation.Generation <= 0 {
			return nil, fmt.Errorf("state block cache report contains an incomplete generation")
		}
		if _, duplicate := seenScopes[generation.VolumeId]; duplicate {
			return nil, fmt.Errorf("state block cache report contains duplicate lineage scope %q", generation.VolumeId)
		}
		seenScopes[generation.VolumeId] = struct{}{}
		records[generation.ExternalId] = generation
		heads = append(heads, generation)
	}
	sort.Slice(heads, func(i, j int) bool { return heads[i].VolumeId < heads[j].VolumeId })
	resolver := &repositoryBlockV1Resolver{workspaceID: request.Workspace.ExternalId, repository: s.backendRepoClient, cas: cas}
	reports := make([]requiredContentReport, 0, len(heads))
	for _, head := range heads {
		visited := make(map[string]struct{})
		queue := []string{head.ExternalId}
		items := make([]types.CacheRequiredContentItem, 0)
		for len(queue) != 0 {
			generationID := queue[0]
			queue = queue[1:]
			if _, ok := visited[generationID]; ok {
				continue
			}
			visited[generationID] = struct{}{}

			record := records[generationID]
			if record == nil {
				response, err := s.backendRepoClient.GetVolumeGeneration(ctx, &pb.GetVolumeGenerationRequest{
					WorkspaceId: request.Workspace.ExternalId, GenerationId: generationID,
				})
				if err != nil {
					return nil, err
				}
				if response == nil || !response.Ok || response.Generation == nil {
					return nil, fmt.Errorf("state block cache ancestor %q is unavailable", generationID)
				}
				record = response.Generation
				records[generationID] = record
			}

			manifest, ok := published[generationID]
			if !ok {
				var err error
				manifest, err = resolver.ResolveBlockV1Manifest(ctx, generationID)
				if err != nil {
					return nil, err
				}
			} else {
				data, digest, err := EncodeBlockV1ManifestCanonical(manifest)
				if err != nil {
					return nil, err
				}
				if digest != record.ManifestDigest || int64(len(data)) != record.ManifestSizeBytes {
					return nil, fmt.Errorf("state block cache generation %q manifest metadata mismatch", generationID)
				}
			}
			if manifest.GenerationID != generationID || manifest.VolumeID != record.VolumeId ||
				manifest.Generation != record.Generation || manifest.ParentGenerationID != record.ParentGenerationId ||
				manifest.CloneParentGenerationID != record.CloneParentGenerationId ||
				manifest.VirtualSizeBytes != record.LogicalSizeBytes || int64(len(manifest.Chunks)) != record.ChunkCount {
				return nil, fmt.Errorf("state block cache generation %q identity or ancestry mismatch", generationID)
			}
			manifestKey, err := stateBlockObjectKey(record.ManifestDigest)
			if err != nil || manifestKey != record.ManifestKey || record.ManifestSizeBytes <= 0 || record.BucketName == "" {
				return nil, fmt.Errorf("state block cache generation %q has invalid origin metadata", generationID)
			}
			items = append(items, stateBlockRequiredContentItem(
				types.CacheContentKindStateManifest, record.ManifestDigest, record.ManifestSizeBytes,
				record.BucketName, record.VolumeId, generationID,
			))
			for _, chunk := range manifest.Chunks {
				items = append(items, stateBlockRequiredContentItem(
					types.CacheContentKindStateChunk, chunk.Digest, chunk.SizeBytes,
					record.BucketName, record.VolumeId, generationID,
				))
			}
			// A parentless compacted anchor replaces this scope without retaining
			// the old physical chain.
			for _, parentID := range []string{manifest.ParentGenerationID, manifest.CloneParentGenerationID} {
				if parentID != "" {
					queue = append(queue, parentID)
				}
			}
		}
		canonical, _, _, err := types.CanonicalCacheRequiredContentSet(items)
		if err != nil {
			return nil, err
		}
		reports = append(reports, requiredContentReport{
			scope: head.VolumeId, revisionGeneration: head.Generation, revisionID: head.ExternalId,
			items: canonical,
		})
	}
	return reports, nil
}

func stateBlockRequiredContentItem(kind types.CacheContentKind, digest string, size int64, bucket, volumeID, generationID string) types.CacheRequiredContentItem {
	key, _ := stateBlockObjectKey(digest)
	return types.CacheRequiredContentItem{
		Hash: digest, ExpectedHash: digest, RoutingKey: digest, SizeBytes: size,
		Source: key, SourceBucket: bucket, Kind: kind, VolumeID: volumeID, GenerationID: generationID,
	}
}

// reportCommittedStateSnapshotContent reconstructs the authenticated physical
// ancestry from repository metadata and synchronously publishes its cache
// requirements. It is deliberately safe to replay: S2 merges exact hashes,
// and no local Ack/journal retirement happens until this method succeeds.
func (s *Worker) reportCommittedStateSnapshotContent(ctx context.Context, request *types.ContainerRequest, snapshot *pb.StateSnapshot) error {
	if err := s.requireStateVolumeCacheCapability(); err != nil {
		return err
	}
	reporter := s.cacheManager.ContentReporter()
	if request == nil || snapshot == nil || snapshot.Status != string(types.StateSnapshotStatusAvailable) || len(snapshot.Generations) == 0 {
		return fmt.Errorf("available state snapshot cache reporting metadata is incomplete")
	}
	terminal := make([]*pb.VolumeGeneration, 0, len(snapshot.Generations))
	seen := make(map[string]struct{}, len(snapshot.Generations))
	for _, member := range snapshot.Generations {
		if member == nil || member.GenerationId == "" {
			return fmt.Errorf("available state snapshot contains an incomplete generation member")
		}
		if _, ok := seen[member.GenerationId]; ok {
			return fmt.Errorf("available state snapshot repeats generation %q", member.GenerationId)
		}
		seen[member.GenerationId] = struct{}{}
		response, err := s.backendRepoClient.GetVolumeGeneration(ctx, &pb.GetVolumeGenerationRequest{
			WorkspaceId: request.Workspace.ExternalId, GenerationId: member.GenerationId,
		})
		if err != nil {
			return err
		}
		if response == nil || !response.Ok || response.Generation == nil {
			return fmt.Errorf("available state snapshot generation %q is unavailable", member.GenerationId)
		}
		terminal = append(terminal, response.Generation)
	}
	cas, _, err := s.newStateVolumeCAS(ctx, request)
	if err != nil {
		return err
	}
	reports, err := s.stateBlockRequiredContentReports(ctx, request, terminal, nil, cas)
	if err != nil {
		return err
	}
	return reporter.reportBatchesAndFlush(request.Workspace.ExternalId, request.Stub.ExternalId, reports)
}
