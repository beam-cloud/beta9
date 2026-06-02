package repository_services

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (s *WorkerRepositoryService) ReportRequiredContent(ctx context.Context, req *pb.ReportRequiredContentRequest) (*pb.ReportRequiredContentResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.requiredContent == nil {
		return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if err := s.requiredContent.MarkStubLocalityAccessed(ctx, req.Locality, req.WorkspaceId, req.StubId, ttl); err != nil {
		return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	var itemCount int64
	var bytes int64
	var kind string
	for _, pbItem := range req.Items {
		item := requiredContentItemFromProto(pbItem)
		if item.Locality == "" {
			item.Locality = req.Locality
		}
		if item.WorkspaceID == "" {
			item.WorkspaceID = req.WorkspaceId
		}
		if item.StubID == "" {
			item.StubID = req.StubId
		}
		if err := s.requiredContent.UpsertRequiredContent(ctx, item, ttl); err != nil {
			return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
		itemCount++
		if item.SizeBytes > 0 {
			bytes += item.SizeBytes
		}
		if kind == "" && item.Kind != "" {
			kind = string(item.Kind)
		}
	}
	s.pushRequiredContentEvent(types.EventStubCacheRequiredContentSchema{
		WorkspaceID: req.WorkspaceId,
		StubID:      req.StubId,
		Locality:    req.Locality,
		Kind:        kind,
		Status:      string(cache.RequiredContentStatusPending),
		ItemCount:   itemCount,
		Bytes:       bytes,
		Source:      "report",
		Timestamp:   time.Now().UTC(),
	})
	return &pb.ReportRequiredContentResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) ListRecentRequiredContentStubs(ctx context.Context, req *pb.ListRecentRequiredContentStubsRequest) (*pb.ListRecentRequiredContentStubsResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ListRecentRequiredContentStubsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.requiredContent == nil {
		return &pb.ListRecentRequiredContentStubsResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.ListRecentRequiredContentStubsResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	since := time.Time{}
	if req.SinceUnixMs > 0 {
		since = time.UnixMilli(req.SinceUnixMs).UTC()
	}
	stubs, err := s.requiredContent.ListRecentStubLocalities(ctx, req.Locality, since, int(req.Limit))
	if err != nil {
		return &pb.ListRecentRequiredContentStubsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	resp := &pb.ListRecentRequiredContentStubsResponse{Ok: true, Stubs: make([]*pb.RequiredContentStubLocality, 0, len(stubs))}
	for _, stub := range stubs {
		resp.Stubs = append(resp.Stubs, requiredContentStubToProto(stub))
	}
	return resp, nil
}

func (s *WorkerRepositoryService) ListRequiredContentForStub(ctx context.Context, req *pb.ListRequiredContentForStubRequest) (*pb.ListRequiredContentForStubResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ListRequiredContentForStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.requiredContent == nil {
		return &pb.ListRequiredContentForStubResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.ListRequiredContentForStubResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	items, err := s.requiredContent.ListRequiredContentForStub(ctx, req.Locality, req.WorkspaceId, req.StubId, int(req.Limit))
	if err != nil {
		return &pb.ListRequiredContentForStubResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	resp := &pb.ListRequiredContentForStubResponse{Ok: true, Items: make([]*pb.RequiredContentItem, 0, len(items))}
	for _, item := range items {
		resp.Items = append(resp.Items, requiredContentItemToProto(item))
	}
	return resp, nil
}

func (s *WorkerRepositoryService) SetRequiredContentReconciliationStatus(ctx context.Context, req *pb.SetRequiredContentReconciliationStatusRequest) (*pb.SetRequiredContentReconciliationStatusResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.SetRequiredContentReconciliationStatusResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if s.requiredContent == nil {
		return &pb.SetRequiredContentReconciliationStatusResponse{Ok: false, ErrorMsg: cache.ErrCoordinatorUnavailable.Error()}, nil
	}
	if req == nil {
		return &pb.SetRequiredContentReconciliationStatusResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}
	if err := s.requiredContent.SetRequiredContentReconciliationStatus(
		ctx,
		req.Locality,
		req.WorkspaceId,
		req.StubId,
		req.Hash,
		req.RoutingKey,
		cache.RequiredContentReconciliationStatus(req.Status),
		req.ErrorMsg,
		time.Duration(req.TtlMs)*time.Millisecond,
	); err != nil {
		return &pb.SetRequiredContentReconciliationStatusResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	s.pushRequiredContentEvent(types.EventStubCacheRequiredContentSchema{
		WorkspaceID: req.WorkspaceId,
		StubID:      req.StubId,
		Locality:    req.Locality,
		Status:      req.Status,
		ItemCount:   1,
		Error:       req.ErrorMsg,
		Source:      "lifecycle",
		Timestamp:   time.Now().UTC(),
	})
	return &pb.SetRequiredContentReconciliationStatusResponse{Ok: true}, nil
}

func (s *WorkerRepositoryService) pushRequiredContentEvent(event types.EventStubCacheRequiredContentSchema) {
	if s == nil || s.eventRepo == nil || event.WorkspaceID == "" || event.StubID == "" {
		return
	}
	go s.eventRepo.PushStubCacheRequiredContentEvent(event)
}

func (s *WorkerRepositoryService) ResolveRequiredContentOrigin(ctx context.Context, req *pb.ResolveRequiredContentOriginRequest) (*pb.ResolveRequiredContentOriginResponse, error) {
	if err := s.authorizeCacheRepositoryRequest(ctx); err != nil {
		return &pb.ResolveRequiredContentOriginResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if req == nil || req.Item == nil {
		return &pb.ResolveRequiredContentOriginResponse{Ok: false, ErrorMsg: "item is required"}, nil
	}

	item := requiredContentItemFromProto(req.Item)
	if item.Source.Type == cache.RequiredContentSourceOCIRegistry {
		originPath := cache.OCIRequiredContentOriginPath(item.Source)
		if originPath == "" {
			return &pb.ResolveRequiredContentOriginResponse{Ok: true, OriginAvailable: false}, nil
		}
		expectedHash := item.ExpectedHash
		if expectedHash == "" {
			expectedHash = item.Hash
		}
		return &pb.ResolveRequiredContentOriginResponse{
			Ok:              true,
			OriginAvailable: true,
			Origin: &pb.RequiredContentOrigin{
				Path:         originPath,
				CachePath:    item.RoutingKey,
				ExpectedHash: expectedHash,
			},
		}, nil
	}

	if item.Source.Type != cache.RequiredContentSourceS3 || item.Source.ObjectPath == "" || s.backendRepo == nil {
		return &pb.ResolveRequiredContentOriginResponse{Ok: true, OriginAvailable: false}, nil
	}

	workspace, err := s.backendRepo.GetWorkspaceByExternalId(ctx, item.WorkspaceID)
	if err != nil {
		return &pb.ResolveRequiredContentOriginResponse{Ok: true, OriginAvailable: false}, nil
	}
	fullWorkspace, err := s.backendRepo.GetWorkspace(ctx, workspace.Id)
	if err != nil || fullWorkspace == nil || !fullWorkspace.StorageAvailable() {
		return &pb.ResolveRequiredContentOriginResponse{Ok: true, OriginAvailable: false}, nil
	}

	storage := fullWorkspace.Storage
	endpointURL := firstNonEmptyString(item.Source.EndpointURL, ptrString(storage.EndpointUrl))
	origin := &pb.RequiredContentOrigin{
		Path:           item.Source.ObjectPath,
		BucketName:     firstNonEmptyString(item.Source.BucketName, ptrString(storage.BucketName)),
		Region:         firstNonEmptyString(item.Source.Region, ptrString(storage.Region)),
		EndpointUrl:    endpointURL,
		AccessKey:      ptrString(storage.AccessKey),
		SecretKey:      ptrString(storage.SecretKey),
		CachePath:      item.RoutingKey,
		ForcePathStyle: item.Source.ForcePathStyle || endpointURL != "",
		ExpectedHash:   item.ExpectedHash,
	}
	if origin.ExpectedHash == "" {
		origin.ExpectedHash = item.Hash
	}
	if origin.BucketName == "" || origin.AccessKey == "" || origin.SecretKey == "" {
		return &pb.ResolveRequiredContentOriginResponse{Ok: true, OriginAvailable: false}, nil
	}

	// Origin credentials are returned only in this short-lived RPC response.
	// Required-content Redis records carry non-secret source descriptors only.
	return &pb.ResolveRequiredContentOriginResponse{Ok: true, OriginAvailable: true, Origin: origin}, nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func ptrString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func requiredContentItemFromProto(item *pb.RequiredContentItem) cache.RequiredContentItem {
	if item == nil {
		return cache.RequiredContentItem{}
	}
	return cache.RequiredContentItem{
		Locality:     item.Locality,
		WorkspaceID:  item.WorkspaceId,
		StubID:       item.StubId,
		Kind:         cache.RequiredContentKind(item.Kind),
		Hash:         item.Hash,
		RoutingKey:   item.RoutingKey,
		SizeBytes:    item.SizeBytes,
		ExpectedHash: item.ExpectedHash,
		Source:       requiredContentSourceFromProto(item.Source),
		FirstSeen:    unixMillisToTime(item.FirstSeenUnixMs),
		LastSeen:     unixMillisToTime(item.LastSeenUnixMs),
		AccessCount:  item.AccessCount,
		Status:       cache.RequiredContentReconciliationStatus(item.Status),
		LastStatusAt: unixMillisToTime(item.LastStatusUnixMs),
		LastError:    item.LastError,
	}.Normalized()
}

func requiredContentItemToProto(item cache.RequiredContentItem) *pb.RequiredContentItem {
	item = item.Normalized()
	return &pb.RequiredContentItem{
		Locality:         item.Locality,
		WorkspaceId:      item.WorkspaceID,
		StubId:           item.StubID,
		Kind:             string(item.Kind),
		Hash:             item.Hash,
		RoutingKey:       item.RoutingKey,
		SizeBytes:        item.SizeBytes,
		ExpectedHash:     item.ExpectedHash,
		Source:           requiredContentSourceToProto(item.Source),
		FirstSeenUnixMs:  timeToUnixMillis(item.FirstSeen),
		LastSeenUnixMs:   timeToUnixMillis(item.LastSeen),
		AccessCount:      item.AccessCount,
		Status:           string(item.Status),
		LastStatusUnixMs: timeToUnixMillis(item.LastStatusAt),
		LastError:        item.LastError,
	}
}

func requiredContentSourceFromProto(source *pb.RequiredContentSource) cache.RequiredContentSource {
	if source == nil {
		return cache.RequiredContentSource{}
	}
	return cache.RequiredContentSource{
		Type:           cache.RequiredContentSourceType(source.Type),
		Descriptor:     source.GetDescriptor_(),
		Registry:       source.Registry,
		Repository:     source.Repository,
		Reference:      source.Reference,
		LayerDigest:    source.LayerDigest,
		BucketName:     source.BucketName,
		Region:         source.Region,
		EndpointURL:    source.EndpointUrl,
		ObjectPath:     source.ObjectPath,
		ForcePathStyle: source.ForcePathStyle,
	}
}

func requiredContentSourceToProto(source cache.RequiredContentSource) *pb.RequiredContentSource {
	return &pb.RequiredContentSource{
		Type:           string(source.Type),
		Descriptor_:    source.Descriptor,
		Registry:       source.Registry,
		Repository:     source.Repository,
		Reference:      source.Reference,
		LayerDigest:    source.LayerDigest,
		BucketName:     source.BucketName,
		Region:         source.Region,
		EndpointUrl:    source.EndpointURL,
		ObjectPath:     source.ObjectPath,
		ForcePathStyle: source.ForcePathStyle,
	}
}

func requiredContentStubToProto(stub cache.RequiredContentStubLocality) *pb.RequiredContentStubLocality {
	return &pb.RequiredContentStubLocality{
		Locality:       stub.Locality,
		WorkspaceId:    stub.WorkspaceID,
		StubId:         stub.StubID,
		LastSeenUnixMs: timeToUnixMillis(stub.LastSeen),
	}
}

func timeToUnixMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMilli()
}

func unixMillisToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
