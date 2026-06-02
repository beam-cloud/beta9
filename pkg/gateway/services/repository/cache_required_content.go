package repository_services

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const requiredContentEventLimit = 4096

type requiredContentStatusReader interface {
	GetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string) (cache.RequiredContentReconciliationStatus, time.Time, string, bool, error)
}

type persistentEventStore interface {
	PersistentEventStoreConfigured() bool
}

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
	if !requiredContentEventsConfigured(s.eventRepo) {
		return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: repository.ErrEventReadUnsupported.Error()}, nil
	}

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if err := s.requiredContent.MarkStubLocalityAccessed(ctx, req.Locality, req.WorkspaceId, req.StubId, ttl); err != nil {
		return &pb.ReportRequiredContentResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	items := s.requiredContentReportItems(req)

	eventItems := requiredContentEventItems(items)
	if len(eventItems) > 0 {
		s.pushRequiredContentEvent(requiredContentEvent(req.WorkspaceId, req.StubId, req.Locality, eventItems))
	}
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
	if !requiredContentEventsConfigured(s.eventRepo) {
		return &pb.ListRequiredContentForStubResponse{Ok: false, ErrorMsg: repository.ErrEventReadUnsupported.Error()}, nil
	}

	items, err := s.listRequiredContentFromEvents(ctx, req.Locality, req.WorkspaceId, req.StubId, int(req.Limit))
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
	s.pushPlatformCacheEvent(types.EventPlatformCacheSchema{
		Action:      "required_content_status",
		Source:      "gateway",
		Status:      req.Status,
		Result:      req.Status,
		WorkspaceID: req.WorkspaceId,
		StubID:      req.StubId,
		Locality:    req.Locality,
		Hash:        req.Hash,
		RoutingKey:  req.RoutingKey,
		Count:       1,
		Error:       req.ErrorMsg,
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

func (s *WorkerRepositoryService) pushPlatformCacheEvent(event types.EventPlatformCacheSchema) {
	if s == nil || s.eventRepo == nil || event.Action == "" {
		return
	}
	go s.eventRepo.PushPlatformCacheEvent(event)
}

func (s *WorkerRepositoryService) requiredContentReportItems(req *pb.ReportRequiredContentRequest) []cache.RequiredContentItem {
	if req == nil {
		return nil
	}
	byID := map[string]cache.RequiredContentItem{}
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
		item = item.Normalized()
		if item.Hash == "" {
			continue
		}
		byID[requiredContentItemKey(item.Hash, item.RoutingKey)] = item
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]cache.RequiredContentItem, 0, len(ids))
	for _, id := range ids {
		items = append(items, byID[id])
	}
	return items
}

func requiredContentEventItems(items []cache.RequiredContentItem) []types.EventStubCacheRequiredContentItem {
	byID := map[string]types.EventStubCacheRequiredContentItem{}
	for _, item := range items {
		eventItem := requiredContentEventItemFromCacheItem(item)
		id := requiredContentItemKey(eventItem.Hash, eventItem.RoutingKey)
		byID[id] = eventItem
	}
	if len(byID) == 0 {
		return nil
	}

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	eventItems := make([]types.EventStubCacheRequiredContentItem, 0, len(ids))
	for _, id := range ids {
		eventItems = append(eventItems, byID[id])
	}
	return eventItems
}

func (s *WorkerRepositoryService) listRequiredContentFromEvents(ctx context.Context, locality, workspaceID, stubID string, limit int) ([]cache.RequiredContentItem, error) {
	if limit <= 0 {
		limit = cache.DefaultRequiredContentBatchSize
	}
	history, err := s.eventRepo.GetEventHistory(ctx, types.EventQuery{
		WorkspaceID: workspaceID,
		StubID:      stubID,
		EventTypes:  []string{types.EventStubCacheRequiredContent},
		Limit:       requiredContentEventLimit,
	})
	if err != nil {
		return nil, err
	}

	byID := map[string]cache.RequiredContentItem{}
	for _, record := range history.Events {
		if record.Type != types.EventStubCacheRequiredContent || len(record.Data) == 0 {
			continue
		}
		event := types.EventStubCacheRequiredContentSchema{}
		if err := json.Unmarshal(record.Data, &event); err != nil {
			continue
		}
		for _, eventItem := range event.Items {
			item := requiredContentItemFromEventItem(locality, workspaceID, stubID, eventItem)
			if item.Hash == "" {
				continue
			}
			byID[requiredContentItemKey(item.Hash, item.RoutingKey)] = item
		}
	}

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	items := make([]cache.RequiredContentItem, 0, len(ids))
	for _, id := range ids {
		items = append(items, s.withRequiredContentStatus(ctx, byID[id]))
	}
	return items, nil
}

func (s *WorkerRepositoryService) withRequiredContentStatus(ctx context.Context, item cache.RequiredContentItem) cache.RequiredContentItem {
	reader, ok := s.requiredContent.(requiredContentStatusReader)
	if !ok {
		return item
	}
	status, at, errMsg, found, err := reader.GetRequiredContentReconciliationStatus(ctx, item.Locality, item.WorkspaceID, item.StubID, item.Hash, item.RoutingKey)
	if err != nil || !found {
		return item
	}
	item.Status = status
	item.LastStatusAt = at
	item.LastError = errMsg
	return item
}

func requiredContentEvent(workspaceID, stubID, locality string, items []types.EventStubCacheRequiredContentItem) types.EventStubCacheRequiredContentSchema {
	var bytes int64
	var kind string
	for _, item := range items {
		if item.SizeBytes > 0 {
			bytes += item.SizeBytes
		}
		if kind == "" && item.Kind != "" {
			kind = item.Kind
		}
	}
	return types.EventStubCacheRequiredContentSchema{
		WorkspaceID: workspaceID,
		StubID:      stubID,
		Locality:    locality,
		Kind:        kind,
		Status:      string(cache.RequiredContentStatusPending),
		ItemCount:   int64(len(items)),
		Bytes:       bytes,
		Source:      "worker_report",
		Items:       items,
		Timestamp:   time.Now().UTC(),
	}
}

func requiredContentEventItemFromCacheItem(item cache.RequiredContentItem) types.EventStubCacheRequiredContentItem {
	item = item.Normalized()
	return types.EventStubCacheRequiredContentItem{
		Kind:         string(item.Kind),
		Hash:         item.Hash,
		RoutingKey:   item.RoutingKey,
		SizeBytes:    item.SizeBytes,
		ExpectedHash: item.ExpectedHash,
		Source: types.EventStubCacheRequiredContentSource{
			Type:           string(item.Source.Type),
			Descriptor:     item.Source.Descriptor,
			Registry:       item.Source.Registry,
			Repository:     item.Source.Repository,
			Reference:      item.Source.Reference,
			LayerDigest:    item.Source.LayerDigest,
			BucketName:     item.Source.BucketName,
			Region:         item.Source.Region,
			EndpointURL:    item.Source.EndpointURL,
			ObjectPath:     item.Source.ObjectPath,
			ForcePathStyle: item.Source.ForcePathStyle,
		},
	}
}

func requiredContentItemFromEventItem(locality, workspaceID, stubID string, eventItem types.EventStubCacheRequiredContentItem) cache.RequiredContentItem {
	return cache.RequiredContentItem{
		Locality:     locality,
		WorkspaceID:  workspaceID,
		StubID:       stubID,
		Kind:         cache.RequiredContentKind(eventItem.Kind),
		Hash:         eventItem.Hash,
		RoutingKey:   eventItem.RoutingKey,
		SizeBytes:    eventItem.SizeBytes,
		ExpectedHash: eventItem.ExpectedHash,
		Source: cache.RequiredContentSource{
			Type:           cache.RequiredContentSourceType(eventItem.Source.Type),
			Descriptor:     eventItem.Source.Descriptor,
			Registry:       eventItem.Source.Registry,
			Repository:     eventItem.Source.Repository,
			Reference:      eventItem.Source.Reference,
			LayerDigest:    eventItem.Source.LayerDigest,
			BucketName:     eventItem.Source.BucketName,
			Region:         eventItem.Source.Region,
			EndpointURL:    eventItem.Source.EndpointURL,
			ObjectPath:     eventItem.Source.ObjectPath,
			ForcePathStyle: eventItem.Source.ForcePathStyle,
		},
	}.Normalized()
}

func requiredContentItemKey(hash, routingKey string) string {
	if routingKey == "" {
		routingKey = hash
	}
	return hash + "\x00" + routingKey
}

func requiredContentEventsConfigured(eventRepo repository.EventRepository) bool {
	if eventRepo == nil {
		return false
	}
	store, ok := eventRepo.(persistentEventStore)
	return !ok || store.PersistentEventStoreConfigured()
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
