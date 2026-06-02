package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	pb "github.com/beam-cloud/beta9/proto"
)

type gatewayCacheMetadataStore struct {
	client pb.WorkerRepositoryServiceClient
}

func newGatewayCacheMetadataStore(client pb.WorkerRepositoryServiceClient) cache.CacheMetadataStore {
	return &gatewayCacheMetadataStore{client: client}
}

func (s *gatewayCacheMetadataStore) SetClientLock(ctx context.Context, hash string, host string) error {
	_, err := handleGRPCResponse(s.client.SetCacheClientLock(ctx, &pb.SetCacheClientLockRequest{Hash: hash, HostId: host}))
	return err
}

func (s *gatewayCacheMetadataStore) RemoveClientLock(ctx context.Context, hash string, host string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheClientLock(ctx, &pb.RemoveCacheClientLockRequest{Hash: hash, HostId: host}))
	return err
}

func (s *gatewayCacheMetadataStore) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_, err := handleGRPCResponse(s.client.SetCacheStoreFromContentLock(ctx, &pb.SetCacheStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath}))
	return err
}

func (s *gatewayCacheMetadataStore) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheStoreFromContentLock(ctx, &pb.RemoveCacheStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath}))
	return err
}

func (s *gatewayCacheMetadataStore) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_, err := handleGRPCResponse(s.client.RefreshCacheStoreFromContentLock(ctx, &pb.RefreshCacheStoreFromContentLockRequest{Locality: locality, SourcePath: sourcePath}))
	return err
}

func (s *gatewayCacheMetadataStore) SetFsNode(ctx context.Context, id string, metadata *cache.FSMetadata) error {
	_, err := handleGRPCResponse(s.client.SetCacheFsNode(ctx, &pb.SetCacheFsNodeRequest{Id: id, Metadata: metadata.ToWorkerCacheProto()}))
	return err
}

func (s *gatewayCacheMetadataStore) GetFsNode(ctx context.Context, id string) (*cache.FSMetadata, error) {
	resp, err := handleGRPCResponse(s.client.GetCacheFsNode(ctx, &pb.GetCacheFsNodeRequest{Id: id}))
	if err != nil {
		return nil, err
	}
	return cache.FSMetadataFromWorkerCacheProto(resp.Metadata), nil
}

func (s *gatewayCacheMetadataStore) RemoveFsNode(ctx context.Context, id string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheFsNode(ctx, &pb.RemoveCacheFsNodeRequest{Id: id}))
	return err
}

func (s *gatewayCacheMetadataStore) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	_, err := handleGRPCResponse(s.client.RemoveCacheFsNodeChild(ctx, &pb.RemoveCacheFsNodeChildRequest{Pid: pid, Id: id}))
	return err
}

func (s *gatewayCacheMetadataStore) GetFsNodeChildren(ctx context.Context, id string) ([]*cache.FSMetadata, error) {
	resp, err := handleGRPCResponse(s.client.GetCacheFsNodeChildren(ctx, &pb.GetCacheFsNodeChildrenRequest{Id: id}))
	if err != nil {
		return nil, err
	}
	children := make([]*cache.FSMetadata, 0, len(resp.Children))
	for _, child := range resp.Children {
		children = append(children, cache.FSMetadataFromWorkerCacheProto(child))
	}
	return children, nil
}

func (s *gatewayCacheMetadataStore) AddFsNodeChild(ctx context.Context, pid, id string) error {
	_, err := handleGRPCResponse(s.client.AddCacheFsNodeChild(ctx, &pb.AddCacheFsNodeChildRequest{Pid: pid, Id: id}))
	return err
}

func (s *gatewayCacheMetadataStore) GetAvailableHosts(ctx context.Context, locality string) ([]*cache.Host, error) {
	if s == nil || s.client == nil {
		return nil, cache.ErrHostNotFound
	}
	resp, err := handleGRPCResponse(s.client.ListCacheHosts(ctx, &pb.ListCacheHostsRequest{Locality: locality}))
	if err != nil {
		return nil, err
	}
	hosts := make([]*cache.Host, 0, len(resp.Hosts))
	for _, host := range resp.Hosts {
		if host == nil || host.LogicalHostId == "" {
			continue
		}
		hosts = append(hosts, &cache.Host{
			HostId:           host.LogicalHostId,
			RegistrationID:   host.RegistrationId,
			PoolName:         host.PoolName,
			Locality:         host.Locality,
			NodeID:           host.NodeId,
			CachePathID:      host.CachePathId,
			Addr:             host.Addr,
			PrivateAddr:      host.PrivateAddr,
			CapacityUsagePct: float64(host.CapacityUsagePct),
		})
	}
	if len(hosts) == 0 {
		return nil, cache.ErrHostNotFound
	}
	return hosts, nil
}

func (s *gatewayCacheMetadataStore) MarkStubLocalityAccessed(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	_, err := handleGRPCResponse(s.client.ReportRequiredContent(ctx, &pb.ReportRequiredContentRequest{
		Locality:    locality,
		WorkspaceId: workspaceID,
		StubId:      stubID,
		TtlMs:       ttl.Milliseconds(),
	}))
	return err
}

func (s *gatewayCacheMetadataStore) UpsertRequiredContent(ctx context.Context, item cache.RequiredContentItem, ttl time.Duration) error {
	item = item.Normalized()
	return s.UpsertRequiredContentBatch(ctx, []cache.RequiredContentItem{item}, ttl)
}

func (s *gatewayCacheMetadataStore) UpsertRequiredContentBatch(ctx context.Context, items []cache.RequiredContentItem, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	first := items[0].Normalized()
	pbItems := make([]*pb.RequiredContentItem, 0, len(items))
	for _, item := range items {
		item = item.Normalized()
		if item.Locality == "" {
			item.Locality = first.Locality
		}
		if item.WorkspaceID == "" {
			item.WorkspaceID = first.WorkspaceID
		}
		if item.StubID == "" {
			item.StubID = first.StubID
		}
		pbItems = append(pbItems, requiredContentItemToProto(item))
	}
	_, err := handleGRPCResponse(s.client.ReportRequiredContent(ctx, &pb.ReportRequiredContentRequest{
		Locality:    first.Locality,
		WorkspaceId: first.WorkspaceID,
		StubId:      first.StubID,
		Items:       pbItems,
		TtlMs:       ttl.Milliseconds(),
	}))
	return err
}

func (s *gatewayCacheMetadataStore) ListRecentStubLocalities(ctx context.Context, locality string, since time.Time, limit int) ([]cache.RequiredContentStubLocality, error) {
	resp, err := handleGRPCResponse(s.client.ListRecentRequiredContentStubs(ctx, &pb.ListRecentRequiredContentStubsRequest{
		Locality:    locality,
		SinceUnixMs: timeToUnixMillis(since),
		Limit:       int32(limit),
	}))
	if err != nil {
		return nil, err
	}
	stubs := make([]cache.RequiredContentStubLocality, 0, len(resp.Stubs))
	for _, stub := range resp.Stubs {
		if stub == nil {
			continue
		}
		stubs = append(stubs, cache.RequiredContentStubLocality{
			Locality:    stub.Locality,
			WorkspaceID: stub.WorkspaceId,
			StubID:      stub.StubId,
			LastSeen:    unixMillisToTime(stub.LastSeenUnixMs),
		})
	}
	return stubs, nil
}

func (s *gatewayCacheMetadataStore) ListRequiredContentForStub(ctx context.Context, locality, workspaceID, stubID string, limit int) ([]cache.RequiredContentItem, error) {
	resp, err := handleGRPCResponse(s.client.ListRequiredContentForStub(ctx, &pb.ListRequiredContentForStubRequest{
		Locality:    locality,
		WorkspaceId: workspaceID,
		StubId:      stubID,
		Limit:       int32(limit),
	}))
	if err != nil {
		return nil, err
	}
	items := make([]cache.RequiredContentItem, 0, len(resp.Items))
	for _, item := range resp.Items {
		items = append(items, requiredContentItemFromProto(item))
	}
	return items, nil
}

func (s *gatewayCacheMetadataStore) SetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string, status cache.RequiredContentReconciliationStatus, errorMsg string, ttl time.Duration) error {
	_, err := handleGRPCResponse(s.client.SetRequiredContentReconciliationStatus(ctx, &pb.SetRequiredContentReconciliationStatusRequest{
		Locality:    locality,
		WorkspaceId: workspaceID,
		StubId:      stubID,
		Hash:        hash,
		RoutingKey:  routingKey,
		Status:      string(status),
		ErrorMsg:    errorMsg,
		TtlMs:       ttl.Milliseconds(),
	}))
	return err
}

func (s *gatewayCacheMetadataStore) AcquireRequiredContentReconciliationLock(ctx context.Context, locality, logicalHostID, hash string, ttl time.Duration) (cache.RequiredContentReconciliationLock, bool, error) {
	sourcePath := requiredContentGatewayLockKey(logicalHostID, hash)
	if err := s.SetStoreFromContentLock(ctx, locality, sourcePath); err != nil {
		return nil, false, nil
	}
	return &gatewayRequiredContentLock{store: s, locality: locality, sourcePath: sourcePath}, true, nil
}

func (s *gatewayCacheMetadataStore) ResolveRequiredContentOrigin(ctx context.Context, item cache.RequiredContentItem) (cache.RequiredContentOriginInstruction, bool, error) {
	resp, err := handleGRPCResponse(s.client.ResolveRequiredContentOrigin(ctx, &pb.ResolveRequiredContentOriginRequest{Item: requiredContentItemToProto(item)}))
	if err != nil {
		return cache.RequiredContentOriginInstruction{}, false, err
	}
	if resp.Origin == nil || !resp.OriginAvailable {
		return cache.RequiredContentOriginInstruction{}, false, nil
	}
	return cache.RequiredContentOriginInstruction{
		Path:           resp.Origin.Path,
		BucketName:     resp.Origin.BucketName,
		Region:         resp.Origin.Region,
		EndpointURL:    resp.Origin.EndpointUrl,
		AccessKey:      resp.Origin.AccessKey,
		SecretKey:      resp.Origin.SecretKey,
		CachePath:      resp.Origin.CachePath,
		ForcePathStyle: resp.Origin.ForcePathStyle,
		ExpectedHash:   resp.Origin.ExpectedHash,
	}, true, nil
}

type gatewayRequiredContentLock struct {
	store      *gatewayCacheMetadataStore
	locality   string
	sourcePath string
}

func (l *gatewayRequiredContentLock) Release(ctx context.Context) error {
	if l == nil || l.store == nil {
		return nil
	}
	return l.store.RemoveStoreFromContentLock(ctx, l.locality, l.sourcePath)
}

func (l *gatewayRequiredContentLock) Refresh(ctx context.Context, _ time.Duration) error {
	if l == nil || l.store == nil {
		return nil
	}
	return l.store.RefreshStoreFromContentLock(ctx, l.locality, l.sourcePath)
}

func requiredContentGatewayLockKey(logicalHostID, hash string) string {
	return fmt.Sprintf("required-content:%s:%s", logicalHostID, hash)
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
