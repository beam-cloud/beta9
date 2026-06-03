package repository_services

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/metadata"
)

func TestAuthorizeCacheRepositoryRequestWithWorkerToken(t *testing.T) {
	ctx := cacheRepositoryAuthContext(types.TokenTypeWorker)

	if err := (&WorkerRepositoryService{}).authorizeCacheRepositoryRequest(ctx); err != nil {
		t.Fatalf("authorizeCacheRepositoryRequest failed: %v", err)
	}
}

func TestAuthorizeCacheRepositoryRequestWithCoordinatorToken(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "coordinator-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))

	if err := service.authorizeCacheRepositoryRequest(ctx); err != nil {
		t.Fatalf("authorizeCacheRepositoryRequest failed: %v", err)
	}
}

func TestAuthorizeCacheRepositoryRequestRejectsMissingOrNonWorkerToken(t *testing.T) {
	for _, ctx := range []context.Context{
		context.Background(),
		cacheRepositoryAuthContext(types.TokenTypeWorkspace),
		cacheRepositoryAuthContext(types.TokenTypeClusterAdmin),
		auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{}),
	} {
		if err := (&WorkerRepositoryService{}).authorizeCacheRepositoryRequest(ctx); err == nil {
			t.Fatal("authorizeCacheRepositoryRequest succeeded, want error")
		}
	}
}

func TestAuthorizeCacheRepositoryRequestRejectsWrongCoordinatorToken(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "coordinator-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong-token"))

	if err := service.authorizeCacheRepositoryRequest(ctx); err == nil {
		t.Fatal("authorizeCacheRepositoryRequest succeeded, want error")
	}
}

func TestConfiguredCacheCoordinatorTokenUsesEnvOverride(t *testing.T) {
	t.Setenv(cacheCoordinatorTokenEnv, "env-coordinator-token")

	if got := configuredCacheCoordinatorToken("config-coordinator-token"); got != "env-coordinator-token" {
		t.Fatalf("configuredCacheCoordinatorToken() = %q, want env-coordinator-token", got)
	}
}

func TestResolveRequiredContentOriginLoadsWorkspaceStorage(t *testing.T) {
	storageID := uint(2)
	storage := &types.WorkspaceStorage{
		Id:          &storageID,
		BucketName:  ptr("workspace-bucket"),
		Region:      ptr("us-east-1"),
		EndpointUrl: ptr("http://localstack:4566"),
		AccessKey:   ptr("access-key"),
		SecretKey:   ptr("secret-key"),
	}
	service := &WorkerRepositoryService{
		backendRepo:           &requiredContentOriginBackendRepo{storageID: storageID, storage: storage},
		cacheCoordinatorToken: "coordinator-token",
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))

	resp, err := service.ResolveRequiredContentOrigin(ctx, &pb.ResolveRequiredContentOriginRequest{
		Item: &pb.RequiredContentItem{
			Locality:     "default",
			WorkspaceId:  "workspace-external-id",
			StubId:       "stub-id",
			Kind:         string(cache.RequiredContentKindVolume),
			Hash:         "hash-a",
			RoutingKey:   "volumes/path/file.bin",
			ExpectedHash: "hash-a",
			Source: &pb.RequiredContentSource{
				Type:       string(cache.RequiredContentSourceS3),
				ObjectPath: "volumes/path/file.bin",
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolveRequiredContentOrigin returned error: %v", err)
	}
	if !resp.Ok || !resp.OriginAvailable {
		t.Fatalf("ResolveRequiredContentOrigin unavailable: ok=%t available=%t error=%q", resp.Ok, resp.OriginAvailable, resp.ErrorMsg)
	}
	if resp.Origin.BucketName != "workspace-bucket" || resp.Origin.AccessKey != "access-key" || resp.Origin.SecretKey != "secret-key" {
		t.Fatalf("origin storage fields were not resolved: %+v", resp.Origin)
	}
	if resp.Origin.Path != "volumes/path/file.bin" || resp.Origin.CachePath != "volumes/path/file.bin" || resp.Origin.ExpectedHash != "hash-a" {
		t.Fatalf("origin content fields were not resolved: %+v", resp.Origin)
	}
	if !resp.Origin.ForcePathStyle {
		t.Fatalf("origin should use path-style addressing for explicit endpoint: %+v", resp.Origin)
	}
}

func TestResolveRequiredContentOriginReturnsOCIOrigin(t *testing.T) {
	service := &WorkerRepositoryService{cacheCoordinatorToken: "coordinator-token"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer coordinator-token"))

	resp, err := service.ResolveRequiredContentOrigin(ctx, &pb.ResolveRequiredContentOriginRequest{
		Item: &pb.RequiredContentItem{
			Locality:     "default",
			WorkspaceId:  "workspace-external-id",
			StubId:       "stub-id",
			Kind:         string(cache.RequiredContentKindClipOCI),
			Hash:         "decompressed-hash",
			RoutingKey:   "decompressed-hash",
			ExpectedHash: "decompressed-hash",
			Source: &pb.RequiredContentSource{
				Type:        string(cache.RequiredContentSourceOCIRegistry),
				Registry:    "registry.localhost:5000",
				Repository:  "stage/beta9-users",
				Reference:   "sha256:manifest",
				LayerDigest: "sha256:layer",
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolveRequiredContentOrigin returned error: %v", err)
	}
	if !resp.Ok || !resp.OriginAvailable {
		t.Fatalf("ResolveRequiredContentOrigin unavailable: ok=%t available=%t error=%q", resp.Ok, resp.OriginAvailable, resp.ErrorMsg)
	}
	source, ok := cache.ParseOCIRequiredContentOriginPath(resp.Origin.Path)
	if !ok {
		t.Fatalf("origin path is not an OCI origin: %s", resp.Origin.Path)
	}
	if source.Registry != "registry.localhost:5000" || source.Repository != "stage/beta9-users" || source.Reference != "sha256:manifest" || source.LayerDigest != "sha256:layer" {
		t.Fatalf("unexpected OCI origin source: %+v", source)
	}
	if resp.Origin.ExpectedHash != "decompressed-hash" || resp.Origin.CachePath != "" {
		t.Fatalf("origin content fields were not resolved: %+v", resp.Origin)
	}
}

func TestReportRequiredContentWritesEventAndRedisStubIndex(t *testing.T) {
	eventRepo := newRequiredContentEventRepo()
	service, redisServer, cleanup := newRequiredContentServiceForTest(t, eventRepo)
	defer cleanup()
	ctx := cacheRepositoryAuthContext(types.TokenTypeWorker)
	req := requiredContentReportRequest(requiredContentOCIItem("hash-a"))

	resp, err := service.ReportRequiredContent(ctx, req)
	if err != nil || !resp.Ok {
		t.Fatalf("ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	event := eventRepo.waitEvent(t)
	if got, want := event.Source, "worker_report"; got != want {
		t.Fatalf("unexpected event source: got %q want %q", got, want)
	}
	if got, want := len(event.Items), 1; got != want {
		t.Fatalf("unexpected required content item count: got %d want %d", got, want)
	}
	if got, want := event.Items[0].Hash, "hash-a"; got != want {
		t.Fatalf("unexpected required content item hash: got %q want %q", got, want)
	}

	resp, err = service.ReportRequiredContent(ctx, req)
	if err != nil || !resp.Ok {
		t.Fatalf("duplicate ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	eventRepo.requireNoEvent(t)
	if len(eventRepo.queries) != 0 {
		t.Fatalf("ReportRequiredContent should not scan event history: %+v", eventRepo.queries)
	}

	runtimeSizeOnly := requiredContentReportRequest(requiredContentRuntimeItem("hash-a", 456))
	resp, err = service.ReportRequiredContent(ctx, runtimeSizeOnly)
	if err != nil || !resp.Ok {
		t.Fatalf("runtime ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	eventRepo.requireNoEvent(t)

	req = requiredContentReportRequest(requiredContentOCIItem("hash-b"))
	resp, err = service.ReportRequiredContent(ctx, req)
	if err != nil || !resp.Ok {
		t.Fatalf("changed ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	event = eventRepo.waitEvent(t)
	if got, want := event.Items[0].Hash, "hash-b"; got != want {
		t.Fatalf("new required content item was not emitted: got %q want %q", got, want)
	}
	if event.Items[0].Source.Type != string(cache.RequiredContentSourceOCIRegistry) {
		t.Fatalf("changed required content item lost origin source: %+v", event.Items[0].Source)
	}
	for _, key := range redisServer.Keys() {
		if strings.Contains(key, ":catalog:") || strings.Contains(key, ":item:") {
			t.Fatalf("required content catalog leaked into redis key %q", key)
		}
	}
}

func TestReportRequiredContentSplitsEventsByKindAndPayloadSize(t *testing.T) {
	eventRepo := newRequiredContentEventRepo()
	service, _, cleanup := newRequiredContentServiceForTest(t, eventRepo)
	defer cleanup()

	items := make([]*pb.RequiredContentItem, 0, requiredContentEventMaxItems+2)
	for i := 0; i < requiredContentEventMaxItems+1; i++ {
		hash := "clip-hash-" + strings.Repeat("0", 4) + "-" + time.UnixMilli(int64(i)).Format("150405.000")
		items = append(items, requiredContentItem(string(cache.RequiredContentKindClipV1), hash, hash, 0, string(cache.RequiredContentSourceUnknown)))
	}
	items = append(items, requiredContentItem(string(cache.RequiredContentKindVolume), "volume-hash", "volumes/path/file.bin", 0, string(cache.RequiredContentSourceS3)))

	resp, err := service.ReportRequiredContent(cacheRepositoryAuthContext(types.TokenTypeWorker), requiredContentReportRequest(items...))
	if err != nil || !resp.Ok {
		t.Fatalf("ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}

	events := []types.EventStubCacheRequiredContentSchema{
		eventRepo.waitEvent(t),
		eventRepo.waitEvent(t),
		eventRepo.waitEvent(t),
	}
	countsByKind := map[string][]int{}
	for _, event := range events {
		if len(event.Items) > requiredContentEventMaxItems {
			t.Fatalf("event exceeded payload cap: %d", len(event.Items))
		}
		countsByKind[event.Kind] = append(countsByKind[event.Kind], len(event.Items))
	}
	if got := countsByKind[string(cache.RequiredContentKindClipV1)]; len(got) != 2 || got[0]+got[1] != requiredContentEventMaxItems+1 {
		t.Fatalf("unexpected clip v1 event split: %+v", countsByKind)
	}
	if got := countsByKind[string(cache.RequiredContentKindVolume)]; len(got) != 1 || got[0] != 1 {
		t.Fatalf("unexpected volume event split: %+v", countsByKind)
	}
	eventRepo.requireNoEvent(t)
}

func TestReportRequiredContentRequiresPersistentEvents(t *testing.T) {
	service, redisServer, cleanup := newRequiredContentServiceWithRepoForTest(t, repository.NewEventClientRepo(types.AppConfig{}))
	defer cleanup()

	resp, err := service.ReportRequiredContent(cacheRepositoryAuthContext(types.TokenTypeWorker), requiredContentReportRequest(requiredContentItem(string(cache.RequiredContentKindClipOCI), "hash-a", "", 0, "")))
	if err != nil {
		t.Fatalf("ReportRequiredContent returned grpc error: %v", err)
	}
	if resp.Ok || resp.ErrorMsg != repository.ErrEventReadUnsupported.Error() {
		t.Fatalf("expected event-read unsupported response: %+v", resp)
	}
	for _, key := range redisServer.Keys() {
		if strings.HasPrefix(key, "cache:required_content:") {
			t.Fatalf("required content index was written without persistent events: %q", key)
		}
	}
}

func TestListRequiredContentForStubReadsRequiredContentFromEvents(t *testing.T) {
	first := requiredContentHistoryRecord(t, types.EventStubCacheRequiredContentSchema{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Locality:    "old-locality",
		Source:      "worker_report",
		Items: []types.EventStubCacheRequiredContentItem{{
			Kind:       string(cache.RequiredContentKindClipV1),
			Hash:       "hash-a",
			RoutingKey: "route-a",
			Source: types.EventStubCacheRequiredContentSource{
				Type: string(cache.RequiredContentSourceUnknown),
			},
		}},
	})
	second := requiredContentHistoryRecord(t, types.EventStubCacheRequiredContentSchema{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Locality:    "old-locality",
		Source:      "worker_report",
		Items: []types.EventStubCacheRequiredContentItem{{
			Kind:       string(cache.RequiredContentKindClipV1),
			Hash:       "hash-a",
			RoutingKey: "route-a",
			SizeBytes:  456,
			Source: types.EventStubCacheRequiredContentSource{
				Type: string(cache.RequiredContentSourceUnknown),
			},
		}},
	})
	origin := requiredContentHistoryRecord(t, types.EventStubCacheRequiredContentSchema{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Locality:    "old-locality",
		Source:      "worker_report",
		Items: []types.EventStubCacheRequiredContentItem{{
			Kind:         string(cache.RequiredContentKindClipOCI),
			Hash:         "hash-b",
			RoutingKey:   "hash-b",
			ExpectedHash: "hash-b",
			Source: types.EventStubCacheRequiredContentSource{
				Type:        string(cache.RequiredContentSourceOCIRegistry),
				Registry:    "registry.localhost:5000",
				Repository:  "beta9-runner",
				Reference:   "sha256:image",
				LayerDigest: "sha256:layer",
			},
		}},
	})
	runtimeSize := requiredContentHistoryRecord(t, types.EventStubCacheRequiredContentSchema{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Locality:    "old-locality",
		Source:      "worker_report",
		Items: []types.EventStubCacheRequiredContentItem{{
			Kind:         string(cache.RequiredContentKindClipOCI),
			Hash:         "hash-b",
			RoutingKey:   "hash-b",
			SizeBytes:    456,
			ExpectedHash: "hash-b",
			Source: types.EventStubCacheRequiredContentSource{
				Type:       string(cache.RequiredContentSourceUnknown),
				Descriptor: "image:image kind:oci-layer-runtime",
			},
		}},
	})
	eventRepo := newRequiredContentEventRepo()
	eventRepo.history = &types.EventHistoryResponse{Events: []types.ContainerEventRecord{first, second, origin, runtimeSize}}
	service, _, cleanup := newRequiredContentServiceForTest(t, eventRepo)
	defer cleanup()

	resp, err := service.ListRequiredContentForStub(cacheRepositoryAuthContext(types.TokenTypeWorker), &pb.ListRequiredContentForStubRequest{
		Locality:    "new-locality",
		WorkspaceId: "workspace-1",
		StubId:      "stub-1",
		Limit:       10,
	})
	if err != nil || !resp.Ok {
		t.Fatalf("ListRequiredContentForStub failed: resp=%+v err=%v", resp, err)
	}
	if got, want := len(resp.Items), 2; got != want {
		t.Fatalf("unexpected item count: got %d want %d", got, want)
	}
	byHash := map[string]*pb.RequiredContentItem{}
	for _, item := range resp.Items {
		byHash[item.Hash] = item
	}
	if item := byHash["hash-a"]; item == nil || item.Locality != "new-locality" || item.SizeBytes != 456 || item.RoutingKey != "route-a" {
		t.Fatalf("unexpected required content item from events: %+v", item)
	}
	item := byHash["hash-b"]
	if item == nil || item.SizeBytes != 456 || item.Source == nil || item.Source.Type != string(cache.RequiredContentSourceOCIRegistry) {
		t.Fatalf("expected OCI source and runtime size to merge: %+v", item)
	}
	if item.Source.LayerDigest != "sha256:layer" || item.Source.Repository != "beta9-runner" {
		t.Fatalf("unexpected OCI source: %+v", item.Source)
	}
	if len(eventRepo.queries) != 1 || eventRepo.queries[0].EventTypes[0] != types.EventStubCacheRequiredContent {
		t.Fatalf("unexpected event query: %+v", eventRepo.queries)
	}
}

func TestSetRequiredContentStatusPushesPlatformCacheEventOnly(t *testing.T) {
	eventRepo := newRequiredContentEventRepo()
	service, _, cleanup := newRequiredContentServiceForTest(t, eventRepo)
	defer cleanup()

	resp, err := service.SetRequiredContentReconciliationStatus(cacheRepositoryAuthContext(types.TokenTypeWorker), &pb.SetRequiredContentReconciliationStatusRequest{
		Locality:    "default",
		WorkspaceId: "workspace-1",
		StubId:      "stub-1",
		Hash:        "hash-a",
		RoutingKey:  "route-a",
		Status:      string(cache.RequiredContentStatusPresent),
		TtlMs:       int64((2 * time.Second).Milliseconds()),
	})
	if err != nil || !resp.Ok {
		t.Fatalf("SetRequiredContentReconciliationStatus failed: resp=%+v err=%v", resp, err)
	}
	eventRepo.requireNoEvent(t)
	platformEvent := eventRepo.waitPlatformEvent(t)
	if platformEvent.Action != "required_content_status" || platformEvent.Status != string(cache.RequiredContentStatusPresent) {
		t.Fatalf("unexpected platform cache event: %+v", platformEvent)
	}
	if platformEvent.WorkspaceID != "workspace-1" || platformEvent.StubID != "stub-1" || platformEvent.Hash != "hash-a" {
		t.Fatalf("platform cache event missing content identifiers: %+v", platformEvent)
	}
}

func requiredContentReportRequest(items ...*pb.RequiredContentItem) *pb.ReportRequiredContentRequest {
	return &pb.ReportRequiredContentRequest{
		Locality:    "default",
		WorkspaceId: "workspace-1",
		StubId:      "stub-1",
		TtlMs:       int64((2 * time.Second).Milliseconds()),
		Items:       items,
	}
}

func requiredContentOCIItem(hash string) *pb.RequiredContentItem {
	item := requiredContentItem(string(cache.RequiredContentKindClipOCI), hash, hash, 0, string(cache.RequiredContentSourceOCIRegistry))
	item.Source.Registry = "registry.localhost:5000"
	item.Source.Repository = "stage/beta9-users"
	item.Source.Reference = "sha256:manifest"
	item.Source.LayerDigest = "sha256:layer"
	return item
}

func requiredContentRuntimeItem(hash string, size int64) *pb.RequiredContentItem {
	item := requiredContentItem(string(cache.RequiredContentKindClipOCI), hash, hash, size, string(cache.RequiredContentSourceUnknown))
	item.Source.Descriptor_ = "runtime access"
	return item
}

func requiredContentItem(kind, hash, routingKey string, size int64, sourceType string) *pb.RequiredContentItem {
	if routingKey == "" {
		routingKey = hash
	}
	item := &pb.RequiredContentItem{
		Kind:         kind,
		Hash:         hash,
		RoutingKey:   routingKey,
		ExpectedHash: hash,
		SizeBytes:    size,
	}
	if sourceType != "" {
		item.Source = &pb.RequiredContentSource{Type: sourceType}
	}
	return item
}

func cacheRepositoryAuthContext(tokenType string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: tokenType},
	})
}

func newRequiredContentServiceForTest(t *testing.T, eventRepo *requiredContentEventRepo) (*WorkerRepositoryService, *miniredis.Miniredis, func()) {
	return newRequiredContentServiceWithRepoForTest(t, eventRepo)
}

func newRequiredContentServiceWithRepoForTest(t *testing.T, eventRepo repository.EventRepository) (*WorkerRepositoryService, *miniredis.Miniredis, func()) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	cacheRepo := repository.NewCacheRedisRepository(rdb)
	return &WorkerRepositoryService{
			requiredContent: cacheRepo,
			eventRepo:       eventRepo,
		}, server, func() {
			_ = rdb.Close()
			server.Close()
		}
}

type requiredContentEventRepo struct {
	repository.EventRepository
	mu             sync.Mutex
	events         []types.EventStubCacheRequiredContentSchema
	platformEvents []types.EventPlatformCacheSchema
	history        *types.EventHistoryResponse
	queries        []types.EventQuery
	ch             chan types.EventStubCacheRequiredContentSchema
	platformCh     chan types.EventPlatformCacheSchema
}

func newRequiredContentEventRepo() *requiredContentEventRepo {
	return &requiredContentEventRepo{
		ch:         make(chan types.EventStubCacheRequiredContentSchema, 8),
		platformCh: make(chan types.EventPlatformCacheSchema, 8),
	}
}

func (r *requiredContentEventRepo) PushStubCacheRequiredContentEvent(event types.EventStubCacheRequiredContentSchema) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	r.ch <- event
}

func (r *requiredContentEventRepo) PushPlatformCacheEvent(event types.EventPlatformCacheSchema) {
	r.mu.Lock()
	r.platformEvents = append(r.platformEvents, event)
	r.mu.Unlock()
	r.platformCh <- event
}

func (r *requiredContentEventRepo) GetEventHistory(ctx context.Context, query types.EventQuery) (*types.EventHistoryResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, query)
	if r.history == nil {
		return &types.EventHistoryResponse{}, nil
	}
	return r.history, nil
}

func (r *requiredContentEventRepo) waitEvent(t *testing.T) types.EventStubCacheRequiredContentSchema {
	t.Helper()
	select {
	case event := <-r.ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for required content event")
	}
	return types.EventStubCacheRequiredContentSchema{}
}

func (r *requiredContentEventRepo) requireNoEvent(t *testing.T) {
	t.Helper()
	select {
	case event := <-r.ch:
		t.Fatalf("unexpected required content event: %+v", event)
	case <-time.After(100 * time.Millisecond):
	}
}

func (r *requiredContentEventRepo) waitPlatformEvent(t *testing.T) types.EventPlatformCacheSchema {
	t.Helper()
	select {
	case event := <-r.platformCh:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for platform cache event")
	}
	return types.EventPlatformCacheSchema{}
}

func requiredContentHistoryRecord(t *testing.T, event types.EventStubCacheRequiredContentSchema) types.ContainerEventRecord {
	t.Helper()
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return types.ContainerEventRecord{
		Type:        types.EventStubCacheRequiredContent,
		Data:        payload,
		WorkspaceID: event.WorkspaceID,
		StubID:      event.StubID,
	}
}

type requiredContentOriginBackendRepo struct {
	repository.BackendRepository
	storageID uint
	storage   *types.WorkspaceStorage
}

func (r *requiredContentOriginBackendRepo) GetWorkspaceByExternalId(ctx context.Context, externalID string) (types.Workspace, error) {
	return types.Workspace{
		Id:         3,
		ExternalId: externalID,
		StorageId:  &r.storageID,
	}, nil
}

func (r *requiredContentOriginBackendRepo) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	return &types.Workspace{
		Id:      workspaceID,
		Storage: r.storage,
	}, nil
}

func ptr(value string) *string {
	return &value
}
