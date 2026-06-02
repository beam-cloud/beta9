package repository_services

import (
	"context"
	"encoding/json"
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

func TestReportRequiredContentWritesDedupedCatalogEvents(t *testing.T) {
	eventRepo := newRequiredContentEventRepo()
	service, cleanup := newRequiredContentServiceForTest(t, eventRepo)
	defer cleanup()
	ctx := cacheRepositoryAuthContext(types.TokenTypeWorker)
	req := &pb.ReportRequiredContentRequest{
		Locality:    "default",
		WorkspaceId: "workspace-1",
		StubId:      "stub-1",
		TtlMs:       int64((2 * time.Second).Milliseconds()),
		Items: []*pb.RequiredContentItem{{
			Kind:         string(cache.RequiredContentKindClipOCI),
			Hash:         "hash-a",
			RoutingKey:   "hash-a",
			ExpectedHash: "hash-a",
			Source: &pb.RequiredContentSource{
				Type:        string(cache.RequiredContentSourceOCIRegistry),
				Registry:    "registry.localhost:5000",
				Repository:  "stage/beta9-users",
				Reference:   "sha256:manifest",
				LayerDigest: "sha256:layer",
			},
		}},
	}

	resp, err := service.ReportRequiredContent(ctx, req)
	if err != nil || !resp.Ok {
		t.Fatalf("ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	event := eventRepo.waitEvent(t)
	if got, want := event.Source, "catalog"; got != want {
		t.Fatalf("unexpected event source: got %q want %q", got, want)
	}
	if got, want := len(event.Items), 1; got != want {
		t.Fatalf("unexpected catalog item count: got %d want %d", got, want)
	}
	if got, want := event.Items[0].Hash, "hash-a"; got != want {
		t.Fatalf("unexpected catalog item hash: got %q want %q", got, want)
	}

	resp, err = service.ReportRequiredContent(ctx, req)
	if err != nil || !resp.Ok {
		t.Fatalf("second ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	eventRepo.requireNoEvent(t)

	req.Items[0].SizeBytes = 123
	resp, err = service.ReportRequiredContent(ctx, req)
	if err != nil || !resp.Ok {
		t.Fatalf("changed ReportRequiredContent failed: resp=%+v err=%v", resp, err)
	}
	event = eventRepo.waitEvent(t)
	if got, want := event.Items[0].SizeBytes, int64(123); got != want {
		t.Fatalf("changed catalog item was not emitted: got %d want %d", got, want)
	}
}

func TestListRequiredContentForStubReadsCatalogFromEvents(t *testing.T) {
	first := requiredContentHistoryRecord(t, types.EventStubCacheRequiredContentSchema{
		WorkspaceID: "workspace-1",
		StubID:      "stub-1",
		Locality:    "old-locality",
		Source:      "catalog",
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
		Source:      "catalog",
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
	eventRepo := newRequiredContentEventRepo()
	eventRepo.history = &types.EventHistoryResponse{Events: []types.ContainerEventRecord{first, second}}
	service, cleanup := newRequiredContentServiceForTest(t, eventRepo)
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
	if got, want := len(resp.Items), 1; got != want {
		t.Fatalf("unexpected item count: got %d want %d", got, want)
	}
	item := resp.Items[0]
	if item.Locality != "new-locality" || item.SizeBytes != 456 || item.RoutingKey != "route-a" {
		t.Fatalf("unexpected item from event catalog: %+v", item)
	}
	if len(eventRepo.queries) != 1 || eventRepo.queries[0].EventTypes[0] != types.EventStubCacheRequiredContent {
		t.Fatalf("unexpected event query: %+v", eventRepo.queries)
	}
}

func TestSetRequiredContentStatusPushesPlatformCacheEventOnly(t *testing.T) {
	eventRepo := newRequiredContentEventRepo()
	service, cleanup := newRequiredContentServiceForTest(t, eventRepo)
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

func cacheRepositoryAuthContext(tokenType string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{TokenType: tokenType},
	})
}

func newRequiredContentServiceForTest(t *testing.T, eventRepo *requiredContentEventRepo) (*WorkerRepositoryService, func()) {
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
		}, func() {
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
