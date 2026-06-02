package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRequiredContentReconcilerMaterializesDeletedLocalContentFromReplica(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectedHost := requiredContentTestHost("node-a", "cache-a")
	replicaHost := requiredContentTestHost("node-b", "cache-b")
	routingKey := routingKeyOwnedBy(t, selectedHost, replicaHost)

	selectedStore := newTestStore(t, 8)
	replicaStore := newTestStore(t, 8)
	content := bytes.Repeat([]byte("required-content-reconciliation-"), 4)
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])

	storedHash, size, err := replicaStore.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, hash, storedHash)
	require.Equal(t, int64(len(content)), size)

	storedHash, size, err = selectedStore.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, hash, storedHash)
	require.Equal(t, int64(len(content)), size)
	require.Equal(t, contentStatusComplete, selectedStore.ContentStatus(hash, size))

	require.NoError(t, os.RemoveAll(selectedStore.pageDir(hash)))
	require.Equal(t, contentStatusMissing, selectedStore.ContentStatus(hash, size))

	repo := newRequiredContentMemoryRepository([]*Host{selectedHost, replicaHost}, requiredContentTestItem(RequiredContentKindClipOCI, hash, routingKey, size))
	peerClient := &Client{
		ctx:          ctx,
		clientConfig: ClientConfig{NTopHosts: 2},
		grpcClients:  make(map[string]proto.CacheClient),
		grpcConns:    make(map[string]*grpc.ClientConn),
		localServers: map[string]*Server{
			selectedHost.HostId: {hostId: selectedHost.HostId, cas: selectedStore},
			replicaHost.HostId:  {hostId: replicaHost.HostId, cas: replicaStore},
		},
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &keyedTestHasher{routes: map[string][]*Host{routingKey: {selectedHost, replicaHost}}},
		maxGetContentAttempts: 1,
	}
	reconciler := requiredContentTestReconciler(ctx, selectedHost, selectedStore, repo,
		RequiredContentConfig{Enabled: true, ReconcileConcurrency: 2, BatchSize: 16, MaxBytesPerCycle: int64(len(content)) * 2},
		func(server *Server, reconciler *requiredContentReconciler) {
			server.peerClient = peerClient
		},
	)

	reconciler.reconcileOnce()

	require.Equal(t, contentStatusComplete, selectedStore.ContentStatus(hash, size))
	dst := make([]byte, len(content))
	n, err := selectedStore.ReadAt(hash, 0, dst)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
	require.Equal(t, RequiredContentStatusPresent, repo.statusFor(hash, routingKey))
}

func TestRequiredContentReconcilerMaterializesDeletedLocalContentFromOrigin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectedHost := requiredContentTestHost("node-a", "cache-a")
	selectedStore := newTestStore(t, 8)
	content := bytes.Repeat([]byte("required-content-origin-"), 4)
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	sourcePath := t.TempDir() + "/required-content.bin"
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))

	item := requiredContentTestItem(RequiredContentKindVolume, hash, "volumes/workspace/file.bin", int64(len(content)))
	repo := newRequiredContentMemoryRepository([]*Host{selectedHost}, item)
	repo.origin = RequiredContentOriginInstruction{
		Path:         sourcePath,
		CachePath:    item.RoutingKey,
		ExpectedHash: hash,
	}
	repo.originOK = true

	reconciler := requiredContentTestReconciler(ctx, selectedHost, selectedStore, repo,
		RequiredContentConfig{Enabled: true, OriginFallbackEnabled: true, ReconcileConcurrency: 2, BatchSize: 16, MaxBytesPerCycle: int64(len(content)) * 2},
	)

	reconciler.reconcileOnce()

	require.Equal(t, contentStatusComplete, selectedStore.ContentStatus(hash, int64(len(content))))
	dst := make([]byte, len(content))
	n, err := selectedStore.ReadAt(hash, 0, dst)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
	require.Equal(t, RequiredContentStatusPresent, repo.statusFor(hash, item.RoutingKey))
}

func TestRequiredContentReconcilerBackfillsUnknownSizeFromCompleteMarker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectedHost := requiredContentTestHost("node-a", "cache-a")
	selectedStore := newTestStore(t, 8)
	content := []byte("complete content with missing required-content size")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	storedHash, size, err := selectedStore.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, hash, storedHash)
	require.Equal(t, int64(len(content)), size)

	item := requiredContentTestItem(RequiredContentKindClipOCI, hash, hash, 0)
	repo := newRequiredContentMemoryRepository([]*Host{selectedHost}, item)
	reconciler := requiredContentTestReconciler(ctx, selectedHost, selectedStore, repo,
		RequiredContentConfig{Enabled: true, ReconcileConcurrency: 1, BatchSize: 16},
	)

	reconciler.reconcileOnce()

	require.Equal(t, RequiredContentStatusPresent, repo.statusFor(hash, item.RoutingKey))
}

func TestRequiredContentReconcilerSkipsUnknownSizeContentWhenBudgetLimited(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectedHost := requiredContentTestHost("node-a", "cache-a")
	selectedStore := newTestStore(t, 8)
	content := []byte("origin content with missing required-content size")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	sourcePath := t.TempDir() + "/required-content.bin"
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))

	item := requiredContentTestItem(RequiredContentKindClipOCI, hash, hash, 0)
	repo := newRequiredContentMemoryRepository([]*Host{selectedHost}, item)
	repo.origin = RequiredContentOriginInstruction{
		Path:         sourcePath,
		CachePath:    item.RoutingKey,
		ExpectedHash: hash,
	}
	repo.originOK = true
	reconciler := requiredContentTestReconciler(ctx, selectedHost, selectedStore, repo,
		RequiredContentConfig{Enabled: true, OriginFallbackEnabled: true, ReconcileConcurrency: 1, BatchSize: 16},
	)

	reconciler.reconcileOnce()

	require.Equal(t, contentStatusMissing, selectedStore.ContentStatus(hash, int64(len(content))))
	require.Equal(t, RequiredContentStatusSkipped, repo.statusFor(hash, item.RoutingKey))
	require.Zero(t, repo.item.SizeBytes)
}

func TestRequiredContentReconcilerMaterializesUnknownSizeOriginContent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectedHost := requiredContentTestHost("node-a", "cache-a")
	selectedStore := newTestStore(t, 8)
	content := []byte("origin content with missing required-content size but known origin")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	sourcePath := t.TempDir() + "/required-content.bin"
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))

	item := requiredContentTestItem(RequiredContentKindClipOCI, hash, hash, 0)
	item.Source = RequiredContentSource{
		Type:        RequiredContentSourceOCIRegistry,
		Registry:    "registry.localhost:5000",
		Repository:  "stage/beta9-users",
		LayerDigest: "sha256:layer",
	}
	repo := newRequiredContentMemoryRepository([]*Host{selectedHost}, item)
	repo.origin = RequiredContentOriginInstruction{
		Path:         sourcePath,
		ExpectedHash: hash,
	}
	repo.originOK = true
	reconciler := requiredContentTestReconciler(ctx, selectedHost, selectedStore, repo,
		RequiredContentConfig{Enabled: true, OriginFallbackEnabled: true, ReconcileConcurrency: 1, BatchSize: 16},
	)

	reconciler.reconcileOnce()

	require.Equal(t, contentStatusComplete, selectedStore.ContentStatus(hash, int64(len(content))))
	require.Equal(t, RequiredContentStatusPresent, repo.statusFor(hash, item.RoutingKey))
}

func TestRequiredContentReconcilerMaterializesWhenHRWOwnerChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newOwner := requiredContentTestHost("node-new", "cache-new")
	oldOwner := requiredContentTestHost("node-old", "cache-old")
	routingKey := routingKeyOwnedBy(t, newOwner, oldOwner)

	newStore := newTestStore(t, 8)
	oldStore := newTestStore(t, 8)
	content := bytes.Repeat([]byte("required-content-owner-change-"), 4)
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	storedHash, size, err := oldStore.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, hash, storedHash)
	require.Equal(t, int64(len(content)), size)

	item := requiredContentTestItem(RequiredContentKindClipOCI, hash, routingKey, size)
	repo := newRequiredContentMemoryRepository([]*Host{newOwner, oldOwner}, item)
	peerClient := &Client{
		ctx:          ctx,
		clientConfig: ClientConfig{NTopHosts: 2},
		grpcClients:  make(map[string]proto.CacheClient),
		grpcConns:    make(map[string]*grpc.ClientConn),
		localServers: map[string]*Server{
			newOwner.HostId: {hostId: newOwner.HostId, cas: newStore},
			oldOwner.HostId: {hostId: oldOwner.HostId, cas: oldStore},
		},
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &keyedTestHasher{routes: map[string][]*Host{routingKey: {newOwner, oldOwner}}},
		maxGetContentAttempts: 1,
	}

	oldReconciler := requiredContentTestReconciler(ctx, oldOwner, oldStore, repo,
		RequiredContentConfig{Enabled: true, ReconcileConcurrency: 1, BatchSize: 16, MaxBytesPerCycle: int64(len(content)) * 2},
	)
	oldReconciler.reconcileOnce()
	require.Equal(t, contentStatusMissing, newStore.ContentStatus(hash, size))
	require.Empty(t, repo.statusFor(hash, routingKey))

	newReconciler := requiredContentTestReconciler(ctx, newOwner, newStore, repo,
		RequiredContentConfig{Enabled: true, ReconcileConcurrency: 1, BatchSize: 16, MaxBytesPerCycle: int64(len(content)) * 2},
		func(server *Server, reconciler *requiredContentReconciler) {
			server.peerClient = peerClient
		},
	)
	newReconciler.reconcileOnce()

	require.Equal(t, contentStatusComplete, newStore.ContentStatus(hash, size))
	require.Equal(t, RequiredContentStatusPresent, repo.statusFor(hash, routingKey))
}

func requiredContentTestHost(nodeID, cachePathID string) *Host {
	return &Host{
		HostId:      fmt.Sprintf("cache-host-default-%s", nodeID),
		Locality:    "default",
		NodeID:      nodeID,
		CachePathID: cachePathID,
	}
}

func requiredContentTestItem(kind RequiredContentKind, hash, routingKey string, size int64) RequiredContentItem {
	return RequiredContentItem{
		Locality:     "default",
		WorkspaceID:  "workspace-1",
		StubID:       "stub-1",
		Kind:         kind,
		Hash:         hash,
		RoutingKey:   routingKey,
		ExpectedHash: hash,
		SizeBytes:    size,
		Status:       RequiredContentStatusPending,
	}
}

type requiredContentTestReconcilerOption func(*Server, *requiredContentReconciler)

func requiredContentTestReconciler(ctx context.Context, host *Host, store *Store, repo *requiredContentMemoryRepository, config RequiredContentConfig, opts ...requiredContentTestReconcilerOption) *requiredContentReconciler {
	server := &Server{
		ctx:           ctx,
		hostId:        host.HostId,
		locality:      host.Locality,
		cas:           store,
		metadataStore: NewMockCacheMetadataStore(),
	}
	reconciler := &requiredContentReconciler{
		server:         server,
		config:         NormalizeRequiredContentConfig(config),
		repository:     repo,
		hostDirectory:  repo,
		originResolver: repo,
	}
	for _, opt := range opts {
		opt(server, reconciler)
	}
	return reconciler
}

func routingKeyOwnedBy(t *testing.T, selectedHost, replicaHost *Host) string {
	t.Helper()

	hasher := newRequiredContentMemoryRepository([]*Host{selectedHost, replicaHost}, RequiredContentItem{}).testHasher(t)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("required-content-routing-key-%d", i)
		hosts := hasher.GetN(2, key)
		if len(hosts) == 2 && hosts[0].HostId == selectedHost.HostId && hosts[1].HostId == replicaHost.HostId {
			return key
		}
	}
	t.Fatal("failed to find routing key owned by selected host")
	return ""
}

type requiredContentMemoryRepository struct {
	mu       sync.Mutex
	hosts    []*Host
	item     RequiredContentItem
	origin   RequiredContentOriginInstruction
	originOK bool
	statuses map[string]RequiredContentReconciliationStatus
	locks    map[string]bool
}

func newRequiredContentMemoryRepository(hosts []*Host, item RequiredContentItem) *requiredContentMemoryRepository {
	return &requiredContentMemoryRepository{
		hosts:    hosts,
		item:     item.Normalized(),
		statuses: map[string]RequiredContentReconciliationStatus{},
		locks:    map[string]bool{},
	}
}

func (r *requiredContentMemoryRepository) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
	hosts := make([]*Host, 0, len(r.hosts))
	for _, host := range r.hosts {
		if host != nil && host.Locality == locality {
			hosts = append(hosts, host)
		}
	}
	if len(hosts) == 0 {
		return nil, ErrHostNotFound
	}
	return hosts, nil
}

func (r *requiredContentMemoryRepository) MarkStubLocalityAccessed(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	return nil
}

func (r *requiredContentMemoryRepository) ListRecentStubLocalities(ctx context.Context, locality string, since time.Time, limit int) ([]RequiredContentStubLocality, error) {
	return []RequiredContentStubLocality{{
		Locality:    r.item.Locality,
		WorkspaceID: r.item.WorkspaceID,
		StubID:      r.item.StubID,
		LastSeen:    time.Now().UTC(),
	}}, nil
}

func (r *requiredContentMemoryRepository) ListRequiredContentForStub(ctx context.Context, locality, workspaceID, stubID string, limit int) ([]RequiredContentItem, error) {
	return []RequiredContentItem{r.item}, nil
}

func (r *requiredContentMemoryRepository) SetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string, status RequiredContentReconciliationStatus, errorMsg string, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses[hash+":"+routingKey] = status
	return nil
}

func (r *requiredContentMemoryRepository) AcquireRequiredContentReconciliationLock(ctx context.Context, locality, logicalHostID, hash string, ttl time.Duration) (RequiredContentReconciliationLock, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := locality + ":" + logicalHostID + ":" + hash
	if r.locks[key] {
		return nil, false, nil
	}
	r.locks[key] = true
	return &requiredContentMemoryLock{repo: r, key: key}, true, nil
}

func (r *requiredContentMemoryRepository) ResolveRequiredContentOrigin(ctx context.Context, item RequiredContentItem) (RequiredContentOriginInstruction, bool, error) {
	return r.origin, r.originOK, nil
}

func (r *requiredContentMemoryRepository) statusFor(hash, routingKey string) RequiredContentReconciliationStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statuses[hash+":"+routingKey]
}

func (r *requiredContentMemoryRepository) testHasher(t *testing.T) RendezvousHasher {
	t.Helper()

	hasher, err := (&requiredContentReconciler{
		server:        &Server{locality: "default"},
		hostDirectory: r,
	}).currentHasher(context.Background())
	require.NoError(t, err)
	return hasher
}

type requiredContentMemoryLock struct {
	repo *requiredContentMemoryRepository
	key  string
}

func (l *requiredContentMemoryLock) Release(ctx context.Context) error {
	l.repo.mu.Lock()
	defer l.repo.mu.Unlock()
	delete(l.repo.locks, l.key)
	return nil
}

func (l *requiredContentMemoryLock) Refresh(ctx context.Context, ttl time.Duration) error {
	return nil
}
