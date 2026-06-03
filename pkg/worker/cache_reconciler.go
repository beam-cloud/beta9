package worker

// This file implements the cache required-content reconciliation that runs on
// workers. Responsibilities are split to keep the worker boundary clear:
//
//   - cacheContentReporter: records, on the worker, which content a stub needs
//     (coalesced to S2) and refreshes the per-stub recency window. It never
//     decides placement or moves bytes.
//   - WorkerCacheManager reconcile loop: on the node that currently hosts the
//     cache server, materializes content the local host owns (HRW) and is
//     missing, copying from a replica or fetching from origin.
//
// The worker is trustless: all coordinator state (recent stubs, locks) is
// brokered through the gateway, and all origin credentials are fetched from the
// gateway on demand and held in memory only. Nothing secret is written to disk,
// Redis, or S2.

import (
	"compress/gzip"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	reporterFlushInterval    = 5 * time.Second
	reporterMaxItemsPerEvent = 512
	reconcileItemTimeout     = 5 * time.Minute
	originCredentialsTTL     = 5 * time.Minute
	// reconcileFailureBackoff throttles retries (and logs) for items that fail
	// to materialize, e.g. an unresolvable origin source.
	reconcileFailureBackoff = 15 * time.Minute
)

// originCredentials holds short-lived, gateway-brokered credentials used to
// fetch content from origin during reconciliation. It is held in memory only
// and never written to disk, Redis, or S2.
type originCredentials struct {
	registryCredentials string
	workspaceStorage    *pb.CacheWorkspaceStorageCredentials
	fetchedAt           time.Time
}

// reconcileInterval is how often a cache host scans for content to reconcile.
func (m *WorkerCacheManager) reconcileInterval() time.Duration {
	seconds := m.config.Cache.Reconciliation.IntervalSeconds
	if seconds <= 0 {
		seconds = cacheDefaultReconcileIntervalS
	}
	return time.Duration(seconds) * time.Second
}

// recentStubTTL is the recency window ("X amount of time"): a stub whose most
// recent container started longer ago than this is dropped from the recent
// index and is no longer reconciled. It is refreshed on every container start.
func (m *WorkerCacheManager) recentStubTTL() time.Duration {
	seconds := m.config.Cache.Reconciliation.RecentStubTTLSeconds
	if seconds <= 0 {
		seconds = cacheDefaultReconcileRecentStubTTLS
	}
	return time.Duration(seconds) * time.Second
}

// reconcileLockTTLSeconds bounds how long a single materialization may hold the
// per-item lifecycle lock.
func (m *WorkerCacheManager) reconcileLockTTLSeconds() int {
	seconds := m.config.Cache.Reconciliation.LockTTLSeconds
	if seconds <= 0 {
		seconds = cacheDefaultReconcileLockTTLS
	}
	return seconds
}

// cacheContentReporter coalesces required-content reports per (stub, kind) and
// flushes them to S2 as bounded events, while keeping a fast-moving recent-stub
// index in Redis. It is created only when reconciliation is enabled; when nil,
// all methods are no-ops so the worker behaves exactly as before.
type cacheContentReporter struct {
	ctx            context.Context
	eventRepo      repo.EventRepository
	metadata       cache.CacheMetadataStore
	locality       string
	recentStubTTL  time.Duration
	volumeMinBytes int64
	activeStubs    func(workspaceID string) []string

	mu       sync.Mutex
	pending  map[reporterKey]map[string]types.CacheRequiredContentItem
	reported map[string]struct{}
}

type reporterKey struct {
	workspaceID string
	stubID      string
	kind        types.CacheContentKind
}

func newCacheContentReporter(
	ctx context.Context,
	eventRepo repo.EventRepository,
	metadata cache.CacheMetadataStore,
	locality string,
	recentStubTTL time.Duration,
	volumeMinBytes int64,
	activeStubs func(workspaceID string) []string,
) *cacheContentReporter {
	r := &cacheContentReporter{
		ctx:            ctx,
		eventRepo:      eventRepo,
		metadata:       metadata,
		locality:       locality,
		recentStubTTL:  recentStubTTL,
		volumeMinBytes: volumeMinBytes,
		activeStubs:    activeStubs,
		pending:        make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		reported:       make(map[string]struct{}),
	}
	go r.run()
	return r
}

// touchRecentStub refreshes the recent-stub window so reconciliation keeps a
// stub's content warm for RecentStubTTL after its most recent container. It is
// cheap and is called on every container start (unlike content generation).
func (r *cacheContentReporter) touchRecentStub(workspaceID, stubID string) {
	if r == nil || r.metadata == nil || workspaceID == "" || stubID == "" {
		return
	}
	if err := r.metadata.AddRecentStub(r.ctx, r.locality, workspaceID, stubID, r.recentStubTTL); err != nil {
		log.Debug().Err(err).Str("workspace_id", workspaceID).Str("stub_id", stubID).Msg("failed to refresh recent stub for cache reconciliation")
	}
}

// shouldGenerateRequiredContent reports whether this is the first load of a stub
// (cluster-wide) and therefore the one time its required content should be
// enumerated and written to S2. Subsequent container starts only refresh the
// recent-stub window via touchRecentStub.
func (r *cacheContentReporter) shouldGenerateRequiredContent(stubID string) bool {
	if r == nil || stubID == "" {
		return false
	}

	r.mu.Lock()
	if _, ok := r.reported[stubID]; ok {
		r.mu.Unlock()
		return false
	}
	r.reported[stubID] = struct{}{}
	r.mu.Unlock()

	if r.metadata == nil {
		return true
	}

	// Cluster-wide one-time claim; on error, fall back to generating.
	claimed, err := r.metadata.MarkStubReported(r.ctx, r.locality, stubID, r.recentStubTTL)
	if err != nil {
		log.Debug().Err(err).Str("stub_id", stubID).Msg("failed to claim one-time required-content generation")
		return true
	}
	return claimed
}

func (r *cacheContentReporter) run() {
	ticker := time.NewTicker(reporterFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			r.flush()
			return
		case <-ticker.C:
			r.flush()
		}
	}
}

// reportItems merges a coalesced set of items for a stub and records the stub in
// the recent index so the reconciliation loop can discover it.
func (r *cacheContentReporter) reportItems(workspaceID, stubID string, kind types.CacheContentKind, items []types.CacheRequiredContentItem) {
	if r == nil || workspaceID == "" || stubID == "" || len(items) == 0 {
		return
	}

	if r.metadata != nil {
		if err := r.metadata.AddRecentStub(r.ctx, r.locality, workspaceID, stubID, r.recentStubTTL); err != nil {
			log.Debug().Err(err).Str("workspace_id", workspaceID).Str("stub_id", stubID).Msg("failed to record recent stub for cache reconciliation")
		}
	}

	key := reporterKey{workspaceID: workspaceID, stubID: stubID, kind: kind}

	r.mu.Lock()
	bucket := r.pending[key]
	if bucket == nil {
		bucket = make(map[string]types.CacheRequiredContentItem)
		r.pending[key] = bucket
	}
	for _, item := range items {
		if item.Hash == "" {
			continue
		}
		if item.RoutingKey == "" {
			item.RoutingKey = item.Hash
		}
		bucket[item.Hash+"\x00"+item.RoutingKey] = item
	}
	total := len(bucket)
	r.mu.Unlock()

	if total >= reporterMaxItemsPerEvent {
		r.flush()
	}
}

func (r *cacheContentReporter) flush() {
	if r == nil {
		return
	}

	r.mu.Lock()
	pending := r.pending
	r.pending = make(map[reporterKey]map[string]types.CacheRequiredContentItem)
	r.mu.Unlock()

	if r.eventRepo == nil {
		return
	}

	for key, bucket := range pending {
		if len(bucket) == 0 {
			continue
		}
		items := make([]types.CacheRequiredContentItem, 0, len(bucket))
		for _, item := range bucket {
			items = append(items, item)
		}
		for start := 0; start < len(items); start += reporterMaxItemsPerEvent {
			end := min(start+reporterMaxItemsPerEvent, len(items))
			r.eventRepo.PushStubCacheRequiredContent(types.EventStubCacheRequiredContentSchema{
				WorkspaceID: key.workspaceID,
				StubID:      key.stubID,
				Locality:    r.locality,
				Kind:        key.kind,
				Items:       items[start:end],
			})
		}
	}
}

// ReportVolumeContent implements storage.VolumeContentReporter. geesefs reports
// workspace object content above the configured size threshold; it is attributed
// to the workspace's currently active stubs.
func (r *cacheContentReporter) ReportVolumeContent(workspaceID, hash, sourcePath string, sizeBytes int64) {
	if r == nil || workspaceID == "" || hash == "" {
		return
	}
	if r.volumeMinBytes > 0 && sizeBytes < r.volumeMinBytes {
		return
	}

	var stubs []string
	if r.activeStubs != nil {
		stubs = r.activeStubs(workspaceID)
	}
	if len(stubs) == 0 {
		return
	}

	item := types.CacheRequiredContentItem{
		Hash:         hash,
		RoutingKey:   hash,
		SizeBytes:    sizeBytes,
		ExpectedHash: hash,
		Source:       sourcePath,
	}
	for _, stubID := range stubs {
		r.reportItems(workspaceID, stubID, types.CacheContentKindVolume, []types.CacheRequiredContentItem{item})
	}
}

// activeStubsForWorkspace lists the external stub ids of containers currently
// running for a workspace, used to attribute geesefs volume content to stubs.
func (m *WorkerCacheManager) activeStubsForWorkspace(workspaceID string) []string {
	if m == nil || m.containerInstances == nil || workspaceID == "" {
		return nil
	}

	seen := map[string]struct{}{}
	stubs := []string{}
	m.containerInstances.Range(func(_ string, instance *ContainerInstance) bool {
		if instance == nil || instance.Request == nil {
			return true
		}
		if instance.Request.WorkspaceId != workspaceID || instance.StubId == "" {
			return true
		}
		if _, ok := seen[instance.StubId]; ok {
			return true
		}
		seen[instance.StubId] = struct{}{}
		stubs = append(stubs, instance.StubId)
		return true
	})
	return stubs
}

// runReconciliation runs the async required-content reconciliation loop. It is
// launched from Start only when reconciliation is enabled and Redis metadata is
// available. Startup remains non-blocking.
func (m *WorkerCacheManager) runReconciliation() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.reconcileInterval())
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.reconcileOnce()
		}
	}
}

func (m *WorkerCacheManager) reconcileOnce() {
	if m.client == nil || m.metadataStore == nil {
		return
	}

	m.mu.Lock()
	server := m.server
	draining := m.draining
	m.mu.Unlock()
	if draining || server == nil {
		// Only a node that currently hosts a cache server can materialize content.
		return
	}

	localHostID := server.HostID()
	if localHostID == "" {
		return
	}

	maxStubs := m.config.Cache.Reconciliation.MaxStubsPerCycle
	if maxStubs <= 0 {
		maxStubs = cacheDefaultReconcileMaxStubsCycle
	}

	// Only stubs accessed within the recency window are reconciled; older stubs
	// have aged out of the recent index and their content is left to expire.
	stubs, err := m.metadataStore.ListRecentStubs(m.ctx, m.locality, m.recentStubTTL(), maxStubs)
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Msg("cache reconciliation failed to list recent stubs")
		return
	}

	for _, stub := range stubs {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		m.reconcileStub(server, localHostID, stub)
	}
}

func (m *WorkerCacheManager) reconcileStub(server *cache.Server, localHostID string, stub cache.RecentStub) {
	items, err := m.eventRepo.ReadStubCacheRequiredContent(m.ctx, stub.WorkspaceID, stub.StubID)
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("cache reconciliation failed to read required content")
		return
	}

	for _, item := range items {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		routingKey := item.RoutingKey
		if routingKey == "" {
			routingKey = item.Hash
		}

		// Materialize only on the host a read would actually resolve to (the
		// primary host in the read's HRW window). This keeps reconciled content
		// on the same host the read path looks up first, preventing misses for
		// recent stubs; other hosts leave the item to the primary.
		primary, err := m.client.PrimaryReadHost(routingKey)
		if err != nil || primary == nil || primary.HostId != localHostID {
			continue
		}

		if server.HasCompleteContent(item.Hash, item.SizeBytes) {
			continue
		}

		// Back off items that recently failed to materialize (e.g. an
		// unresolvable origin source) so they are not retried and re-logged
		// every cycle.
		if m.reconcileBackingOff(item.Hash, routingKey) {
			continue
		}

		m.materializeOwnedItem(server, localHostID, stub, item, routingKey)
	}
}

func (m *WorkerCacheManager) materializeOwnedItem(server *cache.Server, localHostID string, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string) {
	acquired, err := m.metadataStore.AcquireReconcileLock(m.ctx, m.locality, localHostID, item.Hash, m.reconcileLockTTLSeconds())
	if err != nil || !acquired {
		// Another materialization is already in flight for this item (or the
		// coordinator is unavailable); try again next cycle.
		return
	}
	defer func() {
		if err := m.metadataStore.ReleaseReconcileLock(m.ctx, m.locality, localHostID, item.Hash); err != nil {
			log.Debug().Err(err).Str("hash", item.Hash).Msg("failed to release cache reconciliation lock")
		}
	}()

	// Re-check after acquiring the lock; another process may have just completed it.
	if server.HasCompleteContent(item.Hash, item.SizeBytes) {
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, reconcileItemTimeout)
	defer cancel()

	m.reconcileLogFields(log.Info(), localHostID, stub, item).
		Str("source", item.Source).
		Int64("size_bytes", item.SizeBytes).
		Msg("reconciling missing cache content")

	startedAt := time.Now()
	status := m.materialize(ctx, server, stub, item, routingKey)

	result := log.Info()
	if status != types.CacheAuditStatusMaterialized {
		result = log.Warn()
	}
	m.reconcileLogFields(result, localHostID, stub, item).
		Str("status", status).
		Dur("duration", time.Since(startedAt)).
		Msg("cache content reconciled")

	if status == types.CacheAuditStatusMaterialized {
		m.clearReconcileFailure(item.Hash, routingKey)
	} else {
		m.recordReconcileFailure(item.Hash, routingKey)
	}

	m.auditCacheEvent(localHostID, stub, item, routingKey, status)
}

// reconcileBackingOff reports whether an item failed to materialize recently and
// should be skipped until the backoff window elapses. Expired entries are pruned
// so the map only tracks currently-failing items.
func (m *WorkerCacheManager) reconcileBackingOff(hash, routingKey string) bool {
	key := hash + "\x00" + routingKey
	m.reconcileFailuresMu.Lock()
	defer m.reconcileFailuresMu.Unlock()

	failedAt, ok := m.reconcileFailures[key]
	if !ok {
		return false
	}
	if time.Since(failedAt) < reconcileFailureBackoff {
		return true
	}
	delete(m.reconcileFailures, key)
	return false
}

func (m *WorkerCacheManager) recordReconcileFailure(hash, routingKey string) {
	key := hash + "\x00" + routingKey
	m.reconcileFailuresMu.Lock()
	m.reconcileFailures[key] = time.Now()
	m.reconcileFailuresMu.Unlock()
}

func (m *WorkerCacheManager) clearReconcileFailure(hash, routingKey string) {
	key := hash + "\x00" + routingKey
	m.reconcileFailuresMu.Lock()
	delete(m.reconcileFailures, key)
	m.reconcileFailuresMu.Unlock()
}

// reconcileLogFields adds the fields common to per-item reconciliation logs.
func (m *WorkerCacheManager) reconcileLogFields(event *zerolog.Event, localHostID string, stub cache.RecentStub, item types.CacheRequiredContentItem) *zerolog.Event {
	return event.
		Str("locality", m.locality).
		Str("logical_host", localHostID).
		Str("workspace_id", stub.WorkspaceID).
		Str("stub_id", stub.StubID).
		Str("hash", item.Hash).
		Str("kind", string(item.Kind))
}

// materialize copies content for an owned item onto the local cache server. It
// prefers a reachable replica and otherwise fetches from the item's origin in
// the same way the read path does. It never persists credentials in Redis or S2.
func (m *WorkerCacheManager) materialize(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string) string {
	if ok, err := m.client.MaterializeFromReplica(ctx, server, item.Hash, routingKey, item.SizeBytes); err != nil {
		log.Debug().Err(err).Str("hash", item.Hash).Msg("cache reconciliation replica copy failed")
	} else if ok {
		return types.CacheAuditStatusMaterialized
	}

	if !m.config.Cache.Reconciliation.OriginFallbackEnabled || item.Source == "" {
		return types.CacheAuditStatusMiss
	}

	switch item.Kind {
	case types.CacheContentKindClipV2:
		// OCI layers are fetched from the source registry and decompressed,
		// then stored under the decompressed content hash, mirroring the clip
		// read path. The local mounted-source path is not valid for layers.
		return m.materializeOCILayer(ctx, server, stub, item)
	case types.CacheContentKindVolume:
		return m.materializeVolumeObject(ctx, server, stub, item)
	default:
		// CLIP v1 content is content-addressed archive data with no standalone
		// origin source; it is materialized from a replica when available.
		return types.CacheAuditStatusMiss
	}
}

// materializeVolumeObject fetches a workspace object from object storage using
// gateway-brokered workspace storage credentials and stores it under its content
// hash. Credentials ride only in the in-flight store request; they are not
// persisted on the worker.
func (m *WorkerCacheManager) materializeVolumeObject(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem) string {
	creds := m.originCredentials(ctx, stub.WorkspaceID, stub.StubID, "")
	if creds == nil || creds.workspaceStorage == nil {
		log.Debug().Str("hash", item.Hash).Str("workspace_id", stub.WorkspaceID).Msg("cache reconciliation has no workspace storage credentials")
		return types.CacheAuditStatusOriginFailure
	}

	ws := creds.workspaceStorage
	req := &pb.CacheStoreContentFromSourceRequest{
		Source: &pb.CacheSource{
			Path:           item.Source,
			ExpectedHash:   item.Hash,
			BucketName:     ws.BucketName,
			Region:         ws.Region,
			EndpointUrl:    ws.EndpointUrl,
			AccessKey:      ws.AccessKey,
			SecretKey:      ws.SecretKey,
			ForcePathStyle: ws.ForcePathStyle,
		},
	}
	resp, err := server.StoreContentFromSource(ctx, req)
	if err == nil && resp != nil && resp.Ok {
		return types.CacheAuditStatusMaterialized
	}
	log.Debug().Err(err).Str("hash", item.Hash).Str("source", item.Source).Msg("cache reconciliation workspace storage fetch failed")
	return types.CacheAuditStatusOriginFailure
}

// materializeOCILayer fetches an OCI layer from the source registry, decompresses
// it, and stores the result under its decompressed content hash on the local
// cache server. This is the same content the clip read path warms into the cache.
// Registry credentials are brokered from the gateway and used in-memory only.
func (m *WorkerCacheManager) materializeOCILayer(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem) string {
	ref, err := name.NewDigest(item.Source)
	if err != nil {
		log.Debug().Err(err).Str("source", item.Source).Msg("cache reconciliation could not parse oci layer reference")
		return types.CacheAuditStatusOriginFailure
	}

	authOption := remote.WithAuthFromKeychain(authn.DefaultKeychain)
	if creds := m.originCredentials(ctx, stub.WorkspaceID, stub.StubID, ref.Context().RegistryStr()); creds != nil && creds.registryCredentials != "" {
		if authenticator := registryAuthenticator(ctx, ref, creds.registryCredentials); authenticator != nil {
			authOption = remote.WithAuth(authenticator)
		}
	}

	layer, err := remote.Layer(ref, remote.WithContext(ctx), authOption)
	if err != nil {
		log.Debug().Err(err).Str("source", item.Source).Msg("cache reconciliation failed to fetch oci layer")
		return types.CacheAuditStatusOriginFailure
	}

	compressed, err := layer.Compressed()
	if err != nil {
		log.Debug().Err(err).Str("source", item.Source).Msg("cache reconciliation failed to open compressed oci layer")
		return types.CacheAuditStatusOriginFailure
	}
	defer compressed.Close()

	gzr, err := gzip.NewReader(compressed)
	if err != nil {
		log.Debug().Err(err).Str("source", item.Source).Msg("cache reconciliation failed to decompress oci layer")
		return types.CacheAuditStatusOriginFailure
	}
	defer gzr.Close()

	if _, _, err := server.StoreReader(ctx, gzr, item.Hash); err != nil {
		log.Debug().Err(err).Str("hash", item.Hash).Str("source", item.Source).Msg("cache reconciliation failed to store decompressed oci layer")
		return types.CacheAuditStatusOriginFailure
	}
	return types.CacheAuditStatusMaterialized
}

// originCredentials fetches short-lived origin credentials from the gateway and
// caches them in memory only. Credentials are never written to disk, Redis, or
// S2, keeping the worker trustless: it holds no long-lived registry or workspace
// storage secrets.
func (m *WorkerCacheManager) originCredentials(ctx context.Context, workspaceID, stubID, registry string) *originCredentials {
	if m.workerRepo == nil || workspaceID == "" || stubID == "" {
		return nil
	}

	// The registry is part of the key: registry credentials are registry-scoped,
	// so callers requesting different registries (or none, for volume fetches)
	// must not reuse each other's cached auth.
	key := workspaceID + "\x00" + stubID + "\x00" + registry
	m.originCredsMu.Lock()
	if cached, ok := m.originCredsCache[key]; ok && time.Since(cached.fetchedAt) < originCredentialsTTL {
		m.originCredsMu.Unlock()
		return cached
	}
	m.originCredsMu.Unlock()

	resp, err := handleGRPCResponse(m.workerRepo.GetCacheOriginCredentials(ctx, &pb.GetCacheOriginCredentialsRequest{
		WorkspaceId: workspaceID,
		StubId:      stubID,
		Registry:    registry,
	}))
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", workspaceID).Str("stub_id", stubID).Msg("cache reconciliation failed to fetch origin credentials")
		return nil
	}

	creds := &originCredentials{
		registryCredentials: resp.RegistryCredentials,
		workspaceStorage:    resp.WorkspaceStorage,
		fetchedAt:           time.Now(),
	}
	m.originCredsMu.Lock()
	m.originCredsCache[key] = creds
	m.originCredsMu.Unlock()
	return creds
}

// registryAuthenticator converts a brokered registry credentials blob into an
// authenticator for fetching layers. The credentials are used in-memory only.
func registryAuthenticator(ctx context.Context, ref name.Digest, credentials string) authn.Authenticator {
	creds, err := reg.ParseCredentialsFromJSON(credentials)
	if err != nil || len(creds) == 0 {
		parts := strings.SplitN(credentials, ":", 2)
		if len(parts) == 2 {
			creds = map[string]string{"USERNAME": parts[0], "PASSWORD": parts[1]}
		}
	}
	if len(creds) == 0 {
		return nil
	}

	registry := ref.Context().RegistryStr()
	provider := reg.CredentialsToProvider(ctx, registry, creds)
	if provider == nil {
		return nil
	}
	authConfig, err := provider.GetCredentials(ctx, registry, ref.Context().RepositoryStr())
	if err != nil || authConfig == nil {
		return nil
	}
	return authn.FromConfig(*authConfig)
}

func (m *WorkerCacheManager) auditCacheEvent(localHostID string, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey, status string) {
	if m.eventRepo == nil {
		return
	}
	m.eventRepo.PushPlatformCacheEvent(types.EventPlatformCacheSchema{
		Locality:    m.locality,
		LogicalHost: localHostID,
		WorkspaceID: stub.WorkspaceID,
		StubID:      stub.StubID,
		Hash:        item.Hash,
		RoutingKey:  routingKey,
		Status:      status,
		Source:      item.Source,
		SizeBytes:   item.SizeBytes,
	})
}
