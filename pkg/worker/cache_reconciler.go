package worker

// This file implements the cache required-content reconciliation that runs on
// workers. Responsibilities are split to keep the worker boundary clear:
//
//   - cacheContentReporter: records, on the worker, which content a stub needs
//     (coalesced to S2) and refreshes the per-stub recency window. It never
//     decides placement or moves bytes.
//   - WorkerCacheManager reconcile loop: on the node that currently hosts the
//     cache server, materializes content the local host owns (HRW), except
//     checkpoints which materialize on every matching accelerator in locality.
//
// The worker is trustless: all coordinator state (recent stubs, locks) is
// brokered through the gateway, and all origin credentials are fetched from the
// gateway on demand and held in memory only. Nothing secret is written to disk,
// Redis, or S2.

import (
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
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

type reporterStubKey struct {
	workspaceID string
	stubID      string
}

type requiredContentReport struct {
	kind  types.CacheContentKind
	items []types.CacheRequiredContentItem
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

// shouldGenerateRequiredContent reports whether this worker process has already
// enumerated a stub's required content. The durable S2 stream is the source of
// truth, so Redis is marked only after a successful event write.
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

	return true
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
	r.reportBatches(workspaceID, stubID, []requiredContentReport{{kind: kind, items: items}})
}

// reportBatches records all required-content kinds for a stub under one lock.
// This prevents the periodic flush from publishing one image kind, marking the
// stub as reported, and missing another kind from the same image load.
func (r *cacheContentReporter) reportBatches(workspaceID, stubID string, reports []requiredContentReport) {
	if r == nil || workspaceID == "" || stubID == "" || len(reports) == 0 {
		return
	}

	if r.metadata != nil {
		if err := r.metadata.AddRecentStub(r.ctx, r.locality, workspaceID, stubID, r.recentStubTTL); err != nil {
			log.Debug().Err(err).Str("workspace_id", workspaceID).Str("stub_id", stubID).Msg("failed to record recent stub for cache reconciliation")
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, report := range reports {
		if len(report.items) == 0 {
			continue
		}
		key := reporterKey{workspaceID: workspaceID, stubID: stubID, kind: report.kind}
		bucket := r.pending[key]
		if bucket == nil {
			bucket = make(map[string]types.CacheRequiredContentItem)
			r.pending[key] = bucket
		}
		for _, item := range report.items {
			if item.Hash == "" {
				continue
			}
			if item.RoutingKey == "" {
				item.RoutingKey = item.Hash
			}
			bucket[item.Hash+"\x00"+item.RoutingKey] = item
		}
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

	failed := make(map[reporterKey]map[string]types.CacheRequiredContentItem)
	stubOK := make(map[reporterStubKey]bool)
	for key, bucket := range pending {
		if len(bucket) == 0 {
			continue
		}
		stubKey := reporterStubKey{workspaceID: key.workspaceID, stubID: key.stubID}
		if _, ok := stubOK[stubKey]; !ok {
			stubOK[stubKey] = true
		}

		items := make([]types.CacheRequiredContentItem, 0, len(bucket))
		for _, item := range bucket {
			items = append(items, item)
		}
		ok := true
		for start := 0; start < len(items); start += reporterMaxItemsPerEvent {
			end := min(start+reporterMaxItemsPerEvent, len(items))
			if err := r.eventRepo.PushStubCacheRequiredContent(types.EventStubCacheRequiredContentSchema{
				WorkspaceID: key.workspaceID,
				StubID:      key.stubID,
				Locality:    r.locality,
				Kind:        key.kind,
				Items:       items[start:end],
			}); err != nil {
				log.Debug().Err(err).Str("workspace_id", key.workspaceID).Str("stub_id", key.stubID).Str("kind", string(key.kind)).Msg("failed to publish required-content event")
				ok = false
				break
			}
		}
		if !ok {
			stubOK[stubKey] = false
			failed[key] = bucket
			continue
		}
	}

	for stubKey, ok := range stubOK {
		if ok {
			r.markStubReported(stubKey.stubID)
		}
	}

	if len(failed) > 0 {
		r.requeue(failed)
	}
}

func (r *cacheContentReporter) markStubReported(stubID string) {
	if r == nil || r.metadata == nil || stubID == "" {
		return
	}
	if _, err := r.metadata.MarkStubReported(r.ctx, r.locality, stubID, r.recentStubTTL); err != nil {
		log.Debug().Err(err).Str("stub_id", stubID).Msg("failed to mark required-content report complete")
	}
}

func (r *cacheContentReporter) requeue(items map[reporterKey]map[string]types.CacheRequiredContentItem) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, bucket := range items {
		if len(bucket) == 0 {
			continue
		}
		current := r.pending[key]
		if current == nil {
			current = make(map[string]types.CacheRequiredContentItem, len(bucket))
			r.pending[key] = current
		}
		for itemKey, item := range bucket {
			current[itemKey] = item
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
		stubID := cacheRequestStubID(instance.Request)
		if cacheRequestWorkspaceID(instance.Request) != workspaceID || stubID == "" {
			return true
		}
		if _, ok := seen[stubID]; ok {
			return true
		}
		seen[stubID] = struct{}{}
		stubs = append(stubs, stubID)
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
		case <-m.reconcileNow:
			m.reconcileOnce()
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

	m.pruneReconcileFailures()

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

	activeCheckpointIDs := map[string]struct{}{}
	for _, stub := range stubs {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		for _, checkpointID := range m.reconcileStub(server, localHostID, stub) {
			activeCheckpointIDs[checkpointID] = struct{}{}
		}
	}
	if len(stubs) < maxStubs {
		m.pruneLocalCheckpoints(activeCheckpointIDs)
		m.pruneStaleCacheCheckpoints()
	}
}

func (m *WorkerCacheManager) reconcileStub(server *cache.Server, localHostID string, stub cache.RecentStub) []string {
	items, err := m.eventRepo.ReadStubCacheRequiredContent(m.ctx, stub.WorkspaceID, stub.StubID)
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("cache reconciliation failed to read required content")
		return nil
	}

	checkpointIDs := []string{}
	for _, item := range items {
		select {
		case <-m.ctx.Done():
			return checkpointIDs
		default:
		}

		routingKey := item.RoutingKey
		if routingKey == "" {
			routingKey = item.Hash
		}

		if item.Kind == types.CacheContentKindCheckpoint {
			if !m.checkpointAcceleratorMatches(item) {
				continue
			}
			if item.CheckpointID != "" {
				checkpointIDs = append(checkpointIDs, item.CheckpointID)
			}
		} else {
			// Materialize only on the host a read would actually resolve to.
			primary, err := m.client.PrimaryReadHost(routingKey)
			if err != nil || primary == nil || primary.HostId != localHostID {
				continue
			}
		}

		if m.requiredContentComplete(server, item, routingKey) {
			continue
		}

		// Back off items that recently failed to materialize (e.g. an
		// unresolvable origin source) so they are not retried and re-logged
		// every cycle.
		if m.reconcileBackingOff(item.Hash, routingKey, stub.LastSeen) {
			continue
		}

		m.materializeOwnedItem(server, localHostID, stub, item, routingKey)
	}
	return checkpointIDs
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
	if m.requiredContentComplete(server, item, routingKey) {
		return
	}

	ctx, cancel := context.WithTimeout(m.ctx, reconcileItemTimeout)
	defer cancel()

	m.reconcileLogFields(log.Debug(), localHostID, stub, item).
		Str("source", item.Source).
		Int64("size_bytes", item.SizeBytes).
		Msg("reconciling missing cache content")

	startedAt := time.Now()
	status := m.materialize(ctx, server, stub, item, routingKey)
	elapsed := time.Since(startedAt)

	switch {
	case status == types.CacheAuditStatusMaterialized:
		m.clearReconcileFailure(item.Hash, routingKey)
		m.reconcileLogFields(log.Info(), localHostID, stub, item).
			Str("status", status).Dur("duration", elapsed).
			Msg("cache content reconciled")
	case reconcileStatusIsFailure(status):
		// Genuine fetch failure (e.g. an unresolvable origin source) - back off
		// so it is not retried and re-logged every cycle.
		m.recordReconcileFailure(item.Hash, routingKey)
		m.reconcileLogFields(log.Warn(), localHostID, stub, item).
			Str("status", status).Dur("duration", elapsed).
			Msg("cache content reconciliation failed")
	default:
		// A miss (no replica and no usable origin) is expected/transient and is
		// resolved by the normal read path, so it is not backed off.
		m.reconcileLogFields(log.Debug(), localHostID, stub, item).
			Str("status", status).Dur("duration", elapsed).
			Msg("cache content not reconciled")
	}

	m.auditCacheEvent(localHostID, stub, item, routingKey, status)
}

// reconcileStatusIsFailure reports whether a materialization outcome is a genuine
// failure that should be backed off, as opposed to an expected miss.
func reconcileStatusIsFailure(status string) bool {
	switch status {
	case types.CacheAuditStatusOriginFailure,
		types.CacheAuditStatusReplicaFailure,
		types.CacheAuditStatusHostUnavailable:
		return true
	default:
		return false
	}
}

// reconcileBackingOff reports whether an item failed to materialize recently and
// should be skipped until the backoff window elapses. Expired entries are pruned
// so the map only tracks currently-failing items.
func (m *WorkerCacheManager) reconcileBackingOff(hash, routingKey string, stubLastSeen time.Time) bool {
	key := hash + "\x00" + routingKey
	m.reconcileFailuresMu.Lock()
	defer m.reconcileFailuresMu.Unlock()

	failedAt, ok := m.reconcileFailures[key]
	if !ok {
		return false
	}
	if !stubLastSeen.IsZero() && stubLastSeen.After(failedAt) {
		delete(m.reconcileFailures, key)
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

// pruneReconcileFailures drops expired backoff entries so the map stays bounded
// to items that are currently failing, even if they are never retried (e.g. the
// stub ages out or ownership moves).
func (m *WorkerCacheManager) pruneReconcileFailures() {
	m.reconcileFailuresMu.Lock()
	defer m.reconcileFailuresMu.Unlock()
	for key, failedAt := range m.reconcileFailures {
		if time.Since(failedAt) >= reconcileFailureBackoff {
			delete(m.reconcileFailures, key)
		}
	}
}

func (m *WorkerCacheManager) pruneLocalCheckpoints(active map[string]struct{}) {
	if m.checkpointRoot == "" {
		return
	}
	entries, err := os.ReadDir(m.checkpointRoot)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		checkpointID := name
		if strings.HasSuffix(name, checkpointArchiveExtension) {
			checkpointID = strings.TrimSuffix(name, checkpointArchiveExtension)
		} else if !entry.IsDir() {
			continue
		}
		if _, ok := active[checkpointID]; ok {
			continue
		}
		_ = os.RemoveAll(filepath.Join(m.checkpointRoot, name))
	}
}

func (m *WorkerCacheManager) pruneStaleCacheCheckpoints() {
	if m.workerRepo == nil || m.locality == "" {
		return
	}
	resp, err := handleGRPCResponse(m.workerRepo.PruneStaleCacheCheckpoints(m.ctx, &pb.PruneStaleCacheCheckpointsRequest{}))
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Msg("cache reconciliation failed to prune stale checkpoints")
		return
	}
	if resp.Pruned > 0 {
		log.Info().Str("locality", m.locality).Int32("pruned", resp.Pruned).Msg("pruned stale cache checkpoints")
	}
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
	if item.Kind == types.CacheContentKindCheckpoint {
		return m.materializeCheckpoint(ctx, server, stub, item, routingKey)
	}

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
		return m.materializeWorkspaceObject(ctx, server, stub, item)
	case types.CacheContentKindClipV1:
		// The v1 archive is one content-addressed object; re-fetch the whole
		// archive from the image registry (the same source the image-load path
		// pulls it from) and store it under its hash + cachefs path.
		return m.materializeArchiveObject(ctx, server, item, routingKey)
	default:
		return types.CacheAuditStatusMiss
	}
}

// materializeArchiveObject re-fetches the whole CLIP v1 archive from the image
// registry and stores it as a single content object, mirroring the embedded
// image-archive cache that the image-load path populates. It pulls from the same
// source the load path uses: the S3 image registry for the S3 store, or the
// mounted image volume for the local store. No credentials are persisted.
func (m *WorkerCacheManager) materializeArchiveObject(ctx context.Context, server *cache.Server, item types.CacheRequiredContentItem, routingKey string) string {
	source := &pb.CacheSource{
		CachePath:    routingKey,
		ExpectedHash: item.Hash,
	}

	if m.config.ImageService.RegistryStore == reg.S3ImageRegistryStore {
		s3 := m.config.ImageService.Registries.S3
		if s3.BucketName == "" || item.Source == "" {
			return types.CacheAuditStatusMiss
		}
		source.Path = item.Source
		source.BucketName = s3.BucketName
		source.Region = s3.Region
		source.EndpointUrl = s3.Endpoint
		source.AccessKey = s3.AccessKey
		source.SecretKey = s3.SecretKey
		source.ForcePathStyle = s3.ForcePathStyle
	} else {
		// Local registry store: the durable archive lives on the mounted image
		// volume at the cachefs path; read it directly (no bucket/credentials).
		source.Path = routingKey
	}

	resp, err := server.StoreContentFromSource(ctx, &pb.CacheStoreContentFromSourceRequest{Source: source})
	if err == nil && resp != nil && resp.Ok {
		return types.CacheAuditStatusMaterialized
	}
	log.Debug().Err(err).Str("hash", item.Hash).Str("routing_key", routingKey).Msg("cache reconciliation image archive fetch failed")
	return types.CacheAuditStatusOriginFailure
}

func (m *WorkerCacheManager) requiredContentComplete(server *cache.Server, item types.CacheRequiredContentItem, routingKey string) bool {
	if item.Kind == types.CacheContentKindCheckpoint && item.CheckpointID != "" {
		return server.HasCompleteContent(item.Hash, item.SizeBytes) &&
			checkpointMaterialized(filepath.Join(m.checkpointRoot, item.CheckpointID))
	}
	return server.HasCompleteContent(item.Hash, item.SizeBytes)
}

func (m *WorkerCacheManager) checkpointAcceleratorMatches(item types.CacheRequiredContentItem) bool {
	return item.Accelerator == "" || strings.EqualFold(item.Accelerator, m.accelerator)
}

func (m *WorkerCacheManager) materializeCheckpoint(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string) string {
	if item.CheckpointID == "" || item.Hash == "" || item.SizeBytes <= 0 {
		return types.CacheAuditStatusMiss
	}
	if ok, err := m.client.MaterializeFromReplica(ctx, server, item.Hash, routingKey, item.SizeBytes); err != nil {
		log.Debug().Err(err).Str("hash", item.Hash).Msg("cache reconciliation checkpoint replica copy failed")
	} else if !ok {
		if item.Source == "" {
			return types.CacheAuditStatusMiss
		}
		if status := m.materializeWorkspaceObject(ctx, server, stub, item); status != types.CacheAuditStatusMaterialized {
			return status
		}
	}
	if err := m.extractCheckpointArchive(ctx, item, routingKey); err != nil {
		log.Debug().Err(err).Str("checkpoint_id", item.CheckpointID).Msg("cache reconciliation checkpoint extract failed")
		return types.CacheAuditStatusOriginFailure
	}
	return types.CacheAuditStatusMaterialized
}

func (m *WorkerCacheManager) extractCheckpointArchive(ctx context.Context, item types.CacheRequiredContentItem, routingKey string) error {
	checkpointPath := filepath.Join(m.checkpointRoot, item.CheckpointID)
	if checkpointMaterialized(checkpointPath) {
		return nil
	}
	archivePath := filepath.Join(m.checkpointRoot, item.CheckpointID+checkpointArchiveExtension)
	if err := writeCacheContentFile(ctx, m.client, archivePath, item.Hash, item.SizeBytes, routingKey); err != nil {
		return err
	}
	defer os.Remove(archivePath)
	return materializeCheckpointArchive(archivePath, checkpointPath, item.CheckpointID)
}

// materializeWorkspaceObject fetches a workspace object from object storage using
// gateway-brokered workspace storage credentials and stores it under its content
// hash. Credentials ride only in the in-flight store request; they are not
// persisted on the worker.
func (m *WorkerCacheManager) materializeWorkspaceObject(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem) string {
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
		Kind:        item.Kind,
		Status:      status,
		Source:      item.Source,
		SizeBytes:   item.SizeBytes,
	})
}
