package worker

// This file implements the cache required-content reconciliation that runs on
// workers. Responsibilities are split to keep the worker boundary clear:
//
//   - cacheContentReporter: records, on the worker, which content a stub needs
//     (coalesced to S2) and refreshes the per-stub recency window. It never
//     decides placement or moves bytes.
//   - WorkerCacheManager reconcile loop: on the node that currently hosts the
//     cache server, materializes content the local host owns (HRW). Immutable
//     CLIP v2 layers may use a small top-ranked replica set, while checkpoints
//     materialize on every matching accelerator in locality.
//     Ownership has hysteresis: an owner that is briefly endpoint-less (e.g. a
//     rolling deploy) keeps its keys; only after a grace period do its keys
//     fail over to the next-ranked live host. Under disk pressure,
//     materialization is constrained to a ranked recent working set so the
//     newest high-value content survives before older volume-style cache data.
//
// The worker is trustless: all coordinator state (recent stubs, locks) is
// brokered through the gateway, and all origin credentials are fetched from the
// gateway on demand and held in memory only. Nothing secret is written to disk,
// Redis, or S2.

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	reporterFlushInterval    = 5 * time.Second
	reporterMaxItemsPerEvent = 512
	reporterClaimLeaseTTL    = 30 * time.Second
	reconcileItemTimeout     = 5 * time.Minute
	originCredentialsTTL     = 5 * time.Minute
	// The full reconciliation pass intentionally scans the complete recent
	// working set so it can protect and prune content safely. Keep a separate,
	// small hot lane for newly-touched stubs so that scan cannot delay burst
	// image replication.
	reconcileHotInterval = reporterFlushInterval
	reconcileHotMaxStubs = 8
	reconcileHotMaxItems = 16
	// Keep one hot pass shorter than its polling interval. A slow coordinator
	// or cache peer must not pin the only hot-lane goroutine and starve newer stubs.
	reconcileHotPassTimeout = 4 * time.Second
	// reconcileFailureBackoff throttles retries (and logs) for items that fail
	// to materialize, e.g. an unresolvable origin source.
	reconcileFailureBackoff = 15 * time.Minute
)

// originCredentials holds short-lived, gateway-brokered credentials used to
// fetch content from origin during reconciliation. It is held in memory only
// and never written to disk, Redis, or S2.
type originCredentials struct {
	registryCredentials   string
	workspaceStorage      *pb.CacheWorkspaceStorageCredentials
	imageArchiveStorage   *pb.CacheWorkspaceStorageCredentials
	imageArchiveObjectKey string
	imageArchiveURL       string
	imageArchiveDataURL   string
	fetchedAt             time.Time
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

func (m *WorkerCacheManager) reconcileMaxItemsPerCycle() int {
	items := m.config.Cache.Reconciliation.MaxItemsPerCycle
	if items <= 0 {
		items = cacheDefaultReconcileMaxItemsCycle
	}
	return items
}

// reconcileMaxDiskUsagePct is the local disk usage fraction above which
// proactive materialization pauses.
func (m *WorkerCacheManager) reconcileMaxDiskUsagePct() float64 {
	pct := m.config.Cache.Reconciliation.MaxDiskUsagePct
	if pct <= 0 || pct > 1 {
		pct = cacheDefaultReconcileMaxDiskUsagePct
	}
	return pct
}

func reconcileResumeDiskUsagePct(pct float64) float64 {
	resume := pct - cacheReconcileDiskUsageHysteresisPct
	if resume <= 0 || resume >= pct {
		return pct
	}
	return resume
}

func configuredClipV2ReplicaCount(cacheConfig cache.Config) int {
	replicaCount := cacheConfig.Reconciliation.ClipV2ReplicaCount
	if replicaCount <= 0 {
		replicaCount = 1
	}

	maxReplicas := cacheConfig.Client.NTopHosts
	if maxReplicas <= 0 {
		maxReplicas = cacheDefaultNTopHosts
	}
	if replicaCount > maxReplicas {
		replicaCount = maxReplicas
	}
	return replicaCount
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
	reconcileNow   func()

	flushMu            sync.Mutex
	mu                 sync.Mutex
	pending            map[reporterKey]map[string]types.CacheRequiredContentItem
	pendingRecentStubs map[reporterStubKey]struct{}
	reported           map[string]struct{}
}

type reporterKey struct {
	workspaceID    string
	stubID         string
	kind           types.CacheContentKind
	immutableImage bool
}

type reporterStubKey struct {
	workspaceID string
	stubID      string
}

type requiredContentReport struct {
	kind           types.CacheContentKind
	items          []types.CacheRequiredContentItem
	immutableImage bool
}

func newCacheContentReporter(
	ctx context.Context,
	eventRepo repo.EventRepository,
	metadata cache.CacheMetadataStore,
	locality string,
	recentStubTTL time.Duration,
	volumeMinBytes int64,
	activeStubs func(workspaceID string) []string,
	reconcileNow func(),
) *cacheContentReporter {
	r := &cacheContentReporter{
		ctx:                ctx,
		eventRepo:          eventRepo,
		metadata:           metadata,
		locality:           locality,
		recentStubTTL:      recentStubTTL,
		volumeMinBytes:     volumeMinBytes,
		activeStubs:        activeStubs,
		reconcileNow:       reconcileNow,
		pending:            make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		pendingRecentStubs: make(map[reporterStubKey]struct{}),
		reported:           make(map[string]struct{}),
	}
	return r
}

// touchRecentStub queues a refresh of the recent-stub window so reconciliation
// keeps a stub's content warm for RecentStubTTL after its most recent container.
// It is called on every container start, so duplicate touches are coalesced and
// flushed asynchronously instead of issuing a gateway RPC on the startup path.
func (r *cacheContentReporter) touchRecentStub(workspaceID, stubID string) {
	if r == nil || r.metadata == nil || workspaceID == "" || stubID == "" {
		return
	}

	r.mu.Lock()
	r.queueRecentStubLocked(workspaceID, stubID)
	r.mu.Unlock()
}

func (r *cacheContentReporter) queueRecentStubLocked(workspaceID, stubID string) {
	if r.metadata == nil || workspaceID == "" || stubID == "" {
		return
	}
	if r.pendingRecentStubs == nil {
		r.pendingRecentStubs = make(map[reporterStubKey]struct{})
	}
	r.pendingRecentStubs[reporterStubKey{workspaceID: workspaceID, stubID: stubID}] = struct{}{}
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

	r.mu.Lock()
	defer r.mu.Unlock()
	r.queueRecentStubLocked(workspaceID, stubID)
	for _, report := range reports {
		if len(report.items) == 0 {
			continue
		}
		key := reporterKey{
			workspaceID:    workspaceID,
			stubID:         stubID,
			kind:           report.kind,
			immutableImage: report.immutableImage,
		}
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

	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	r.mu.Lock()
	pending := r.pending
	pendingRecentStubs := r.pendingRecentStubs
	r.pending = make(map[reporterKey]map[string]types.CacheRequiredContentItem)
	r.pendingRecentStubs = make(map[reporterStubKey]struct{})
	r.mu.Unlock()

	if r.eventRepo == nil {
		r.requeue(pending)
		r.requeueRecentStubs(pendingRecentStubs)
		return
	}

	failed := make(map[reporterKey]map[string]types.CacheRequiredContentItem)

	// Dynamic records (checkpoints, volumes, and disk snapshots) intentionally
	// bypass the image claim: they may gain new generations after the first
	// container. Only immutable CLIP image records are cluster-coalesced.
	for key, bucket := range pending {
		if len(bucket) == 0 || key.immutableImage {
			continue
		}
		if !r.publishBucket(key, bucket) {
			failed[key] = bucket
		}
	}

	imagePending := make(map[reporterStubKey]map[reporterKey]map[string]types.CacheRequiredContentItem)
	for key, bucket := range pending {
		if len(bucket) == 0 || !key.immutableImage {
			continue
		}
		stubKey := reporterStubKey{workspaceID: key.workspaceID, stubID: key.stubID}
		buckets := imagePending[stubKey]
		if buckets == nil {
			buckets = make(map[reporterKey]map[string]types.CacheRequiredContentItem)
			imagePending[stubKey] = buckets
		}
		buckets[key] = bucket
	}

	for stubKey, buckets := range imagePending {
		if r.metadata == nil {
			for key, bucket := range buckets {
				if !r.publishBucket(key, bucket) {
					failed[key] = bucket
				}
			}
			continue
		}

		token := uuid.NewString()
		ctx, cancel := r.coordinatorContext()
		claim, err := r.metadata.AcquireStubReport(ctx, r.locality, stubKey.stubID, token, reporterClaimLeaseTTL)
		cancel()
		if err != nil {
			log.Debug().Err(err).Str("stub_id", stubKey.stubID).Msg("failed to acquire required-content report lease")
			// The RPC may have committed before its response was lost. A
			// compare-token release is harmless if it did not and avoids
			// waiting the full lease before another reporter can recover.
			r.releaseStubReportClaim(stubKey.stubID, token)
			r.mergeFailed(failed, buckets)
			continue
		}
		if claim == cache.StubReportComplete {
			continue
		}
		if claim != cache.StubReportAcquired {
			r.mergeFailed(failed, buckets)
			continue
		}

		ok := true
		for key, bucket := range buckets {
			ok = r.publishBucket(key, bucket) && ok
		}
		if ok {
			ctx, cancel = r.coordinatorContext()
			ok, err = r.metadata.CompleteStubReport(ctx, r.locality, stubKey.stubID, token, r.recentStubTTL)
			cancel()
			if err != nil {
				log.Debug().Err(err).Str("stub_id", stubKey.stubID).Msg("failed to complete required-content report lease")
			}
		}
		if !ok {
			// The S2 event is idempotent. Requeueing after an ambiguous
			// completion may duplicate it, but can never durably lose it.
			r.releaseStubReportClaim(stubKey.stubID, token)
			r.mergeFailed(failed, buckets)
		}
	}

	if len(failed) > 0 {
		r.requeue(failed)
	}

	// A recent-stub entry is the visibility edge for reconciliation. Publish
	// required content first, then advance LastSeen only after every detached
	// bucket for that stub is durable (or an immutable report is already
	// complete). Otherwise a cache host can read the old stream, remember the
	// new LastSeen as complete, and skip the newly-published content.
	for key := range failed {
		stubKey := reporterStubKey{workspaceID: key.workspaceID, stubID: key.stubID}
		delete(pendingRecentStubs, stubKey)
	}
	indexed := r.flushRecentStubs(pendingRecentStubs)
	if indexed && r.reconcileNow != nil {
		r.reconcileNow()
	}
}

func (r *cacheContentReporter) publishBucket(key reporterKey, bucket map[string]types.CacheRequiredContentItem) bool {
	items := make([]types.CacheRequiredContentItem, 0, len(bucket))
	for _, item := range bucket {
		items = append(items, item)
	}
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
			return false
		}
	}
	return true
}

func (r *cacheContentReporter) coordinatorContext() (context.Context, context.CancelFunc) {
	ctx := r.ctx
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, cacheCoordinatorRPCTimeout)
}

func (r *cacheContentReporter) releaseStubReportClaim(stubID, token string) {
	if r == nil || r.metadata == nil || stubID == "" || token == "" {
		return
	}
	ctx, cancel := r.coordinatorContext()
	defer cancel()
	if _, err := r.metadata.ReleaseStubReport(ctx, r.locality, stubID, token); err != nil {
		log.Debug().Err(err).Str("stub_id", stubID).Msg("failed to release required-content report lease")
	}
}

func (r *cacheContentReporter) mergeFailed(
	failed map[reporterKey]map[string]types.CacheRequiredContentItem,
	buckets map[reporterKey]map[string]types.CacheRequiredContentItem,
) {
	for key, bucket := range buckets {
		failed[key] = bucket
	}
}

func (r *cacheContentReporter) flushRecentStubs(stubs map[reporterStubKey]struct{}) bool {
	if r == nil || r.metadata == nil || len(stubs) == 0 {
		return false
	}

	ctx := r.ctx
	if ctx == nil || ctx.Err() != nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, cacheCoordinatorRPCTimeout)
	defer cancel()

	failed := make(map[reporterStubKey]struct{})
	indexed := false
	for key := range stubs {
		if err := r.metadata.AddRecentStub(ctx, r.locality, key.workspaceID, key.stubID, r.recentStubTTL); err != nil {
			log.Debug().Err(err).Str("workspace_id", key.workspaceID).Str("stub_id", key.stubID).Msg("failed to refresh recent stub for cache reconciliation")
			failed[key] = struct{}{}
		} else {
			indexed = true
		}
	}
	if len(failed) > 0 {
		r.requeueRecentStubs(failed)
	}
	return indexed
}

func (r *cacheContentReporter) requeueRecentStubs(stubs map[reporterStubKey]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingRecentStubs == nil {
		r.pendingRecentStubs = make(map[reporterStubKey]struct{}, len(stubs))
	}
	for key := range stubs {
		r.pendingRecentStubs[key] = struct{}{}
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
		r.queueRecentStubLocked(key.workspaceID, key.stubID)
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

// runHotReconciliation polls a bounded newest-stub window independently of the
// full protection/pruning pass. Required-content publication happens on an
// ordinary worker, so its process-local reconcile wakeup cannot wake the
// cache-server daemonset that owns proactive materialization.
func (m *WorkerCacheManager) runHotReconciliation() {
	defer m.wg.Done()

	m.reconcileHotOnce()
	ticker := time.NewTicker(reconcileHotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.reconcileHotOnce()
		}
	}
}

// reconcileHotOnce immediately reconciles a small newest-first stub window.
// Empty streams, read errors, and incomplete reconciliation are deliberately
// not remembered: a recent-stub touch can become visible just before its S2
// report or placement view, and the next bounded poll must retry it.
func (m *WorkerCacheManager) reconcileHotOnce() {
	if m.client == nil || m.metadataStore == nil || m.eventRepo == nil {
		return
	}

	m.mu.Lock()
	server := m.server
	draining := m.draining
	m.mu.Unlock()
	if draining || server == nil {
		return
	}

	localHostID := server.HostID()
	if localHostID == "" || m.hotReconciliationUnderDiskPressure(server, localHostID) {
		return
	}

	passCtx, cancel := context.WithTimeout(m.ctx, reconcileHotPassTimeout)
	defer cancel()

	stubs, err := m.metadataStore.ListRecentStubs(
		passCtx,
		m.locality,
		m.recentStubTTL(),
		reconcileHotMaxStubs,
	)
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Msg("hot cache reconciliation failed to list recent stubs")
		return
	}
	m.retainHotReconciledWindow(stubs)

	budget := newReconcileBudget(reconcileHotMaxItems)
	for _, stub := range stubs {
		if m.hotReconcileCompletedAt(stub) {
			continue
		}
		items, err := m.eventRepo.ReadStubCacheRequiredContent(passCtx, stub.WorkspaceID, stub.StubID)
		if err != nil {
			log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("hot cache reconciliation failed to read required content")
			continue
		}
		if len(items) == 0 {
			continue
		}
		hotItems := hotReconcileItems(items)
		if len(hotItems) == 0 {
			m.recordHotReconcileCompletion(stub)
			continue
		}
		_, complete := m.reconcileStubContentWithCompletionContext(passCtx, server, localHostID, stub, hotItems, budget, nil, true)
		if complete {
			m.recordHotReconcileCompletion(stub)
		}
		if budget.exhausted() {
			return
		}
	}
}

func hotReconcileItems(items []types.CacheRequiredContentItem) []types.CacheRequiredContentItem {
	hotItems := make([]types.CacheRequiredContentItem, 0, len(items))
	for _, item := range items {
		if item.Kind == types.CacheContentKindClipV2 {
			hotItems = append(hotItems, item)
		}
	}
	return hotItems
}

func (m *WorkerCacheManager) retainHotReconciledWindow(stubs []cache.RecentStub) {
	window := make(map[reporterStubKey]struct{}, len(stubs))
	for _, stub := range stubs {
		window[reporterStubKey{workspaceID: stub.WorkspaceID, stubID: stub.StubID}] = struct{}{}
	}

	m.hotReconciledMu.Lock()
	defer m.hotReconciledMu.Unlock()
	for key := range m.hotReconciled {
		if _, ok := window[key]; !ok {
			delete(m.hotReconciled, key)
		}
	}
}

func (m *WorkerCacheManager) hotReconcileCompletedAt(stub cache.RecentStub) bool {
	key := reporterStubKey{workspaceID: stub.WorkspaceID, stubID: stub.StubID}
	m.hotReconciledMu.Lock()
	defer m.hotReconciledMu.Unlock()
	lastSeen, ok := m.hotReconciled[key]
	return ok && lastSeen.Equal(stub.LastSeen)
}

func (m *WorkerCacheManager) recordHotReconcileCompletion(stub cache.RecentStub) {
	key := reporterStubKey{workspaceID: stub.WorkspaceID, stubID: stub.StubID}
	m.hotReconciledMu.Lock()
	defer m.hotReconciledMu.Unlock()
	if m.hotReconciled == nil {
		m.hotReconciled = make(map[reporterStubKey]time.Time, reconcileHotMaxStubs)
	}
	if _, ok := m.hotReconciled[key]; !ok && len(m.hotReconciled) >= reconcileHotMaxStubs {
		for staleKey := range m.hotReconciled {
			delete(m.hotReconciled, staleKey)
			break
		}
	}
	m.hotReconciled[key] = stub.LastSeen
}

func (m *WorkerCacheManager) hotReconciliationUnderDiskPressure(server *cache.Server, localHostID string) bool {
	usage, err := server.RefreshDiskUsage()
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Str("logical_host", localHostID).Msg("hot cache reconciliation failed to refresh disk usage")
		return true
	}
	return hotReconciliationBlockedByDiskUsage(
		usage,
		m.reconcileMaxDiskUsagePct(),
		server.DiskMinFreeBytes(),
		server.DiskPressureExceeded(),
	)
}

func hotReconciliationBlockedByDiskUsage(usage cache.DiskUsage, maxUsagePct float64, minFreeBytes int64, hardPressure bool) bool {
	if usage.TotalBytes == 0 || usage.UsagePct >= reconcileResumeDiskUsagePct(maxUsagePct) {
		return true
	}
	return reconcilePressureBytesToFree(
		usage,
		maxUsagePct,
		minFreeBytes,
	) > 0 || hardPressure
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
	m.pruneReconcileSuccesses()

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

	stubContent, requiredContentComplete := m.loadRecentRequiredContent(stubs)
	protectedContent, activeCheckpointIDs := protectedContentFromRecentStubs(stubContent, m.accelerator)
	protectedSetComplete := requiredContentComplete && len(stubs) < maxStubs
	server.SetProtectedContent(protectedContent)
	if protectedSetComplete {
		m.pruneOwnerLocalCache(server, protectedContent, activeCheckpointIDs)
	}
	m.pruneOwnerStubCodeCache(server)

	gated, reconcileAllowlist := m.reconcileGatedByDiskUsage(server, localHostID, protectedContent, stubContent, requiredContentComplete)
	if gated {
		return
	}

	budget := newReconcileBudget(m.reconcileMaxItemsPerCycle())
	for _, content := range stubContent {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		m.reconcileStubContent(server, localHostID, content.stub, content.items, budget, reconcileAllowlist)
		if budget.exhausted() {
			break
		}
	}
}

func (m *WorkerCacheManager) reconcileStub(server *cache.Server, localHostID string, stub cache.RecentStub, budget *reconcileBudget) []string {
	items, err := m.eventRepo.ReadStubCacheRequiredContent(m.ctx, stub.WorkspaceID, stub.StubID)
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("cache reconciliation failed to read required content")
		return nil
	}
	return m.reconcileStubContent(server, localHostID, stub, items, budget, nil)
}

func (m *WorkerCacheManager) reconcileStubContent(server *cache.Server, localHostID string, stub cache.RecentStub, items []types.CacheRequiredContentItem, budget *reconcileBudget, allowlist map[string]struct{}) []string {
	checkpointIDs, _ := m.reconcileStubContentWithCompletion(server, localHostID, stub, items, budget, allowlist)
	return checkpointIDs
}

func (m *WorkerCacheManager) reconcileStubContentWithCompletion(server *cache.Server, localHostID string, stub cache.RecentStub, items []types.CacheRequiredContentItem, budget *reconcileBudget, allowlist map[string]struct{}) ([]string, bool) {
	return m.reconcileStubContentWithCompletionContext(m.ctx, server, localHostID, stub, items, budget, allowlist, false)
}

func (m *WorkerCacheManager) reconcileStubContentWithCompletionContext(ctx context.Context, server *cache.Server, localHostID string, stub cache.RecentStub, items []types.CacheRequiredContentItem, budget *reconcileBudget, allowlist map[string]struct{}, replicaOnly bool) ([]string, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	checkpointIDs := []string{}
	complete := true
	for _, item := range orderedRequiredContentItems(items) {
		select {
		case <-ctx.Done():
			return checkpointIDs, false
		default:
		}

		routingKey := item.RoutingKey
		if routingKey == "" {
			routingKey = item.Hash
		}
		if allowlist != nil {
			if _, ok := allowlist[item.Hash]; !ok {
				continue
			}
		}

		if item.Kind == types.CacheContentKindCheckpoint {
			if !cacheContentAppliesToAccelerator(item, m.accelerator) {
				continue
			}
			if item.CheckpointID != "" {
				checkpointIDs = append(checkpointIDs, item.CheckpointID)
			}
		} else {
			reconciles, present := m.localHostReconcileState(localHostID, routingKey, item.Kind)
			if !reconciles {
				// A partially discovered placement ring is not proof that this
				// host has no responsibility for the item. Only remember a
				// completed hot pass once this host is explicitly represented.
				if !present {
					complete = false
				}
				continue
			}
		}

		if m.requiredContentComplete(server, item, routingKey) {
			continue
		}
		if reconcileSuccessBackoffApplies(item) && m.reconcileRecentlySucceeded(item.Hash, routingKey) {
			complete = false
			continue
		}

		// Back off items that recently failed to materialize (e.g. an
		// unresolvable origin source) so they are not retried and re-logged
		// every cycle.
		if m.reconcileBackingOff(item.Hash, routingKey, stub.LastSeen) {
			complete = false
			continue
		}
		if !budget.take() {
			return checkpointIDs, false
		}

		m.materializeOwnedItem(ctx, server, localHostID, stub, item, routingKey, replicaOnly)
		if !m.requiredContentComplete(server, item, routingKey) {
			complete = false
		}
	}
	return checkpointIDs, complete
}

type recentStubContent struct {
	stub  cache.RecentStub
	items []types.CacheRequiredContentItem
}

func (m *WorkerCacheManager) loadRecentRequiredContent(stubs []cache.RecentStub) ([]recentStubContent, bool) {
	content := make([]recentStubContent, 0, len(stubs))
	complete := true
	for _, stub := range stubs {
		items, err := m.eventRepo.ReadStubCacheRequiredContent(m.ctx, stub.WorkspaceID, stub.StubID)
		if err != nil {
			log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("cache reconciliation failed to read required content")
			complete = false
			continue
		}
		content = append(content, recentStubContent{stub: stub, items: items})
	}
	return content, complete
}

func protectedContentFromRecentStubs(stubs []recentStubContent, accelerator string) (map[string]struct{}, map[string]struct{}) {
	protected := map[string]struct{}{}
	activeCheckpointIDs := map[string]struct{}{}
	for _, stub := range stubs {
		for _, item := range stub.items {
			if item.Hash != "" && cacheContentAppliesToAccelerator(item, accelerator) {
				protected[item.Hash] = struct{}{}
			}
			if item.Kind == types.CacheContentKindCheckpoint && item.CheckpointID != "" && cacheContentAppliesToAccelerator(item, accelerator) {
				activeCheckpointIDs[item.CheckpointID] = struct{}{}
			}
		}
	}
	return protected, activeCheckpointIDs
}

func pressureProtectedContentFromRecentStubs(stubs []recentStubContent, accelerator string, usage cache.DiskUsage, softWatermark float64, minFreeBytes int64) map[string]struct{} {
	budget := pressureProtectionBudgetBytes(usage, softWatermark, minFreeBytes)
	if budget <= 0 {
		return map[string]struct{}{}
	}

	orderedStubs := append([]recentStubContent(nil), stubs...)
	sort.SliceStable(orderedStubs, func(i, j int) bool {
		return orderedStubs[i].stub.LastSeen.After(orderedStubs[j].stub.LastSeen)
	})

	protected := map[string]struct{}{}
	var protectedBytes int64
	for _, stub := range orderedStubs {
		for _, item := range orderedRequiredContentItems(stub.items) {
			if item.Hash == "" || !cacheContentAppliesToAccelerator(item, accelerator) {
				continue
			}
			if _, ok := protected[item.Hash]; ok {
				continue
			}
			sizeBytes := maxInt64(item.SizeBytes, 0)
			if sizeBytes > 0 && protectedBytes+sizeBytes > budget {
				continue
			}
			protected[item.Hash] = struct{}{}
			protectedBytes += sizeBytes
		}
	}
	return protected
}

func pressureProtectionBudgetBytes(usage cache.DiskUsage, softWatermark float64, minFreeBytes int64) int64 {
	if usage.TotalBytes == 0 {
		return 0
	}
	resumeWatermark := reconcileResumeDiskUsagePct(softWatermark)
	budget := int64(resumeWatermark * float64(usage.TotalBytes))
	if minFreeBytes > 0 {
		if reserveBudget := int64(usage.TotalBytes) - minFreeBytes; reserveBudget < budget {
			budget = reserveBudget
		}
	}
	return maxInt64(budget, 0)
}

func orderedRequiredContentItems(items []types.CacheRequiredContentItem) []types.CacheRequiredContentItem {
	ordered := append([]types.CacheRequiredContentItem(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftPriority := cacheContentKindPriority(ordered[i].Kind)
		rightPriority := cacheContentKindPriority(ordered[j].Kind)
		if leftPriority != rightPriority {
			return leftPriority > rightPriority
		}
		if ordered[i].SizeBytes > 0 && ordered[j].SizeBytes > 0 && ordered[i].SizeBytes != ordered[j].SizeBytes {
			return ordered[i].SizeBytes < ordered[j].SizeBytes
		}
		return ordered[i].Hash < ordered[j].Hash
	})
	return ordered
}

func cacheContentKindPriority(kind types.CacheContentKind) int {
	switch kind {
	case types.CacheContentKindCheckpoint:
		return 500
	case types.CacheContentKindClipV1, types.CacheContentKindClipV2:
		return 400
	case types.CacheContentKindDiskSnapshot:
		return 300
	case types.CacheContentKindVolume:
		return 100
	default:
		return 0
	}
}

func cacheContentAppliesToAccelerator(item types.CacheRequiredContentItem, accelerator string) bool {
	return item.Kind != types.CacheContentKindCheckpoint || item.Accelerator == "" || strings.EqualFold(item.Accelerator, accelerator)
}

func (m *WorkerCacheManager) pruneOwnerLocalCache(server *cache.Server, protected map[string]struct{}, activeCheckpointIDs map[string]struct{}) {
	if server == nil {
		return
	}
	if evicted, freed := server.PruneContentNotProtected(protected, m.recentStubTTL()); evicted > 0 {
		log.Info().
			Int("evicted", evicted).
			Int64("freed_bytes", freed).
			Dur("recent_stub_ttl", m.recentStubTTL()).
			Msg("pruned stale embedded cache content")
	}
	m.pruneLocalCheckpoints(activeCheckpointIDs)
	m.pruneStaleCacheCheckpoints()
}

const (
	stubCodeReadyMarker      = ".beta9-cache-ready"
	stubCodeTempDirGraceTime = 30 * time.Minute
)

type stubCodeEntry struct {
	path      string
	lastUsed  time.Time
	temporary bool
}

func (m *WorkerCacheManager) pruneOwnerStubCodeCache(server *cache.Server) {
	if m == nil || server == nil {
		return
	}
	root := stubCodeCacheRoot(m.config, m.poolConfig)
	if root == "" {
		return
	}
	pruned, freed := pruneStubCodeCache(root, m.recentStubTTL())
	if pruned > 0 {
		log.Info().Int("pruned", pruned).Int64("freed_bytes", freed).Str("root", root).Msg("pruned stale stub-code cache entries")
	}

	usage, err := fastDiskUsage(root)
	if err != nil {
		return
	}
	targetFree := reconcilePressureBytesToFree(usage, cacheDefaultStubCodeEvictWatermark, m.config.Cache.Disk.MinFreeBytes)
	if targetFree <= 0 {
		return
	}
	evicted, pressureFreed := pressureEvictStubCodeCache(root, targetFree)
	if evicted > 0 {
		log.Warn().
			Int("evicted", evicted).
			Int64("freed_bytes", pressureFreed).
			Float64("disk_usage_pct", usage.UsagePct).
			Str("root", root).
			Msg("pressure-evicted stub-code cache entries")
	}
}

func pruneStubCodeCache(root string, ttl time.Duration) (int, int64) {
	if ttl <= 0 {
		return 0, 0
	}
	entries := listStubCodeEntries(root)
	now := time.Now()
	cutoff := now.Add(-ttl)
	tempCutoff := now.Add(-stubCodeTempDirGraceTime)
	pruned := 0
	var freed int64
	for _, entry := range entries {
		if entry.temporary {
			if entry.lastUsed.After(tempCutoff) {
				continue
			}
		} else if entry.lastUsed.After(cutoff) {
			continue
		}
		sizeBytes := dirSizeBytesRecursive(entry.path)
		if err := os.RemoveAll(entry.path); err != nil {
			log.Debug().Err(err).Str("path", entry.path).Msg("failed to prune stub-code cache entry")
			continue
		}
		pruned++
		freed += sizeBytes
	}
	return pruned, freed
}

func pressureEvictStubCodeCache(root string, bytesToFree int64) (int, int64) {
	entries := listStubCodeEntries(root)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUsed.Before(entries[j].lastUsed)
	})
	tempCutoff := time.Now().Add(-stubCodeTempDirGraceTime)
	evicted := 0
	var freed int64
	for _, entry := range entries {
		if bytesToFree > 0 && freed >= bytesToFree {
			break
		}
		if entry.temporary && entry.lastUsed.After(tempCutoff) {
			continue
		}
		sizeBytes := dirSizeBytesRecursive(entry.path)
		if err := os.RemoveAll(entry.path); err != nil {
			log.Debug().Err(err).Str("path", entry.path).Msg("failed to pressure-evict stub-code cache entry")
			continue
		}
		evicted++
		freed += sizeBytes
	}
	return evicted, freed
}

func listStubCodeEntries(root string) []stubCodeEntry {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	out := make([]stubCodeEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		item := stubCodeEntry{
			path:     path,
			lastUsed: info.ModTime(),
		}
		if strings.Contains(entry.Name(), ".tmp.") {
			item.temporary = true
			out = append(out, item)
			continue
		}
		if readyInfo, err := os.Stat(filepath.Join(path, stubCodeReadyMarker)); err == nil {
			item.lastUsed = readyInfo.ModTime()
			out = append(out, item)
		}
	}
	return out
}

func dirSizeBytesRecursive(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if info, err := entry.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

func fastDiskUsage(path string) (cache.DiskUsage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return cache.DiskUsage{}, err
	}
	totalBytes := uint64(stat.Blocks) * uint64(stat.Bsize)
	availableBytes := uint64(stat.Bavail) * uint64(stat.Bsize)
	usedBytes := totalBytes - availableBytes
	usagePct := 0.0
	if totalBytes > 0 {
		usagePct = float64(usedBytes) / float64(totalBytes)
	}
	return cache.DiskUsage{
		TotalBytes:     totalBytes,
		AvailableBytes: availableBytes,
		UsedBytes:      usedBytes,
		UsagePct:       usagePct,
	}, nil
}

// reconcileGatedByDiskUsage keeps reconciliation balanced under pressure. Above
// the soft watermark it evicts content outside the ranked recent working set and
// pauses the current cycle; between the resume and soft watermarks it reconciles
// only that ranked set. This avoids download/evict loops while still favoring
// the newest useful content on a mostly-full node.
func (m *WorkerCacheManager) reconcileGatedByDiskUsage(server *cache.Server, localHostID string, protected map[string]struct{}, stubContent []recentStubContent, requiredContentComplete bool) (bool, map[string]struct{}) {
	usage, err := server.RefreshDiskUsage()
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Str("logical_host", localHostID).Msg("cache reconciliation failed to refresh disk usage")
		usage = cache.DiskUsage{
			UsagePct:       server.UsagePct(),
			AvailableBytes: uint64(maxInt64(server.AvailableDiskBytes(), 0)),
		}
	}

	softWatermark := m.reconcileMaxDiskUsagePct()
	resumeWatermark := reconcileResumeDiskUsagePct(softWatermark)
	pressureMode := usage.UsagePct >= resumeWatermark
	if !m.reconcilePausedAt.IsZero() {
		if usage.UsagePct > resumeWatermark {
			pressureMode = true
		} else {
			log.Info().
				Str("locality", m.locality).
				Str("logical_host", localHostID).
				Dur("paused_for", time.Since(m.reconcilePausedAt)).
				Float64("disk_usage_pct", usage.UsagePct).
				Float64("resume_watermark_pct", resumeWatermark).
				Msg("cache reconciliation resumed: disk usage below resume watermark")
			m.reconcilePausedAt = time.Time{}
		}
	}

	reconcileAllowlist := map[string]struct{}(nil)
	protectedForPressure := protected
	if pressureMode {
		if usage.TotalBytes > 0 {
			reconcileAllowlist = pressureProtectedContentFromRecentStubs(stubContent, m.accelerator, usage, softWatermark, server.DiskMinFreeBytes())
			protectedForPressure = reconcileAllowlist
		}
		server.SetProtectedContent(protectedForPressure)
	}

	bytesToFree := reconcilePressureBytesToFree(usage, softWatermark, server.DiskMinFreeBytes())
	if bytesToFree > 0 && !requiredContentComplete {
		server.SetProtectedContent(protected)
		if m.reconcilePausedAt.IsZero() {
			m.reconcilePausedAt = time.Now()
			log.Warn().
				Str("locality", m.locality).
				Str("logical_host", localHostID).
				Int64("target_free_bytes", bytesToFree).
				Float64("disk_usage_pct", usage.UsagePct).
				Float64("soft_watermark_pct", softWatermark).
				Msg("cache reconciliation paused: required content set incomplete under disk pressure")
		}
		return true, nil
	}
	if bytesToFree > 0 {
		evicted, freed := server.PressureEvictContent(protectedForPressure, bytesToFree)
		if evicted > 0 {
			log.Warn().
				Str("locality", m.locality).
				Str("logical_host", localHostID).
				Int("evicted", evicted).
				Int64("freed_bytes", freed).
				Int64("target_free_bytes", bytesToFree).
				Float64("disk_usage_pct", usage.UsagePct).
				Float64("soft_watermark_pct", softWatermark).
				Float64("resume_watermark_pct", resumeWatermark).
				Int("protected_candidates", len(protectedForPressure)).
				Msg("pressure-evicted lower-priority cache content before pausing reconciliation")
		}
		if m.reconcilePausedAt.IsZero() {
			m.reconcilePausedAt = time.Now()
		}
		return true, nil
	}

	if server.DiskPressureExceeded() {
		if m.reconcilePausedAt.IsZero() {
			m.reconcilePausedAt = time.Now()
			log.Warn().
				Str("locality", m.locality).
				Str("logical_host", localHostID).
				Float64("disk_usage_pct", usage.UsagePct).
				Uint64("available_bytes", usage.AvailableBytes).
				Float64("soft_watermark_pct", softWatermark).
				Int64("min_free_bytes", server.DiskMinFreeBytes()).
				Msg("cache reconciliation paused: hard disk write gate active")
		}
		return true, nil
	}

	if pressureMode {
		return false, reconcileAllowlist
	}
	server.SetProtectedContent(protected)
	return false, nil
}

func reconcilePressureBytesToFree(usage cache.DiskUsage, softWatermark float64, minFreeBytes int64) int64 {
	bytesToFree := int64(0)
	if softWatermark > 0 && softWatermark < 1 && usage.TotalBytes > 0 && usage.UsagePct > softWatermark {
		targetUsed := int64(softWatermark * float64(usage.TotalBytes))
		if deficit := int64(usage.UsedBytes) - targetUsed; deficit > bytesToFree {
			bytesToFree = deficit
		}
	}
	if minFreeBytes > 0 {
		if deficit := minFreeBytes - int64(usage.AvailableBytes); deficit > bytesToFree {
			bytesToFree = deficit
		}
	}
	return bytesToFree
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// localHostOwnsForReconcile reports whether this host should proactively
// materialize the given key. The HRW owner keeps its keys while it is live or
// only briefly endpoint-less (e.g. a rolling deploy: its on-disk content
// survives the restart, and duplicating its entire key range onto peers is
// what causes post-deploy materialization storms). Once the owner has been
// endpoint-less past the grace period, ownership for reconciliation purposes
// falls through to the next-ranked live host, preserving self-healing when a
// node is really gone. Hosts that stay gone also age out of the ring itself.
func (m *WorkerCacheManager) localHostOwnsForReconcile(localHostID, routingKey string) bool {
	return m.localHostReplicatesForReconcile(localHostID, routingKey, 1)
}

func (m *WorkerCacheManager) localHostReconcilesContent(localHostID, routingKey string, kind types.CacheContentKind) bool {
	reconciles, _ := m.localHostReconcileState(localHostID, routingKey, kind)
	return reconciles
}

func (m *WorkerCacheManager) localHostReconcileState(localHostID, routingKey string, kind types.CacheContentKind) (bool, bool) {
	replicaCount := 1
	if kind == types.CacheContentKindClipV2 {
		replicaCount = configuredClipV2ReplicaCount(m.config.Cache)
	}
	return m.localHostReplicaState(localHostID, routingKey, replicaCount)
}

func (m *WorkerCacheManager) localHostReplicatesForReconcile(localHostID, routingKey string, replicaCount int) bool {
	replicates, _ := m.localHostReplicaState(localHostID, routingKey, replicaCount)
	return replicates
}

func (m *WorkerCacheManager) localHostReplicaState(localHostID, routingKey string, replicaCount int) (bool, bool) {
	hosts := m.client.RankedReadHosts(routingKey)
	// Membership must come from the full discovered host map, not the bounded
	// read window. Most cache hosts are intentionally outside the top-N for a
	// given key and should complete the hot pass without polling S2 forever.
	present := m.client.HasHost(localHostID)
	if replicaCount <= 0 {
		return false, present
	}

	now := time.Now()
	rank := 0
	for _, host := range hosts {
		switch {
		case host == nil:
			continue
		case host.HasEndpoint():
			m.ownerSeenLive(host.HostId, now)
		case now.Sub(m.ownerLastLiveAt(host.HostId, now)) < cacheReconcileOwnerGracePeriod:
			// A replica that is endpoint-less but still within grace retains
			// its rank so a rolling restart does not reshuffle its key range.
		default:
			// Endpoint-less past grace: fill this replica rank from the next
			// host in rendezvous order.
			continue
		}

		if host.HostId == localHostID {
			return true, present
		}
		rank++
		if rank >= replicaCount {
			return false, present
		}
	}
	return false, present
}

func (m *WorkerCacheManager) ownerSeenLive(hostID string, now time.Time) {
	m.ownerLastLiveMu.Lock()
	defer m.ownerLastLiveMu.Unlock()
	if m.ownerLastLive == nil {
		m.ownerLastLive = make(map[string]time.Time)
	}
	m.ownerLastLive[hostID] = now
}

// ownerLastLiveAt returns when hostID was last observed with a live endpoint.
// A host seen for the first time while endpoint-less starts its grace window
// now, so a freshly-restarted process doesn't immediately treat peers that
// were down before it started as permanently gone.
func (m *WorkerCacheManager) ownerLastLiveAt(hostID string, now time.Time) time.Time {
	m.ownerLastLiveMu.Lock()
	defer m.ownerLastLiveMu.Unlock()
	if m.ownerLastLive == nil {
		m.ownerLastLive = make(map[string]time.Time)
	}
	if lastLive, ok := m.ownerLastLive[hostID]; ok {
		return lastLive
	}
	m.ownerLastLive[hostID] = now
	return now
}

type reconcileBudget struct {
	remaining int
	limited   bool
	empty     bool
}

func newReconcileBudget(limit int) *reconcileBudget {
	return &reconcileBudget{remaining: limit, limited: limit > 0}
}

func (b *reconcileBudget) take() bool {
	if b == nil || !b.limited {
		return true
	}
	if b.remaining <= 0 {
		b.empty = true
		return false
	}
	b.remaining--
	return true
}

func (b *reconcileBudget) exhausted() bool {
	return b != nil && b.empty
}

func (m *WorkerCacheManager) materializeOwnedItem(ctx context.Context, server *cache.Server, localHostID string, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string, replicaOnly bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	acquired, err := m.metadataStore.AcquireReconcileLock(ctx, m.locality, localHostID, item.Hash, m.reconcileLockTTLSeconds())
	if err != nil || !acquired {
		// Another materialization is already in flight for this item (or the
		// coordinator is unavailable); try again next cycle.
		return
	}
	defer func() {
		// Release remains best-effort even when the pass deadline or manager
		// context expires; otherwise a four-second hot pass could strand a
		// five-minute lock and delay the next healthy attempt.
		releaseCtx, cancel := context.WithTimeout(context.Background(), cacheCoordinatorRPCTimeout)
		defer cancel()
		if err := m.metadataStore.ReleaseReconcileLock(releaseCtx, m.locality, localHostID, item.Hash); err != nil {
			log.Debug().Err(err).Str("hash", item.Hash).Msg("failed to release cache reconciliation lock")
		}
	}()

	// Re-check after acquiring the lock; another process may have just completed it.
	if m.requiredContentComplete(server, item, routingKey) {
		return
	}

	itemCtx, cancel := context.WithTimeout(ctx, reconcileItemTimeout)
	defer cancel()

	m.reconcileLogFields(log.Debug(), localHostID, stub, item).
		Str("source", item.Source).
		Int64("size_bytes", item.SizeBytes).
		Msg("reconciling missing cache content")

	startedAt := time.Now()
	status := m.materialize(itemCtx, server, stub, item, routingKey, replicaOnly)
	elapsed := time.Since(startedAt)

	switch {
	case status == types.CacheAuditStatusMaterialized:
		m.clearReconcileFailure(item.Hash, routingKey)
		if reconcileSuccessBackoffApplies(item) {
			m.recordReconcileSuccess(item.Hash, routingKey)
		} else {
			m.clearReconcileSuccess(item.Hash, routingKey)
		}
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

// Success backoff is only valid when the cache blob is the complete materialized
// state. Checkpoints also require an extracted runtime/filesystem payload, which
// can be pruned independently of the archive blob.
func reconcileSuccessBackoffApplies(item types.CacheRequiredContentItem) bool {
	return item.Kind != types.CacheContentKindCheckpoint
}

// reconcileBackingOff reports whether an item failed to materialize recently and
// should be skipped until the backoff window elapses. Expired entries are pruned
// so the map only tracks currently-failing items.
func (m *WorkerCacheManager) reconcileBackingOff(hash, routingKey string, stubLastSeen time.Time) bool {
	key := reconcileItemKey(hash, routingKey)
	m.reconcileFailuresMu.Lock()
	defer m.reconcileFailuresMu.Unlock()

	failedAt, ok := m.reconcileFailures[key]
	if !ok {
		return false
	}
	if reconcileStubSeenAfterFailure(stubLastSeen, failedAt) {
		delete(m.reconcileFailures, key)
		return false
	}
	if time.Since(failedAt) < reconcileFailureBackoff {
		return true
	}
	delete(m.reconcileFailures, key)
	return false
}

func reconcileStubSeenAfterFailure(stubLastSeen, failedAt time.Time) bool {
	if stubLastSeen.IsZero() {
		return false
	}
	if stubLastSeen.After(failedAt) {
		return true
	}
	if stubLastSeen.Nanosecond() == 0 {
		return !stubLastSeen.Before(failedAt.Truncate(time.Second))
	}
	return false
}

func (m *WorkerCacheManager) recordReconcileFailure(hash, routingKey string) {
	key := reconcileItemKey(hash, routingKey)
	m.reconcileFailuresMu.Lock()
	if m.reconcileFailures == nil {
		m.reconcileFailures = make(map[string]time.Time)
	}
	m.reconcileFailures[key] = time.Now()
	m.reconcileFailuresMu.Unlock()
}

func (m *WorkerCacheManager) clearReconcileFailure(hash, routingKey string) {
	key := reconcileItemKey(hash, routingKey)
	m.reconcileFailuresMu.Lock()
	delete(m.reconcileFailures, key)
	m.reconcileFailuresMu.Unlock()
}

func (m *WorkerCacheManager) reconcileRecentlySucceeded(hash, routingKey string) bool {
	key := reconcileItemKey(hash, routingKey)
	m.reconcileSuccessesMu.Lock()
	defer m.reconcileSuccessesMu.Unlock()

	succeededAt, ok := m.reconcileSuccesses[key]
	if !ok {
		return false
	}
	if time.Since(succeededAt) < cacheReconcileSuccessBackoff {
		return true
	}
	delete(m.reconcileSuccesses, key)
	return false
}

func (m *WorkerCacheManager) recordReconcileSuccess(hash, routingKey string) {
	key := reconcileItemKey(hash, routingKey)
	m.reconcileSuccessesMu.Lock()
	if m.reconcileSuccesses == nil {
		m.reconcileSuccesses = make(map[string]time.Time)
	}
	m.reconcileSuccesses[key] = time.Now()
	m.reconcileSuccessesMu.Unlock()
}

func (m *WorkerCacheManager) clearReconcileSuccess(hash, routingKey string) {
	key := reconcileItemKey(hash, routingKey)
	m.reconcileSuccessesMu.Lock()
	delete(m.reconcileSuccesses, key)
	m.reconcileSuccessesMu.Unlock()
}

func (m *WorkerCacheManager) pruneReconcileSuccesses() {
	m.reconcileSuccessesMu.Lock()
	defer m.reconcileSuccessesMu.Unlock()
	for key, succeededAt := range m.reconcileSuccesses {
		if time.Since(succeededAt) >= cacheReconcileSuccessBackoff {
			delete(m.reconcileSuccesses, key)
		}
	}
}

func reconcileItemKey(hash, routingKey string) string {
	return hash + "\x00" + routingKey
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
	pruneCutoff := time.Now().Add(-m.recentStubTTL())
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
		path := filepath.Join(m.checkpointRoot, name)
		if checkpointLocalPathFresh(path, pruneCutoff) {
			continue
		}
		_ = os.RemoveAll(path)
	}
}

func checkpointLocalPathFresh(path string, pruneCutoff time.Time) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.ModTime().After(pruneCutoff)
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
		log.Info().Str("locality", m.locality).Int32("pruned", resp.Pruned).Msg("pruned stale cache-managed checkpoints")
	}
}

// reconcileLogFields adds the fields common to per-item reconciliation logs.
func (m *WorkerCacheManager) reconcileLogFields(event *zerolog.Event, localHostID string, stub cache.RecentStub, item types.CacheRequiredContentItem) *zerolog.Event {
	return event.
		Str("locality", m.locality).
		Str("logical_host", localHostID).
		Str("workspace_id", stub.WorkspaceID).
		Str("stub_id", stub.StubID).
		Str("image_id", item.ImageID).
		Str("hash", item.Hash).
		Str("kind", string(item.Kind))
}

// materialize copies content for an owned item onto the local cache server. It
// prefers a reachable replica and otherwise fetches from the item's origin in
// the same way the read path does. It never persists credentials in Redis or S2.
func (m *WorkerCacheManager) materialize(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string, replicaOnly bool) string {
	if item.Kind == types.CacheContentKindCheckpoint {
		return m.materializeCheckpoint(ctx, server, stub, item, routingKey)
	}

	replicaSize := m.replicaMaterializationSize(ctx, item)
	if ok, err := m.client.MaterializeFromReplica(ctx, server, item.Hash, routingKey, replicaSize); err != nil {
		log.Debug().Err(err).Str("hash", item.Hash).Msg("cache reconciliation replica copy failed")
	} else if ok {
		return types.CacheAuditStatusMaterialized
	}

	if replicaOnly {
		return types.CacheAuditStatusMiss
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
	case types.CacheContentKindVolume, types.CacheContentKindDiskSnapshot:
		return m.materializeWorkspaceObject(ctx, server, stub, item)
	case types.CacheContentKindClipV1:
		// The v1 archive is one content-addressed object; re-fetch the whole
		// archive from the image registry (the same source the image-load path
		// pulls it from) and store it under its hash + cachefs path.
		return m.materializeArchiveObject(ctx, server, stub, item, routingKey)
	default:
		return types.CacheAuditStatusMiss
	}
}

// replicaMaterializationSize resolves the exact byte length required by the
// bounded replica stream. CLIP v2 reports intentionally stay off the startup
// critical path and therefore may not include a size; the build/runtime cache
// already records the exact layer size under its cachefs metadata path.
func (m *WorkerCacheManager) replicaMaterializationSize(ctx context.Context, item types.CacheRequiredContentItem) int64 {
	if item.SizeBytes > 0 {
		return item.SizeBytes
	}
	if item.Kind != types.CacheContentKindClipV2 || m.client == nil || item.Hash == "" {
		return 0
	}

	metadata, err := m.client.CacheFSMetadata(ctx, imageLayerContentCachePath(item.Hash))
	if err != nil || metadata == nil || metadata.Hash != item.Hash || metadata.Size == 0 {
		return 0
	}
	if metadata.Size > uint64(^uint64(0)>>1) {
		return 0
	}
	return int64(metadata.Size)
}

// materializeArchiveObject re-fetches the whole CLIP v1 archive from the image
// registry and stores it as a single content object, mirroring the embedded
// image-archive cache that the image-load path populates. It pulls from the same
// source the load path uses: the S3 image registry for the S3 store, or the
// mounted image volume for the local store. No credentials are persisted.
func (m *WorkerCacheManager) materializeArchiveObject(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string) string {
	source := &pb.CacheSource{
		CachePath:    routingKey,
		ExpectedHash: item.Hash,
	}

	if m.config.ImageService.RegistryStore == reg.S3ImageRegistryStore {
		s3 := m.config.ImageService.Registries.S3
		if s3.BucketName == "" || s3.AccessKey == "" || s3.SecretKey == "" {
			imageID := item.ImageID
			if imageID == "" {
				imageID = imageIDFromArchiveSource(item.Source)
			}
			creds := m.originCredentials(ctx, stub.WorkspaceID, stub.StubID, "", imageID)
			if creds != nil && creds.imageArchiveStorage != nil {
				s3 = imageArchiveRegistryConfig(creds.imageArchiveStorage)
			} else if creds != nil && creds.imageArchiveDataURL != "" {
				// No S3 credentials are vended to private-pool workers; fetch
				// the archive through the gateway-presigned URL instead.
				return m.materializeArchiveObjectFromURL(ctx, server, item, routingKey, creds.imageArchiveDataURL)
			}
		}
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

// materializeArchiveObjectFromURL downloads the CLIP v1 data archive through a
// gateway-presigned URL into a temp file on the cache disk and stores it on the
// local cache server under its content hash + cachefs path. Used by
// private-pool workers, which hold no S3 credentials.
func (m *WorkerCacheManager) materializeArchiveObjectFromURL(ctx context.Context, server *cache.Server, item types.CacheRequiredContentItem, routingKey, url string) string {
	tmp, err := os.CreateTemp(filepath.Dir(m.checkpointRoot), "archive-origin-*.tmp")
	if err != nil {
		log.Debug().Err(err).Str("hash", item.Hash).Msg("cache reconciliation failed to create archive temp file")
		return types.CacheAuditStatusOriginFailure
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	if err := downloadImageArchiveURL(ctx, url, tmpPath); err != nil {
		log.Debug().Err(err).Str("hash", item.Hash).Str("routing_key", routingKey).Msg("cache reconciliation image archive url fetch failed")
		return types.CacheAuditStatusOriginFailure
	}

	resp, err := server.StoreContentFromSource(ctx, &pb.CacheStoreContentFromSourceRequest{
		Source: &pb.CacheSource{
			Path:         tmpPath,
			CachePath:    routingKey,
			ExpectedHash: item.Hash,
		},
	})
	if err == nil && resp != nil && resp.Ok {
		return types.CacheAuditStatusMaterialized
	}
	log.Debug().Err(err).Str("hash", item.Hash).Str("routing_key", routingKey).Msg("cache reconciliation image archive url store failed")
	return types.CacheAuditStatusOriginFailure
}

// imageIDFromArchiveSource derives the image ID from a CLIP v1 required-content
// source descriptor (the data archive object key, "<imageId>.clip").
func imageIDFromArchiveSource(source string) string {
	base := filepath.Base(source)
	if !strings.HasSuffix(base, "."+reg.LocalImageFileExtension) {
		return ""
	}
	return strings.TrimSuffix(base, "."+reg.LocalImageFileExtension)
}

func (m *WorkerCacheManager) requiredContentComplete(server *cache.Server, item types.CacheRequiredContentItem, routingKey string) bool {
	if item.Kind == types.CacheContentKindCheckpoint && item.CheckpointID != "" {
		return server.HasCompleteContent(item.Hash, item.SizeBytes) &&
			checkpointMaterialized(filepath.Join(m.checkpointRoot, item.CheckpointID))
	}
	return server.HasCompleteContent(item.Hash, item.SizeBytes)
}

func (m *WorkerCacheManager) materializeCheckpoint(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string) string {
	if item.CheckpointID == "" || item.Hash == "" || item.SizeBytes <= 0 {
		return types.CacheAuditStatusMiss
	}
	release, err := m.acquireCheckpointMaterialization(ctx, item.CheckpointID)
	if err != nil {
		log.Debug().Err(err).Str("checkpoint_id", item.CheckpointID).Msg("cache reconciliation checkpoint materialization canceled")
		return types.CacheAuditStatusSkipped
	}
	defer release()
	if m.requiredContentComplete(server, item, routingKey) {
		return types.CacheAuditStatusMaterialized
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
	if err := m.extractCheckpointArchive(ctx, server, item); err != nil {
		log.Debug().Err(err).Str("checkpoint_id", item.CheckpointID).Msg("cache reconciliation checkpoint extract failed")
		return types.CacheAuditStatusOriginFailure
	}
	return types.CacheAuditStatusMaterialized
}

func (m *WorkerCacheManager) extractCheckpointArchive(ctx context.Context, server *cache.Server, item types.CacheRequiredContentItem) error {
	checkpointPath := filepath.Join(m.checkpointRoot, item.CheckpointID)
	if checkpointMaterialized(checkpointPath) {
		return nil
	}
	if server == nil {
		return fmt.Errorf("cache server is unavailable")
	}
	reader := newCheckpointCacheReader(ctx, item.Hash, item.SizeBytes, server.ReadContentInto)
	return materializeCheckpointReader(ctx, reader, item.Hash, item.SizeBytes, checkpointPath, item.CheckpointID, nil)
}

// materializeWorkspaceObject fetches a workspace object from object storage using
// gateway-brokered workspace storage credentials and stores it under its content
// hash. Credentials ride only in the in-flight store request; they are not
// persisted on the worker.
func (m *WorkerCacheManager) materializeWorkspaceObject(ctx context.Context, server *cache.Server, stub cache.RecentStub, item types.CacheRequiredContentItem) string {
	creds := m.originCredentials(ctx, stub.WorkspaceID, stub.StubID, "", "")
	if creds == nil || creds.workspaceStorage == nil {
		log.Debug().Str("hash", item.Hash).Str("workspace_id", stub.WorkspaceID).Msg("cache reconciliation has no workspace storage credentials")
		return types.CacheAuditStatusOriginFailure
	}

	ws := creds.workspaceStorage
	bucketName := ws.BucketName
	if item.SourceBucket != "" {
		bucketName = item.SourceBucket
	}
	req := &pb.CacheStoreContentFromSourceRequest{
		Source: &pb.CacheSource{
			Path:           item.Source,
			ExpectedHash:   item.Hash,
			BucketName:     bucketName,
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
	if !isSHA256HexDigest(item.Hash) {
		log.Debug().Str("hash", item.Hash).Msg("cache reconciliation invalid oci layer content hash")
		return types.CacheAuditStatusOriginFailure
	}

	ref, err := name.NewDigest(item.Source)
	if err != nil {
		log.Debug().Err(err).Str("source", item.Source).Msg("cache reconciliation could not parse oci layer reference")
		return types.CacheAuditStatusOriginFailure
	}

	authOption := remote.WithAuthFromKeychain(authn.DefaultKeychain)
	if creds := m.originCredentials(ctx, stub.WorkspaceID, stub.StubID, ref.Context().RegistryStr(), item.ImageID); creds != nil && creds.registryCredentials != "" {
		if authenticator := registryAuthenticator(ctx, ref, creds.registryCredentials); authenticator != nil {
			authOption = remote.WithAuth(authenticator)
		}
	} else if m.poolConfig.Mode == types.PoolModePrivate {
		log.Debug().
			Str("source", item.Source).
			Str("workspace_id", stub.WorkspaceID).
			Str("stub_id", stub.StubID).
			Str("image_id", item.ImageID).
			Msg("private worker has no gateway-vended registry credentials for oci layer")
		return types.CacheAuditStatusOriginFailure
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
func (m *WorkerCacheManager) originCredentials(ctx context.Context, workspaceID, stubID, registry, imageID string) *originCredentials {
	if m.workerRepo == nil || workspaceID == "" || stubID == "" {
		return nil
	}

	// The registry and image are part of the key: registry credentials are
	// registry-scoped and presigned archive URLs are image-scoped, so callers
	// requesting different registries or images (or none, for volume fetches)
	// must not reuse each other's cached auth.
	key := workspaceID + "\x00" + stubID + "\x00" + registry + "\x00" + imageID
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
		ImageId:     imageID,
	}))
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", workspaceID).Str("stub_id", stubID).Msg("cache reconciliation failed to fetch origin credentials")
		return nil
	}

	creds := &originCredentials{
		registryCredentials:   resp.RegistryCredentials,
		workspaceStorage:      resp.WorkspaceStorage,
		imageArchiveStorage:   resp.ImageArchiveStorage,
		imageArchiveObjectKey: resp.ImageArchiveObjectKey,
		imageArchiveURL:       resp.ImageArchiveUrl,
		imageArchiveDataURL:   resp.ImageArchiveDataUrl,
		fetchedAt:             time.Now(),
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
		Timestamp:   time.Now().UTC(),
	})
}

func (m *WorkerCacheManager) attachCacheChurnSink(server *cache.Server, localHostID string) {
	if server == nil {
		return
	}
	server.SetChurnSink(func(event cache.CacheChurnEvent) {
		m.auditCacheChurnEvent(localHostID, event)
	})
}

func (m *WorkerCacheManager) auditCacheChurnEvent(localHostID string, event cache.CacheChurnEvent) {
	if m.eventRepo == nil {
		return
	}

	machineID := strings.TrimSpace(os.Getenv(types.WorkerMachineEnv))
	workspaceID := cacheChurnWorkspaceID(m.config)
	m.eventRepo.PushPlatformCacheEvent(types.EventPlatformCacheSchema{
		Locality:            m.locality,
		LogicalHost:         localHostID,
		WorkspaceID:         workspaceID,
		WorkerID:            m.workerID,
		MachineID:           machineID,
		PoolName:            m.poolName,
		NodeID:              m.nodeID,
		Status:              event.Status,
		Operation:           event.Operation,
		CachePath:           event.Path,
		FreedBytes:          event.FreedBytes,
		ProtectedFreedBytes: event.ProtectedFreedBytes,
		EvictedObjects:      event.EvictedObjects,
		ProtectedObjects:    event.ProtectedObjects,
		UsagePct:            event.UsagePct,
		WatermarkPct:        event.WatermarkPct,
		AvailableBytes:      event.AvailableBytes,
		ReserveBytes:        event.ReserveBytes,
		TargetFreeBytes:     event.TargetFreeBytes,
		TotalCandidates:     event.TotalCandidates,
		ProtectedCandidates: event.ProtectedCandidates,
		RecentCandidates:    event.RecentCandidates,
		EligibleCandidates:  event.EligibleCandidates,
		Timestamp:           event.Timestamp,
	})
}

func cacheChurnWorkspaceID(config types.AppConfig) string {
	if workspaceID := strings.TrimSpace(config.ManagedCompute.SellerWorkspaceID); workspaceID != "" {
		return workspaceID
	}

	parts := strings.Split(strings.Trim(config.Database.S2.EventStreamPrefix, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "workspaces" {
			return parts[i+1]
		}
	}
	return ""
}
