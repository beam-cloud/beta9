package worker

// This file implements the cache required-content reconciliation that runs on
// workers. Responsibilities are split to keep the worker boundary clear:
//
//   - cacheContentReporter: records, on the worker, which content a stub needs
//     (coalesced to S2) and refreshes the per-stub recency window. It never
//     decides placement or moves bytes.
//   - WorkerCacheManager reconcile loop: on the node that currently hosts the
//     cache server, materializes content the local host owns (HRW), except
//     checkpoints and disk snapshots which materialize on every matching
//     accelerator in locality. Ownership has hysteresis: an owner that is
//     briefly endpoint-less (e.g. a rolling deploy) keeps its keys; only after
//     a grace period do its keys fail over to the next-ranked live host.
//
// The loop runs at two cadences. A sync (every few seconds, and immediately
// when this worker publishes) lists the locality's recent stubs, refreshes the
// store's protected set and pulls what is missing; the store answers every
// completeness check from memory, so a quiet sync costs one coordinator round
// trip. A maintenance pass (the configured interval) does the work that walks
// disks: TTL pruning of content, checkpoints, image and stub-code caches.
//
// Disk pressure is handled by one mechanism: the reconciler decides what is
// protected and the store evicts everything else, LRU, when usage crosses the
// eviction watermark. Above the watermark the protected set shrinks to the
// newest stubs' content that fits below it and materialization pauses; the
// pause lifts with hysteresis so the two never churn against each other.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/clip/pkg/clip"
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
	registryCredentials   string
	workspaceStorage      *pb.CacheWorkspaceStorageCredentials
	imageArchiveStorage   *pb.CacheWorkspaceStorageCredentials
	imageArchiveObjectKey string
	imageArchiveURL       string
	imageArchiveDataURL   string
	fetchedAt             time.Time
}

// reconcileInterval is how often a cache host runs the maintenance pass.
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

func (m *WorkerCacheManager) reconcileConcurrency() int {
	n := m.config.Cache.Reconciliation.MaxConcurrentFetches
	if n <= 0 {
		n = cacheDefaultReconcileConcurrency
	}
	return n
}

// reconcileMaxBytesPerCycle bounds bytes fetched per cycle so the disk-usage
// gate is re-evaluated regularly; exhausted cycles re-kick immediately.
func (m *WorkerCacheManager) reconcileMaxBytesPerCycle() int64 {
	b := m.config.Cache.Reconciliation.MaxBytesPerCycle
	if b <= 0 {
		b = cacheDefaultReconcileMaxBytesCycle
	}
	return b
}

func reconcileResumeDiskUsagePct(pct float64) float64 {
	resume := pct - cacheReconcileDiskUsageHysteresisPct
	if resume <= 0 || resume >= pct {
		return pct
	}
	return resume
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

	mu       sync.Mutex
	pending  map[reporterKey]map[string]types.CacheRequiredContentItem
	recent   map[reporterStubKey]struct{}
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
	reconcileNow func(),
) *cacheContentReporter {
	r := &cacheContentReporter{
		ctx:            ctx,
		eventRepo:      eventRepo,
		metadata:       metadata,
		locality:       locality,
		recentStubTTL:  recentStubTTL,
		volumeMinBytes: volumeMinBytes,
		activeStubs:    activeStubs,
		reconcileNow:   reconcileNow,
		pending:        make(map[reporterKey]map[string]types.CacheRequiredContentItem),
		recent:         make(map[reporterStubKey]struct{}),
		reported:       make(map[string]struct{}),
	}
	go r.run()
	return r
}

// touchRecentStub coalesces burst traffic into the reporter's periodic flush.
func (r *cacheContentReporter) touchRecentStub(workspaceID, stubID string) {
	if r == nil || r.metadata == nil || workspaceID == "" || stubID == "" {
		return
	}
	r.mu.Lock()
	r.recent[reporterStubKey{workspaceID: workspaceID, stubID: stubID}] = struct{}{}
	r.mu.Unlock()
}

// shouldGenerateRequiredContent reports whether this worker process has already
// enumerated a stub's required content. The durable S2 stream is the source of
// truth; the map only stops one process from re-enumerating the same stub.
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
	r.recent[reporterStubKey{workspaceID: workspaceID, stubID: stubID}] = struct{}{}
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
	recent := r.recent
	r.pending = make(map[reporterKey]map[string]types.CacheRequiredContentItem)
	r.recent = make(map[reporterStubKey]struct{})
	r.mu.Unlock()

	if r.eventRepo == nil {
		return
	}

	failed := make(map[reporterKey]map[string]types.CacheRequiredContentItem)
	published := false
	for key, bucket := range pending {
		if len(bucket) == 0 {
			continue
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
			failed[key] = bucket
			continue
		}
		published = true
	}

	// A stub whose recency refresh fails stays invisible to every reconciler
	// in the locality even though its content is published, so it is retried
	// with the next flush like a failed publish.
	indexed := false
	unindexed := make(map[reporterStubKey]struct{})
	if r.metadata != nil {
		for key := range recent {
			if err := r.metadata.AddRecentStub(r.ctx, r.locality, key.workspaceID, key.stubID, r.recentStubTTL); err != nil {
				log.Debug().Err(err).Str("workspace_id", key.workspaceID).Str("stub_id", key.stubID).Msg("failed to refresh recent stub for cache reconciliation")
				unindexed[key] = struct{}{}
				continue
			}
			indexed = true
		}
	}
	if len(failed) > 0 || len(unindexed) > 0 {
		r.requeue(failed, unindexed)
	}
	if (published || indexed) && r.reconcileNow != nil {
		r.reconcileNow()
	}
}

func (r *cacheContentReporter) requeue(items map[reporterKey]map[string]types.CacheRequiredContentItem, recent map[reporterStubKey]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range recent {
		r.recent[key] = struct{}{}
	}
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

	sync := time.NewTicker(cacheReconcileSyncInterval)
	defer sync.Stop()
	maintain := time.NewTicker(m.reconcileInterval())
	defer maintain.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.reconcileNow:
			m.reconcileOnce(false)
		case <-sync.C:
			m.reconcileOnce(false)
		case <-maintain.C:
			m.reconcileOnce(true)
		}
	}
}

// reconcileOnce runs one sync, and the maintenance pass too when asked.
func (m *WorkerCacheManager) reconcileOnce(maintain bool) {
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

	started := time.Now()
	var stubCount int
	defer func() {
		// A cycle should cost a coordinator round trip and little else; the
		// trace says so, or says where a cycle went. Work that changes the
		// cache (materialized items, prunes) logs itself at info.
		log.Debug().
			Str("locality", m.locality).
			Bool("maintain", maintain).
			Int("stubs", stubCount).
			Dur("duration", time.Since(started)).
			Msg("cache reconciliation cycle")
	}()

	stubs, err := m.listRecentStubs(maintain)
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Msg("cache reconciliation failed to list recent stubs")
		return
	}
	stubCount = len(stubs)

	stubContent, requiredContentComplete := m.loadRecentRequiredContent(stubs)
	protectedContent, activeCheckpointIDs := protectedContentFromRecentStubs(stubContent, m.accelerator)
	server.SetProtectedContent(protectedContent)

	if maintain {
		// TTL pruning is only safe with a complete picture of what is
		// required; a failed required-content read defers it to the next pass.
		if requiredContentComplete {
			m.pruneOwnerLocalCache(server, protectedContent, activeCheckpointIDs)
		}
		m.pruneOwnerImageCache(stubContent, server.EvictWatermarkPct(), server.DiskMinFreeBytes(), requiredContentComplete)
		m.pruneOwnerStubCodeCache(server)
	}

	gated, reconcileAllowlist := m.reconcileGatedByDiskUsage(server, localHostID, protectedContent, stubContent)
	if gated {
		return
	}

	// Stubs arrive MRU-first from the recent index.
	budget := newReconcileBudget(m.reconcileMaxItemsPerCycle(), m.reconcileMaxBytesPerCycle())
	pool := newReconcilePool(m.reconcileConcurrency())
	for _, content := range stubContent {
		select {
		case <-m.ctx.Done():
			pool.wait()
			return
		default:
		}
		m.reconcileStubContent(server, localHostID, content.stub, content.items, budget, reconcileAllowlist, pool)
		if budget.exhausted() {
			break
		}
	}
	pool.wait()

	// An exhausted budget means known pending work: re-kick immediately with
	// a fresh MRU listing instead of waiting for the next tick. Requiring
	// local progress avoids hot-looping on contended or missing items.
	if budget.exhausted() && pool.didWork() {
		m.requestReconcile()
	}
}

func (m *WorkerCacheManager) reconcileStub(server *cache.Server, localHostID string, stub cache.RecentStub, budget *reconcileBudget) []string {
	items, err := m.eventRepo.ReadStubCacheRequiredContent(m.ctx, stub.WorkspaceID, stub.StubID)
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("cache reconciliation failed to read required content")
		return nil
	}
	return m.reconcileStubContent(server, localHostID, stub, items, budget, nil, nil)
}

func (m *WorkerCacheManager) reconcileStubContent(server *cache.Server, localHostID string, stub cache.RecentStub, items []types.CacheRequiredContentItem, budget *reconcileBudget, allowlist map[string]struct{}, pool *reconcilePool) []string {
	checkpointIDs := []string{}
	for _, item := range orderedRequiredContentItems(items) {
		select {
		case <-m.ctx.Done():
			return checkpointIDs
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
		} else if !reconcileOnEveryHost(item.Kind) && !m.localHostOwnsForReconcile(localHostID, routingKey) {
			continue
		}

		if m.requiredContentComplete(server, item, routingKey) {
			continue
		}
		if reconcileSuccessBackoffApplies(item) && m.reconcileRecentlySucceeded(item.Hash, routingKey) {
			continue
		}

		// Back off items that recently failed to materialize (e.g. an
		// unresolvable origin source) so they are not retried and re-logged
		// every cycle.
		if m.reconcileBackingOff(item.Hash, routingKey, stub.LastSeen) {
			continue
		}
		// Stubs share content (e.g. base image layers); dedupe submissions
		// within the cycle instead of burning budget on lock contention.
		if !pool.claim(reconcileItemKey(item.Hash, routingKey)) {
			continue
		}
		if !budget.take(item.SizeBytes) {
			return checkpointIDs
		}

		pool.submit(func() {
			if m.materializeOwnedItem(server, localHostID, stub, item, routingKey) {
				pool.markProgress()
			}
		})
	}
	return checkpointIDs
}

// reconcileOnEveryHost reports whether a content kind replicates to every host
// in the locality instead of sharding by ring owner. Checkpoints and disk
// snapshot chunks are what a machine restore reads, so full replication lets a
// post-reconcile start run at local disk speed on any host; everything else
// stays owner-sharded and is served to peers over the network.
func reconcileOnEveryHost(kind types.CacheContentKind) bool {
	return kind == types.CacheContentKindCheckpoint || kind == types.CacheContentKindDiskSnapshot
}

// reconcilePool bounds concurrent materializations within a reconcile cycle.
// A nil pool runs work inline (the sequential path used by reconcileStub).
type reconcilePool struct {
	wg       sync.WaitGroup
	ch       chan struct{}
	progress atomic.Bool
	// seen dedupes (hash, routingKey) submissions within one cycle.
	// Reconcile loop goroutine only; no lock.
	seen map[string]struct{}
}

func newReconcilePool(concurrency int) *reconcilePool {
	if concurrency <= 1 {
		return nil
	}
	return &reconcilePool{ch: make(chan struct{}, concurrency), seen: map[string]struct{}{}}
}

// claim marks the item as submitted this cycle, returning false on repeats.
// A nil pool never dedupes; the sequential path sees completed items via
// requiredContentComplete on the next iteration.
func (p *reconcilePool) claim(key string) bool {
	if p == nil {
		return true
	}
	if _, ok := p.seen[key]; ok {
		return false
	}
	p.seen[key] = struct{}{}
	return true
}

// markProgress records that a task landed content locally, i.e. the cycle
// moved the working set forward rather than spinning on misses or contention.
func (p *reconcilePool) markProgress() {
	if p != nil {
		p.progress.Store(true)
	}
}

func (p *reconcilePool) didWork() bool {
	return p != nil && p.progress.Load()
}

func (p *reconcilePool) submit(fn func()) {
	if p == nil {
		fn()
		return
	}
	p.ch <- struct{}{}
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.ch
			p.wg.Done()
		}()
		fn()
	}()
}

func (p *reconcilePool) wait() {
	if p == nil {
		return
	}
	p.wg.Wait()
}

type recentStubContent struct {
	stub  cache.RecentStub
	items []types.CacheRequiredContentItem
}

// recentStubWindow is the locality's recent-stub index as this worker last
// saw it: every stub accessed within the recency window, keyed by
// workspace|stub. Last-seen times arrive at second resolution, so each stub
// also carries the order it was listed in: the coordinator lists MRU-first
// at full precision, and later listings carry later activity.
type recentStubWindow struct {
	stubs    map[string]recentStubEntry
	listedAt time.Time
	seq      uint64
}

type recentStubEntry struct {
	stub cache.RecentStub
	seq  uint64
}

// recentStubExpiryTolerance covers the coordinator truncating last-seen times
// to whole seconds, so a stub is never aged out locally before it leaves the
// coordinator's window.
const recentStubExpiryTolerance = time.Second

// listRecentStubs returns every stub accessed within the recency window,
// MRU-first. The whole window is needed, not a page of it: the protected set
// derived from it is what keeps pruning from removing content a recent stub
// still needs, and a truncated set would be unsafe.
//
// Only a full pass lists the whole window. A sync lists the stubs touched
// since the previous listing and merges them in, so its cost tracks activity
// rather than the number of stubs a busy locality has seen this week; stubs
// age out of the local copy on their own, since a last-seen time and the
// window say when.
func (m *WorkerCacheManager) listRecentStubs(full bool) ([]cache.RecentStub, error) {
	window := m.recentStubTTL()
	now := time.Now()
	lookback := window
	full = full || m.recentStubs.listedAt.IsZero()
	if !full {
		// Overlap by one interval: the coordinator truncates the lookback to
		// whole seconds and its clock is not ours. Re-listing a stub is free.
		lookback = min(now.Sub(m.recentStubs.listedAt)+cacheReconcileSyncInterval, window)
	}

	listed, err := m.metadataStore.ListRecentStubs(m.ctx, m.locality, lookback, 0)
	if err != nil {
		return nil, err
	}
	if full {
		m.recentStubs.stubs = make(map[string]recentStubEntry, len(listed))
	}
	// Walk the listing LRU-first so the most recent stub takes the highest
	// sequence number.
	for i := len(listed) - 1; i >= 0; i-- {
		stub := listed[i]
		m.recentStubs.seq++
		m.recentStubs.stubs[stub.WorkspaceID+"|"+stub.StubID] = recentStubEntry{stub: stub, seq: m.recentStubs.seq}
	}
	m.recentStubs.listedAt = now

	cutoff := now.Add(-window - recentStubExpiryTolerance)
	entries := make([]recentStubEntry, 0, len(m.recentStubs.stubs))
	for key, entry := range m.recentStubs.stubs {
		if entry.stub.LastSeen.Before(cutoff) {
			delete(m.recentStubs.stubs, key)
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if !a.stub.LastSeen.Equal(b.stub.LastSeen) {
			return a.stub.LastSeen.After(b.stub.LastSeen)
		}
		return a.seq > b.seq
	})
	stubs := make([]cache.RecentStub, len(entries))
	for i, entry := range entries {
		stubs[i] = entry.stub
	}
	return stubs, nil
}

type requiredContentCacheEntry struct {
	lastSeen time.Time
	items    []types.CacheRequiredContentItem
}

// loadRecentRequiredContent resolves required-content items for each recent
// stub, cached keyed on the stub's recent-index score: publishers append to S2
// before bumping the score, so an unchanged score means the cached items are
// current. Only stubs with new activity cost an S2 read per cycle.
func (m *WorkerCacheManager) loadRecentRequiredContent(stubs []cache.RecentStub) ([]recentStubContent, bool) {
	content := make([]recentStubContent, 0, len(stubs))
	complete := true
	next := make(map[string]requiredContentCacheEntry, len(stubs))
	for _, stub := range stubs {
		key := stub.WorkspaceID + "|" + stub.StubID
		if entry, ok := m.requiredContentCache[key]; ok && entry.lastSeen.Equal(stub.LastSeen) {
			next[key] = entry
			content = append(content, recentStubContent{stub: stub, items: entry.items})
			continue
		}
		items, err := m.eventRepo.ReadStubCacheRequiredContent(m.ctx, stub.WorkspaceID, stub.StubID)
		if err != nil {
			log.Debug().Err(err).Str("workspace_id", stub.WorkspaceID).Str("stub_id", stub.StubID).Msg("cache reconciliation failed to read required content")
			complete = false
			continue
		}
		next[key] = requiredContentCacheEntry{lastSeen: stub.LastSeen, items: items}
		content = append(content, recentStubContent{stub: stub, items: items})
	}
	m.requiredContentCache = next
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

type imageCacheProtection struct {
	names    map[string]struct{}
	complete bool
}

type imageCacheEntry struct {
	path     string
	name     string
	size     int64
	modified time.Time
}

func (m *WorkerCacheManager) pruneOwnerImageCache(stubs []recentStubContent, softWatermark float64, minFreeBytes int64, protectedSetComplete bool) int64 {
	root := getImageCachePath()
	usage, err := fastDiskUsage(root)
	if err != nil {
		return 0
	}
	bytesToFree := maxInt64(reconcilePressureBytesToFree(usage, softWatermark, minFreeBytes), 0)
	var allowlist map[string]struct{}
	if bytesToFree > 0 {
		allowlist = pressureProtectedContentFromRecentStubs(stubs, m.accelerator, usage, softWatermark, minFreeBytes)
	}
	mountRoot := filepath.Join(types.AgentImagesPath, "mnt")
	mountSetComplete := m.pruneStaleImageMountPaths(mountRoot)
	protected := protectedImageCache(stubs, allowlist, root, mountRoot)
	protected.complete = protected.complete && mountSetComplete
	cutoff := time.Time{}
	if protectedSetComplete {
		cutoff = time.Now().Add(-m.recentStubTTL())
	}
	evicted, freed := evictImageCache(root, protected, cutoff, bytesToFree)
	if evicted > 0 {
		event := log.Info()
		message := "pruned stale image cache content"
		if bytesToFree > 0 {
			event = log.Warn()
			message = "pressure-evicted image cache content"
		}
		event.
			Int("evicted", evicted).
			Int64("freed_bytes", freed).
			Int64("target_free_bytes", bytesToFree).
			Float64("disk_usage_pct", usage.UsagePct).
			Bool("mount_protection_complete", protected.complete).
			Msg(message)
	}
	return freed
}

func (m *WorkerCacheManager) pruneStaleImageMountPaths(mountRoot string) bool {
	if m.workerRepo == nil {
		return false
	}

	rpcCtx, cancel := context.WithTimeout(m.ctx, cacheCoordinatorRPCTimeout)
	defer cancel()
	pruned, complete := pruneStaleImageMountPathsWithLookup(mountRoot, func(workerID string) (bool, error) {
		_, err := handleGRPCResponse(m.workerRepo.GetWorkerById(rpcCtx, &pb.GetWorkerByIdRequest{WorkerId: workerID}))
		if err == nil {
			return true, nil
		}
		notFoundErr := &types.ErrWorkerNotFound{}
		if notFoundErr.From(err) {
			return false, nil
		}
		return false, err
	})
	if pruned > 0 {
		log.Info().Int("pruned", pruned).Str("mount_root", mountRoot).Msg("pruned stale worker image mount paths")
	}
	return complete
}

func pruneStaleImageMountPathsWithLookup(mountRoot string, workerExists func(string) (bool, error)) (int, bool) {
	workers, err := os.ReadDir(mountRoot)
	if err != nil {
		return 0, false
	}

	pruned := 0
	complete := true
	for _, worker := range workers {
		if !worker.IsDir() || worker.Type()&os.ModeSymlink != 0 {
			continue
		}
		exists, err := workerExists(worker.Name())
		if err != nil {
			complete = false
			continue
		}
		if exists {
			continue
		}
		if err := cleanupImageMountPath(filepath.Join(mountRoot, worker.Name())); err != nil {
			complete = false
			continue
		}
		pruned++
	}
	return pruned, complete
}

func protectedImageCache(stubs []recentStubContent, allowlist map[string]struct{}, cacheRoot, mountRoot string) imageCacheProtection {
	protected := imageCacheProtection{names: map[string]struct{}{}, complete: true}
	protectImage := func(imageID string) {
		if imageID == "" {
			return
		}
		for _, suffix := range []string{".clip", ".rclip", ".cache"} {
			protected.names[imageID+suffix] = struct{}{}
		}
	}

	for _, stub := range stubs {
		for _, item := range stub.items {
			if allowlist != nil {
				if _, ok := allowlist[item.Hash]; !ok {
					continue
				}
			}
			protectImage(item.ImageID)
			if item.Kind == types.CacheContentKindClipV2 && isSHA256HexDigest(item.Hash) {
				protected.names[item.Hash] = struct{}{}
			}
		}
	}

	workers, err := os.ReadDir(mountRoot)
	if err != nil {
		protected.complete = false
		return protected
	}
	for _, worker := range workers {
		if !worker.IsDir() || worker.Type()&os.ModeSymlink != 0 {
			continue
		}
		images, err := os.ReadDir(filepath.Join(mountRoot, worker.Name()))
		if err != nil {
			protected.complete = false
			continue
		}
		for _, image := range images {
			if !image.IsDir() || image.Type()&os.ModeSymlink != 0 {
				continue
			}
			protectImage(image.Name())
			layers, ok := activeImageLayers(cacheRoot, image.Name())
			if !ok {
				protected.complete = false
				continue
			}
			for _, hash := range layers {
				protected.names[hash] = struct{}{}
			}
		}
	}
	return protected
}

func activeImageLayers(cacheRoot, imageID string) ([]string, bool) {
	for _, suffix := range []string{".rclip", ".clip"} {
		path := filepath.Join(cacheRoot, imageID+suffix)
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			continue
		}
		meta, err := clip.NewClipArchiver().ExtractMetadata(path)
		if err != nil {
			continue
		}
		oci, ok := ociStorageInfo(meta)
		if !ok {
			return nil, true
		}
		layers := make([]string, 0, len(oci.DecompressedHashByLayer))
		for _, hash := range oci.DecompressedHashByLayer {
			if isSHA256HexDigest(hash) {
				layers = append(layers, hash)
			}
		}
		return layers, true
	}
	return nil, false
}

func evictImageCache(root string, protected imageCacheProtection, cutoff time.Time, bytesToFree int64) (int, int64) {
	if cutoff.IsZero() && bytesToFree <= 0 {
		return 0, 0
	}
	scanStarted := time.Now()
	entries := listImageCacheEntries(root)
	sort.Slice(entries, func(i, j int) bool { return entries[i].modified.Before(entries[j].modified) })

	evicted := 0
	var freed int64
	lockCtx, cancelLocks := context.WithCancel(context.Background())
	cancelLocks()
	for _, entry := range entries {
		stale := !cutoff.IsZero() && entry.modified.Before(cutoff)
		pressure := bytesToFree > 0 && freed < bytesToFree
		if !stale && !pressure {
			continue
		}
		if _, ok := protected.names[entry.name]; ok {
			continue
		}
		// Without complete mount metadata, raw layers cannot be attributed to
		// active images safely. Archive names are still protected directly by
		// their mount directory, so unrelated archives remain evictable.
		if !protected.complete && isSHA256HexDigest(entry.name) {
			continue
		}

		unlock, err := lockImageArchiveFile(lockCtx, entry.path)
		if err != nil {
			continue
		}
		info, statErr := os.Stat(entry.path)
		unchanged := statErr == nil && info.Mode().IsRegular() && info.Size() == entry.size && info.ModTime().Equal(entry.modified) && !info.ModTime().After(scanStarted)
		if unchanged && os.Remove(entry.path) == nil {
			evicted++
			freed += entry.size
		}
		unlock()
	}
	return evicted, freed
}

func listImageCacheEntries(root string) []imageCacheEntry {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	out := make([]imageCacheEntry, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		layer := isSHA256HexDigest(name)
		if !layer {
			switch filepath.Ext(name) {
			case ".clip", ".rclip", ".cache":
			default:
				continue
			}
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			continue
		}
		out = append(out, imageCacheEntry{path: filepath.Join(root, name), name: name, size: info.Size(), modified: info.ModTime()})
	}
	return out
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

// reconcileGatedByDiskUsage keeps materialization and eviction from churning
// against each other on a mostly-full node. Above the eviction watermark the
// protected set shrinks to the newest stubs' content that fits below it, the
// store evicts everything else, and the cycle pauses; the pause holds until
// usage falls below the resume watermark. Between the two watermarks
// materialization is limited to that same ranked set, so nothing is pulled
// that the next eviction would remove.
func (m *WorkerCacheManager) reconcileGatedByDiskUsage(server *cache.Server, localHostID string, protected map[string]struct{}, stubContent []recentStubContent) (bool, map[string]struct{}) {
	usage, err := server.RefreshDiskUsage()
	if err != nil {
		log.Debug().Err(err).Str("locality", m.locality).Str("logical_host", localHostID).Msg("cache reconciliation failed to refresh disk usage")
		usage = cache.DiskUsage{
			UsagePct:       server.UsagePct(),
			AvailableBytes: uint64(maxInt64(server.AvailableDiskBytes(), 0)),
		}
	}

	watermark := server.EvictWatermarkPct()
	resumeWatermark := reconcileResumeDiskUsagePct(watermark)
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

	var reconcileAllowlist map[string]struct{}
	if pressureMode && usage.TotalBytes > 0 {
		reconcileAllowlist = pressureProtectedContentFromRecentStubs(stubContent, m.accelerator, usage, watermark, server.DiskMinFreeBytes())
		server.SetProtectedContent(reconcileAllowlist)
	}

	minFreeBytes := server.DiskMinFreeBytes()
	if reconcilePressureBytesToFree(usage, watermark, minFreeBytes) > 0 && m.reconcilePausedAt.IsZero() {
		// The protected set just shrank; reclaim now rather than on the store
		// monitor's next tick. While a pause holds, the monitor owns eviction:
		// repeating it every sync would only re-walk the same candidates and
		// re-log the same outcome.
		if reclaimed, err := server.ReclaimDisk(); err == nil {
			usage = reclaimed
		}
	}
	if reconcilePressureBytesToFree(usage, watermark, minFreeBytes) > 0 {
		if m.reconcilePausedAt.IsZero() {
			m.reconcilePausedAt = time.Now()
			log.Warn().
				Str("locality", m.locality).
				Str("logical_host", localHostID).
				Float64("disk_usage_pct", usage.UsagePct).
				Uint64("available_bytes", usage.AvailableBytes).
				Float64("watermark_pct", watermark).
				Float64("resume_watermark_pct", resumeWatermark).
				Int("protected_candidates", len(reconcileAllowlist)).
				Msg("cache reconciliation paused: disk above eviction watermark")
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
				Int64("min_free_bytes", minFreeBytes).
				Msg("cache reconciliation paused: hard disk write gate active")
		}
		return true, nil
	}

	return false, reconcileAllowlist
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
	now := time.Now()
	for _, host := range m.client.RankedReadHosts(routingKey) {
		switch {
		case host == nil:
			continue
		case host.HasEndpoint():
			m.ownerSeenLive(host.HostId, now)
			return host.HostId == localHostID
		case now.Sub(m.ownerLastLiveAt(host.HostId, now)) < cacheReconcileOwnerGracePeriod:
			// The owner is endpoint-less but within grace: nobody takes over
			// its keys yet
			return host.HostId == localHostID
		}
		// Owner endpoint-less past grace: fall through to the next-ranked host
	}
	return false
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

// reconcileBudget bounds one reconcile pass by item count and by bytes, so
// gate rechecks stay regular regardless of item size. An item crossing the
// byte boundary is still admitted (a single oversized item must not starve);
// the budget exhausts after it. Reconcile loop goroutine only.
type reconcileBudget struct {
	remaining      int
	limited        bool
	bytesRemaining int64
	byteLimited    bool
	empty          bool
}

func newReconcileBudget(limit int, byteLimit int64) *reconcileBudget {
	return &reconcileBudget{
		remaining:      limit,
		limited:        limit > 0,
		bytesRemaining: byteLimit,
		byteLimited:    byteLimit > 0,
	}
}

func (b *reconcileBudget) take(sizeBytes int64) bool {
	if b == nil || (!b.limited && !b.byteLimited) {
		return true
	}
	if (b.limited && b.remaining <= 0) || (b.byteLimited && b.bytesRemaining <= 0) {
		b.empty = true
		return false
	}
	b.remaining--
	if sizeBytes > 0 {
		b.bytesRemaining -= sizeBytes
	}
	return true
}

func (b *reconcileBudget) exhausted() bool {
	return b != nil && b.empty
}

// materializeOwnedItem fetches a single missing item into the local cache,
// returning true only if the item ended up complete. Misses, failures, and
// lock contention return false so retries alone never re-kick the loop.
func (m *WorkerCacheManager) materializeOwnedItem(server *cache.Server, localHostID string, stub cache.RecentStub, item types.CacheRequiredContentItem, routingKey string) bool {
	acquired, err := m.metadataStore.AcquireReconcileLock(m.ctx, m.locality, localHostID, item.Hash, m.reconcileLockTTLSeconds())
	if err != nil || !acquired {
		// Another materialization is already in flight for this item (or the
		// coordinator is unavailable); try again next cycle.
		return false
	}
	defer func() {
		if err := m.metadataStore.ReleaseReconcileLock(m.ctx, m.locality, localHostID, item.Hash); err != nil {
			log.Debug().Err(err).Str("hash", item.Hash).Msg("failed to release cache reconciliation lock")
		}
	}()

	// Re-check after acquiring the lock; another process may have just completed it.
	if m.requiredContentComplete(server, item, routingKey) {
		return true
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
		if reconcileSuccessBackoffApplies(item) {
			m.recordReconcileSuccess(item.Hash, routingKey)
		} else {
			m.clearReconcileSuccess(item.Hash, routingKey)
		}
		m.reconcileLogFields(log.Debug(), localHostID, stub, item).
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
	return status == types.CacheAuditStatusMaterialized
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
