package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	rendezvous "github.com/beam-cloud/rendezvous"
)

const requiredContentLockMinTTL = 30 * time.Second

type requiredContentReconciler struct {
	server         *Server
	config         RequiredContentConfig
	repository     RequiredContentRepository
	hostDirectory  HostDirectory
	originResolver RequiredContentOriginResolver
	startOnce      sync.Once
}

func newRequiredContentReconciler(server *Server, config RequiredContentConfig) *requiredContentReconciler {
	if server == nil || !config.Enabled {
		return nil
	}
	repository, ok := server.metadataStore.(RequiredContentRepository)
	if !ok || repository == nil {
		Logger.Warnf("required content reconciliation disabled: required content repository unavailable")
		return nil
	}
	hostDirectory, ok := server.metadataStore.(HostDirectory)
	if !ok || hostDirectory == nil {
		Logger.Warnf("required content reconciliation disabled: host directory unavailable")
		return nil
	}
	var originResolver RequiredContentOriginResolver
	if resolver, ok := server.metadataStore.(RequiredContentOriginResolver); ok {
		originResolver = resolver
	}
	return &requiredContentReconciler{
		server:         server,
		config:         NormalizeRequiredContentConfig(config),
		repository:     repository,
		hostDirectory:  hostDirectory,
		originResolver: originResolver,
	}
}

func (r *requiredContentReconciler) Start() {
	if r == nil || r.server == nil {
		return
	}
	r.startOnce.Do(func() {
		go r.run()
	})
}

func (r *requiredContentReconciler) run() {
	r.reconcileOnce()

	ticker := time.NewTicker(r.config.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.server.ctx.Done():
			return
		case <-ticker.C:
			r.reconcileOnce()
		}
	}
}

func (r *requiredContentReconciler) reconcileOnce() {
	ctx := r.server.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	since := time.Now().UTC().Add(-r.config.StubTTL)
	stubs, err := r.repository.ListRecentStubLocalities(ctx, r.server.locality, since, r.config.BatchSize)
	if err != nil {
		Logger.Debugf("required content reconciliation list stubs failed: %v", err)
		return
	}
	if len(stubs) == 0 {
		return
	}

	hasher, err := r.currentHasher(ctx)
	if err != nil {
		Logger.Debugf("required content reconciliation host list failed: %v", err)
		return
	}

	jobs := make(chan RequiredContentItem)
	var wg sync.WaitGroup
	workers := r.config.ReconcileConcurrency
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				r.reconcileItem(ctx, item)
			}
		}()
	}

	budget := requiredContentCycleBudget{max: r.config.MaxBytesPerCycle}
	stop := false
	for _, stub := range stubs {
		if stop {
			break
		}
		items, err := r.repository.ListRequiredContentForStub(ctx, stub.Locality, stub.WorkspaceID, stub.StubID, r.config.BatchSize)
		if err != nil {
			Logger.Debugf("required content reconciliation list items failed: locality=%s workspace=%s stub=%s err=%v", stub.Locality, stub.WorkspaceID, stub.StubID, err)
			continue
		}
		for _, item := range items {
			item = item.Normalized()
			if item.Hash == "" || !r.ownsItem(hasher, item) {
				continue
			}
			schedule, exhausted := r.canScheduleItem(ctx, item, &budget)
			if exhausted {
				stop = true
				break
			}
			if !schedule {
				continue
			}
			select {
			case jobs <- item:
			case <-ctx.Done():
				stop = true
			}
			if stop {
				break
			}
		}
	}
	close(jobs)
	wg.Wait()
}

func (r *requiredContentReconciler) canScheduleItem(ctx context.Context, item RequiredContentItem, budget *requiredContentCycleBudget) (schedule bool, exhausted bool) {
	if item.SizeBytes > 0 {
		if !budget.takeAvailable(item.SizeBytes) {
			return false, true
		}
		return budget.take(item.SizeBytes), false
	}
	if r.localContentComplete(item) {
		return true, false
	}
	if budget.limited() {
		r.setStatus(ctx, item, RequiredContentStatusSkipped, "required content size unknown")
		return false, false
	}
	return true, false
}

type requiredContentCycleBudget struct {
	max  int64
	used int64
}

func (b *requiredContentCycleBudget) limited() bool {
	return b != nil && b.max > 0
}

func (b *requiredContentCycleBudget) take(size int64) bool {
	if size <= 0 || b.max <= 0 {
		return true
	}
	if !b.takeAvailable(size) {
		return false
	}
	b.used += size
	return true
}

func (b *requiredContentCycleBudget) takeAvailable(size int64) bool {
	return b == nil || b.max <= 0 || size <= 0 || b.used+size <= b.max
}

func (r *requiredContentReconciler) currentHasher(ctx context.Context) (RendezvousHasher, error) {
	hosts, err := r.hostDirectory.GetAvailableHosts(ctx, r.server.locality)
	if err != nil {
		return nil, err
	}
	sort.Slice(hosts, func(i, j int) bool {
		return hosts[i].HostId < hosts[j].HostId
	})
	seen := map[string]struct{}{}
	logicalHosts := make([]*Host, 0, len(hosts))
	for _, host := range hosts {
		if host == nil || host.HostId == "" {
			continue
		}
		if _, ok := seen[host.HostId]; ok {
			continue
		}
		seen[host.HostId] = struct{}{}
		logicalHosts = append(logicalHosts, host.LogicalOnly())
	}
	if len(logicalHosts) == 0 {
		return nil, ErrHostNotFound
	}
	hasher := rendezvous.New[*Host]()
	hasher.Add(logicalHosts...)
	return hasher, nil
}

func (r *requiredContentReconciler) ownsItem(hasher RendezvousHasher, item RequiredContentItem) bool {
	if hasher == nil || r.server == nil || r.server.hostId == "" {
		return false
	}
	key := item.RoutingKey
	if key == "" {
		key = item.Hash
	}
	hosts := hasher.GetN(1, key)
	return len(hosts) == 1 && hosts[0] != nil && hosts[0].HostId == r.server.hostId
}

func (r *requiredContentReconciler) reconcileItem(ctx context.Context, item RequiredContentItem) {
	item = item.Normalized()
	if r.server.cas == nil {
		return
	}
	if r.localContentComplete(item) {
		r.markPresent(ctx, item)
		return
	}

	lockTTL := r.config.ReconcileInterval * 2
	if lockTTL < requiredContentLockMinTTL {
		lockTTL = requiredContentLockMinTTL
	}
	lock, acquired, err := r.repository.AcquireRequiredContentReconciliationLock(ctx, r.server.locality, r.server.hostId, item.Hash, lockTTL)
	if err != nil {
		r.setStatus(ctx, item, RequiredContentStatusError, err.Error())
		return
	}
	if !acquired {
		return
	}
	defer lock.Release(context.Background())

	refreshDone := make(chan struct{})
	go refreshRequiredContentLock(ctx, lock, lockTTL, refreshDone)
	defer close(refreshDone)

	r.setStatus(ctx, item, RequiredContentStatusMaterializing, "")
	if r.localContentComplete(item) {
		r.markPresent(ctx, item)
		return
	}
	if err := r.materializeFromReplica(ctx, item); err != nil {
		if !errors.Is(err, ErrContentNotFound) && !errors.Is(err, ErrSelectedHostUnavailable) && !errors.Is(err, ErrHostNotFound) {
			Logger.Debugf("required content replica materialization failed: hash=%s routing_key=%s err=%v", item.Hash, item.RoutingKey, err)
		}
	} else {
		r.markPresent(ctx, item)
		return
	}

	if !r.config.OriginFallbackEnabled || r.originResolver == nil {
		r.setStatus(ctx, item, RequiredContentStatusSourceMissing, "no cache replica available")
		return
	}
	if err := r.materializeFromOrigin(ctx, item); err != nil {
		if errors.Is(err, ErrContentNotFound) {
			r.setStatus(ctx, item, RequiredContentStatusSourceMissing, "no cache replica or origin available")
			return
		}
		r.setStatus(ctx, item, RequiredContentStatusError, err.Error())
		return
	}
	r.markPresent(ctx, item)
}

func (r *requiredContentReconciler) markPresent(ctx context.Context, item RequiredContentItem) {
	r.recordLocalContentSize(ctx, item)
	r.setStatus(ctx, item, RequiredContentStatusPresent, "")
}

func (r *requiredContentReconciler) recordLocalContentSize(ctx context.Context, item RequiredContentItem) {
	if item.SizeBytes > 0 || r.repository == nil || r.server == nil || r.server.cas == nil {
		return
	}
	size, ok := r.server.cas.ContentSize(item.Hash)
	if !ok || size <= 0 {
		return
	}
	item.SizeBytes = size
	if err := r.repository.UpsertRequiredContent(ctx, item, r.config.StubTTL); err != nil {
		Logger.Debugf("required content size update failed: hash=%s size=%d err=%v", item.Hash, size, err)
	}
}

func refreshRequiredContentLock(ctx context.Context, lock RequiredContentReconciliationLock, ttl time.Duration, done <-chan struct{}) {
	if lock == nil || ttl <= 0 {
		return
	}
	interval := ttl / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			_ = lock.Refresh(ctx, ttl)
		}
	}
}

func (r *requiredContentReconciler) localContentComplete(item RequiredContentItem) bool {
	if r == nil || r.server == nil || r.server.cas == nil {
		return false
	}
	status := r.server.cas.ContentStatus(item.Hash, item.SizeBytes)
	return status == contentStatusComplete
}

func (r *requiredContentReconciler) materializeFromReplica(ctx context.Context, item RequiredContentItem) error {
	if r.server.peerClient == nil {
		return ErrContentNotFound
	}
	if item.SizeBytes <= 0 {
		return fmt.Errorf("required content size is required for replica materialization")
	}
	hash := item.ExpectedHash
	if hash == "" {
		hash = item.Hash
	}
	concurrency := r.config.ReconcileConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	_, storedSize, err := r.server.cas.AddPageSourceWithExpectedHash(ctx, hash, item.SizeBytes, concurrency, func(ctx context.Context, _ int64, offset int64, length int64) ([]byte, error) {
		buf := make([]byte, int(length))
		n, err := r.server.peerClient.ReadContentInto(ctx, item.Hash, offset, buf, ClientOptions{RoutingKey: item.RoutingKey})
		if err != nil {
			return nil, err
		}
		if n != length {
			return nil, io.ErrUnexpectedEOF
		}
		return buf, nil
	})
	if err != nil {
		return err
	}
	if storedSize != item.SizeBytes {
		return fmt.Errorf("stored required content size mismatch: expected %d got %d", item.SizeBytes, storedSize)
	}
	Logger.Debugf("required content materialized from cache replica: hash=%s routing_key=%s bytes=%d", item.Hash, item.RoutingKey, storedSize)
	return nil
}

func (r *requiredContentReconciler) materializeFromOrigin(ctx context.Context, item RequiredContentItem) error {
	instruction, ok, err := r.originResolver.ResolveRequiredContentOrigin(ctx, item)
	if err != nil {
		return err
	}
	if !ok {
		return ErrContentNotFound
	}
	expectedHash := firstNonEmpty(instruction.ExpectedHash, item.ExpectedHash, item.Hash)
	if !isContentHash(expectedHash) {
		return fmt.Errorf("valid expected hash is required for origin materialization")
	}
	Logger.Debugf(
		"required content origin resolved: hash=%s routing_key=%s source=%s bucket=%s cache_path=%s expected_hash=%s",
		item.Hash,
		item.RoutingKey,
		instruction.Path,
		instruction.BucketName,
		instruction.CachePath,
		expectedHash,
	)
	req := &proto.CacheStoreContentFromSourceRequest{Source: &proto.CacheSource{
		Path:           instruction.Path,
		BucketName:     instruction.BucketName,
		Region:         instruction.Region,
		EndpointUrl:    instruction.EndpointURL,
		AccessKey:      instruction.AccessKey,
		SecretKey:      instruction.SecretKey,
		CachePath:      instruction.CachePath,
		ForcePathStyle: instruction.ForcePathStyle,
		ExpectedHash:   expectedHash,
	}}
	resp, err := r.server.storeContentFromSource(ctx, req)
	if err != nil {
		return err
	}
	if resp == nil || !resp.Ok {
		if resp != nil && resp.ErrorMsg != "" {
			return fmt.Errorf("%s", resp.ErrorMsg)
		}
		return ErrUnableToPopulateContent
	}
	if resp.Hash != expectedHash {
		return fmt.Errorf("origin materialized unexpected hash: expected %s got %s", expectedHash, resp.Hash)
	}
	completeItem := item
	completeItem.Hash = expectedHash
	completeItem.ExpectedHash = expectedHash
	if !r.localContentComplete(completeItem) {
		return fmt.Errorf("origin materialized hash %s but local content is incomplete", expectedHash)
	}
	return nil
}

func (r *requiredContentReconciler) setStatus(ctx context.Context, item RequiredContentItem, status RequiredContentReconciliationStatus, errMsg string) {
	if r.repository == nil {
		return
	}
	if err := r.repository.SetRequiredContentReconciliationStatus(ctx, item.Locality, item.WorkspaceID, item.StubID, item.Hash, item.RoutingKey, status, errMsg, r.config.StubTTL); err != nil {
		Logger.Debugf("required content status update failed: hash=%s status=%s err=%v", item.Hash, status, err)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
