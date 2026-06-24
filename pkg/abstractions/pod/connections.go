package pod

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/common"
	redis "github.com/redis/go-redis/v9"
)

const (
	connectionSyncInterval      time.Duration = 100 * time.Millisecond
	connectionSnapshotTTL       time.Duration = 2 * time.Second
	connectionIndexRefreshEvery time.Duration = connectionSnapshotTTL / 2
)

func (pb *PodProxyBuffer) incrementTotalConnections() (int64, error) {
	val := pb.totalConnections.Add(1)
	if val == 1 {
		pb.publishTotalConnectionSnapshot(1)
	}
	return val, nil
}

func (pb *PodProxyBuffer) decrementTotalConnections() error {
	if decrementCounter(&pb.totalConnections) == 0 {
		pb.idleSnapshotUntil.Store(time.Now().Add(connectionSnapshotTTL).UnixNano())
	}
	return nil
}

func (pb *PodProxyBuffer) incrementContainerConnections(containerId string) error {
	counter, ok := pb.containerConnectionCounter(containerId)
	if !ok {
		return nil
	}
	if counter.Add(1) == 1 {
		pb.publishContainerConnectionSnapshot(containerId, 1)
	}
	return nil
}

func (pb *PodProxyBuffer) decrementContainerConnections(containerId string) error {
	defer pb.signalWork()
	go pb.setPodKeepWarmLock(containerId)

	counter, ok := pb.containerConnectionCounter(containerId)
	if !ok {
		return nil
	}

	decrementCounter(counter)
	return nil
}

func (pb *PodProxyBuffer) totalConnectionCount() int64 {
	return pb.totalConnections.Load()
}

func (pb *PodProxyBuffer) containerConnectionCounter(containerId string) (*atomic.Int64, bool) {
	if containerId == "" {
		return nil, false
	}
	actual, _ := pb.containerConnections.LoadOrStore(containerId, &atomic.Int64{})
	return actual.(*atomic.Int64), true
}

func (pb *PodProxyBuffer) containerConnectionCount(containerId string) int64 {
	value, ok := pb.containerConnections.Load(containerId)
	if !ok {
		return 0
	}
	return value.(*atomic.Int64).Load()
}

func (pb *PodProxyBuffer) sharedContainerConnectionCount(containerId string) (int, error) {
	if pb.workspace == nil {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	connections, err := sharedPodContainerConnections(ctx, pb.rdb, pb.workspace.Name, pb.stubId, containerId)
	if err != nil {
		return 0, err
	}
	return int(connections), nil
}

func (pb *PodProxyBuffer) syncConnectionState() {
	ticker := time.NewTicker(connectionSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pb.ctx.Done():
			return
		case <-ticker.C:
			pb.flushConnectionState()
		}
	}
}

func (pb *PodProxyBuffer) flushConnectionState() {
	if pb.rdb == nil || pb.workspace == nil || pb.stubId == "" || pb.proxyId == "" {
		return
	}

	now := time.Now()
	total := pb.totalConnections.Load()
	hasContainerSnapshots := false
	pb.containerConnections.Range(func(_, _ any) bool {
		hasContainerSnapshots = true
		return true
	})
	if total == 0 && !hasContainerSnapshots && now.UnixNano() > pb.idleSnapshotUntil.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := pb.rdb.Pipeline()
	refreshingIndex := pb.queueProxyConnectionIndex(ctx, pipe)
	pipe.Set(ctx, Keys.podProxyConnections(pb.workspace.Name, pb.stubId, pb.proxyId, "total"), total, connectionSnapshotTTL)
	pb.containerConnections.Range(func(key, value any) bool {
		containerId, ok := key.(string)
		if !ok {
			return true
		}
		count := value.(*atomic.Int64).Load()
		pipe.Set(ctx, Keys.podProxyConnections(pb.workspace.Name, pb.stubId, pb.proxyId, containerId), count, connectionSnapshotTTL)
		if count == 0 {
			pb.containerConnections.Delete(key)
		}
		return true
	})
	if _, err := pipe.Exec(ctx); err != nil {
		if refreshingIndex {
			pb.proxyIndexRefreshAfter.Store(0)
		}
		log.Debug().Err(err).Str("stub_id", pb.stubId).Msg("failed to publish pod connection snapshot")
	}
}

func (pb *PodProxyBuffer) publishContainerConnectionSnapshot(containerId string, count int64) {
	if pb.rdb == nil || pb.workspace == nil || pb.stubId == "" || pb.proxyId == "" || containerId == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := pb.rdb.Pipeline()
	refreshingIndex := pb.queueProxyConnectionIndex(ctx, pipe)
	pipe.Set(ctx, Keys.podProxyConnections(pb.workspace.Name, pb.stubId, pb.proxyId, containerId), count, connectionSnapshotTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		if refreshingIndex {
			pb.proxyIndexRefreshAfter.Store(0)
		}
		log.Debug().Err(err).Str("stub_id", pb.stubId).Str("container_id", containerId).Msg("failed to publish pod container busy snapshot")
	}
}

func (pb *PodProxyBuffer) publishTotalConnectionSnapshot(count int64) {
	if pb.rdb == nil || pb.workspace == nil || pb.stubId == "" || pb.proxyId == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := pb.rdb.Pipeline()
	refreshingIndex := pb.queueProxyConnectionIndex(ctx, pipe)
	pipe.Set(ctx, Keys.podProxyConnections(pb.workspace.Name, pb.stubId, pb.proxyId, "total"), count, connectionSnapshotTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		if refreshingIndex {
			pb.proxyIndexRefreshAfter.Store(0)
		}
		log.Debug().Err(err).Str("stub_id", pb.stubId).Msg("failed to publish pod total busy snapshot")
	}
}

func (pb *PodProxyBuffer) queueProxyConnectionIndex(ctx context.Context, pipe redis.Pipeliner) bool {
	if !pb.shouldRefreshProxyConnectionIndex(time.Now()) {
		return false
	}

	indexKey := Keys.podProxyConnectionIndex(pb.workspace.Name, pb.stubId)
	pipe.SAdd(ctx, indexKey, pb.proxyId)
	pipe.Expire(ctx, indexKey, connectionSnapshotTTL)
	return true
}

func (pb *PodProxyBuffer) shouldRefreshProxyConnectionIndex(now time.Time) bool {
	nowNS := now.UnixNano()
	for {
		next := pb.proxyIndexRefreshAfter.Load()
		if next > nowNS {
			return false
		}
		if pb.proxyIndexRefreshAfter.CompareAndSwap(next, now.Add(connectionIndexRefreshEvery).UnixNano()) {
			return true
		}
	}
}

func (pb *PodProxyBuffer) setPodKeepWarmLock(containerID string) {
	if pb.containerRepo == nil || pb.workspace == nil {
		return
	}

	if containerID == "" || pb.stubConfig == nil || pb.stubConfig.KeepWarmSeconds <= 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	setPodKeepWarmLock(ctx, pb.containerRepo, pb.workspace.Name, pb.stubId, containerID, pb.stubConfig.KeepWarmSeconds)
}

func decrementCounter(counter *atomic.Int64) int64 {
	for {
		current := counter.Load()
		if current <= 0 {
			return 0
		}
		next := current - 1
		if counter.CompareAndSwap(current, next) {
			return next
		}
	}
}

func sharedPodTotalConnections(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId string) (int64, error) {
	if rdb == nil || workspaceName == "" || stubId == "" {
		return 0, nil
	}

	total, _, err := sumRedisIntKeys(ctx, rdb, Keys.podProxyConnectionIndex(workspaceName, stubId), func(proxyId string) string {
		return Keys.podProxyConnections(workspaceName, stubId, proxyId, "total")
	}, true)
	return total, err
}

func sharedPodContainerConnections(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, containerId string) (int64, error) {
	if rdb == nil || workspaceName == "" || stubId == "" || containerId == "" {
		return 0, nil
	}

	total, _, err := sumRedisIntKeys(ctx, rdb, Keys.podProxyConnectionIndex(workspaceName, stubId), func(proxyId string) string {
		return Keys.podProxyConnections(workspaceName, stubId, proxyId, containerId)
	}, false)
	return total, err
}

func sumRedisIntKeys(ctx context.Context, rdb *common.RedisClient, indexKey string, keyForProxy func(string) string, pruneMissing bool) (int64, bool, error) {
	var total int64

	proxyIds, err := rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return 0, false, err
	}
	if len(proxyIds) == 0 {
		return 0, false, nil
	}

	for _, proxyId := range proxyIds {
		value, err := rdb.Get(ctx, keyForProxy(proxyId)).Int64()
		if err == redis.Nil {
			if pruneMissing {
				_ = rdb.SRem(ctx, indexKey, proxyId).Err()
			}
			continue
		}
		if err != nil {
			return 0, false, err
		}
		if value > 0 {
			total += value
		}
	}

	return total, true, nil
}
