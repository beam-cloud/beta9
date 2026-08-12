package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type memoryCoordinatorRepository struct {
	index         map[string]map[string]struct{}
	registrations map[string]map[string]CoordinatorHost
	logicalHosts  map[string]CoordinatorHost
	active        map[string]string
}

func newMemoryCoordinatorRepository() *memoryCoordinatorRepository {
	return &memoryCoordinatorRepository{
		index:         map[string]map[string]struct{}{},
		registrations: map[string]map[string]CoordinatorHost{},
		logicalHosts:  map[string]CoordinatorHost{},
		active:        map[string]string{},
	}
}

func (r *memoryCoordinatorRepository) SetCacheRegistration(ctx context.Context, host CoordinatorHost, ttl time.Duration) error {
	indexKey := fmt.Sprintf("%s:%s", host.PoolName, host.Locality)
	if r.index[indexKey] == nil {
		r.index[indexKey] = map[string]struct{}{}
	}
	r.index[indexKey][host.LogicalHostID] = struct{}{}

	if r.registrations[host.LogicalHostID] == nil {
		r.registrations[host.LogicalHostID] = map[string]CoordinatorHost{}
	}
	r.registrations[host.LogicalHostID][host.RegistrationID] = host
	r.logicalHosts[host.LogicalHostID] = host.LogicalOnly()
	return nil
}

func (r *memoryCoordinatorRepository) GetActiveCacheRegistration(ctx context.Context, logicalHostID string) (string, bool, error) {
	registrationID := r.active[logicalHostID]
	return registrationID, registrationID != "", nil
}

func (r *memoryCoordinatorRepository) SetActiveCacheRegistration(ctx context.Context, logicalHostID, registrationID string, ttl time.Duration) error {
	r.active[logicalHostID] = registrationID
	return nil
}

func (r *memoryCoordinatorRepository) ListCacheLogicalHosts(ctx context.Context, poolName, locality string) ([]string, error) {
	seen := map[string]struct{}{}
	if poolName == "" {
		for indexKey, indexed := range r.index {
			if indexKey == fmt.Sprintf("%s:%s", poolName, locality) {
				continue
			}
			if len(indexKey) < len(locality)+1 || indexKey[len(indexKey)-len(locality)-1:] != ":"+locality {
				continue
			}
			for id := range indexed {
				seen[id] = struct{}{}
			}
		}
	} else {
		indexKey := fmt.Sprintf("%s:%s", poolName, locality)
		for id := range r.index[indexKey] {
			seen[id] = struct{}{}
		}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *memoryCoordinatorRepository) ListCacheRegistrations(ctx context.Context, logicalHostID string) ([]string, error) {
	ids := make([]string, 0, len(r.registrations[logicalHostID]))
	for id := range r.registrations[logicalHostID] {
		ids = append(ids, id)
	}
	return ids, nil
}

func (r *memoryCoordinatorRepository) GetCacheRegistration(ctx context.Context, logicalHostID, registrationID string) (CoordinatorHost, bool, error) {
	host, ok := r.registrations[logicalHostID][registrationID]
	return host, ok, nil
}

func (r *memoryCoordinatorRepository) GetCacheLogicalHost(ctx context.Context, logicalHostID string) (CoordinatorHost, bool, error) {
	host, ok := r.logicalHosts[logicalHostID]
	return host.LogicalOnly(), ok, nil
}

func (r *memoryCoordinatorRepository) RemoveCacheRegistration(ctx context.Context, logicalHostID, registrationID string) error {
	delete(r.registrations[logicalHostID], registrationID)
	if r.active[logicalHostID] == registrationID {
		delete(r.active, logicalHostID)
	}
	return nil
}

func (r *memoryCoordinatorRepository) CountCacheRegistrations(ctx context.Context, logicalHostID string) (int64, error) {
	return int64(len(r.registrations[logicalHostID])), nil
}

func (r *memoryCoordinatorRepository) RemoveCacheLogicalHostFromPool(ctx context.Context, poolName, locality, logicalHostID string) error {
	delete(r.index[fmt.Sprintf("%s:%s", poolName, locality)], logicalHostID)
	return nil
}

func (r *memoryCoordinatorRepository) RemoveCacheLogicalHost(ctx context.Context, poolName, locality, logicalHostID string) error {
	_ = r.RemoveCacheLogicalHostFromPool(ctx, poolName, locality, logicalHostID)
	delete(r.registrations, logicalHostID)
	delete(r.logicalHosts, logicalHostID)
	delete(r.active, logicalHostID)
	return nil
}

func TestCoordinatorListsLiveRegistrationsWithActiveFirst(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)

	ctx := context.Background()
	logicalHostID := "cache-host-default-node-a-path-0"

	for _, registration := range []struct {
		id   string
		addr string
	}{
		{id: "worker-a", addr: "10.0.0.1:2049"},
		{id: "worker-b", addr: "10.0.0.2:2049"},
	} {
		err := coordinator.RegisterHost(ctx, CoordinatorHost{
			LogicalHostID:  logicalHostID,
			RegistrationID: registration.id,
			PoolName:       "default",
			Locality:       "default",
			NodeID:         "node-a",
			CachePathID:    "path",
			Addr:           registration.addr,
			PrivateAddr:    registration.addr,
		}, 30*time.Second)
		require.NoError(t, err)
	}

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 2)
	require.Equal(t, logicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, "worker-b", hosts[0].RegistrationID)
	require.Equal(t, "10.0.0.2:2049", hosts[0].PrivateAddr)
	require.Equal(t, logicalHostID, hosts[1].LogicalHostID)
	require.Equal(t, "worker-a", hosts[1].RegistrationID)
	require.Equal(t, "10.0.0.1:2049", hosts[1].PrivateAddr)

	err = coordinator.UnregisterHost(ctx, "default", "default", logicalHostID, "worker-a")
	require.NoError(t, err)

	hosts, err = coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, logicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, "worker-b", hosts[0].RegistrationID)
	require.Equal(t, "10.0.0.2:2049", hosts[0].PrivateAddr)
}

func TestCoordinatorListsBackupRegistrationWhenActiveRegistrationStillExists(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	logicalHostID := "cache-host-default-node-a-path-0"
	for _, registration := range []struct {
		id   string
		addr string
	}{
		{id: "worker-a", addr: "10.0.0.1:2049"},
		{id: "worker-b", addr: "10.0.0.2:2049"},
	} {
		require.NoError(t, coordinator.RegisterHost(ctx, CoordinatorHost{
			LogicalHostID:  logicalHostID,
			RegistrationID: registration.id,
			PoolName:       "default",
			Locality:       "default",
			NodeID:         "node-a",
			CachePathID:    "path",
			Addr:           registration.addr,
			PrivateAddr:    registration.addr,
		}, 30*time.Second))
	}

	require.NoError(t, repo.SetActiveCacheRegistration(ctx, logicalHostID, "worker-a", 30*time.Second))

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 2)
	require.Equal(t, "worker-a", hosts[0].RegistrationID)
	require.Equal(t, "worker-b", hosts[1].RegistrationID)
}

func TestCoordinatorPromotesRegisteringRegistrationWhenActiveRegistrationIsGone(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	host := CoordinatorHost{
		LogicalHostID:  "cache-host-default-node-a-path-0",
		RegistrationID: "worker-a",
		PoolName:       "default",
		Locality:       "default",
		Addr:           "10.0.0.1:2049",
		PrivateAddr:    "10.0.0.1:2049",
	}
	require.NoError(t, coordinator.RegisterHost(ctx, host, 30*time.Second))
	require.Equal(t, "worker-a", repo.active[host.LogicalHostID])

	delete(repo.registrations[host.LogicalHostID], "worker-a")
	host.RegistrationID = "worker-b"
	host.Addr = "10.0.0.2:2049"
	host.PrivateAddr = "10.0.0.2:2049"
	require.NoError(t, coordinator.RegisterHost(ctx, host, 30*time.Second))
	require.Equal(t, "worker-b", repo.active[host.LogicalHostID])
}

func TestCoordinatorListsLogicalHostWhenNoEndpointRegistrationIsActive(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	host := CoordinatorHost{
		LogicalHostID:  "cache-host-default-node-a-path-0",
		RegistrationID: "worker-a",
		PoolName:       "default",
		Locality:       "default",
		NodeID:         "node-a",
		CachePathID:    "path",
		Addr:           "10.0.0.1:2049",
		PrivateAddr:    "10.0.0.1:2049",
	}
	require.NoError(t, coordinator.RegisterHost(ctx, host, 30*time.Second))

	require.NoError(t, repo.RemoveCacheRegistration(ctx, host.LogicalHostID, host.RegistrationID))

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, host.LogicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, host.NodeID, hosts[0].NodeID)
	require.Equal(t, host.CachePathID, hosts[0].CachePathID)
	require.Empty(t, hosts[0].RegistrationID)
	require.Empty(t, hosts[0].PrivateAddr)
}

func TestCoordinatorUnregisterPreservesLogicalHostForEndpointChurn(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	host := CoordinatorHost{
		LogicalHostID:  "cache-host-default-node-a-path-0",
		RegistrationID: "worker-a",
		PoolName:       "default",
		Locality:       "default",
		NodeID:         "node-a",
		CachePathID:    "path",
		Addr:           "10.0.0.1:2049",
		PrivateAddr:    "10.0.0.1:2049",
	}
	require.NoError(t, coordinator.RegisterHost(ctx, host, 30*time.Second))
	require.NoError(t, coordinator.UnregisterHost(ctx, host.PoolName, host.Locality, host.LogicalHostID, host.RegistrationID))

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, host.LogicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, host.NodeID, hosts[0].NodeID)
	require.Equal(t, host.CachePathID, hosts[0].CachePathID)
	require.Empty(t, hosts[0].RegistrationID)
	require.Empty(t, hosts[0].PrivateAddr)
}

func TestCoordinatorCanListCacheHostsAcrossWorkerPools(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	logicalHostID := "cache-host-default-node-a-path-0"
	for _, pool := range []string{"build", "default"} {
		require.NoError(t, coordinator.RegisterHost(ctx, CoordinatorHost{
			LogicalHostID:  logicalHostID,
			RegistrationID: "worker-" + pool,
			PoolName:       pool,
			Locality:       "default",
			NodeID:         "node-a",
			CachePathID:    "path",
			Addr:           "10.0.0.1:2049",
			PrivateAddr:    "10.0.0.1:2049",
		}, 30*time.Second))
	}

	hosts, err := coordinator.ListHosts(ctx, "", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 2)
	require.Equal(t, logicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, logicalHostID, hosts[1].LogicalHostID)

	defaultHosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, defaultHosts, 1)
	require.Equal(t, "default", defaultHosts[0].PoolName)
}

func TestCoordinatorRemoveNodeDeletesOnlyReleasedNodeState(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	targetLogicalHostID := "cache-host-workspace-pool-machine-a-path"
	for _, registrationID := range []string{"worker-a-1", "worker-a-2"} {
		require.NoError(t, coordinator.RegisterHost(ctx, CoordinatorHost{
			LogicalHostID:  targetLogicalHostID,
			RegistrationID: registrationID,
			PoolName:       "pool",
			Locality:       "workspace/pool",
			NodeID:         "machine-a",
			CachePathID:    "path",
			PrivateAddr:    "10.0.0.1:2049",
		}, 30*time.Second))
	}
	require.NoError(t, coordinator.RegisterHost(ctx, CoordinatorHost{
		LogicalHostID:  "cache-host-workspace-pool-machine-b-path",
		RegistrationID: "worker-b",
		PoolName:       "pool",
		Locality:       "workspace/pool",
		NodeID:         "machine-b",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.2:2049",
	}, 30*time.Second))

	require.NoError(t, coordinator.RemoveNode(ctx, "pool", "workspace/pool", "machine-a"))
	require.NoError(t, coordinator.RemoveNode(ctx, "pool", "workspace/pool", "machine-a"))

	_, found := repo.logicalHosts[targetLogicalHostID]
	require.False(t, found)
	require.NotContains(t, repo.registrations, targetLogicalHostID)
	require.NotContains(t, repo.active, targetLogicalHostID)
	hosts, err := coordinator.ListHosts(ctx, "pool", "workspace/pool")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, "machine-b", hosts[0].NodeID)
}

func TestCoordinatorRemoveNodePreservesSharedLogicalHostForAnotherPool(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()
	logicalHostID := "cache-host-shared-machine-a-path"

	for _, host := range []CoordinatorHost{
		{
			LogicalHostID:  logicalHostID,
			RegistrationID: "worker-pool-a",
			PoolName:       "pool-a",
			Locality:       "shared",
			NodeID:         "machine-a",
			CachePathID:    "path",
			PrivateAddr:    "10.0.0.1:2049",
		},
		{
			LogicalHostID:  logicalHostID,
			RegistrationID: "worker-pool-b",
			PoolName:       "pool-b",
			Locality:       "shared",
			NodeID:         "machine-a",
			CachePathID:    "path",
			PrivateAddr:    "10.0.0.1:2050",
		},
	} {
		require.NoError(t, coordinator.RegisterHost(ctx, host, 30*time.Second))
	}

	require.NoError(t, coordinator.RemoveNode(ctx, "pool-a", "shared", "machine-a"))

	poolAHosts, err := coordinator.ListHosts(ctx, "pool-a", "shared")
	require.NoError(t, err)
	require.Empty(t, poolAHosts)
	poolBHosts, err := coordinator.ListHosts(ctx, "pool-b", "shared")
	require.NoError(t, err)
	require.Len(t, poolBHosts, 1)
	require.Equal(t, "worker-pool-b", poolBHosts[0].RegistrationID)
	require.Contains(t, repo.logicalHosts, logicalHostID)
}
