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

func (r *memoryCoordinatorRepository) RemoveCacheLogicalHost(ctx context.Context, poolName, locality, logicalHostID string) error {
	delete(r.index[fmt.Sprintf("%s:%s", poolName, locality)], logicalHostID)
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
	require.Equal(t, "worker-a", hosts[0].RegistrationID)
	require.Equal(t, "10.0.0.1:2049", hosts[0].PrivateAddr)
	require.Equal(t, logicalHostID, hosts[1].LogicalHostID)
	require.Equal(t, "worker-b", hosts[1].RegistrationID)
	require.Equal(t, "10.0.0.2:2049", hosts[1].PrivateAddr)

	err = coordinator.UnregisterHost(ctx, "default", "default", logicalHostID, "worker-a")
	require.NoError(t, err)

	hosts, err = coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, logicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, "worker-b", hosts[0].RegistrationID)
	require.Equal(t, "10.0.0.2:2049", hosts[0].PrivateAddr)
}

func TestCoordinatorPrefersHigherPriorityRegistration(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	logicalHostID := "cache-host-default-node-a-path-0"
	worker := CoordinatorHost{
		LogicalHostID:  logicalHostID,
		RegistrationID: "worker-a",
		Role:           DefaultCacheServerRoleWorker,
		Priority:       DefaultWorkerCacheServerPriority,
		PoolName:       "default",
		Locality:       "default",
		NodeID:         "node-a",
		CachePathID:    "path",
		Addr:           "10.0.0.1:2049",
		PrivateAddr:    "10.0.0.1:2049",
	}
	agent := worker
	agent.RegistrationID = "cache-agent-a"
	agent.Role = DefaultCacheServerRoleAgent
	agent.Priority = DefaultAgentCacheServerPriority
	agent.Addr = "10.0.0.2:2049"
	agent.PrivateAddr = "10.0.0.2:2049"

	require.NoError(t, coordinator.RegisterHost(ctx, worker, 30*time.Second))
	require.Equal(t, worker.RegistrationID, repo.active[logicalHostID])
	require.NoError(t, coordinator.RegisterHost(ctx, agent, 30*time.Second))
	require.Equal(t, agent.RegistrationID, repo.active[logicalHostID])

	worker.RegistrationID = "worker-b"
	worker.Addr = "10.0.0.3:2049"
	worker.PrivateAddr = "10.0.0.3:2049"
	require.NoError(t, coordinator.RegisterHost(ctx, worker, 30*time.Second))
	require.Equal(t, agent.RegistrationID, repo.active[logicalHostID])

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 3)
	require.Equal(t, agent.RegistrationID, hosts[0].RegistrationID)
	require.Equal(t, DefaultCacheServerRoleAgent, hosts[0].Role)
}

func TestCoordinatorFallsBackToWorkerWhenAgentRegistrationLeaves(t *testing.T) {
	repo := newMemoryCoordinatorRepository()
	coordinator := NewCoordinator(repo)
	ctx := context.Background()

	logicalHostID := "cache-host-default-node-a-path-0"
	agent := CoordinatorHost{
		LogicalHostID:  logicalHostID,
		RegistrationID: "cache-agent-a",
		Role:           DefaultCacheServerRoleAgent,
		Priority:       DefaultAgentCacheServerPriority,
		PoolName:       "default",
		Locality:       "default",
		NodeID:         "node-a",
		CachePathID:    "path",
		Addr:           "10.0.0.1:2049",
		PrivateAddr:    "10.0.0.1:2049",
	}
	worker := agent
	worker.RegistrationID = "worker-a"
	worker.Role = DefaultCacheServerRoleWorker
	worker.Priority = DefaultWorkerCacheServerPriority
	worker.Addr = "10.0.0.2:2049"
	worker.PrivateAddr = "10.0.0.2:2049"

	require.NoError(t, coordinator.RegisterHost(ctx, agent, 30*time.Second))
	require.NoError(t, coordinator.RegisterHost(ctx, worker, 30*time.Second))
	require.Equal(t, agent.RegistrationID, repo.active[logicalHostID])

	require.NoError(t, coordinator.UnregisterHost(ctx, "default", "default", logicalHostID, agent.RegistrationID))

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, worker.RegistrationID, hosts[0].RegistrationID)
	require.Equal(t, worker.RegistrationID, repo.active[logicalHostID])
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
