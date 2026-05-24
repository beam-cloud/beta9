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
	active        map[string]string
}

func newMemoryCoordinatorRepository() *memoryCoordinatorRepository {
	return &memoryCoordinatorRepository{
		index:         map[string]map[string]struct{}{},
		registrations: map[string]map[string]CoordinatorHost{},
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
	indexKey := fmt.Sprintf("%s:%s", poolName, locality)
	ids := make([]string, 0, len(r.index[indexKey]))
	for id := range r.index[indexKey] {
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
	delete(r.active, logicalHostID)
	return nil
}

func TestCoordinatorDeduplicatesLogicalHostsAndPromotesRegistrations(t *testing.T) {
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
			Slot:           0,
			Addr:           registration.addr,
			PrivateAddr:    registration.addr,
		}, 30*time.Second)
		require.NoError(t, err)
	}

	hosts, err := coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, logicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, "worker-a", hosts[0].RegistrationID)

	err = coordinator.UnregisterHost(ctx, "default", "default", logicalHostID, "worker-a")
	require.NoError(t, err)

	hosts, err = coordinator.ListHosts(ctx, "default", "default")
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, logicalHostID, hosts[0].LogicalHostID)
	require.Equal(t, "worker-b", hosts[0].RegistrationID)
	require.Equal(t, "10.0.0.2:2049", hosts[0].PrivateAddr)
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
