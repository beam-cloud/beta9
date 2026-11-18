package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCoordinatorRetainsValidatedRouteWithoutPhysicalAddress(t *testing.T) {
	coordinator := NewCoordinator(newMemoryCoordinatorRepository())
	host := CoordinatorHost{LogicalHostID: "cache-host", RegistrationID: "lease", PoolName: "pool", Locality: "workspace/pool", CacheRouteID: "validated-route"}
	require.NoError(t, coordinator.RegisterHost(context.Background(), host, time.Minute))
	hosts, err := coordinator.ListHosts(context.Background(), "pool", "workspace/pool")
	require.NoError(t, err)
	require.Equal(t, []CoordinatorHost{host}, hosts)
	require.Empty(t, host.LogicalOnly().CacheRouteID, "a logical-only host must not retain an expired route capability")
	endpoint := &Host{CacheRouteID: host.CacheRouteID}
	require.True(t, endpoint.HasEndpoint())
	require.False(t, endpoint.LogicalOnly().HasEndpoint())
}
