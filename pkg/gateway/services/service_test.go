package gatewayservices

import (
	"testing"

	pb "github.com/beam-cloud/beta9/proto"
)

func TestNewGatewayServiceRequiresComputeRepoOrRedis(t *testing.T) {
	_, err := NewGatewayService(&GatewayServiceOpts{})
	if err == nil {
		t.Fatal("expected missing compute repository and redis client to fail")
	}
}

func TestConfigurePoolSelectorNamesReservedPool(t *testing.T) {
	pool := poolConfigFromProto(&pb.PoolConfig{
		Nodes:     1,
		Ttl:       "1h",
		MaxSpend:  2,
		Providers: []string{"shadeform"},
	})

	configurePoolSelector(pool, "workspace-1", "handler")
	if !pool.RequiresReservation() {
		t.Fatal("test setup expected pool to require reservation")
	}
	if got, want := pool.Selector, "private-workspace-1-handler"; got != want {
		t.Fatalf("selector = %q, want %q", got, want)
	}
	if got, want := pool.Name, pool.Selector; got != want {
		t.Fatalf("name = %q, want selector %q", got, want)
	}
}
