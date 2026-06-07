package gatewayservices

import "testing"

func TestNewGatewayServiceRequiresComputeRepoOrRedis(t *testing.T) {
	_, err := NewGatewayService(&GatewayServiceOpts{})
	if err == nil {
		t.Fatal("expected missing compute repository and redis client to fail")
	}
}
