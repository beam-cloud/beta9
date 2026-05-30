package pod

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestPodBackendURLUsesPlaceholderHostForRouteAddresses(t *testing.T) {
	routeAddress := types.BackendRouteAddress("machine:worker:container:container:8001")

	got := podBackendURL("ws", routeAddress, "/socket", "token=1")
	if got != "ws://backend.route/socket?token=1" {
		t.Fatalf("backend route url = %q, want placeholder host url", got)
	}
}

func TestPodBackendURLPreservesDirectAddress(t *testing.T) {
	got := podBackendURL("http", "127.0.0.1:8001", "metrics", "")
	if got != "http://127.0.0.1:8001/metrics" {
		t.Fatalf("direct backend url = %q", got)
	}
}
