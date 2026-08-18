package compute

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestRoutePrewarmerDeduplicatesInFlightProxyTarget(t *testing.T) {
	prewarmer := routePrewarmer{}

	if !prewarmer.tryStart("agent.tailnet:29443") {
		t.Fatal("expected first prewarm attempt")
	}
	if prewarmer.tryStart(" agent.tailnet:29443 ") {
		t.Fatal("expected duplicate in-flight proxy target to be rejected")
	}
	prewarmer.finish("agent.tailnet:29443")
	if !prewarmer.tryStart("agent.tailnet:29443") {
		t.Fatal("expected proxy target to be admitted after its prewarm finished")
	}
	prewarmer.finish("agent.tailnet:29443")
}

func TestRoutePrewarmerSkipsEmptyProxyTarget(t *testing.T) {
	prewarmer := routePrewarmer{}
	if prewarmer.tryStart(" ") {
		t.Fatal("expected empty proxy target to be skipped")
	}
}

func TestRoutePrewarmerCapsConcurrencyWithoutRetainingRejectedTarget(t *testing.T) {
	prewarmer := routePrewarmer{}
	for i := 0; i < routePrewarmConcurrency; i++ {
		if !prewarmer.tryStart(fmt.Sprintf("agent-%d.tailnet:29443", i)) {
			t.Fatalf("expected attempt %d to be admitted", i)
		}
	}

	overflowTarget := "overflow-agent.tailnet:29443"
	if prewarmer.tryStart(overflowTarget) {
		t.Fatal("expected attempt above the concurrency cap to be rejected")
	}
	prewarmer.finish("agent-0.tailnet:29443")
	if !prewarmer.tryStart(overflowTarget) {
		t.Fatal("expected rejected target to be admitted immediately after capacity freed")
	}

	for i := 1; i < routePrewarmConcurrency; i++ {
		prewarmer.finish(fmt.Sprintf("agent-%d.tailnet:29443", i))
	}
	prewarmer.finish(overflowTarget)
}

func TestRoutePrewarmerConcurrentStormNeverExceedsCap(t *testing.T) {
	const attempts = 128
	prewarmer := routePrewarmer{}
	start := make(chan struct{})
	release := make(chan struct{})
	var attempted sync.WaitGroup
	var finished sync.WaitGroup
	var admitted atomic.Int32
	var active atomic.Int32
	var peak atomic.Int32

	attempted.Add(attempts)
	finished.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func(i int) {
			defer finished.Done()
			<-start
			target := fmt.Sprintf("agent-%d.tailnet:29443", i)
			if !prewarmer.tryStart(target) {
				attempted.Done()
				return
			}
			admitted.Add(1)
			current := active.Add(1)
			for {
				previous := peak.Load()
				if current <= previous || peak.CompareAndSwap(previous, current) {
					break
				}
			}
			attempted.Done()
			<-release
			active.Add(-1)
			prewarmer.finish(target)
		}(i)
	}

	close(start)
	attempted.Wait()
	if got := admitted.Load(); got != routePrewarmConcurrency {
		t.Fatalf("admitted = %d, want %d", got, routePrewarmConcurrency)
	}
	if got := peak.Load(); got > routePrewarmConcurrency {
		t.Fatalf("peak active = %d, want at most %d", got, routePrewarmConcurrency)
	}
	close(release)
	finished.Wait()
}

func TestRouteEligibleForPrewarmRequiresReadyTSNetRoute(t *testing.T) {
	tests := []struct {
		name  string
		route types.BackendRoute
		want  bool
	}{
		{
			name: "ready tsnet route",
			route: types.BackendRoute{
				State:       types.BackendRouteStateReady,
				Transport:   types.BackendRouteTransportTSNet,
				ProxyTarget: "agent.tailnet:29443",
			},
			want: true,
		},
		{
			name: "opening route",
			route: types.BackendRoute{
				State:       types.BackendRouteStateOpening,
				Transport:   types.BackendRouteTransportTSNet,
				ProxyTarget: "agent.tailnet:29443",
			},
		},
		{
			name: "degraded route",
			route: types.BackendRoute{
				State:       types.BackendRouteStateDegraded,
				Transport:   types.BackendRouteTransportTSNet,
				ProxyTarget: "agent.tailnet:29443",
			},
		},
		{
			name: "direct route",
			route: types.BackendRoute{
				State:       types.BackendRouteStateReady,
				Transport:   types.BackendRouteTransportDirect,
				ProxyTarget: "agent.tailnet:29443",
			},
		},
		{
			name: "empty proxy target",
			route: types.BackendRoute{
				State:       types.BackendRouteStateReady,
				Transport:   types.BackendRouteTransportTSNet,
				ProxyTarget: " ",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := routeEligibleForPrewarm(tt.route); got != tt.want {
				t.Fatalf("routeEligibleForPrewarm() = %v, want %v", got, tt.want)
			}
		})
	}
}
