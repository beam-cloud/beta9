package compute

import (
	"testing"
	"time"
)

func TestRoutePrewarmerThrottlesByProxyTarget(t *testing.T) {
	prewarmer := routePrewarmer{}
	now := time.Unix(100, 0)

	if !prewarmer.shouldAttempt("agent.tailnet:29443", now) {
		t.Fatal("expected first prewarm attempt")
	}
	if prewarmer.shouldAttempt("agent.tailnet:29443", now.Add(routePrewarmInterval-time.Millisecond)) {
		t.Fatal("expected second prewarm attempt inside interval to be throttled")
	}
	if !prewarmer.shouldAttempt("agent.tailnet:29443", now.Add(routePrewarmInterval)) {
		t.Fatal("expected prewarm attempt after interval")
	}
	if !prewarmer.shouldAttempt("other-agent.tailnet:29443", now.Add(time.Second)) {
		t.Fatal("expected distinct proxy target to have independent throttle")
	}
}

func TestRoutePrewarmerSkipsEmptyProxyTarget(t *testing.T) {
	prewarmer := routePrewarmer{}
	if prewarmer.shouldAttempt(" ", time.Now()) {
		t.Fatal("expected empty proxy target to be skipped")
	}
}

func TestPeerMatchesHost(t *testing.T) {
	tests := []struct {
		name     string
		hostName string
		dnsName  string
		target   string
		want     bool
	}{
		{
			name:    "dns match",
			dnsName: "beam-agent-machine.tailnet.ts.net.",
			target:  "beam-agent-machine.tailnet.ts.net",
			want:    true,
		},
		{
			name:    "short target matches dns prefix",
			dnsName: "beam-agent-machine.tailnet.ts.net",
			target:  "beam-agent-machine",
			want:    true,
		},
		{
			name:     "host match",
			hostName: "beam-agent-machine",
			target:   "beam-agent-machine",
			want:     true,
		},
		{
			name:    "no match",
			dnsName: "beam-agent-other.tailnet.ts.net",
			target:  "beam-agent-machine",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := peerMatchesHost(tt.hostName, tt.dnsName, tt.target); got != tt.want {
				t.Fatalf("peerMatchesHost() = %v, want %v", got, tt.want)
			}
		})
	}
}
