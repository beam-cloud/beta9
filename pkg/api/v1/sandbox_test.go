package apiv1

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestMostRelevantContainerPrefersRunning(t *testing.T) {
	containers := []types.ContainerState{
		{ContainerId: "a", Status: types.ContainerStatusStopping},
		{ContainerId: "b", Status: types.ContainerStatusPending},
		{ContainerId: "c", Status: types.ContainerStatusRunning},
	}

	best := mostRelevantContainer(containers)
	if best == nil || best.ContainerId != "c" {
		t.Fatalf("expected running container 'c', got %+v", best)
	}

	if mostRelevantContainer(nil) != nil {
		t.Fatalf("expected nil for empty container list")
	}
}

func TestLiveSandboxStatus(t *testing.T) {
	cases := []struct {
		status types.ContainerStatus
		want   string
	}{
		{types.ContainerStatusRunning, SandboxStatusRunning},
		{types.ContainerStatusPending, SandboxStatusPending},
		{types.ContainerStatusStopping, SandboxStatusStopping},
	}

	for _, c := range cases {
		got := liveSandboxStatus([]types.ContainerState{{Status: c.status}})
		if got != c.want {
			t.Errorf("liveSandboxStatus(%s) = %s, want %s", c.status, got, c.want)
		}
	}

	if got := liveSandboxStatus(nil); got != "" {
		t.Errorf("liveSandboxStatus(nil) = %q, want empty", got)
	}
}

func TestTerminalStatusFromContainerEvents(t *testing.T) {
	failed := terminalStatusFromContainerEvents(&types.ContainerEventsResponse{RootCauseEvent: "runtime.oom_killed"})
	if failed != SandboxStatusFailed {
		t.Errorf("expected FAILED for oom, got %s", failed)
	}

	failed = terminalStatusFromContainerEvents(&types.ContainerEventsResponse{StopReason: "container failed to start"})
	if failed != SandboxStatusFailed {
		t.Errorf("expected FAILED for failure reason, got %s", failed)
	}

	stopped := terminalStatusFromContainerEvents(&types.ContainerEventsResponse{Status: "stopped", RootCauseEvent: "scheduler.stop_requested"})
	if stopped != SandboxStatusStopped {
		t.Errorf("expected STOPPED for clean stop, got %s", stopped)
	}
}

func TestBuildCreatedBuckets(t *testing.T) {
	if got := buildCreatedBuckets(nil); len(got) != 0 {
		t.Fatalf("expected empty buckets for no stubs, got %d", len(got))
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stubs := []types.StubWithRelated{
		{Stub: types.Stub{CreatedAt: types.Time{Time: base}}},
		{Stub: types.Stub{CreatedAt: types.Time{Time: base.Add(1 * time.Hour)}}},
		{Stub: types.Stub{CreatedAt: types.Time{Time: base.Add(2 * time.Hour)}}},
	}

	buckets := buildCreatedBuckets(stubs)
	if len(buckets) == 0 {
		t.Fatalf("expected non-empty buckets")
	}

	total := 0
	for _, bucket := range buckets {
		total += bucket.Count
	}
	if total != len(stubs) {
		t.Errorf("expected bucket counts to sum to %d, got %d", len(stubs), total)
	}

	// Single timestamp should collapse to one bucket.
	single := buildCreatedBuckets([]types.StubWithRelated{{Stub: types.Stub{CreatedAt: types.Time{Time: base}}}})
	if len(single) != 1 || single[0].Count != 1 {
		t.Errorf("expected single bucket with count 1, got %+v", single)
	}
}
