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
	now := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)
	if got := buildCreatedBucketsAt(nil, "last_hour", now); len(got) != 60 {
		t.Fatalf("expected fixed 60 buckets for no stubs, got %d", len(got))
	}

	base := now.Add(-30 * time.Minute)
	stubs := []types.StubWithRelated{
		{Stub: types.Stub{CreatedAt: types.Time{Time: base}}},
		{Stub: types.Stub{CreatedAt: types.Time{Time: base.Add(10 * time.Second)}}},
		{Stub: types.Stub{CreatedAt: types.Time{Time: base.Add(30 * time.Minute)}}},
		{Stub: types.Stub{CreatedAt: types.Time{Time: now.Add(-2 * time.Hour)}}},
	}

	buckets := buildCreatedBucketsAt(stubs, "last_hour", now)
	if len(buckets) != 60 {
		t.Fatalf("expected 60 fixed buckets, got %d", len(buckets))
	}

	total := 0
	for _, bucket := range buckets {
		total += bucket.Count
	}
	if total != 3 {
		t.Errorf("expected in-range bucket counts to sum to 3, got %d", total)
	}

	week := buildCreatedBucketsAt(stubs, "last_week", now)
	if len(week) != 60 {
		t.Errorf("expected last_week to return fixed 60 buckets, got %d", len(week))
	}
	day := buildCreatedBucketsAt(stubs, "last_day", now)
	if len(day) != 60 {
		t.Errorf("expected last_day to return fixed 60 buckets, got %d", len(day))
	}
}

func TestBuildSandboxSummaryCreatedBucketsDedupeContainers(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)
	base := now.Add(-30 * time.Minute)
	summaries := []sandboxContainerSummary{
		{ContainerID: "sandbox-1", CreatedAt: base},
		{ContainerID: "sandbox-2", CreatedAt: base.Add(10 * time.Second)},
		{ContainerID: "sandbox-3", CreatedAt: now.Add(-2 * time.Hour)},
	}

	buckets := buildSandboxSummaryCreatedBucketsAt(summaries, "last_hour", now)
	if got, want := len(buckets), 60; got != want {
		t.Fatalf("expected %d buckets, got %d", want, got)
	}

	total := 0
	for _, bucket := range buckets {
		total += bucket.Count
	}
	if total != 2 {
		t.Fatalf("expected in-range bucket counts to sum to 2, got %d", total)
	}
}
