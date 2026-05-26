package apiv1

import "testing"

func TestSummarizeContainerEventsBatchReturnsCoverageAndBottleneck(t *testing.T) {
	items := []ContainerEventSummary{
		{
			ContainerID: "container-1",
			EventCount:  4,
			Summary: map[string]int64{
				"container_startup_ms":         200,
				"image_ms":                     10,
				"network_setup_ms":             150,
				"network_ms":                   150,
				"runtime_ms":                   200,
				"runtime_start_to_pid_ms":      20,
				"scheduler_backlog_ms":         5,
				"clip_read_total_us":           500000,
				"worker_queue_ms":              2,
				"worker_receive_to_running_ms": 190,
				"running_to_first_log_ms":      30,
			},
		},
		{
			ContainerID: "container-2",
			EventCount:  4,
			Missing:     []string{"image.load"},
			Summary: map[string]int64{
				"container_startup_ms":         100,
				"network_setup_ms":             90,
				"network_ms":                   90,
				"runtime_ms":                   100,
				"runtime_start_to_pid_ms":      10,
				"scheduler_backlog_ms":         4,
				"worker_queue_ms":              2,
				"worker_receive_to_running_ms": 95,
				"running_to_first_log_ms":      25,
			},
		},
	}

	response := summarizeContainerEventsBatch(items, 5, []string{"container.startup", "image.load"}, []string{"image_ms", "network_ms"})

	if got, want := response.Coverage.ContainersWithEvents, 2; got != want {
		t.Fatalf("unexpected event coverage: got %d want %d", got, want)
	}
	if got, want := response.Coverage.RequiredLifecycleMissing["image.load"], 1; got != want {
		t.Fatalf("unexpected image lifecycle misses: got %d want %d", got, want)
	}
	if got, want := response.Coverage.RequiredMetricMissing["image_ms"], 1; got != want {
		t.Fatalf("unexpected image metric misses: got %d want %d", got, want)
	}
	if response.PrimaryBottleneck == nil {
		t.Fatal("expected primary bottleneck")
	}
	if got, want := response.PrimaryBottleneck.EventID, "network.setup"; got != want {
		t.Fatalf("unexpected primary bottleneck: got %q want %q", got, want)
	}
	if len(response.Stages) == 0 {
		t.Fatal("expected user-facing stage summaries")
	}
	if got, want := response.Stages[0].EventID, "worker_queue"; got != want {
		t.Fatalf("unexpected first stage: got %q want %q", got, want)
	}
	for _, phase := range response.Phases {
		if phase.MetricKey == "clip_read_total_us" {
			t.Fatal("non-millisecond metrics should not be exposed as phases")
		}
	}
}

func TestNormalizeContainerEventsBatchTargets(t *testing.T) {
	targets := normalizeContainerEventsBatchTargets(ContainerEventsBatchRequest{
		Targets: []ContainerEventsBatchTarget{
			{ContainerID: " container-1 ", StubID: " stub-1 "},
			{ContainerID: "container-1", StubID: "stub-1"},
			{TaskID: " task-1 "},
		},
		ContainerIDs: []string{"container-2", ""},
		TaskIDs:      []string{"task-1", "task-2"},
	})

	if got, want := len(targets), 4; got != want {
		t.Fatalf("unexpected targets: got %d want %d: %#v", got, want, targets)
	}
	if got, want := targets[0].ContainerID, "container-1"; got != want {
		t.Fatalf("unexpected normalized container id: got %q want %q", got, want)
	}
	if got, want := targets[0].StubID, "stub-1"; got != want {
		t.Fatalf("unexpected normalized stub id: got %q want %q", got, want)
	}
	if got, want := targets[1].TaskID, "task-1"; got != want {
		t.Fatalf("unexpected normalized task id: got %q want %q", got, want)
	}
}
