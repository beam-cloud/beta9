package worker

import (
	"fmt"
	"os"
	"strings"
	"testing"

	common "github.com/beam-cloud/beta9/pkg/common"
	"gvisor.dev/gvisor/pkg/sync"
)

func TestIntegrationGPUIsolation(t *testing.T) {
	if os.Getenv("GPU_INTEGRATION") != "1" {
		t.Skip("set GPU_INTEGRATION=1 to run on a real GPU node")
	}

	// Step 1: Read PID 1's env (what os.Getenv sees — the broken path)
	pid1Env := "(unknown)"
	data, err := os.ReadFile("/proc/1/environ")
	if err == nil {
		for _, entry := range strings.Split(string(data), "\x00") {
			if strings.HasPrefix(entry, "NVIDIA_VISIBLE_DEVICES=") {
				pid1Env = strings.TrimPrefix(entry, "NVIDIA_VISIBLE_DEVICES=")
				break
			}
		}
	}
	t.Logf("PID 1 NVIDIA_VISIBLE_DEVICES = %q", pid1Env)

	// Step 2: Call the REAL resolveVisibleDevices() from gpu_info.go
	resolved := resolveVisibleDevices()
	t.Logf("resolveVisibleDevices() = %q", resolved)

	if resolved == "void" || resolved == "" {
		t.Fatalf("resolveVisibleDevices() returned %q — void bug NOT fixed", resolved)
	}
	if !strings.HasPrefix(resolved, "GPU-") {
		t.Fatalf("resolveVisibleDevices() returned %q — expected GPU UUID", resolved)
	}

	// Step 3: Create the REAL NvidiaInfoClient with the resolved value (same as NewContainerNvidiaManager)
	client := &NvidiaInfoClient{visibleDevices: resolved}

	// Step 4: Call the REAL AvailableGPUDevices()
	devices, err := client.AvailableGPUDevices()
	if err != nil {
		t.Fatalf("AvailableGPUDevices() error: %v", err)
	}
	t.Logf("AvailableGPUDevices() = %v", devices)

	if len(devices) == 0 {
		t.Fatal("AvailableGPUDevices() returned empty — GPU not visible")
	}
	if len(devices) != 1 {
		t.Fatalf("AvailableGPUDevices() returned %d GPUs, expected exactly 1 for per-worker isolation", len(devices))
	}

	// Step 5: Verify the OLD path (void) would have failed
	oldClient := &NvidiaInfoClient{visibleDevices: pid1Env}
	oldDevices, err := oldClient.AvailableGPUDevices()
	if err != nil {
		t.Logf("Old path error (expected): %v", err)
	}
	t.Logf("Old path (PID 1 env=%q) -> AvailableGPUDevices() = %v", pid1Env, oldDevices)

	if pid1Env == "void" && len(oldDevices) > 0 {
		t.Error("Old code path with void should return empty, but got devices — test logic wrong")
	}

	// Step 6: Exercise the REAL ContainerNvidiaManager.AssignGPUDevices (chooseDevices)
	manager := &ContainerNvidiaManager{
		gpuAllocationMap:       common.NewSafeMap[[]int](),
		gpuCount:               1,
		mu:                     sync.Mutex{},
		infoClient:             client,
		resolvedVisibleDevices: resolved,
	}

	assigned, err := manager.AssignGPUDevices("test-container-1", 1)
	if err != nil {
		t.Fatalf("AssignGPUDevices() failed: %v", err)
	}
	t.Logf("AssignGPUDevices(\"test-container-1\", 1) = %v", assigned)

	if len(assigned) != 1 {
		t.Fatalf("Expected 1 assigned GPU, got %d", len(assigned))
	}
	if assigned[0] != devices[0] {
		t.Fatalf("Assigned GPU %d doesn't match available GPU %d", assigned[0], devices[0])
	}

	// Step 7: Verify second allocation to same worker FAILS (only 1 GPU available)
	_, err = manager.AssignGPUDevices("test-container-2", 1)
	if err == nil {
		t.Fatal("Second allocation should fail — only 1 GPU per worker")
	}
	t.Logf("Second allocation correctly failed: %v", err)

	fmt.Printf("\nRESULT: resolved=%s gpu_index=%d old_path_would_fail=%v PASS\n",
		resolved, assigned[0], pid1Env == "void" && len(oldDevices) == 0)
}
