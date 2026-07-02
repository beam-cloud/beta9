package vast

import (
	"strconv"
	"testing"

	"github.com/beam-cloud/beta9/pkg/agent"
)

func TestDefaultPerGPUCapacityHonorsExplicitValues(t *testing.T) {
	maxCPU, maxMemory := defaultPerGPUCapacity("4", "8Gi", 8)
	if maxCPU != "4" || maxMemory != "8Gi" {
		t.Fatalf("capacity = (%q, %q), want explicit values preserved", maxCPU, maxMemory)
	}
}

func TestDefaultPerGPUCapacityDividesHostCapacity(t *testing.T) {
	gpuCount := 2
	maxCPU, maxMemory := defaultPerGPUCapacity("", "", gpuCount)

	wantCores := agent.SystemCPUCount() / gpuCount
	if wantCores < 1 {
		wantCores = 1
	}
	if maxCPU != strconv.Itoa(wantCores) {
		t.Fatalf("maxCPU = %q, want %q", maxCPU, strconv.Itoa(wantCores))
	}

	if hostMemoryMB := agent.SystemMemoryMB(); hostMemoryMB > 0 {
		want := strconv.FormatUint(hostMemoryMB/uint64(gpuCount), 10) + "Mi"
		if maxMemory != want {
			t.Fatalf("maxMemory = %q, want %q", maxMemory, want)
		}
	} else if maxMemory != "" {
		t.Fatalf("maxMemory = %q, want empty when host memory is undetectable", maxMemory)
	}
}

func TestDefaultPerGPUCapacityNeverAdvertisesZeroCores(t *testing.T) {
	maxCPU, _ := defaultPerGPUCapacity("", "", agent.SystemCPUCount()*4)
	if maxCPU != "1" {
		t.Fatalf("maxCPU = %q, want %q", maxCPU, "1")
	}
}
