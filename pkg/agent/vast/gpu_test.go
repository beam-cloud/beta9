package vast

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestDetectVisibleGPUUUIDFromEnv(t *testing.T) {
	t.Setenv(types.NvidiaVisibleDevicesEnv, "GPU-abc")
	uuid, err := DetectVisibleGPUUUID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if uuid != "GPU-abc" {
		t.Fatalf("uuid = %q, want %q", uuid, "GPU-abc")
	}
}

func TestDetectVisibleGPUUUIDFromEnvIgnoresEmptyEntries(t *testing.T) {
	t.Setenv(types.NvidiaVisibleDevicesEnv, " GPU-abc , ")
	uuid, err := DetectVisibleGPUUUID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if uuid != "GPU-abc" {
		t.Fatalf("uuid = %q, want %q", uuid, "GPU-abc")
	}
}

func TestDetectVisibleGPUUUIDFromEnvRejectsMultipleGPUs(t *testing.T) {
	t.Setenv(types.NvidiaVisibleDevicesEnv, "GPU-abc,GPU-def")
	if _, err := DetectVisibleGPUUUID(context.Background()); err == nil {
		t.Fatal("expected error for multiple visible GPUs")
	}
}
