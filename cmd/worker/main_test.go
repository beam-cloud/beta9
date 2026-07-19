package main

import (
	"runtime"
	"strconv"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestConfigureGOMAXPROCSFromWorkerCPULimit(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	t.Setenv("GOMAXPROCS", "")
	t.Setenv(types.WorkerCPUEnv, "64000")

	configureGOMAXPROCS()

	if got := runtime.GOMAXPROCS(0); got != 64 {
		t.Fatalf("GOMAXPROCS = %d, want 64", got)
	}
}

func TestConfigureGOMAXPROCSRoundsFractionalCPUUp(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	t.Setenv("GOMAXPROCS", "")
	t.Setenv(types.WorkerCPUEnv, "1500")

	configureGOMAXPROCS()

	if got := runtime.GOMAXPROCS(0); got != 2 {
		t.Fatalf("GOMAXPROCS = %d, want 2", got)
	}
}

func TestConfigureGOMAXPROCSPreservesExplicitSetting(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	explicit := min(runtime.NumCPU(), 3)
	runtime.GOMAXPROCS(explicit)
	t.Setenv("GOMAXPROCS", strconv.Itoa(explicit))
	t.Setenv(types.WorkerCPUEnv, "64000")

	configureGOMAXPROCS()

	if got := runtime.GOMAXPROCS(0); got != explicit {
		t.Fatalf("GOMAXPROCS = %d, want explicit setting %d", got, explicit)
	}
}
