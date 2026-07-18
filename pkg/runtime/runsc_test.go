package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestRunscRestoreSignalsStartedAfterStateRunning(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "runsc.log")
	readyPath := filepath.Join(dir, "restore-ready")
	waitPath := filepath.Join(dir, "restore-waited")
	bundlePath := writeRunscBundle(t, dir, false)
	runscPath := filepath.Join(dir, "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
set -eu
cmd=""
for arg in "$@"; do
  case "$arg" in
    flags|restore|state|wait|delete)
      cmd="$arg"
      break
      ;;
  esac
done
case "$cmd" in
  flags)
    echo "-TESTONLY-allow-packet-endpoint-write"
    ;;
  restore)
    echo "$*" >> "$RUNSC_FAKE_LOG"
    echo restore-start >> "$RUNSC_FAKE_LOG"
    sleep 0.2
    touch "$RUNSC_FAKE_READY"
    echo restore-ready >> "$RUNSC_FAKE_LOG"
    sleep 5
    echo restore-done >> "$RUNSC_FAKE_LOG"
    ;;
  state)
    echo state >> "$RUNSC_FAKE_LOG"
    if [ ! -f "$RUNSC_FAKE_READY" ]; then
      exit 1
    fi
    printf '{"id":"container-1","pid":4321,"status":"running"}'
    ;;
  wait)
    echo wait-start >> "$RUNSC_FAKE_LOG"
    sleep 0.2
    touch "$RUNSC_FAKE_WAITED"
    echo wait-done >> "$RUNSC_FAKE_LOG"
    ;;
  delete)
    echo delete >> "$RUNSC_FAKE_LOG"
    ;;
  *)
    echo "unexpected args: $*" >&2
    exit 1
    ;;
esac
`), 0o755))
	t.Setenv("RUNSC_FAKE_LOG", logPath)
	t.Setenv("RUNSC_FAKE_READY", readyPath)
	t.Setenv("RUNSC_FAKE_WAITED", waitPath)

	rt, err := NewRunsc(Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		_, err := rt.Restore(ctx, "container-1", &RestoreOpts{
			ImagePath:  filepath.Join(dir, "checkpoint"),
			BundlePath: bundlePath,
			Started:    started,
		})
		result <- err
	}()

	select {
	case pid := <-started:
		t.Fatalf("restore signaled started before restored runtime state was available, pid=%d", pid)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case pid := <-started:
		require.Equal(t, 4321, pid)
		require.FileExists(t, waitPath)
	case <-time.After(time.Second):
		t.Fatal("restore did not signal started from restored runtime state")
	}

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("restore did not return after restored runtime state was available")
	}

	logData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logData), "restore-start\n")
	require.Contains(t, string(logData), "restore-ready\n")
	require.Contains(t, string(logData), "wait-done\n")
	require.NotContains(t, string(logData), "restore-done\n")
	require.Contains(t, string(logData), "restore --background")
}

func TestRunscPrepareMarksOnlyGPUBundles(t *testing.T) {
	dir := t.TempDir()
	helpPath := filepath.Join(dir, "cuda-checkpoint")
	require.NoError(t, os.WriteFile(helpPath, []byte("#!/bin/sh\n"), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	rt := &Runsc{}
	gpuSpec := &specs.Spec{Linux: &specs.Linux{Devices: []specs.LinuxDevice{{Path: "/dev/nvidia0"}}}}
	require.NoError(t, rt.Prepare(context.Background(), gpuSpec))
	require.Equal(t, "true", gpuSpec.Annotations[runscGPUAnnotation])
	require.Empty(t, gpuSpec.Linux.Devices)
	require.Contains(t, gpuSpec.Mounts, specs.Mount{
		Destination: cudaCheckpointContainerPath,
		Type:        "bind",
		Source:      helpPath,
		Options:     []string{"bind", "ro"},
	})

	cpuSpec := &specs.Spec{Linux: &specs.Linux{}, Annotations: map[string]string{runscGPUAnnotation: "true"}}
	require.NoError(t, rt.Prepare(context.Background(), cpuSpec))
	require.NotContains(t, cpuSpec.Annotations, runscGPUAnnotation)
}

func TestRunscCheckpointUsesNativeCUDAHookForGPUBundle(t *testing.T) {
	tests := []struct {
		name         string
		gpu          bool
		wantCUDAHook bool
	}{
		{name: "gpu", gpu: true, wantCUDAHook: true},
		{name: "cpu", gpu: false, wantCUDAHook: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			bundlePath := writeRunscBundle(t, dir, tt.gpu)
			logPath := filepath.Join(dir, "runsc.log")
			runscPath := filepath.Join(dir, "runsc")
			require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$RUNSC_FAKE_LOG"
for arg in "$@"; do
  case "$arg" in
    flags)
      exit 0
      ;;
    state)
      printf '{"id":"container-1","pid":4321,"status":"running","bundle":"%s"}' "$RUNSC_FAKE_BUNDLE"
      exit 0
      ;;
    checkpoint)
      exit 0
      ;;
  esac
done
exit 1
`), 0o755))
			t.Setenv("RUNSC_FAKE_LOG", logPath)
			t.Setenv("RUNSC_FAKE_BUNDLE", bundlePath)

			rt, err := NewRunsc(Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")})
			require.NoError(t, err)
			require.NoError(t, rt.Checkpoint(context.Background(), "container-1", &CheckpointOpts{
				ImagePath: filepath.Join(dir, "checkpoint"),
			}))

			logData, err := os.ReadFile(logPath)
			require.NoError(t, err)
			hasCUDAHook := strings.Contains(string(logData), "--cuda-checkpoint-path "+cudaCheckpointContainerPath)
			require.Equal(t, tt.wantCUDAHook, hasCUDAHook)
		})
	}
}

func writeRunscBundle(t *testing.T, dir string, gpu bool) string {
	t.Helper()
	bundlePath := filepath.Join(dir, "bundle")
	require.NoError(t, os.MkdirAll(bundlePath, 0o755))
	spec := specs.Spec{Linux: &specs.Linux{}}
	if gpu {
		spec.Annotations = map[string]string{runscGPUAnnotation: "true"}
	}
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bundlePath, "config.json"), data, 0o644))
	return bundlePath
}
