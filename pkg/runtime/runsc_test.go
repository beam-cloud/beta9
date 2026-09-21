package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestNewRunscDefaultsUnsupportedDriverCompatibility(t *testing.T) {
	dir := t.TempDir()
	runscPath := filepath.Join(dir, "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "enabled by default", want: runscAllowUnsupportedDriver},
		{name: "explicit opt out", args: []string{runscAllowUnsupportedDriver + "=false"}, want: runscAllowUnsupportedDriver + "=false"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt, err := NewRunsc(Config{RunscPath: runscPath, RunscExtraArgs: test.args})
			require.NoError(t, err)
			require.Contains(t, rt.cfg.RunscExtraArgs, test.want)
			require.Len(t, rt.cfg.RunscExtraArgs, 1)
		})
	}
}

func TestRunscStateRecognizesMissingContainerFromCurrentRunsc(t *testing.T) {
	dir := t.TempDir()
	runscPath := filepath.Join(dir, "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
echo 'FetchSpec failed: loading container: file does not exist' >&2
exit 128
`), 0o755))

	rt := &Runsc{cfg: Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")}}
	_, err := rt.State(context.Background(), "already-gone")

	var notFound ErrContainerNotFound
	require.ErrorAs(t, err, &notFound)
	require.Equal(t, "already-gone", notFound.ContainerID)
}

func TestRunscStateDoesNotHideUnrelatedExit128(t *testing.T) {
	dir := t.TempDir()
	runscPath := filepath.Join(dir, "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
echo 'runsc state failed: permission denied' >&2
exit 128
`), 0o755))

	rt := &Runsc{cfg: Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")}}
	_, err := rt.State(context.Background(), "unreadable")

	var notFound ErrContainerNotFound
	require.False(t, errors.As(err, &notFound))
	require.ErrorContains(t, err, "permission denied")
}

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
	require.Contains(t, string(logData), "restore --background --direct")
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

var (
	testProcMount   = specs.Mount{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}}
	testCgroupMount = specs.Mount{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: []string{"nosuid", "noexec", "nodev", "relatime"}}
)

func writeRunscBundleWithMounts(t *testing.T, dir string, mounts ...specs.Mount) string {
	t.Helper()
	bundlePath := filepath.Join(dir, "bundle")
	require.NoError(t, os.MkdirAll(bundlePath, 0o755))
	data, err := json.Marshal(specs.Spec{Linux: &specs.Linux{}, Mounts: mounts})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bundlePath, "config.json"), data, 0o644))
	return bundlePath
}

// writeRunscCheckpointImage lays out checkpoint.img the way runsc does: magic,
// big-endian metadata length, JSON metadata holding the container specs, then
// opaque state data.
func writeRunscCheckpointImage(t *testing.T, dir string, mounts ...specs.Mount) string {
	t.Helper()
	imagePath := filepath.Join(dir, "checkpoint")
	require.NoError(t, os.MkdirAll(imagePath, 0o755))
	containerSpecs, err := json.Marshal(map[string]*specs.Spec{"__no_name_0": {Mounts: mounts}})
	require.NoError(t, err)
	metadata, err := json.Marshal(map[string]string{"runsc_version": "test", runscCheckpointSpecsKey: string(containerSpecs)})
	require.NoError(t, err)
	var image bytes.Buffer
	image.Write(runscStateFileMagic)
	require.NoError(t, binary.Write(&image, binary.BigEndian, uint64(len(metadata))))
	image.Write(metadata)
	image.WriteString("state data that must never be parsed")
	require.NoError(t, os.WriteFile(filepath.Join(imagePath, runscCheckpointImageName), image.Bytes(), 0o644))
	return imagePath
}

func readBundleMounts(t *testing.T, bundlePath string) []specs.Mount {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(bundlePath, "config.json"))
	require.NoError(t, err)
	var spec specs.Spec
	require.NoError(t, json.Unmarshal(data, &spec))
	return spec.Mounts
}

// Checkpoints taken before the base config requested /sys/fs/cgroup must keep
// restoring: runsc rejects a restore whose mounts differ from the checkpoint.
func TestAlignRestoreSpecCgroupMount(t *testing.T) {
	tests := []struct {
		name       string
		checkpoint []specs.Mount
		bundle     []specs.Mount
		want       []specs.Mount
	}{
		{
			name:       "drops mount the checkpoint predates",
			checkpoint: []specs.Mount{testProcMount},
			bundle:     []specs.Mount{testProcMount, testCgroupMount},
			want:       []specs.Mount{testProcMount},
		},
		{
			name:       "adds mount the checkpoint was taken with",
			checkpoint: []specs.Mount{testProcMount, testCgroupMount},
			bundle:     []specs.Mount{testProcMount},
			want:       []specs.Mount{testProcMount, testCgroupMount},
		},
		{
			name:       "leaves matching specs alone",
			checkpoint: []specs.Mount{testProcMount, testCgroupMount},
			bundle:     []specs.Mount{testProcMount, testCgroupMount},
			want:       []specs.Mount{testProcMount, testCgroupMount},
		},
		{
			name:       "leaves specs without the mount alone",
			checkpoint: []specs.Mount{testProcMount},
			bundle:     []specs.Mount{testProcMount},
			want:       []specs.Mount{testProcMount},
		},
		{
			name:       "only touches the cgroup mount",
			checkpoint: []specs.Mount{testProcMount, {Destination: "/volumes/a", Type: "bind", Source: "/a"}},
			bundle:     []specs.Mount{testProcMount, testCgroupMount, {Destination: "/volumes/b", Type: "bind", Source: "/b"}},
			want:       []specs.Mount{testProcMount, {Destination: "/volumes/b", Type: "bind", Source: "/b"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			bundlePath := writeRunscBundleWithMounts(t, dir, test.bundle...)
			imagePath := writeRunscCheckpointImage(t, dir, test.checkpoint...)

			require.NoError(t, alignRestoreSpecCgroupMount(bundlePath, imagePath))
			require.Equal(t, test.want, readBundleMounts(t, bundlePath))
		})
	}
}

func TestAlignRestoreSpecCgroupMountLeavesBundleWhenCheckpointUnreadable(t *testing.T) {
	dir := t.TempDir()
	bundlePath := writeRunscBundleWithMounts(t, dir, testProcMount, testCgroupMount)

	missing := filepath.Join(dir, "missing")
	require.Error(t, alignRestoreSpecCgroupMount(bundlePath, missing))
	require.Equal(t, []specs.Mount{testProcMount, testCgroupMount}, readBundleMounts(t, bundlePath))

	corrupt := filepath.Join(dir, "corrupt")
	require.NoError(t, os.MkdirAll(corrupt, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(corrupt, runscCheckpointImageName), []byte("not a state file"), 0o644))
	require.Error(t, alignRestoreSpecCgroupMount(bundlePath, corrupt))
	require.Equal(t, []specs.Mount{testProcMount, testCgroupMount}, readBundleMounts(t, bundlePath))
}

func TestReadRunscCheckpointSpecs(t *testing.T) {
	dir := t.TempDir()
	imagePath := writeRunscCheckpointImage(t, dir, testProcMount, testCgroupMount)

	containerSpecs, err := readRunscCheckpointSpecs(imagePath)
	require.NoError(t, err)
	require.Len(t, containerSpecs, 1)
	require.Equal(t, []specs.Mount{testProcMount, testCgroupMount}, containerSpecs["__no_name_0"].Mounts)
}

// The restore command must see the aligned bundle, not the one the worker wrote.
func TestRunscRestoreAlignsBundleCgroupMountBeforeRestoring(t *testing.T) {
	dir := t.TempDir()
	bundlePath := writeRunscBundleWithMounts(t, dir, testProcMount, testCgroupMount)
	imagePath := writeRunscCheckpointImage(t, dir, testProcMount)
	mountsAtRestore := filepath.Join(dir, "mounts-at-restore.json")
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
  flags) ;;
  restore)
    cp "$RUNSC_FAKE_BUNDLE/config.json" "$RUNSC_FAKE_MOUNTS"
    ;;
  state)
    printf '{"id":"container-1","pid":4321,"status":"running"}'
    ;;
  wait|delete) ;;
  *)
    echo "unexpected args: $*" >&2
    exit 1
    ;;
esac
`), 0o755))
	t.Setenv("RUNSC_FAKE_BUNDLE", bundlePath)
	t.Setenv("RUNSC_FAKE_MOUNTS", mountsAtRestore)

	rt, err := NewRunsc(Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan int, 1)
	_, err = rt.Restore(ctx, "container-1", &RestoreOpts{ImagePath: imagePath, BundlePath: bundlePath, Started: started})
	require.NoError(t, err)

	data, err := os.ReadFile(mountsAtRestore)
	require.NoError(t, err)
	var spec specs.Spec
	require.NoError(t, json.Unmarshal(data, &spec))
	require.Equal(t, []specs.Mount{testProcMount}, spec.Mounts)
}
