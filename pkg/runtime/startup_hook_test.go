package runtime

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestStartupHookRuntimeWithholdsStartedUntilHooksComplete(t *testing.T) {
	rt := newStartupHookMockRuntime(1234)
	hook := &blockingStartupHook{
		name:    "install",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	wrapped := WithStartupHooks(rt, hook)

	started := make(chan int, 1)
	done := make(chan error, 1)
	go func() {
		_, err := wrapped.Run(context.Background(), "container-1", "/bundle", &RunOpts{Started: started})
		done <- err
	}()

	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("startup hook did not run")
	}
	select {
	case pid := <-started:
		t.Fatalf("started was published before hook completion: %d", pid)
	default:
	}

	close(hook.release)
	select {
	case pid := <-started:
		require.Equal(t, 1234, pid)
	case <-time.After(time.Second):
		t.Fatal("started was not published after hook completion")
	}

	rt.finishRun()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("run did not return after runtime finished")
	}
}

func TestStartupHookRuntimeRunsHooksInOrder(t *testing.T) {
	rt := newStartupHookMockRuntime(4321)
	calls := []string{}
	wrapped := WithStartupHooks(
		rt,
		&recordingStartupHook{name: "first", calls: &calls},
		&recordingStartupHook{name: "second", calls: &calls},
	)

	started := make(chan int, 1)
	done := make(chan error, 1)
	go func() {
		_, err := wrapped.Run(context.Background(), "container-1", "/bundle", &RunOpts{Started: started})
		done <- err
	}()

	select {
	case pid := <-started:
		require.Equal(t, 4321, pid)
	case <-time.After(time.Second):
		t.Fatal("started was not published")
	}
	require.Equal(t, []string{"first", "second"}, calls)

	rt.finishRun()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("run did not return after runtime finished")
	}
}

func TestStartupHookRuntimeRunsHooksWhenRuntimeReturnsAfterStarted(t *testing.T) {
	for i := 0; i < 100; i++ {
		rt := newStartupHookMockRuntime(2468)
		rt.finishRun()
		calls := []string{}
		wrapped := WithStartupHooks(rt, &recordingStartupHook{name: "install", calls: &calls})

		started := make(chan int, 1)
		exitCode, err := wrapped.Run(context.Background(), "container-1", "/bundle", &RunOpts{Started: started})

		require.NoError(t, err)
		require.Equal(t, 0, exitCode)
		require.Equal(t, []string{"install"}, calls)
		select {
		case pid := <-started:
			require.Equal(t, 2468, pid)
		default:
			t.Fatal("started was not published after hook completion")
		}
	}
}

func TestWaitForStartupPIDPrioritizesStartedWhenDoneReady(t *testing.T) {
	started := make(chan int, 1)
	done := make(chan startupHookRunResult, 1)
	started <- 1357
	done <- startupHookRunResult{exitCode: 0}

	pid, completed, ok, err := waitForStartupPID(context.Background(), started, done)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1357, pid)
	require.Nil(t, completed)
}

func TestStartupHookRuntimeWithholdsRestoreStartedUntilHooksComplete(t *testing.T) {
	rt := newStartupHookMockRuntime(5678)
	hook := &blockingStartupHook{
		name:    "restore-install",
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	wrapped := WithStartupHooks(rt, hook)

	started := make(chan int, 1)
	done := make(chan error, 1)
	go func() {
		_, err := wrapped.Restore(context.Background(), "container-1", &RestoreOpts{Started: started})
		done <- err
	}()

	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("startup hook did not run during restore")
	}
	select {
	case pid := <-started:
		t.Fatalf("restore started was published before hook completion: %d", pid)
	default:
	}

	close(hook.release)
	select {
	case pid := <-started:
		require.Equal(t, 5678, pid)
	case <-time.After(time.Second):
		t.Fatal("restore started was not published after hook completion")
	}

	rt.finishRun()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("restore did not return after runtime finished")
	}
}

func TestStartupHookRuntimeDelegatesOptionalRuntimeMethods(t *testing.T) {
	rt := newStartupHookMockRuntime(0)
	rt.restoreWaitsForExit = true
	wrapped := WithStartupHooks(rt, &recordingStartupHook{name: "install"})

	restoreRuntime, ok := wrapped.(interface{ RestoreWaitsForExit() bool })
	require.True(t, ok)
	require.True(t, restoreRuntime.RestoreWaitsForExit())

	updater, ok := wrapped.(interface {
		UpdateResources(context.Context, string, *specs.LinuxResources) error
	})
	require.True(t, ok)
	resources := &specs.LinuxResources{CPU: &specs.LinuxCPU{Cpus: "0"}}
	require.NoError(t, updater.UpdateResources(context.Background(), "container-1", resources))
	require.Same(t, resources, rt.updatedResources[0])
}

func TestStartupHookRuntimeFailureKillsContainerAndDoesNotPublishStarted(t *testing.T) {
	rt := newStartupHookMockRuntime(1234)
	wrapped := WithStartupHooks(rt, &recordingStartupHook{
		name: "install",
		err:  errors.New("installer failed"),
	})

	started := make(chan int, 1)
	exitCode, err := wrapped.Run(context.Background(), "container-1", "/bundle", &RunOpts{Started: started})

	require.Equal(t, -1, exitCode)
	require.Error(t, err)
	require.Contains(t, err.Error(), `startup hook "install" failed`)
	require.Contains(t, err.Error(), "installer failed")
	select {
	case pid := <-started:
		t.Fatalf("started was published after hook failure: %d", pid)
	default:
	}
	require.Equal(t, []syscall.Signal{syscall.SIGKILL}, rt.killSignals())
	require.Equal(t, []bool{true}, rt.killAllOpts())
}

func TestStartupExecHookRunsProcessWithExec(t *testing.T) {
	rt := newStartupHookMockRuntime(0)
	output := &bytes.Buffer{}
	process := specs.Process{
		Args: []string{"sh", "-c", "curl install"},
		Cwd:  "/workspace",
		Env:  []string{"PATH=/usr/bin"},
	}
	hook := StartupExecHook{
		HookName:     "thunder_install",
		Process:      process,
		OutputWriter: output,
	}

	require.Equal(t, "thunder_install", hook.Name())
	require.NoError(t, hook.Run(context.Background(), rt, "container-1"))
	require.Equal(t, []specs.Process{process}, rt.execCalls())
	require.Len(t, rt.execOutputWriters(), 1)
	require.Same(t, output, rt.execOutputWriters()[0])
}

type blockingStartupHook struct {
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *blockingStartupHook) Name() string {
	return h.name
}

func (h *blockingStartupHook) Run(ctx context.Context, rt Runtime, containerID string) error {
	h.once.Do(func() { close(h.entered) })
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type recordingStartupHook struct {
	name  string
	calls *[]string
	err   error
}

func (h *recordingStartupHook) Name() string {
	return h.name
}

func (h *recordingStartupHook) Run(ctx context.Context, rt Runtime, containerID string) error {
	if h.calls != nil {
		*h.calls = append(*h.calls, h.name)
	}
	return h.err
}

type startupHookMockRuntime struct {
	pid        int
	finish     chan struct{}
	finishOnce sync.Once

	mu                  sync.Mutex
	kills               []syscall.Signal
	killOpts            []*KillOpts
	execs               []specs.Process
	execWriterCaptures  []OutputWriter
	execErr             error
	updatedResources    []*specs.LinuxResources
	checkpointCallCount int
	restoreWaitsForExit bool
}

func newStartupHookMockRuntime(pid int) *startupHookMockRuntime {
	return &startupHookMockRuntime{
		pid:    pid,
		finish: make(chan struct{}),
	}
}

func (m *startupHookMockRuntime) Name() string {
	return "mock"
}

func (m *startupHookMockRuntime) Capabilities() Capabilities {
	return Capabilities{}
}

func (m *startupHookMockRuntime) Prepare(ctx context.Context, spec *specs.Spec) error {
	return nil
}

func (m *startupHookMockRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	if opts != nil && opts.Started != nil {
		opts.Started <- m.pid
	}
	select {
	case <-m.finish:
		return 0, nil
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (m *startupHookMockRuntime) Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execs = append(m.execs, proc)
	if opts != nil {
		m.execWriterCaptures = append(m.execWriterCaptures, opts.OutputWriter)
	} else {
		m.execWriterCaptures = append(m.execWriterCaptures, nil)
	}
	return m.execErr
}

func (m *startupHookMockRuntime) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error {
	m.mu.Lock()
	m.kills = append(m.kills, sig)
	m.killOpts = append(m.killOpts, opts)
	m.mu.Unlock()
	m.finishRun()
	return nil
}

func (m *startupHookMockRuntime) Delete(ctx context.Context, containerID string, opts *DeleteOpts) error {
	return nil
}

func (m *startupHookMockRuntime) State(ctx context.Context, containerID string) (State, error) {
	return State{}, nil
}

func (m *startupHookMockRuntime) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	return nil, nil
}

func (m *startupHookMockRuntime) Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error {
	m.checkpointCallCount++
	return nil
}

func (m *startupHookMockRuntime) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	if opts != nil && opts.Started != nil {
		opts.Started <- m.pid
	}
	select {
	case <-m.finish:
		return 0, nil
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (m *startupHookMockRuntime) UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updatedResources = append(m.updatedResources, resources)
	return nil
}

func (m *startupHookMockRuntime) RestoreWaitsForExit() bool {
	return m.restoreWaitsForExit
}

func (m *startupHookMockRuntime) Close() error {
	return nil
}

func (m *startupHookMockRuntime) finishRun() {
	m.finishOnce.Do(func() { close(m.finish) })
}

func (m *startupHookMockRuntime) killSignals() []syscall.Signal {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]syscall.Signal(nil), m.kills...)
}

func (m *startupHookMockRuntime) killAllOpts() []bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]bool, 0, len(m.killOpts))
	for _, opts := range m.killOpts {
		out = append(out, opts != nil && opts.All)
	}
	return out
}

func (m *startupHookMockRuntime) execCalls() []specs.Process {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]specs.Process(nil), m.execs...)
}

func (m *startupHookMockRuntime) execOutputWriters() []OutputWriter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]OutputWriter(nil), m.execWriterCaptures...)
}
