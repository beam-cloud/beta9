package runtime

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

const startupHookShutdownTimeout = 5 * time.Second

// StartupHook runs after the container init process starts but before the
// runtime publishes the container as started to worker lifecycle consumers.
type StartupHook interface {
	Name() string
	Run(ctx context.Context, rt Runtime, containerID string) error
}

type startupHookRuntime struct {
	Runtime
	hooks []StartupHook
}

// WithStartupHooks wraps a runtime so hooks complete before Run publishes the
// container PID to lifecycle consumers.
func WithStartupHooks(rt Runtime, hooks ...StartupHook) Runtime {
	if rt == nil || len(hooks) == 0 {
		return rt
	}

	filtered := make([]StartupHook, 0, len(hooks))
	for _, hook := range hooks {
		if hook != nil {
			filtered = append(filtered, hook)
		}
	}
	if len(filtered) == 0 {
		return rt
	}

	return &startupHookRuntime{
		Runtime: rt,
		hooks:   filtered,
	}
}

// StartupExecHook runs a process inside the container as a startup hook.
type StartupExecHook struct {
	HookName     string
	Process      specs.Process
	Timeout      time.Duration
	OutputWriter OutputWriter
}

func (h StartupExecHook) Name() string {
	if h.HookName != "" {
		return h.HookName
	}
	return "startup_exec"
}

func (h StartupExecHook) Run(ctx context.Context, rt Runtime, containerID string) error {
	if rt == nil {
		return fmt.Errorf("runtime is required for startup hook %q", h.Name())
	}

	if h.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}

	return rt.Exec(ctx, containerID, h.Process, &ExecOpts{
		OutputWriter: h.OutputWriter,
	})
}

type startupHookRunResult struct {
	exitCode int
	err      error
}

func (r *startupHookRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	runOpts := &RunOpts{}
	if opts != nil {
		*runOpts = *opts
	}
	originalStarted := runOpts.Started
	exitCode, err := r.runWithStartedHook(ctx, containerID, originalStarted, func(ctx context.Context, started chan<- int) (int, error) {
		runOpts.Started = started
		return r.Runtime.Run(ctx, containerID, bundlePath, runOpts)
	})
	return exitCode, err
}

func (r *startupHookRuntime) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	restoreOpts := &RestoreOpts{}
	if opts != nil {
		*restoreOpts = *opts
	}
	originalStarted := restoreOpts.Started
	exitCode, err := r.runWithStartedHook(ctx, containerID, originalStarted, func(ctx context.Context, started chan<- int) (int, error) {
		restoreOpts.Started = started
		return r.Runtime.Restore(ctx, containerID, restoreOpts)
	})
	return exitCode, err
}

func (r *startupHookRuntime) runWithStartedHook(ctx context.Context, containerID string, originalStarted chan<- int, run func(context.Context, chan<- int) (int, error)) (int, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	started := make(chan int, 1)
	done := make(chan startupHookRunResult, 1)
	go func() {
		exitCode, err := run(runCtx, started)
		done <- startupHookRunResult{exitCode: exitCode, err: err}
	}()

	pid, completed, ok, err := waitForStartupPID(ctx, started, done)
	if err != nil {
		return -1, err
	}
	if !ok {
		return completed.exitCode, completed.err
	}

	for _, hook := range r.hooks {
		if err := hook.Run(ctx, r.Runtime, containerID); err != nil {
			return r.failStartupHook(ctx, containerID, done, completed, hook, err)
		}
	}

	if originalStarted != nil {
		select {
		case originalStarted <- pid:
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}

	if completed != nil {
		return completed.exitCode, completed.err
	}

	select {
	case result := <-done:
		return result.exitCode, result.err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func waitForStartupPID(ctx context.Context, started <-chan int, done <-chan startupHookRunResult) (int, *startupHookRunResult, bool, error) {
	for {
		select {
		case pid := <-started:
			return pid, nil, true, nil
		default:
		}

		select {
		case pid := <-started:
			return pid, nil, true, nil
		case result := <-done:
			select {
			case pid := <-started:
				return pid, &result, true, nil
			default:
				return 0, &result, false, nil
			}
		case <-ctx.Done():
			return 0, nil, false, ctx.Err()
		}
	}
}

func (r *startupHookRuntime) failStartupHook(ctx context.Context, containerID string, done <-chan startupHookRunResult, completed *startupHookRunResult, hook StartupHook, hookErr error) (int, error) {
	_ = r.Runtime.Kill(context.Background(), containerID, syscall.SIGKILL, &KillOpts{All: true})

	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), startupHookShutdownTimeout)
	defer cancel()

	if completed == nil {
		select {
		case <-done:
		case <-waitCtx.Done():
		}
	}

	return -1, fmt.Errorf("startup hook %q failed: %w", hook.Name(), hookErr)
}

func (r *startupHookRuntime) UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error {
	updater, ok := r.Runtime.(interface {
		UpdateResources(context.Context, string, *specs.LinuxResources) error
	})
	if !ok {
		return fmt.Errorf("runtime %s does not support resource updates", r.Name())
	}
	return updater.UpdateResources(ctx, containerID, resources)
}

func (r *startupHookRuntime) RestoreWaitsForExit() bool {
	restoreRuntime, ok := r.Runtime.(interface{ RestoreWaitsForExit() bool })
	return ok && restoreRuntime.RestoreWaitsForExit()
}
