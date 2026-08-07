package runtime

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

const startupHookShutdownTimeout = 5 * time.Second

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

	var pid int
	select {
	case result := <-done:
		return result.exitCode, result.err
	case pid = <-started:
	case <-ctx.Done():
		return -1, ctx.Err()
	}

	for _, hook := range r.hooks {
		if err := hook.Run(ctx, r.Runtime, containerID); err != nil {
			return r.failStartupHook(ctx, containerID, done, hook, err)
		}
	}

	if originalStarted != nil {
		select {
		case result := <-done:
			return result.exitCode, result.err
		case originalStarted <- pid:
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}

	select {
	case result := <-done:
		return result.exitCode, result.err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (r *startupHookRuntime) failStartupHook(ctx context.Context, containerID string, done <-chan startupHookRunResult, hook StartupHook, hookErr error) (int, error) {
	_ = r.Runtime.Kill(context.Background(), containerID, syscall.SIGKILL, &KillOpts{All: true})

	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), startupHookShutdownTimeout)
	defer cancel()

	select {
	case <-done:
	case <-waitCtx.Done():
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
