package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// StartupHook runs after the container init process starts but before the
// runtime publishes the container as started to worker lifecycle consumers.
type StartupHook interface {
	Name() string
	Run(ctx context.Context, rt Runtime, containerID string) error
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
