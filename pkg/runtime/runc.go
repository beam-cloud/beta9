package runtime

import (
	"context"
	"os/exec"
	"syscall"
	"time"

	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// Runc implements Runtime using the runc container runtime
type Runc struct {
	handle runc.Runc
	cfg    Config
}

// NewRunc creates a new runc runtime
func NewRunc(cfg Config) (*Runc, error) {
	if cfg.RuncPath == "" {
		cfg.RuncPath = "runc"
	}

	// Check if runc is available
	if _, err := exec.LookPath(cfg.RuncPath); err != nil {
		return nil, ErrRuntimeNotAvailable{
			Runtime: "runc",
			Reason:  "runc binary not found in PATH",
		}
	}

	return &Runc{
		handle: runc.Runc{
			Debug: cfg.Debug,
		},
		cfg: cfg,
	}, nil
}

func (r *Runc) Name() string {
	return "runc"
}

func (r *Runc) Capabilities() Capabilities {
	return Capabilities{
		CheckpointRestore: true, // runc supports CRIU
		GPU:               true, // runc supports device passthrough
		OOMEvents:         true, // runc provides events
		JoinExistingNetNS: true, // runc can join network namespaces
		CDI:               true, // runc supports CDI
	}
}

// Prepare is a no-op for runc as it doesn't need spec mutations
func (r *Runc) Prepare(ctx context.Context, spec *specs.Spec) error {
	// runc can handle specs as-is
	return nil
}

func (r *Runc) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	runcOpts := &runc.CreateOpts{}
	
	if opts != nil {
		if opts.OutputWriter != nil {
			runcOpts.OutputWriter = opts.OutputWriter
		}
		if opts.Started != nil {
			runcOpts.Started = opts.Started
		}
	}

	return r.handle.Run(ctx, containerID, bundlePath, runcOpts)
}

func (r *Runc) Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error {
	runcOpts := &runc.ExecOpts{}
	
	if opts != nil {
		if opts.OutputWriter != nil {
			runcOpts.OutputWriter = opts.OutputWriter
		}
		if opts.Started != nil {
			runcOpts.Started = opts.Started
		}
	}

	return r.handle.Exec(ctx, containerID, proc, runcOpts)
}

func (r *Runc) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error {
	runcOpts := &runc.KillOpts{}
	
	if opts != nil {
		runcOpts.All = opts.All
	}

	return r.handle.Kill(ctx, containerID, int(sig), runcOpts)
}

func (r *Runc) Delete(ctx context.Context, containerID string, opts *DeleteOpts) error {
	runcOpts := &runc.DeleteOpts{}
	
	if opts != nil {
		runcOpts.Force = opts.Force
	}

	return r.handle.Delete(ctx, containerID, runcOpts)
}

func (r *Runc) State(ctx context.Context, containerID string) (State, error) {
	state, err := r.handle.State(ctx, containerID)
	if err != nil {
		return State{}, err
	}

	return State{
		ID:     state.ID,
		Pid:    state.Pid,
		Status: state.Status,
	}, nil
}

func (r *Runc) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	// runc supports events, but we'll convert to our Event type
	runcEvents, err := r.handle.Events(ctx, containerID, 100*time.Millisecond)
	if err != nil {
		return nil, err
	}

	events := make(chan Event)
	go func() {
		defer close(events)
		for runcEvent := range runcEvents {
			event := Event{
				Type: runcEvent.Type,
			}
			if runcEvent.Err != nil {
				event.Err = runcEvent.Err
			}
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	return events, nil
}

func (r *Runc) Close() error {
	// No resources to clean up for runc
	return nil
}
