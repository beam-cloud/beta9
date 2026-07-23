package runtime

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	runcRestoreStateTimeout       = 30 * time.Second
	runcRestoreStatePollInterval  = 25 * time.Millisecond
	runcRestoreOutputDrainTimeout = 2 * time.Second
)

type runcCommandResult struct {
	exitCode int
	err      error
}

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
			Command: cfg.RuncPath,
			Debug:   cfg.Debug,
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

func (r *Runc) UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error {
	if resources == nil {
		return fmt.Errorf("resources cannot be nil")
	}

	return r.handle.Update(ctx, containerID, resources)
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
		if runcContainerNotFound(err) {
			return State{}, ErrContainerNotFound{ContainerID: containerID}
		}
		return State{}, err
	}

	return State{
		ID:     state.ID,
		Pid:    state.Pid,
		Status: state.Status,
	}, nil
}

func runcContainerNotFound(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such container")
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

func (r *Runc) Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error {
	if opts == nil {
		return fmt.Errorf("checkpoint options cannot be nil")
	}

	runcOpts := &runc.CheckpointOpts{
		ImagePath:    opts.ImagePath,
		WorkDir:      opts.WorkDir,
		LeaveRunning: opts.LeaveRunning,
		AllowOpenTCP: opts.AllowOpenTCP,
		SkipInFlight: opts.SkipInFlight,
		LinkRemap:    opts.LinkRemap,
		Cgroups:      runc.Soft,
	}

	if opts.OutputWriter != nil {
		runcOpts.OutputWriter = opts.OutputWriter
	}

	return r.handle.Checkpoint(ctx, containerID, runcOpts)
}

func (r *Runc) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	if opts == nil {
		return -1, fmt.Errorf("restore options cannot be nil")
	}

	cmd := exec.Command(r.runcCommand(), r.restoreArgs(containerID, opts)...)
	var outputRead, outputWrite *os.File
	var outputDone chan struct{}
	if opts.OutputWriter != nil {
		var err error
		outputRead, outputWrite, err = os.Pipe()
		if err != nil {
			return -1, fmt.Errorf("create restore output pipe: %w", err)
		}
		cmd.Stdout = outputWrite
		cmd.Stderr = outputWrite
	}

	if err := cmd.Start(); err != nil {
		if outputRead != nil {
			_ = outputRead.Close()
			_ = outputWrite.Close()
		}
		return -1, err
	}
	if outputRead != nil {
		_ = outputWrite.Close()
		outputDone = make(chan struct{})
		go func() {
			defer close(outputDone)
			_, _ = io.Copy(opts.OutputWriter, outputRead)
			_ = outputRead.Close()
		}()
	}

	resultCh := make(chan runcCommandResult, 1)
	go func() {
		resultCh <- runcWaitResult(cmd.Wait())
	}()

	var result runcCommandResult
	var contextErr error
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		select {
		case result = <-resultCh:
		default:
			_ = cmd.Process.Kill()
			result = <-resultCh
			contextErr = ctx.Err()
		}
	}
	if contextErr != nil {
		drainRestoreOutput(ctx, outputRead, outputDone)
		return result.exitCode, contextErr
	}
	if result.err != nil {
		drainRestoreOutput(ctx, outputRead, outputDone)
		return result.exitCode, result.err
	}

	pid, _, err := r.waitForRestoredContainerPID(ctx, containerID, nil)
	if err != nil {
		return -1, err
	}

	if opts.Started != nil {
		select {
		case opts.Started <- pid:
		default:
			return -1, fmt.Errorf("restore started but started channel was unavailable")
		}
	}

	return result.exitCode, nil
}

func drainRestoreOutput(ctx context.Context, outputRead *os.File, outputDone <-chan struct{}) {
	if outputDone == nil {
		return
	}

	timer := time.NewTimer(runcRestoreOutputDrainTimeout)
	defer timer.Stop()
	select {
	case <-outputDone:
		return
	case <-ctx.Done():
	case <-timer.C:
	}

	// A partially restored child may still hold the pipe open. Stop waiting so
	// the caller can clean up the failed runtime container and retry.
	_ = outputRead.Close()
}

func (r *Runc) runcCommand() string {
	if r.handle.Command != "" {
		return r.handle.Command
	}
	return runc.DefaultCommand
}

func (r *Runc) restoreArgs(containerID string, opts *RestoreOpts) []string {
	args := r.globalArgs()
	args = append(args, "restore", "--detach")
	args = append(args, restoreCheckpointArgs(opts)...)
	if opts.AllowOpenTCP {
		args = append(args, "--tcp-established")
	} else if opts.TCPClose {
		args = append(args, "--tcp-close")
	}
	args = append(args, "--bundle", opts.BundlePath, containerID)
	return args
}

func (r *Runc) globalArgs() []string {
	var args []string
	if r.handle.Root != "" {
		args = append(args, "--root", r.handle.Root)
	}
	if r.handle.Debug {
		args = append(args, "--debug")
	}
	if r.handle.Log != "" {
		args = append(args, "--log", r.handle.Log)
	}
	if string(r.handle.LogFormat) != "" {
		args = append(args, "--log-format", string(r.handle.LogFormat))
	}
	if r.handle.SystemdCgroup {
		args = append(args, "--systemd-cgroup")
	}
	if r.handle.Rootless != nil {
		args = append(args, "--rootless="+strconv.FormatBool(*r.handle.Rootless))
	}
	return append(args, r.handle.ExtraArgs...)
}

func restoreCheckpointArgs(opts *RestoreOpts) []string {
	var args []string
	if opts.ImagePath != "" {
		args = append(args, "--image-path", opts.ImagePath)
	}
	if opts.WorkDir != "" {
		args = append(args, "--work-path", opts.WorkDir)
	}
	args = append(args, "--link-remap", "--manage-cgroups-mode", string(runc.Soft))
	return args
}

func (r *Runc) waitForRestoredContainerPID(ctx context.Context, containerID string, restoreDone <-chan runcCommandResult) (int, *runcCommandResult, error) {
	return pollRestoredContainerPID(ctx, containerID, restoreDone, runcRestoreStateTimeout, runcRestoreStatePollInterval, r.State)
}

func pollRestoredContainerPID(
	ctx context.Context,
	containerID string,
	restoreDone <-chan runcCommandResult,
	timeout time.Duration,
	interval time.Duration,
	stateFn func(context.Context, string) (State, error),
) (int, *runcCommandResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastErr error
	for {
		state, err := stateFn(ctx, containerID)
		if err == nil && state.Pid > 0 {
			return state.Pid, nil, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("restored container state has no pid")
		}

		select {
		case <-ctx.Done():
			return -1, nil, fmt.Errorf("restore succeeded but restored container state was unavailable: %w", lastErr)
		case result := <-restoreDone:
			return -1, &result, nil
		case <-ticker.C:
		}
	}
}

func runcWaitResult(err error) runcCommandResult {
	if err == nil {
		return runcCommandResult{exitCode: 0}
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if ws.Exited() {
				return runcCommandResult{exitCode: ws.ExitStatus(), err: err}
			}
			if ws.Signaled() {
				return runcCommandResult{exitCode: 128 + int(ws.Signal()), err: err}
			}
		}
		return runcCommandResult{exitCode: exitErr.ExitCode(), err: err}
	}

	return runcCommandResult{exitCode: -1, err: err}
}

func (r *Runc) RestoreWaitsForExit() bool {
	return false
}

func (r *Runc) Close() error {
	// No resources to clean up for runc
	return nil
}
