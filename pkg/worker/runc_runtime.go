package worker

import (
	"context"
	"io"
	"os/exec"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// RuncRuntime implements ContainerRuntime using runc
type RuncRuntime struct {
	runcHandle runc.Runc
	config     types.AppConfig
}

// NewRuncRuntime creates a new runc runtime
func NewRuncRuntime(config types.AppConfig) (*RuncRuntime, error) {
	return &RuncRuntime{
		runcHandle: runc.Runc{Debug: config.DebugMode},
		config:     config,
	}, nil
}

func (r *RuncRuntime) Run(ctx context.Context, containerId string, bundlePath string, opts *ContainerRuntimeOpts) (int, error) {
	runcOpts := &runc.CreateOpts{
		OutputWriter: opts.OutputWriter,
		Started:      opts.Started,
	}
	return r.runcHandle.Run(ctx, containerId, bundlePath, runcOpts)
}

func (r *RuncRuntime) Kill(ctx context.Context, containerId string, signal int, opts *KillOpts) error {
	runcOpts := &runc.KillOpts{
		All: opts.All,
	}
	return r.runcHandle.Kill(ctx, containerId, signal, runcOpts)
}

func (r *RuncRuntime) Delete(ctx context.Context, containerId string, opts *DeleteOpts) error {
	runcOpts := &runc.DeleteOpts{
		Force: opts.Force,
	}
	return r.runcHandle.Delete(ctx, containerId, runcOpts)
}

func (r *RuncRuntime) State(ctx context.Context, containerId string) (*ContainerState, error) {
	state, err := r.runcHandle.State(ctx, containerId)
	if err != nil {
		return nil, err
	}
	return &ContainerState{
		Status: state.Status,
		Pid:    state.Pid,
	}, nil
}

func (r *RuncRuntime) Exec(ctx context.Context, containerId string, process specs.Process, opts *ExecOpts) error {
	runcOpts := &runc.ExecOpts{
		IO:      convertContainerIO(opts.IO),
		Started: opts.Started,
	}
	return r.runcHandle.Exec(ctx, containerId, process, runcOpts)
}

func (r *RuncRuntime) Available() bool {
	_, err := exec.LookPath("runc")
	return err == nil
}

func (r *RuncRuntime) Name() string {
	return "runc"
}

func (r *RuncRuntime) Events(ctx context.Context, containerId string, interval time.Duration) (<-chan *runc.Event, error) {
	return r.runcHandle.Events(ctx, containerId, interval)
}

type runcIOAdapter struct {
	containerIO ContainerIO
	stdinR      io.ReadCloser
	stdinW      io.WriteCloser
	stdoutR     io.ReadCloser
	stdoutW     io.WriteCloser
	stderrR     io.ReadCloser
	stderrW     io.WriteCloser
}

func newRuncIOAdapter(containerIO ContainerIO) *runcIOAdapter {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	adapter := &runcIOAdapter{
		containerIO: containerIO,
		stdinR:      stdinR,
		stdinW:      stdinW,
		stdoutR:     stdoutR,
		stdoutW:     stdoutW,
		stderrR:     stderrR,
		stderrW:     stderrW,
	}

	// Copy data between the pipes
	go func() {
		io.Copy(adapter.stdinW, adapter.stdinR)
	}()
	go func() {
		io.Copy(adapter.stdoutW, adapter.containerIO.Stdout())
	}()
	go func() {
		io.Copy(adapter.stderrW, adapter.containerIO.Stderr())
	}()

	return adapter
}

func (a *runcIOAdapter) Stdin() io.WriteCloser {
	return a.containerIO.Stdin()
}

func (a *runcIOAdapter) Stdout() io.ReadCloser {
	return a.containerIO.Stdout()
}

func (a *runcIOAdapter) Stderr() io.ReadCloser {
	return a.containerIO.Stderr()
}

func (a *runcIOAdapter) Done() chan int {
	return a.containerIO.Done()
}

func (a *runcIOAdapter) Close() error {
	a.stdinR.Close()
	a.stdinW.Close()
	a.stdoutR.Close()
	a.stdoutW.Close()
	a.stderrR.Close()
	a.stderrW.Close()
	return a.containerIO.Close()
}

func (a *runcIOAdapter) Set(cmd *exec.Cmd) {
	cmd.Stdin = a.stdinR
	cmd.Stdout = a.stdoutW
	cmd.Stderr = a.stderrW
}

// convertContainerIO converts our ContainerIO interface to runc's IO interface
func convertContainerIO(containerIO ContainerIO) runc.IO {
	return newRuncIOAdapter(containerIO)
}
