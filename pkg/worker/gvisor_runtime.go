package worker

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// GvisorRuntime implements ContainerRuntime using gvisor
type GvisorRuntime struct {
	config types.AppConfig
}

// NewGvisorRuntime creates a new gvisor runtime
func NewGvisorRuntime(config types.AppConfig) (*GvisorRuntime, error) {
	return &GvisorRuntime{
		config: config,
	}, nil
}

func (g *GvisorRuntime) Run(ctx context.Context, containerId string, bundlePath string, opts *ContainerRuntimeOpts) (int, error) {
	// Run container using runsc
	cmd := exec.CommandContext(ctx, "runsc",
		"--root", "/run/gvisor",
		"run",
		"--bundle", bundlePath,
		containerId,
	)

	if opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = opts.OutputWriter
	}

	// Start the container
	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("failed to start container: %v", err)
	}

	// Signal that container has started
	if opts.Started != nil {
		opts.Started <- cmd.Process.Pid
	}

	// Wait for container to exit
	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), nil
		}
		return -1, err
	}

	return 0, nil
}

func (g *GvisorRuntime) Kill(ctx context.Context, containerId string, signal int, opts *KillOpts) error {
	cmd := exec.CommandContext(ctx, "runsc",
		"--root", "/run/gvisor",
		"kill",
		containerId,
		fmt.Sprintf("%d", signal),
	)
	return cmd.Run()
}

func (g *GvisorRuntime) Delete(ctx context.Context, containerId string, opts *DeleteOpts) error {
	args := []string{
		"--root", "/run/gvisor",
		"delete",
	}
	if opts.Force {
		args = append(args, "--force")
	}
	args = append(args, containerId)

	cmd := exec.CommandContext(ctx, "runsc", args...)
	return cmd.Run()
}

func (g *GvisorRuntime) State(ctx context.Context, containerId string) (*ContainerState, error) {
	cmd := exec.CommandContext(ctx, "runsc",
		"--root", "/run/gvisor",
		"state",
		containerId,
	)

	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	// Parse state output to get status and PID
	// This is a simplified version - you'll need to properly parse the JSON output
	state := &ContainerState{
		Status: "running", // You'll need to parse this from the actual output
		Pid:    0,         // You'll need to parse this from the actual output
	}

	return state, nil
}

type gvisorIOAdapter struct {
	containerIO ContainerIO
	stdinR      io.ReadCloser
	stdinW      io.WriteCloser
	stdoutR     io.ReadCloser
	stdoutW     io.WriteCloser
	stderrR     io.ReadCloser
	stderrW     io.WriteCloser
}

func newGvisorIOAdapter(containerIO ContainerIO) *gvisorIOAdapter {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	adapter := &gvisorIOAdapter{
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
		io.Copy(adapter.containerIO.Stdin(), adapter.stdinR)
	}()
	go func() {
		io.Copy(adapter.stdoutW, adapter.containerIO.Stdout())
	}()
	go func() {
		io.Copy(adapter.stderrW, adapter.containerIO.Stderr())
	}()

	return adapter
}

func (g *GvisorRuntime) Exec(ctx context.Context, containerId string, process specs.Process, opts *ExecOpts) error {
	// Convert process spec to runsc exec command
	args := []string{
		"--root", "/run/gvisor",
		"exec",
		"--pid-file", filepath.Join("/run/gvisor", containerId, "exec.pid"),
	}

	// Add process args
	args = append(args, containerId)
	args = append(args, process.Args...)

	cmd := exec.CommandContext(ctx, "runsc", args...)

	// Set up I/O
	if opts.IO != nil {
		adapter := newGvisorIOAdapter(opts.IO)
		cmd.Stdin = adapter.stdinR
		cmd.Stdout = adapter.stdoutW
		cmd.Stderr = adapter.stderrW
	}

	// Start the process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start exec process: %v", err)
	}

	// Signal that process has started
	if opts.Started != nil {
		opts.Started <- cmd.Process.Pid
	}

	// Wait for process to exit
	go func() {
		err := cmd.Wait()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				opts.IO.Done() <- exitErr.ExitCode()
			} else {
				opts.IO.Done() <- -1
			}
		} else {
			opts.IO.Done() <- 0
		}
	}()

	return nil
}

func (g *GvisorRuntime) Available() bool {
	_, err := exec.LookPath("runsc")
	return err == nil
}

func (g *GvisorRuntime) Name() string {
	return "gvisor"
}

func (g *GvisorRuntime) Events(ctx context.Context, containerId string, interval time.Duration) (<-chan *runc.Event, error) {
	// Gvisor doesn't support events, return a closed channel
	ch := make(chan *runc.Event)
	close(ch)
	return ch, nil
}
