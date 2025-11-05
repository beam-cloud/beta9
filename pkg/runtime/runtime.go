package runtime

import (
	"context"
	"syscall"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// Capabilities describes what features a runtime supports
type Capabilities struct {
	CheckpointRestore bool // CRIU support
	GPU               bool // GPU device passthrough
	OOMEvents         bool // Runtime-native OOM events (use cgroup poller as fallback)
	JoinExistingNetNS bool // Can join existing network namespace
	CDI               bool // Container Device Interface support
}

// State represents the current state of a container
type State struct {
	ID     string
	Pid    int
	Status string // "running", "stopped", etc.
}

// Event represents a container event
type Event struct {
	Type string // "oom", "exit", "error"
	Err  error
}

// RunOpts contains options for running a container
type RunOpts struct {
	OutputWriter OutputWriter
	Started      chan<- int // PID channel
}

// ExecOpts contains options for executing a command in a container
type ExecOpts struct {
	OutputWriter OutputWriter
	Started      chan<- int
}

// KillOpts contains options for killing a container
type KillOpts struct {
	All bool // Kill all processes in the container
}

// DeleteOpts contains options for deleting a container
type DeleteOpts struct {
	Force bool // Force deletion
}

// OutputWriter is an interface for writing container output
type OutputWriter interface {
	Write(p []byte) (n int, err error)
}

// Runtime defines the interface for container runtime implementations
type Runtime interface {
	// Name returns the name of the runtime (e.g., "runc", "gvisor")
	Name() string

	// Capabilities returns what features this runtime supports
	Capabilities() Capabilities

	// Prepare may mutate spec to fit runtime quirks (e.g., seccomp, mounts)
	// Called before writing config.json
	Prepare(ctx context.Context, spec *specs.Spec) error

	// Run starts a container with the given configuration
	Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error)

	// Exec executes a command inside a running container
	Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error

	// Kill sends a signal to a container
	Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error

	// Delete removes a container
	Delete(ctx context.Context, containerID string, opts *DeleteOpts) error

	// State returns the current state of a container
	State(ctx context.Context, containerID string) (State, error)

	// Events returns a channel for receiving container events
	// Optional; use cgroup poller as portable fallback
	Events(ctx context.Context, containerID string) (<-chan Event, error)

	// Close cleans up any resources held by the runtime
	Close() error
}

// Config contains configuration for creating a runtime
type Config struct {
	Type          string // "runc" | "gvisor"
	RuncPath      string // Path to runc binary (default: "runc")
	RunscPath     string // Path to runsc binary (default: "runsc")
	RunscPlatform string // "kvm" | "ptrace" (optional)
	RunscRoot     string // Root directory for runsc state (default: "/run/gvisor")
	Debug         bool   // Enable debug mode
}

// New creates a new Runtime based on the provided configuration
func New(cfg Config) (Runtime, error) {
	switch cfg.Type {
	case "runc", "":
		return NewRunc(cfg)
	case "gvisor":
		return NewRunsc(cfg)
	default:
		return nil, ErrUnsupportedRuntime{Runtime: cfg.Type}
	}
}
