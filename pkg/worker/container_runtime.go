package worker

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// ContainerRuntime defines the interface for container runtime implementations
type ContainerRuntime interface {
	// Run starts a container with the given configuration
	Run(ctx context.Context, containerId string, bundlePath string, opts *ContainerRuntimeOpts) (int, error)

	// Kill sends a signal to a container
	Kill(ctx context.Context, containerId string, signal int, opts *KillOpts) error

	// Delete removes a container
	Delete(ctx context.Context, containerId string, opts *DeleteOpts) error

	// State returns the current state of a container
	State(ctx context.Context, containerId string) (*ContainerState, error)

	// Exec executes a command inside a running container
	Exec(ctx context.Context, containerId string, process specs.Process, opts *ExecOpts) error

	// Available returns whether the runtime is available on the system
	Available() bool

	// Name returns the name of the runtime (e.g. "runc", "gvisor")
	Name() string

	// Events returns a channel to receive container events
	Events(ctx context.Context, containerId string, interval time.Duration) (<-chan *runc.Event, error)
}

// ContainerRuntimeOpts contains options for running a container
type ContainerRuntimeOpts struct {
	OutputWriter io.Writer
	Started      chan int
}

// KillOpts contains options for killing a container
type KillOpts struct {
	All bool
}

// DeleteOpts contains options for deleting a container
type DeleteOpts struct {
	Force bool
}

// ExecOpts contains options for executing a command in a container
type ExecOpts struct {
	IO      ContainerIO
	Started chan int
}

// ContainerIO defines the interface for container I/O
type ContainerIO interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Done() chan int
	Close() error
}

// ContainerState represents the state of a container
type ContainerState struct {
	Status string
	Pid    int
}

// RuntimeType represents the type of container runtime to use
type RuntimeType string

const (
	RuntimeTypeRunc   RuntimeType = "runc"
	RuntimeTypeGvisor RuntimeType = "gvisor"
)

// NewContainerRuntime creates a new container runtime based on the specified type
func NewContainerRuntime(runtimeType RuntimeType, config types.AppConfig) (ContainerRuntime, error) {
	switch runtimeType {
	case RuntimeTypeRunc:
		return NewRuncRuntime(config)
	case RuntimeTypeGvisor:
		return NewGvisorRuntime(config)
	default:
		return nil, fmt.Errorf("unsupported runtime type: %s", runtimeType)
	}
}
