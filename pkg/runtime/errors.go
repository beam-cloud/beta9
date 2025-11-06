package runtime

import "fmt"

// ErrUnsupportedRuntime is returned when an unsupported runtime type is requested
type ErrUnsupportedRuntime struct {
	Runtime string
}

func (e ErrUnsupportedRuntime) Error() string {
	return fmt.Sprintf("unsupported runtime type: %s", e.Runtime)
}

// ErrRuntimeNotAvailable is returned when a runtime is not available on the system
type ErrRuntimeNotAvailable struct {
	Runtime string
	Reason  string
}

func (e ErrRuntimeNotAvailable) Error() string {
	return fmt.Sprintf("runtime %s is not available: %s", e.Runtime, e.Reason)
}

// ErrContainerNotFound is returned when a container is not found
type ErrContainerNotFound struct {
	ContainerID string
}

func (e ErrContainerNotFound) Error() string {
	return fmt.Sprintf("container not found: %s", e.ContainerID)
}
