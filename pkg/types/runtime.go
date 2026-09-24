package types

type ContainerRuntime string

const (
	ContainerRuntimeRunc   ContainerRuntime = "runc"
	ContainerRuntimeGvisor ContainerRuntime = "gvisor"
	// ContainerRuntimeMicroVM boots each container as a Cloud Hypervisor
	// virtual machine. Only opt-in CPU sandboxes are scheduled onto it.
	ContainerRuntimeMicroVM ContainerRuntime = "microvm"
)

func (r ContainerRuntime) String() string {
	return string(r)
}
