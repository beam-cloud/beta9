package types

type ContainerRuntime string

const (
	ContainerRuntimeRunc        ContainerRuntime = "runc"
	ContainerRuntimeGvisor      ContainerRuntime = "gvisor"
	ContainerRuntimeFirecracker ContainerRuntime = "firecracker"
)

func (r ContainerRuntime) String() string {
	return string(r)
}
