package types

type ContainerRuntime string

const (
	ContainerRuntimeRunc    ContainerRuntime = "runc"
	ContainerRuntimeGvisor  ContainerRuntime = "gvisor"
	ContainerRuntimeMicroVM ContainerRuntime = "microvm"
)

func (r ContainerRuntime) String() string {
	return string(r)
}
