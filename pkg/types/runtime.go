package types

type ContainerRuntime string

const (
	ContainerRuntimeRunc   ContainerRuntime = "runc"
	ContainerRuntimeGvisor ContainerRuntime = "gvisor"
)

func (r ContainerRuntime) String() string {
	return string(r)
}
