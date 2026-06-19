package runtime

import "github.com/opencontainers/runtime-spec/specs-go"

// DockerInDockerCapabilities returns the Linux capabilities required for
// running Docker inside a sandbox container.
func DockerInDockerCapabilities() []string {
	return append([]string(nil), dockerInDockerCapabilities...)
}

// AddDockerInDockerCapabilities adds the capabilities required for running
// Docker inside a sandbox container.
func AddDockerInDockerCapabilities(spec *specs.Spec) {
	if spec.Process == nil {
		spec.Process = &specs.Process{}
	}

	if spec.Process.Capabilities == nil {
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
	}

	spec.Process.Capabilities.Bounding = mergeCapabilities(spec.Process.Capabilities.Bounding, dockerInDockerCapabilities)
	spec.Process.Capabilities.Effective = mergeCapabilities(spec.Process.Capabilities.Effective, dockerInDockerCapabilities)
	spec.Process.Capabilities.Permitted = mergeCapabilities(spec.Process.Capabilities.Permitted, dockerInDockerCapabilities)
	spec.Process.Capabilities.Inheritable = mergeCapabilities(spec.Process.Capabilities.Inheritable, dockerInDockerCapabilities)
}

var dockerInDockerCapabilities = []string{
	"CAP_AUDIT_WRITE",
	"CAP_CHOWN",
	"CAP_DAC_OVERRIDE",
	"CAP_FOWNER",
	"CAP_FSETID",
	"CAP_KILL",
	"CAP_MKNOD",
	"CAP_NET_BIND_SERVICE",
	"CAP_NET_ADMIN",
	"CAP_NET_RAW",
	"CAP_SETFCAP",
	"CAP_SETGID",
	"CAP_SETPCAP",
	"CAP_SETUID",
	"CAP_SYS_ADMIN",
	"CAP_SYS_CHROOT",
	"CAP_SYS_PTRACE",
	"CAP_SYS_RESOURCE",
}
