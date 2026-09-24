//go:build !linux

package runtime

import types "github.com/beam-cloud/beta9/pkg/types"

// NewMicroVM needs KVM, netlink, and Linux mounts; on other platforms the
// runtime is simply not available.
func NewMicroVM(cfg Config) (Runtime, error) {
	return nil, ErrRuntimeNotAvailable{
		Runtime: types.ContainerRuntimeMicroVM.String(),
		Reason:  "the microvm runtime requires Linux with KVM",
	}
}
