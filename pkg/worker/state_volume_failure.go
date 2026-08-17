package worker

import "fmt"

// handleStateVolumeQSDExit immediately fences writers. It deliberately does
// not reconnect QSD beneath a mounted filesystem; normal lifecycle teardown
// may unmount only if the kernel remains responsive.
func (s *Worker) handleStateVolumeQSDExit(containerID string, cause error) {
	s.fenceStateVolumeContainer(containerID, fmt.Errorf("%w: %v", ErrStateVolumeQSDExited, cause))
}
