//go:build linux

package disk

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// widenSocketSendBuffers sets the send buffer of sockets another process
// holds, through copies of its descriptors.
func widenSocketSendBuffers(pid int, fds []int, size int) error {
	pidfd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return fmt.Errorf("open process %d: %w", pid, err)
	}
	defer unix.Close(pidfd)
	for _, target := range fds {
		fd, err := unix.PidfdGetfd(pidfd, target, 0)
		if err != nil {
			return fmt.Errorf("copy descriptor %d of process %d: %w", target, pid, err)
		}
		err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUFFORCE, size)
		unix.Close(fd)
		if err != nil {
			return fmt.Errorf("widen send buffer of descriptor %d of process %d: %w", target, pid, err)
		}
	}
	return nil
}
