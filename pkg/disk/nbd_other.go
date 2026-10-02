//go:build !linux

package disk

import "errors"

func widenSocketSendBuffers(int, []int, int) error {
	return errors.New("socket send buffers can only be widened on linux")
}
