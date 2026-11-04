//go:build linux

package common

import (
	"os"

	"golang.org/x/sys/unix"
)

func init() {
	commandMount := mountOverlayFS
	mountOverlayFS = func(target, options string) ([]byte, error) {
		if os.Getenv("BETA9_NATIVE_OVERLAY_MOUNT") == "1" {
			return nil, unix.Mount("overlay", target, "overlay", 0, options)
		}
		return commandMount(target, options)
	}
}
