//go:build linux

package runtime

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

// A read-only bind shares executable pages across fresh canvases without
// allowing a guest to modify the worker's executable. Checkpoints still copy
// their exact executable, and restores bind that pinned copy.
func (inst *microVMInstance) installGuestInit(source string) error {
	target := filepath.Join(inst.canvas, microvm.InitPath)
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("guest init is not a regular file")
	}
	if err := writeFileNoFollow(target, nil, 0o755); err != nil {
		return err
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return err
	}
	// Register before remount so ordinary teardown also handles its failure.
	inst.submounts = append(inst.submounts, target)
	return unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
}
