//go:build linux

package runtime

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
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
	return inst.bindCanvas(source, target, false, true)
}
