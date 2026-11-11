//go:build linux

package runtime

import (
	"fmt"
	"os"
	"path/filepath"
)

// installGuestBinary puts a host executable at rel in the canvas. A read-only
// bind shares executable pages across fresh canvases without allowing a
// guest to modify the worker's executable. Checkpoints still copy the exact
// guest init, and restores bind that pinned copy.
func (inst *microVMInstance) installGuestBinary(source, rel string) error {
	target := filepath.Join(inst.canvas, rel)
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", source)
	}
	if err := writeFileNoFollow(target, nil, 0o755); err != nil {
		return err
	}
	return inst.bindCanvas(source, target, false, true)
}
