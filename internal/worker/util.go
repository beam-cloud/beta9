package worker

import (
	"fmt"
	"os"
)

// Creates a symlink, but will remove any existing symlinks, files, or directories
// before doing so.
func forceSymlink(source, link string) error {
	err := os.RemoveAll(link)
	if err != nil {
		return fmt.Errorf("error removing existing file or directory: %v", err)
	}

	return os.Symlink(source, link)
}
