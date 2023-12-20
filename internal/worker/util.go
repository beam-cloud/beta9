package worker

import (
	"fmt"
	"io/ioutil"
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

func copyFile(src, dst string) error {
	input, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(dst, input, 0644)
}
