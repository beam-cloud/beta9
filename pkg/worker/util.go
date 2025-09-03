package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/opencontainers/runtime-spec/specs-go"
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

func copyDirectory(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		return copyFile(path, dstPath)
	})
}

func copyFile(src, dst string) error {
	cmd := exec.Command("cp", src, dst)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func createTar(srcDir, destTar string) error {
	cmd := exec.Command("tar", "-cf", destTar, "-C", filepath.Dir(srcDir), filepath.Base(srcDir))
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func untarTar(srcTar, destDir string) error {
	cmd := exec.Command("tar", "-xf", srcTar, "-C", destDir)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type FileLock struct {
	file *os.File
	path string
}

func NewFileLock(path string) *FileLock {
	return &FileLock{path: path}
}

func (fl *FileLock) Acquire() error {
	file, err := os.OpenFile(fl.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		file.Close()
		return err
	}

	fl.file = file
	return nil
}

func (fl *FileLock) Release() error {
	if fl.file == nil {
		return fmt.Errorf("file lock not acquired")
	}

	err := syscall.Flock(int(fl.file.Fd()), syscall.LOCK_UN)
	if err != nil {
		return err
	}

	fl.file.Close()
	fl.file = nil

	err = os.Remove(fl.path)
	if err != nil {
		return fmt.Errorf("failed to delete lock file: %v", err)
	}

	fl.file = nil
	return nil
}

// Adds extra env vars to an existing OCI spec
func addEnvToSpec(specPath string, extraEnv []string) error {
	f, err := os.OpenFile(specPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var spec specs.Spec
	if err := json.NewDecoder(f).Decode(&spec); err != nil {
		return err
	}

	spec.Process.Env = append(spec.Process.Env, extraEnv...)

	f.Seek(0, 0)
	f.Truncate(0)
	return json.NewEncoder(f).Encode(&spec)
}
