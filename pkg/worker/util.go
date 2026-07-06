package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

func tarCommandError(action string, err error, stderr bytes.Buffer) error {
	message := stderr.String()
	if message == "" {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s: %w: %s", action, err, message)
}

// Creates a symlink, but will remove any existing symlinks, files, or directories
// before doing so.
func forceSymlink(source, link string) error {
	err := os.RemoveAll(link)
	if err != nil {
		return fmt.Errorf("error removing existing file or directory: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(link), 0755); err != nil {
		return fmt.Errorf("error creating symlink parent directory: %v", err)
	}

	return os.Symlink(source, link)
}

func copyDirectory(src, dst string, excludePaths []string) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("create destination directory %s: %w", dst, err)
	}

	rootExcludes := map[string]struct{}{}
	for _, excludePath := range excludePaths {
		cleanPath := filepath.ToSlash(filepath.Clean(excludePath))
		if cleanPath == "." || cleanPath == "" {
			continue
		}
		if strings.Contains(cleanPath, "/") {
			return copyDirectoryWalk(src, dst, excludePaths)
		}
		rootExcludes[cleanPath] = struct{}{}
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("read source directory %s: %w", src, err)
	}

	tarArgs := []string{"-cf", "-", "-C", src, "--"}
	for _, entry := range entries {
		if _, excluded := rootExcludes[entry.Name()]; excluded {
			continue
		}
		tarArgs = append(tarArgs, "./"+entry.Name())
	}
	if len(tarArgs) == 5 {
		return nil
	}

	reader, writer := io.Pipe()
	archiveCmd := exec.Command("tar", tarArgs...)
	extractCmd := exec.Command("tar", "-xf", "-", "-C", dst)
	var archiveStderr, extractStderr bytes.Buffer
	archiveCmd.Stdout = writer
	archiveCmd.Stderr = &archiveStderr
	extractCmd.Stdin = reader
	extractCmd.Stderr = &extractStderr

	if err := extractCmd.Start(); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return fmt.Errorf("start directory extraction: %w", err)
	}
	if err := archiveCmd.Start(); err != nil {
		_ = reader.Close()
		_ = writer.Close()
		_ = extractCmd.Wait()
		return fmt.Errorf("start directory archive: %w", err)
	}

	archiveDone := make(chan error, 1)
	go func() {
		err := archiveCmd.Wait()
		_ = writer.CloseWithError(err)
		archiveDone <- err
	}()

	extractErr := extractCmd.Wait()
	_ = reader.Close()
	archiveErr := <-archiveDone

	if archiveErr != nil {
		return tarCommandError(fmt.Sprintf("archive directory %s", src), archiveErr, archiveStderr)
	}
	if extractErr != nil {
		return tarCommandError(fmt.Sprintf("extract directory to %s", dst), extractErr, extractStderr)
	}
	return nil
}

func copyDirectoryWalk(src, dst string, excludePaths []string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		for _, excludePath := range excludePaths {
			if relPath == excludePath {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		dstPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}
		if info.Mode()&os.ModeSocket != 0 {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("read symlink %s: %w", path, err)
			}
			if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
				return fmt.Errorf("create symlink parent %s: %w", filepath.Dir(dstPath), err)
			}
			_ = os.Remove(dstPath)
			return os.Symlink(linkTarget, dstPath)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, dstPath)
	})
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file %s: %w", src, err)
	}
	defer srcFile.Close()

	info, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("stat source file %s: %w", src, err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("create destination parent %s: %w", filepath.Dir(dst), err)
	}

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return fmt.Errorf("open destination file %s: %w", dst, err)
	}

	_, copyErr := io.Copy(dstFile, srcFile)
	closeErr := dstFile.Close()
	if copyErr != nil {
		return fmt.Errorf("copy %s to %s: %w", src, dst, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close destination file %s: %w", dst, closeErr)
	}

	return nil
}

func createTar(srcDir, destTar string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		_ = os.Remove(destTar)

		cmd := exec.Command("tar", "-cf", destTar, "-C", filepath.Dir(srcDir), filepath.Base(srcDir))
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 250 * time.Millisecond)
			continue
		}

		return nil
	}

	return lastErr
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
