package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var memoryLimitPaths = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: cgroup-memory <limit-bytes>")
		os.Exit(2)
	}

	limitBytes, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil || limitBytes <= 0 {
		fmt.Fprintf(os.Stderr, "invalid memory limit %q\n", os.Args[1])
		os.Exit(2)
	}

	if err := setVisibleMemoryLimit(memoryLimitPaths, limitBytes); err != nil {
		fmt.Fprintf(os.Stderr, "set visible memory limit: %v\n", err)
		os.Exit(1)
	}
}

func setVisibleMemoryLimit(paths []string, limitBytes int64) error {
	if limitBytes <= 0 {
		return errors.New("memory limit must be positive")
	}

	var unavailable []error
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				unavailable = append(unavailable, err)
				continue
			}
			return fmt.Errorf("stat %s: %w", path, err)
		}

		return writeAndVerifyMemoryLimit(path, limitBytes)
	}

	return fmt.Errorf("memory limit file is unavailable: %w", errors.Join(unavailable...))
}

func writeAndVerifyMemoryLimit(path string, limitBytes int64) error {
	expected := strconv.FormatInt(limitBytes, 10)
	if err := os.WriteFile(path, []byte(expected), 0); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	actual := strings.TrimSpace(string(contents))
	if actual != expected {
		return fmt.Errorf("readback mismatch for %s: got %q, want %q", path, actual, expected)
	}
	return nil
}
