package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetVisibleMemoryLimitWritesFirstAvailablePath(t *testing.T) {
	limitPath := filepath.Join(t.TempDir(), "memory.limit_in_bytes")
	require.NoError(t, os.WriteFile(limitPath, []byte("max\n"), 0o600))

	err := setVisibleMemoryLimit([]string{
		filepath.Join(t.TempDir(), "memory.max"),
		limitPath,
	}, 16*1024*1024*1024)

	require.NoError(t, err)
	contents, err := os.ReadFile(limitPath)
	require.NoError(t, err)
	require.Equal(t, "17179869184", string(contents))
}

func TestSetVisibleMemoryLimitRejectsInvalidLimit(t *testing.T) {
	err := setVisibleMemoryLimit([]string{filepath.Join(t.TempDir(), "memory.max")}, 0)
	require.ErrorContains(t, err, "must be positive")
}

func TestSetVisibleMemoryLimitRequiresCgroupFile(t *testing.T) {
	err := setVisibleMemoryLimit([]string{filepath.Join(t.TempDir(), "memory.max")}, 1024)
	require.ErrorContains(t, err, "memory limit file is unavailable")
}

func TestWriteAndVerifyMemoryLimitRejectsDirectory(t *testing.T) {
	err := writeAndVerifyMemoryLimit(t.TempDir(), 1024)
	require.ErrorContains(t, err, "write")
}
