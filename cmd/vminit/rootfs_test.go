//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestClearOverlayOriginsPreservesRootContents(t *testing.T) {
	upper, outside := t.TempDir(), t.TempDir()
	file := filepath.Join(upper, "installed-package")
	outsideFile := filepath.Join(outside, "outside")
	for _, path := range []string{file, outsideFile} {
		require.NoError(t, os.WriteFile(path, []byte("package data"), 0600))
		err := unix.Lsetxattr(path, "trusted.overlay.origin", []byte("old handle"), 0)
		if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skip("requires trusted xattrs and CAP_SYS_ADMIN")
		}
		require.NoError(t, err)
	}
	require.NoError(t, unix.Lsetxattr(upper, "trusted.overlay.origin", []byte("old directory handle"), 0))
	require.NoError(t, unix.Lsetxattr(upper, "trusted.overlay.opaque", []byte("y"), 0))
	require.NoError(t, unix.Lsetxattr(file, "user.persistence", []byte("customer metadata"), 0))
	require.NoError(t, os.Symlink(outsideFile, filepath.Join(upper, "link")))

	for range 2 {
		require.NoError(t, clearOverlayOrigins(upper))
	}
	for _, path := range []string{upper, file} {
		_, err := unix.Lgetxattr(path, "trusted.overlay.origin", nil)
		require.ErrorIs(t, err, unix.ENODATA)
	}
	for _, test := range []struct{ path, attr, value string }{
		{upper, "trusted.overlay.opaque", "y"},
		{file, "user.persistence", "customer metadata"},
		{outsideFile, "trusted.overlay.origin", "old handle"},
	} {
		buf := make([]byte, 128)
		n, err := unix.Lgetxattr(test.path, test.attr, buf)
		require.NoError(t, err)
		require.Equal(t, test.value, string(buf[:n]))
	}
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "package data", string(data))
}
