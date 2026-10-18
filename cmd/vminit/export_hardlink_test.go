//go:build linux

package main

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func exportedTree(t *testing.T, root string, exclude []string) (map[string]*tar.Header, map[string][]byte) {
	t.Helper()
	var archive bytes.Buffer
	require.NoError(t, writeTree(&archive, root, exclude))
	headers, payloads := map[string]*tar.Header{}, map[string][]byte{}
	reader := tar.NewReader(&archive)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		copy := *header
		headers[header.Name] = &copy
		payloads[header.Name], err = io.ReadAll(reader)
		require.NoError(t, err)
	}
	return headers, payloads
}

func TestWriteTreePreservesRegularHardLinks(t *testing.T) {
	for _, payload := range [][]byte{nil, bytes.Repeat([]byte{0, 255, 12, 34}, 1025)} {
		t.Run(map[bool]string{true: "empty", false: "binary"}[len(payload) == 0], func(t *testing.T) {
			root := t.TempDir()
			// Both the name and target exceed USTAR's 100-byte link field.
			first := strings.Repeat("a", 90) + "/" + strings.Repeat("b", 90) + "/file"
			alias := "z-" + strings.Repeat("c", 110)
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, first)), 0755))
			require.NoError(t, os.WriteFile(filepath.Join(root, first), payload, 0640))
			if os.Geteuid() == 0 {
				require.NoError(t, os.Chown(filepath.Join(root, first), 1234, 2345))
			}
			require.NoError(t, os.Link(filepath.Join(root, first), filepath.Join(root, alias)))
			// A symlink sorts before the carrier and must never enter the inode map.
			require.NoError(t, os.Symlink(first, filepath.Join(root, "0-symbolic")))
			info, err := os.Stat(filepath.Join(root, first))
			require.NoError(t, err)
			owner := info.Sys().(*syscall.Stat_t)
			headers, bodies := exportedTree(t, root, nil)
			require.Equal(t, byte(tar.TypeReg), headers[first].Typeflag)
			require.True(t, bytes.Equal(payload, bodies[first]))
			require.Equal(t, byte(tar.TypeLink), headers[alias].Typeflag)
			require.Equal(t, first, headers[alias].Linkname)
			require.Zero(t, headers[alias].Size)
			require.Empty(t, bodies[alias])
			for _, name := range []string{first, alias} {
				require.Equal(t, int64(0640), headers[name].Mode)
				require.Equal(t, int(owner.Uid), headers[name].Uid)
				require.Equal(t, int(owner.Gid), headers[name].Gid)
			}
			require.Equal(t, byte(tar.TypeSymlink), headers["0-symbolic"].Typeflag)
			require.Equal(t, first, headers["0-symbolic"].Linkname)
		})
	}
}

func TestWriteTreeExcludedCarrierDoesNotCreateDanglingHardLink(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "a-excluded"), []byte("retained"), 0600))
	require.NoError(t, os.Link(filepath.Join(root, "a-excluded"), filepath.Join(root, "z-retained")))
	headers, bodies := exportedTree(t, root, []string{"a-excluded"})
	require.NotContains(t, headers, "a-excluded")
	require.Equal(t, byte(tar.TypeReg), headers["z-retained"].Typeflag)
	require.Equal(t, []byte("retained"), bodies["z-retained"])
}
