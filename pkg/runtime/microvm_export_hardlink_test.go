//go:build linux

package runtime

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractTreePreservesLongHardLinkMetadata(t *testing.T) {
	first := strings.Repeat("a", 90) + "/" + strings.Repeat("b", 90) + "/file"
	alias := "z-" + strings.Repeat("c", 110)
	uid, gid := os.Geteuid(), os.Getegid()
	if uid == 0 {
		uid, gid = 1234, 2345
	}
	dst := t.TempDir()
	// guestTar intentionally replaces ownership for unprivileged older tests.
	// Build these exact headers directly to exercise non-root archive ownership.
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, entry := range []treeEntry{
		{hdr: tar.Header{Name: first, Typeflag: tar.TypeReg, Mode: 0640, Uid: uid, Gid: gid, Format: tar.FormatPAX}, body: "binary\x00\xff"},
		{hdr: tar.Header{Name: "symbolic", Typeflag: tar.TypeSymlink, Linkname: first, Uid: uid, Gid: gid}},
		{hdr: tar.Header{Name: alias, Typeflag: tar.TypeLink, Linkname: first, Format: tar.FormatPAX}},
	} {
		hdr := entry.hdr
		hdr.Size, hdr.ModTime = int64(len(entry.body)), treeModTime
		require.NoError(t, writer.WriteHeader(&hdr))
		_, err := writer.Write([]byte(entry.body))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	require.NoError(t, extractTree(&archive, dst))
	left, err := os.Stat(filepath.Join(dst, first))
	require.NoError(t, err)
	right, err := os.Stat(filepath.Join(dst, alias))
	require.NoError(t, err)
	require.True(t, os.SameFile(left, right))
	require.Equal(t, uint64(2), uint64(right.Sys().(*syscall.Stat_t).Nlink))
	require.Equal(t, os.FileMode(0640), right.Mode().Perm())
	require.Equal(t, uint32(uid), right.Sys().(*syscall.Stat_t).Uid)
	require.Equal(t, uint32(gid), right.Sys().(*syscall.Stat_t).Gid)
	data, err := os.ReadFile(filepath.Join(dst, alias))
	require.NoError(t, err)
	require.Equal(t, []byte("binary\x00\xff"), data)
	target, err := os.Readlink(filepath.Join(dst, "symbolic"))
	require.NoError(t, err)
	require.Equal(t, first, target)
}
