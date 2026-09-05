package worker

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// readLayerHeaders returns the tar headers of a packed layer by name, with
// PAX records as written.
func readLayerHeaders(t *testing.T, path string) map[string]*tar.Header {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	headers := map[string]*tar.Header{}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		headers[hdr.Name] = hdr
	}
	return headers
}

func TestPackOverlayLayersHoldsTheLayerCap(t *testing.T) {
	// Files just over half the target never share a layer, so greedy filling
	// alone would make one layer per file: 40 here against a cap of 16.
	upper := t.TempDir()
	for i := 0; i < 40; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(upper, fmt.Sprintf("f%02d", i)), make([]byte, 600), 0o644))
	}
	layers, err := packOverlayLayersWithTarget(upper, t.TempDir(), ggcrtypes.DockerLayer, 1000)
	require.NoError(t, err)
	require.LessOrEqual(t, len(layers), layerSplitMaxLayers)
	require.Greater(t, len(layers), 1, "the delta is still split")

	seen := map[string]int{}
	for _, layer := range layers {
		for name := range readLayerHeaders(t, layer.path) {
			seen[name]++
		}
	}
	require.Len(t, seen, 40)
	for name, count := range seen {
		require.Equal(t, 1, count, name)
	}
}

func TestPackOverlayLayersKeepsFileXattrsButNotOverlayOnes(t *testing.T) {
	upper := t.TempDir()
	file := filepath.Join(upper, "bin")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o755))
	if err := unix.Lsetxattr(file, "user.beta9.test", []byte("kept"), 0); err != nil {
		t.Skipf("xattrs unsupported here: %v", err)
	}
	// Overlay bookkeeping needs privileges to set on Linux; when it can be
	// set it must be left out of the image.
	overlayAttrSet := unix.Lsetxattr(file, "trusted.overlay.origin", []byte("gone"), 0) == nil

	layers, err := packOverlayLayers(upper, t.TempDir(), ggcrtypes.DockerLayer)
	require.NoError(t, err)
	require.Len(t, layers, 1)
	hdr := readLayerHeaders(t, layers[0].path)["bin"]
	require.NotNil(t, hdr)
	require.Equal(t, "kept", hdr.PAXRecords["SCHILY.xattr.user.beta9.test"])
	if overlayAttrSet {
		require.NotContains(t, hdr.PAXRecords, "SCHILY.xattr.trusted.overlay.origin")
	}
	require.NotContains(t, hdr.PAXRecords, "SCHILY.xattr.trusted.overlay.opaque")
}

func TestSkippedXattr(t *testing.T) {
	require.True(t, skippedXattr("trusted.overlay.opaque"))
	require.True(t, skippedXattr("user.overlay.redirect"))
	require.True(t, skippedXattr("security.selinux"))
	require.False(t, skippedXattr("security.capability"))
	require.False(t, skippedXattr("user.overlayish"))
	require.False(t, skippedXattr("system.posix_acl_access"))
}
