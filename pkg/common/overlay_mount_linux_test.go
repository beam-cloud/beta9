//go:build linux

package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNativeOverlayPreservesPersistentUpperAndWhiteouts(t *testing.T) {
	if os.Getenv("BETA9_OVERLAY_REAL_MOUNTS") != "1" {
		t.Skip("requires an isolated privileged mount namespace")
	}
	t.Setenv("BETA9_NATIVE_OVERLAY_MOUNT", "1")
	lower, state, scratch := t.TempDir(), t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(lower, "deleted"), []byte("base"), 0644))
	upper, work := filepath.Join(state, "upper"), filepath.Join(state, "work")
	overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: "native-overlay-test"}, lower, scratch)
	t.Cleanup(func() { overlay.Cleanup() })
	require.NoError(t, overlay.SetupWithWritable(upper, work))
	require.NoError(t, os.Remove(filepath.Join(overlay.TopLayerPath(), "deleted")))
	require.NoError(t, os.WriteFile(filepath.Join(overlay.TopLayerPath(), "written"), []byte("durable"), 0644))
	require.NoError(t, overlay.Reset())
	require.NoFileExists(t, filepath.Join(overlay.TopLayerPath(), "deleted"))
	data, err := os.ReadFile(filepath.Join(overlay.TopLayerPath(), "written"))
	require.NoError(t, err)
	require.Equal(t, "durable", string(data))
	require.NoError(t, overlay.Cleanup())
	require.FileExists(t, filepath.Join(upper, "written"))
}
