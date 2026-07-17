package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainerOverlayCleanupLayersPreservesBundleState(t *testing.T) {
	overlayPath := t.TempDir()
	containerID := "container-reset"
	bundlePath := filepath.Join(overlayPath, containerID)
	signalPath := filepath.Join(bundlePath, "criu", "CONTAINER_ID")

	require.NoError(t, os.MkdirAll(filepath.Dir(signalPath), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(bundlePath, "config.json"), []byte("{}"), 0644))
	require.NoError(t, os.WriteFile(signalPath, []byte(containerID), 0644))

	overlay := &ContainerOverlay{containerId: containerID, overlayPath: overlayPath}
	require.NoError(t, overlay.cleanupLayers())
	require.FileExists(t, filepath.Join(bundlePath, "config.json"))
	require.FileExists(t, signalPath)

	require.NoError(t, overlay.Cleanup())
	require.NoDirExists(t, bundlePath)
}
