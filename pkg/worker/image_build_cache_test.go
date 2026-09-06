package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func writeBuildahStoreJSON(t *testing.T, graphroot, kind, name string, v any) {
	t.Helper()
	dir := filepath.Join(graphroot, "overlay-"+kind)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	data, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o644))
}

func TestBuildLayerStoreBytesSumsRecordedLayerSizes(t *testing.T) {
	graphroot := t.TempDir()

	// An empty store (files not written yet) is zero, not an error.
	size, err := buildLayerStoreBytes(graphroot, "overlay")
	require.NoError(t, err)
	require.Zero(t, size)

	writeBuildahStoreJSON(t, graphroot, "layers", "layers.json", []buildahStoredLayer{
		{ID: "a", DiffSize: 100},
		{ID: "b", Parent: "a", DiffSize: 250},
		{ID: "c", DiffSize: -1}, // size unknown: not counted, not an error
	})
	size, err = buildLayerStoreBytes(graphroot, "overlay")
	require.NoError(t, err)
	require.Equal(t, int64(350), size)
}

// TestOldestRemovableBuildahImagesOrdersByLocalAddTime mirrors the prod store:
// a base image whose upstream created date is 2023 was pulled last week, while
// a build image created yesterday has been here longer. The pull time (its top
// layer's created) decides, so the popular base is not the first to go.
func TestOldestRemovableBuildahImagesOrdersByLocalAddTime(t *testing.T) {
	graphroot := t.TempDir()
	now := time.Now().UTC()
	writeBuildahStoreJSON(t, graphroot, "layers", "layers.json", []buildahStoredLayer{
		{ID: "base-top", Created: now.Add(-7 * 24 * time.Hour), DiffSize: 10},
		{ID: "old-build-top", Created: now.Add(-30 * 24 * time.Hour), DiffSize: 10},
		{ID: "new-build-top", Created: now.Add(-time.Hour), DiffSize: 10},
		{ID: "busy-top", Created: now.Add(-60 * 24 * time.Hour), DiffSize: 10},
	})
	writeBuildahStoreJSON(t, graphroot, "images", "images.json", []buildahStoredImage{
		{ID: "base", Layer: "base-top", Created: time.Date(2023, 11, 10, 0, 0, 0, 0, time.UTC), Names: []string{"docker.io/library/python:3.12"}},
		{ID: "old-build", Layer: "old-build-top", Created: now.Add(-24 * time.Hour)},
		{ID: "new-build", Layer: "new-build-top", Created: now.Add(-time.Hour)},
		{ID: "busy", Layer: "busy-top", Created: now.Add(-60 * 24 * time.Hour)},
		{ID: "no-layer-record", Layer: "missing", Created: now.Add(-14 * 24 * time.Hour)},
	})
	writeBuildahStoreJSON(t, graphroot, "containers", "containers.json", []buildahStoredContainer{
		{ID: "working", ImageID: "busy"},
	})

	ids, err := oldestRemovableBuildahImages(graphroot, "overlay", nil, 10)
	require.NoError(t, err)
	// busy is excluded (a working container is built from it); the rest are
	// oldest-added first, falling back to the image's own created when the
	// top layer has no record.
	require.Equal(t, []string{"old-build", "no-layer-record", "base", "new-build"}, ids)

	// limit and skip narrow the batch.
	ids, err = oldestRemovableBuildahImages(graphroot, "overlay", map[string]struct{}{"old-build": {}}, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"no-layer-record", "base"}, ids)
}

func TestBuildLayerCacheMaxBytesDisabledByZeroOrWholeDisk(t *testing.T) {
	c := &ImageClient{}
	c.config.ImageService.BuildLayerCacheMaxPct = 0
	require.Zero(t, c.buildLayerCacheMaxBytes(t.TempDir()))
	c.config.ImageService.BuildLayerCacheMaxPct = 1
	require.Zero(t, c.buildLayerCacheMaxBytes(t.TempDir()))

	c.config.ImageService.BuildLayerCacheMaxPct = 0.2
	usage, err := fastDiskUsage(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, int64(0.2*float64(usage.TotalBytes)), c.buildLayerCacheMaxBytes(t.TempDir()))
}

func TestBuildLayerCacheMaxPctDefault(t *testing.T) {
	t.Setenv("CONFIG_PATH", "")
	t.Setenv("CONFIG_JSON", "")
	cm, err := common.NewConfigManager[types.AppConfig]()
	require.NoError(t, err)
	require.Equal(t, 0.20, cm.GetConfig().ImageService.BuildLayerCacheMaxPct)
}
