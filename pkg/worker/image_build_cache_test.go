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
		{ID: "old-build", Layer: "old-build-top", Created: now.Add(-24 * time.Hour), Names: []string{"registry.localhost:5000/beta9-users:old-build"}},
		{ID: "new-build", Layer: "new-build-top", Created: now.Add(-time.Hour)},
		{ID: "busy", Layer: "busy-top", Created: now.Add(-60 * 24 * time.Hour)},
		{ID: "no-layer-record", Layer: "missing", Created: now.Add(-14 * 24 * time.Hour)},
	})
	writeBuildahStoreJSON(t, graphroot, "containers", "containers.json", []buildahStoredContainer{
		{ID: "working", ImageID: "busy"},
	})

	all := trimExclusions{minAdded: now}
	ids, err := oldestRemovableBuildahImages(graphroot, "overlay", all, 10)
	require.NoError(t, err)
	// busy is excluded (a working container is built from it); the rest are
	// oldest-added first, falling back to the image's own created when the
	// top layer has no record.
	require.Equal(t, []string{"old-build", "no-layer-record", "base", "new-build"}, ids)

	// limit and skip narrow the batch.
	ids, err = oldestRemovableBuildahImages(graphroot, "overlay", trimExclusions{skip: map[string]struct{}{"old-build": {}}, minAdded: now}, 2)
	require.NoError(t, err)
	require.Equal(t, []string{"no-layer-record", "base"}, ids)

	// Anything added within the minimum age stays: a base a starting build
	// just pulled, an image a finishing build committed but has not pushed.
	ids, err = oldestRemovableBuildahImages(graphroot, "overlay", trimExclusions{minAdded: now.Add(-buildLayerCacheMinAge)}, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"old-build", "no-layer-record", "base"}, ids)

	// Images named for a build in flight stay: the build's own image by id,
	// its base by repo:tag regardless of registry prefix.
	ids, err = oldestRemovableBuildahImages(graphroot, "overlay", trimExclusions{minAdded: now, inFlight: []inFlightBuild{{imageID: "old-build", baseImage: "python:3.12"}}}, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"no-layer-record", "new-build"}, ids)
}

func TestOldestRemovableBuildahImagesRefusesUnknownLiveness(t *testing.T) {
	graphroot := t.TempDir()
	writeBuildahStoreJSON(t, graphroot, "images", "images.json", []buildahStoredImage{{ID: "img", Layer: "l"}})
	dir := filepath.Join(graphroot, "overlay-containers")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "containers.json"), []byte("{not json"), 0o644))

	_, err := oldestRemovableBuildahImages(graphroot, "overlay", trimExclusions{minAdded: time.Now()}, 10)
	require.Error(t, err, "a corrupt container index must stop the trim, not read as no containers")
}

func TestImageNamedForBuilds(t *testing.T) {
	named := func(names ...string) buildahStoredImage { return buildahStoredImage{Names: names} }
	img := named(
		"187248174200.dkr.ecr.us-east-1.amazonaws.com/prod/beta9-users:6ec9ea3006729fb8",
		"docker.io/library/python:3.12",
		"public.ecr.aws/n4e0e1y0/beta9-runner@sha256:9ad4225de28a135f30a83dd7711d38bce0c3ead4b2167c3cc9fd485b07a00af5",
	)
	build := func(id, base string) []inFlightBuild { return []inFlightBuild{{imageID: id, baseImage: base}} }

	// The build's own image, by id.
	require.True(t, imageNamedForBuilds(img, build("6ec9ea3006729fb8", "")), "build id as the tag")
	require.True(t, imageNamedForBuilds(named("6ec9ea3006729fb8:latest"), build("6ec9ea3006729fb8", "")), "build id as the repo (bud path)")
	require.False(t, imageNamedForBuilds(img, build("6ec9ea30", "")), "a prefix of the id is not the id")
	require.False(t, imageNamedForBuilds(named("6ec9ea3006729fb8-x:latest", "x:6ec9ea3006729fb8x"), build("6ec9ea3006729fb8", "")), "the id must be the whole repo or tag")

	// The build's base image, by repo:tag with :latest implicit.
	require.True(t, imageNamedForBuilds(img, build("", "python:3.12")), "bare repo:tag matches the fully qualified name")
	require.True(t, imageNamedForBuilds(img, build("", "docker.io/library/python:3.12")))
	require.True(t, imageNamedForBuilds(img, build("", "beta9-runner@sha256:9ad4225de28a135f30a83dd7711d38bce0c3ead4b2167c3cc9fd485b07a00af5")))
	require.True(t, imageNamedForBuilds(named("docker.io/library/python:latest"), build("", "python")))
	require.True(t, imageNamedForBuilds(named("docker.io/library/python:latest"), build("", "docker.io/library/python")))
	require.False(t, imageNamedForBuilds(img, build("", "python")), "python is python:latest, not python:3.12")
	require.False(t, imageNamedForBuilds(named("other:python"), build("", "python")), "a base never matches as a tag")
	require.False(t, imageNamedForBuilds(img, build("", "python:3.11")))
	require.False(t, imageNamedForBuilds(img, build("other-build", "beta9")))
	require.False(t, imageNamedForBuilds(buildahStoredImage{}, build("x", "python:3.12")), "intermediate images have no names")
	require.False(t, imageNamedForBuilds(img, nil))
}

func TestBuildLayerCacheTrimmerProtectsInFlightBuilds(t *testing.T) {
	tr := &buildLayerCacheTrimmer{inFlight: map[string]inFlightBuild{}}
	require.Empty(t, tr.inFlightBuilds())
	done := tr.protectBuildImages("build-1", "python:3.12")
	require.Equal(t, []inFlightBuild{{imageID: "build-1", baseImage: "python:3.12"}}, tr.inFlightBuilds())
	done()
	require.Empty(t, tr.inFlightBuilds())
}

func TestTrimBuildLayerCacheInBackgroundCoalesces(t *testing.T) {
	c := &ImageClient{} // cap disabled: each trim returns at once
	tr := buildLayerCacheTrims
	tr.mu.Lock()
	tr.running, tr.pending = true, false // hold the trimmer as if a trim were underway
	tr.mu.Unlock()

	c.trimBuildLayerCacheInBackground(t.TempDir(), "overlay")
	tr.mu.Lock()
	require.True(t, tr.pending, "a request during a trim is remembered, not dropped")
	tr.running = false // let go of the fake trim
	tr.mu.Unlock()

	// The next request runs, and consumes the pending mark.
	c.trimBuildLayerCacheInBackground(t.TempDir(), "overlay")
	require.Eventually(t, func() bool {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		return !tr.running && !tr.pending
	}, 5*time.Second, 10*time.Millisecond)
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
