package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// The buildah graphroot under the persistent build cache keeps every layer a
// build on the node ever produced or pulled; buildah itself expires nothing.
// Left alone it grows until the node's disk is full (prod build nodes reached
// 160-210 GB), starving the content cache on the same volume. After each build
// the store is trimmed to its share of the filesystem, oldest images first.
// buildah refuses to remove an image whose layers a working container or a
// newer image still builds on, so a build running alongside the trim is never
// pulled from under; those images are simply skipped.

// buildLayerCacheTrimHysteresis is the fraction of the cap the trim aims for,
// so a store just over the cap is not trimmed again after every build.
const buildLayerCacheTrimHysteresis = 0.9

// buildLayerCacheTrimBatch is how many images one buildah rmi removes before
// the store is re-measured.
const buildLayerCacheTrimBatch = 16

const buildLayerCacheTrimTimeout = 30 * time.Minute

// buildLayerCacheTrimMu serializes trims: a second build finishing while one
// trim runs does not start another.
var buildLayerCacheTrimMu sync.Mutex

// buildahStoredLayer is a containers/storage layer record as written to
// overlay-layers/layers.json (vfs-layers for the vfs driver).
type buildahStoredLayer struct {
	ID       string    `json:"id"`
	Parent   string    `json:"parent"`
	Created  time.Time `json:"created"`
	DiffSize int64     `json:"diff-size"`
}

// buildahStoredImage is an image record from overlay-images/images.json.
type buildahStoredImage struct {
	ID      string    `json:"id"`
	Layer   string    `json:"layer"`
	Names   []string  `json:"names"`
	Created time.Time `json:"created"`
}

// buildahStoredContainer is a working-container record from
// overlay-containers/containers.json.
type buildahStoredContainer struct {
	ID      string `json:"id"`
	ImageID string `json:"image"`
}

// buildLayerCacheMaxBytes is the store's cap on the filesystem holding
// graphroot, or 0 when the cap is disabled or the filesystem cannot be sized.
func (c *ImageClient) buildLayerCacheMaxBytes(graphroot string) int64 {
	pct := c.config.ImageService.BuildLayerCacheMaxPct
	if pct <= 0 || pct >= 1 {
		return 0
	}
	usage, err := fastDiskUsage(graphroot)
	if err != nil || usage.TotalBytes == 0 {
		return 0
	}
	return int64(pct * float64(usage.TotalBytes))
}

// trimBuildLayerCacheInBackground trims the persistent store once the build
// that used it has returned. At most one trim runs at a time.
func (c *ImageClient) trimBuildLayerCacheInBackground(graphroot, driver string) {
	if !buildLayerCacheTrimMu.TryLock() {
		return
	}
	go func() {
		defer buildLayerCacheTrimMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), buildLayerCacheTrimTimeout)
		defer cancel()
		c.trimBuildLayerCache(ctx, graphroot, driver)
	}()
}

func (c *ImageClient) trimBuildLayerCache(ctx context.Context, graphroot, driver string) {
	maxBytes := c.buildLayerCacheMaxBytes(graphroot)
	if maxBytes <= 0 {
		return
	}
	size, err := buildLayerStoreBytes(graphroot, driver)
	if err != nil {
		log.Warn().Err(err).Str("graphroot", graphroot).Msg("build layer cache trim skipped: cannot size the store")
		return
	}
	if size <= maxBytes {
		return
	}
	target := int64(float64(maxBytes) * buildLayerCacheTrimHysteresis)

	runroot := mustMkdirTempBuildahDir("buildah-trim-run-")
	defer os.RemoveAll(runroot)
	tmpdir := mustMkdirTempBuildahDir("buildah-trim-tmp-")
	defer os.RemoveAll(tmpdir)
	storageConf, err := c.writeStorageConf(graphroot, runroot, driver)
	if err != nil {
		log.Warn().Err(err).Msg("build layer cache trim skipped: cannot write storage config")
		return
	}
	defer os.Remove(storageConf)
	store := &buildahStore{graphroot: graphroot, runroot: runroot, tmpdir: tmpdir, driver: driver, conf: storageConf}
	env := c.buildahEnv(runroot, tmpdir, storageConf)

	started := time.Now()
	startBytes := size
	removed, failed := 0, 0
	// Every pass re-reads the store so removals that failed or freed less than
	// expected (shared layers) do not end the trim early; an image that failed
	// once is not retried within this trim.
	skip := map[string]struct{}{}
	for size > target {
		if ctx.Err() != nil {
			break
		}
		batch, err := oldestRemovableBuildahImages(graphroot, driver, skip, buildLayerCacheTrimBatch)
		if err != nil {
			log.Warn().Err(err).Msg("build layer cache trim stopped: cannot list images")
			break
		}
		if len(batch) == 0 {
			break
		}
		for _, id := range batch {
			skip[id] = struct{}{}
		}
		var out strings.Builder
		args := append(store.args("rmi"), batch...)
		if err := newBuildahCommand(ctx, args, env, &out, &out).Run(); err != nil {
			// rmi keeps going past images it cannot remove and exits non-zero;
			// the re-measure below shows what it did remove.
			failed++
			log.Debug().Err(err).Str("output", strings.TrimSpace(out.String())).Msg("build layer cache trim: some images not removed")
		}
		removed += len(batch)
		if size, err = buildLayerStoreBytes(graphroot, driver); err != nil {
			log.Warn().Err(err).Msg("build layer cache trim stopped: cannot re-size the store")
			break
		}
	}

	event := log.Info()
	if size > target {
		event = log.Warn()
	}
	event.
		Int64("start_bytes", startBytes).
		Int64("end_bytes", size).
		Int64("max_bytes", maxBytes).
		Int64("target_bytes", target).
		Int("images_attempted", removed).
		Int("batches_with_failures", failed).
		Dur("duration", time.Since(started)).
		Msg("trimmed build layer cache")
}

// buildLayerStoreBytes sums the recorded diff size of every layer in the
// store. It is read from the store's own layer index, not a walk of the
// overlay tree, which on a store this size would take minutes of IOPS.
func buildLayerStoreBytes(graphroot, driver string) (int64, error) {
	layers, err := readBuildahStoredLayers(graphroot, driver)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, layer := range layers {
		if layer.DiffSize > 0 {
			total += layer.DiffSize
		}
	}
	return total, nil
}

// oldestRemovableBuildahImages returns up to limit image ids, oldest first by
// when their top layer was added to this store (a pulled image's upstream
// creation date says nothing about when this node fetched it). Images a
// working container is built from, and ids in skip, are left out.
func oldestRemovableBuildahImages(graphroot, driver string, skip map[string]struct{}, limit int) ([]string, error) {
	layers, err := readBuildahStoredLayers(graphroot, driver)
	if err != nil {
		return nil, err
	}
	layerCreated := make(map[string]time.Time, len(layers))
	for _, layer := range layers {
		layerCreated[layer.ID] = layer.Created
	}

	var images []buildahStoredImage
	if err := readBuildahStoreJSON(graphroot, driver, "images", "images.json", &images); err != nil {
		return nil, err
	}
	inUse := map[string]struct{}{}
	var containers []buildahStoredContainer
	if err := readBuildahStoreJSON(graphroot, driver, "containers", "containers.json", &containers); err == nil {
		for _, container := range containers {
			inUse[container.ImageID] = struct{}{}
		}
	}

	type aged struct {
		id    string
		added time.Time
	}
	candidates := make([]aged, 0, len(images))
	for _, image := range images {
		if _, ok := skip[image.ID]; ok {
			continue
		}
		if _, ok := inUse[image.ID]; ok {
			continue
		}
		added, ok := layerCreated[image.Layer]
		if !ok || added.IsZero() {
			added = image.Created
		}
		candidates = append(candidates, aged{id: image.ID, added: added})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].added.Equal(candidates[j].added) {
			return candidates[i].added.Before(candidates[j].added)
		}
		return candidates[i].id < candidates[j].id
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.id)
	}
	return ids, nil
}

func readBuildahStoredLayers(graphroot, driver string) ([]buildahStoredLayer, error) {
	var layers []buildahStoredLayer
	if err := readBuildahStoreJSON(graphroot, driver, "layers", "layers.json", &layers); err != nil {
		return nil, err
	}
	return layers, nil
}

// readBuildahStoreJSON decodes graphroot/<driver>-<kind>/<name>. A store that
// has not written the file yet is empty, not an error.
func readBuildahStoreJSON(graphroot, driver, kind, name string, out any) error {
	path := filepath.Join(graphroot, driver+"-"+kind, name)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}
