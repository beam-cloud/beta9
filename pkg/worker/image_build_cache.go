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
//
// A trim runs alongside other builds, so it must not remove what they need:
//   - anything added to the store within buildLayerCacheMinAge is left alone,
//     which covers a base image a starting build has just pulled and an image
//     a finishing build has committed but not yet pushed;
//   - images named for a build in flight on this worker (its id, its base
//     image) are left alone whatever their age;
//   - images a working container is built from are left alone;
//   - buildah itself refuses to remove a layer another image or container
//     still builds on, so a stale view of the store fails safe.
//
// bud's stage containers are volatile, so they and their read-write layers
// are indexed in volatile-containers.json and volatile-layers.json, not the
// non-volatile files. A build killed mid-way (the gateway's build timeout,
// a worker exit) leaves them behind: their layers hold gigabytes the layer
// index does not size, and they pin their images so rmi refuses them. Every
// trim removes working containers older than any build can run.

// buildLayerCacheTrimHysteresis is the fraction of the cap the trim aims for,
// so a store just over the cap is not trimmed again after every build.
const buildLayerCacheTrimHysteresis = 0.9

// buildLayerCacheMinAge: images added to the store more recently than this
// are never removed, so a build in progress cannot lose what it just pulled
// or committed.
const buildLayerCacheMinAge = time.Hour

// buildLayerCacheTrimBatch is how many images one buildah rmi removes before
// the store is re-measured.
const buildLayerCacheTrimBatch = 16

// buildLayerCacheStaleContainerAge: a working container older than this
// belongs to a build that is gone. Dockerfile builds are stopped by the
// gateway after an hour (dockerfileContainerSpinupTimeout); the other bud
// paths run for minutes.
const buildLayerCacheStaleContainerAge = 3 * time.Hour

const buildLayerCacheTrimTimeout = 30 * time.Minute

// buildLayerCacheTrimmer runs one trim at a time. A build finishing while a
// trim is underway marks it pending, and the running trim goes again.
type buildLayerCacheTrimmer struct {
	mu      sync.Mutex
	running bool
	pending bool

	// inFlight holds the builds this worker is running, keyed by build; a
	// trim never removes an image named for one of them.
	inFlight map[string]inFlightBuild
}

// inFlightBuild is what a running build names in the store: the image it
// commits under its id, and the base image it starts from.
type inFlightBuild struct {
	imageID   string
	baseImage string
}

var buildLayerCacheTrims = &buildLayerCacheTrimmer{inFlight: map[string]inFlightBuild{}}

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
// overlay-containers/containers.json or volatile-containers.json.
type buildahStoredContainer struct {
	ID      string    `json:"id"`
	ImageID string    `json:"image"`
	Created time.Time `json:"created"`
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

// protectBuildImages records a build as in flight until the returned func
// runs.
func (t *buildLayerCacheTrimmer) protectBuildImages(imageID, baseImage string) func() {
	t.mu.Lock()
	t.inFlight[imageID] = inFlightBuild{imageID: imageID, baseImage: baseImage}
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		delete(t.inFlight, imageID)
		t.mu.Unlock()
	}
}

func (t *buildLayerCacheTrimmer) inFlightBuilds() []inFlightBuild {
	t.mu.Lock()
	defer t.mu.Unlock()
	builds := make([]inFlightBuild, 0, len(t.inFlight))
	for _, b := range t.inFlight {
		builds = append(builds, b)
	}
	return builds
}

// trimBuildLayerCacheInBackground trims the persistent store once the build
// that used it has returned. One trim runs at a time; a request during a trim
// makes it run once more when done.
func (c *ImageClient) trimBuildLayerCacheInBackground(graphroot, driver string) {
	t := buildLayerCacheTrims
	t.mu.Lock()
	if t.running {
		t.pending = true
		t.mu.Unlock()
		return
	}
	t.running = true
	t.mu.Unlock()

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), buildLayerCacheTrimTimeout)
			c.trimBuildLayerCache(ctx, graphroot, driver)
			cancel()

			t.mu.Lock()
			if !t.pending {
				t.running = false
				t.mu.Unlock()
				return
			}
			t.pending = false
			t.mu.Unlock()
		}
	}()
}

func (c *ImageClient) trimBuildLayerCache(ctx context.Context, graphroot, driver string) {
	maxBytes := c.buildLayerCacheMaxBytes(graphroot)
	if maxBytes <= 0 {
		return
	}

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

	// Leaked containers first: they hold unsized layers and pin images.
	if stale, err := staleBuildahContainers(graphroot, driver, time.Now().Add(-buildLayerCacheStaleContainerAge), buildLayerCacheTrims.inFlightBuilds()); err != nil {
		log.Warn().Err(err).Msg("build layer cache trim: cannot list working containers")
	} else if len(stale) > 0 {
		var out strings.Builder
		args := append(store.args("rm"), stale...)
		err := newBuildahCommand(ctx, args, env, &out, &out).Run()
		log.Info().Err(err).Int("containers", len(stale)).Str("output", strings.TrimSpace(out.String())).
			Msg("removed stale build containers")
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

	started := time.Now()
	startBytes := size
	attempted, removed, stuck := 0, 0, 0
	// Each pass lists the oldest removable images afresh and re-measures the
	// store afterwards, so removals that freed less than expected (shared
	// layers) do not end the trim early. An image buildah refused (a newer
	// image builds on it) is retried while passes still make progress, since
	// removing that newer image may have freed it; it is set aside only once a
	// whole pass removes nothing.
	skip := map[string]struct{}{}
	for size > target && ctx.Err() == nil {
		batch, err := oldestRemovableBuildahImages(graphroot, driver, trimExclusions{
			skip:     skip,
			inFlight: buildLayerCacheTrims.inFlightBuilds(),
			minAdded: time.Now().Add(-buildLayerCacheMinAge),
		}, buildLayerCacheTrimBatch)
		if err != nil {
			log.Warn().Err(err).Msg("build layer cache trim stopped: cannot list images")
			break
		}
		if len(batch) == 0 {
			break
		}
		attempted += len(batch)
		var out strings.Builder
		args := append(store.args("rmi"), batch...)
		if err := newBuildahCommand(ctx, args, env, &out, &out).Run(); err != nil {
			// rmi keeps going past images it cannot remove and exits
			// non-zero; the re-read below shows which ones went.
			log.Debug().Err(err).Str("output", strings.TrimSpace(out.String())).Msg("build layer cache trim: some images not removed")
		}

		remaining, err := buildahStoredImageIDs(graphroot, driver)
		if err != nil {
			log.Warn().Err(err).Msg("build layer cache trim stopped: cannot re-read images")
			break
		}
		progress := 0
		for _, id := range batch {
			if _, still := remaining[id]; !still {
				progress++
			}
		}
		removed += progress
		if progress == 0 {
			for _, id := range batch {
				skip[id] = struct{}{}
			}
			stuck += len(batch)
		}
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
		Int("images_attempted", attempted).
		Int("images_removed", removed).
		Int("images_stuck", stuck).
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

// trimExclusions is what a trim pass must leave in the store.
type trimExclusions struct {
	skip     map[string]struct{} // image ids set aside this trim
	inFlight []inFlightBuild     // builds running on this worker
	minAdded time.Time           // images added at or after this stay
}

// oldestRemovableBuildahImages returns up to limit image ids, oldest first by
// when their top layer was added to this store (a pulled image's upstream
// creation date says nothing about when this node fetched it). Left out:
// images a working container is built from, images named for a build in
// flight, images added since minAdded, and ids in skip.
func oldestRemovableBuildahImages(graphroot, driver string, excl trimExclusions, limit int) ([]string, error) {
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
	// An unreadable container index means unknown liveness: nothing is
	// removable rather than everything.
	containers, err := readBuildahStoredContainers(graphroot, driver)
	if err != nil {
		return nil, err
	}
	inUse := map[string]struct{}{}
	for _, container := range containers {
		inUse[container.ImageID] = struct{}{}
	}

	type aged struct {
		id    string
		added time.Time
	}
	candidates := make([]aged, 0, len(images))
	for _, image := range images {
		if _, ok := excl.skip[image.ID]; ok {
			continue
		}
		if _, ok := inUse[image.ID]; ok {
			continue
		}
		if imageNamedForBuilds(image, excl.inFlight) {
			continue
		}
		added, ok := layerCreated[image.Layer]
		if !ok || added.IsZero() {
			added = image.Created
		}
		if !added.Before(excl.minAdded) {
			continue
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

// imageNamedForBuilds reports whether the image is named for a build in
// flight: it is the image the build commits under its id (the id as the tag,
// registry/beta9-users:<id>, or as the repo, <id>:latest), or it is the
// build's base image, compared by final repo:tag or repo@digest with an
// implicit :latest made explicit, so python and docker.io/library/python
// both cover docker.io/library/python:latest and nothing else.
func imageNamedForBuilds(image buildahStoredImage, builds []inFlightBuild) bool {
	for _, build := range builds {
		var baseTail string
		if build.baseImage != "" {
			baseTail = imageRefTail(build.baseImage)
		}
		for _, name := range image.Names {
			tail := imageRefTail(name)
			if baseTail != "" && tail == baseTail {
				return true
			}
			if build.imageID != "" {
				repo, tag := splitImageRefTail(tail)
				if tag == build.imageID || (repo == build.imageID && tag == "latest") {
					return true
				}
			}
		}
	}
	return false
}

// imageRefTail is the last path element of an image reference with its tag or
// digest, repo:tag or repo@sha256:..., with an implicit :latest made explicit.
func imageRefTail(ref string) string {
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		ref = ref[i+1:]
	}
	if !strings.ContainsAny(ref, ":@") {
		ref += ":latest"
	}
	return ref
}

// splitImageRefTail splits repo:tag or repo@digest into its two parts.
func splitImageRefTail(tail string) (repo, tagOrDigest string) {
	if i := strings.Index(tail, "@"); i >= 0 {
		return tail[:i], tail[i+1:]
	}
	if i := strings.Index(tail, ":"); i >= 0 {
		return tail[:i], tail[i+1:]
	}
	return tail, ""
}

func buildahStoredImageIDs(graphroot, driver string) (map[string]struct{}, error) {
	var images []buildahStoredImage
	if err := readBuildahStoreJSON(graphroot, driver, "images", "images.json", &images); err != nil {
		return nil, err
	}
	ids := make(map[string]struct{}, len(images))
	for _, image := range images {
		ids[image.ID] = struct{}{}
	}
	return ids, nil
}

// readBuildahStoredContainers returns every working container in the store,
// volatile (bud's stage containers) and not.
func readBuildahStoredContainers(graphroot, driver string) ([]buildahStoredContainer, error) {
	var containers []buildahStoredContainer
	for _, name := range []string{"containers.json", "volatile-containers.json"} {
		var part []buildahStoredContainer
		if err := readBuildahStoreJSON(graphroot, driver, "containers", name, &part); err != nil {
			return nil, err
		}
		containers = append(containers, part...)
	}
	return containers, nil
}

// staleBuildahContainers returns the ids of working containers created before
// cutoff, sorted for deterministic removal. Containers created from an image
// named for a build in flight are kept whatever their age, as the rmi path
// keeps the images themselves.
func staleBuildahContainers(graphroot, driver string, cutoff time.Time, inFlight []inFlightBuild) ([]string, error) {
	containers, err := readBuildahStoredContainers(graphroot, driver)
	if err != nil {
		return nil, err
	}
	protected := map[string]struct{}{}
	if len(inFlight) > 0 {
		var images []buildahStoredImage
		if err := readBuildahStoreJSON(graphroot, driver, "images", "images.json", &images); err != nil {
			return nil, err
		}
		for _, image := range images {
			if imageNamedForBuilds(image, inFlight) {
				protected[image.ID] = struct{}{}
			}
		}
	}
	var stale []string
	for _, container := range containers {
		if container.Created.IsZero() || !container.Created.Before(cutoff) {
			continue
		}
		if _, ok := protected[container.ImageID]; ok {
			continue
		}
		stale = append(stale, container.ID)
	}
	sort.Strings(stale)
	return stale, nil
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
