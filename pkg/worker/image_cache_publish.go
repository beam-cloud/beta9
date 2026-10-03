package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/clip/pkg/clip"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// Build caches have private AWS addresses. Publish layers to public runtime
// localities from the build worker, not through a WAN fallback on first read.
func (c *ImageClient) publishImageRuntimeLayers(ctx context.Context, imageID, archivePath string) {
	if c.cacheClient == nil || c.workerRepoClient == nil {
		return
	}
	pools := imageRuntimeCachePools(c.config, c.workerPoolName)
	if len(pools) == 0 {
		return
	}
	budget := imageCachePublicationTimeout(ctx)
	if budget == 0 || ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	meta, err := clip.NewClipArchiver().ExtractMetadata(archivePath)
	if err != nil {
		log.Warn().Err(err).Str("image_id", imageID).Msg("runtime image cache publication skipped")
		return
	}
	oci, ok := ociStorageInfo(meta)
	if !ok {
		return
	}
	for _, poolName := range pools {
		pool := c.config.Worker.Pools[poolName]
		locality := imageRuntimeCacheLocality(c.config, pool)
		directory := &gatewayCacheHostDirectory{client: c.workerRepoClient, poolName: poolName}
		if _, err := directory.GetAvailableHosts(ctx, locality); err != nil {
			continue // no runtime capacity in this locality yet
		}
		cfg := normalizeCacheConfig(c.config, pool, "", locality)
		cfg.Client.CacheFS.Enabled = false
		cfg.Server.DiskCacheDir = ""
		peer, err := cache.NewClientWithHostDirectory(ctx, cfg, newGatewayCacheMetadataStore(c.workerRepoClient), directory, locality)
		if err != nil {
			continue
		}
		deadline, _ := ctx.Deadline()
		if err := peer.WaitForHosts(min(2*time.Second, time.Until(deadline))); err != nil {
			_ = peer.Cleanup()
			continue
		}
		var group errgroup.Group
		group.SetLimit(imageLayerPrepareConcurrency)
		for _, hash := range oci.DecompressedHashByLayer {
			group.Go(func() error {
				if err := publishImageLayer(ctx, c.cacheClient, peer, hash, c.layerSpoolDir()); err != nil {
					log.Warn().Err(err).Str("image_id", imageID).Str("pool", poolName).Str("hash", shortHash(hash)).Msg("runtime image layer cache publication failed")
				}
				return nil // caching remains best effort; registry fallback is unchanged
			})
		}
		_ = group.Wait()
		cachePath := c.imageArchiveCachePath(imageID)
		if _, err := peer.StoreContentFromLocalFile(cache.LocalContentSource{Path: archivePath, CachePath: cachePath}, cache.StoreContentOptions{RoutingKey: cachePath, Lock: true}); err != nil {
			log.Warn().Err(err).Str("image_id", imageID).Str("pool", poolName).Msg("runtime image archive cache publication failed")
		}
		_ = peer.Cleanup()
	}
}

// The registry upload must already have succeeded. Leave time for the build's
// final response, and bound all best-effort localities together, not per pool.
func imageCachePublicationTimeout(ctx context.Context) time.Duration {
	budget := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)-5*time.Second)
	}
	return max(0, budget)
}

func imageRuntimeCacheLocality(config types.AppConfig, pool types.WorkerPoolConfig) string {
	if pool.ConfigGroup != "" {
		return pool.ConfigGroup
	}
	if config.Cache.Global.DefaultLocality != "" {
		return config.Cache.Global.DefaultLocality
	}
	return cacheDefaultLocality
}

func imageRuntimeCachePools(config types.AppConfig, buildPoolName string) []string {
	buildPool, ok := config.Worker.Pools[buildPoolName]
	// Only cluster builders can dial both AWS-private and public runtime caches.
	if !ok || buildPool.Mode != types.PoolModeLocal || buildPool.RequiresPoolSelector {
		return nil
	}
	seen := map[string]bool{cacheLocality(config, buildPool): true}
	names := make([]string, 0, len(config.Worker.Pools))
	for name := range config.Worker.Pools {
		names = append(names, name)
	}
	sort.Strings(names)
	pools := []string{}
	for _, name := range names {
		pool := config.Worker.Pools[name]
		locality := imageRuntimeCacheLocality(config, pool)
		if pool.AgentHosted() || pool.RequiresPoolSelector || seen[locality] {
			continue
		}
		seen[locality] = true
		pools = append(pools, name)
	}
	return pools
}

type imageLayerSource interface {
	CacheFSMetadata(context.Context, string) (*cache.FSMetadata, error)
	ReadContentInto(context.Context, string, int64, []byte, cache.ClientOptions) (int64, error)
}

type imageLayerDestination interface {
	IsCachedOnSelectedHost(string, string, ...int64) (bool, error)
	StoreContentFromLocalFile(cache.LocalContentSource, cache.StoreContentOptions) (string, error)
}

func publishImageLayer(ctx context.Context, source imageLayerSource, destination imageLayerDestination, hash, spoolDir string) error {
	meta, err := source.CacheFSMetadata(ctx, imageLayerContentCachePath(hash))
	if err != nil {
		return fmt.Errorf("read image layer metadata: %w", err)
	}
	if meta == nil || meta.Hash != hash || int64(meta.Size) <= 0 {
		return fmt.Errorf("invalid image layer cache metadata")
	}
	size := int64(meta.Size)
	if complete, err := destination.IsCachedOnSelectedHost(hash, hash, size); err == nil && complete {
		return nil
	}
	file, err := os.CreateTemp(spoolDir, "runtime-layer-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hasher := sha256.New()
	writer := io.MultiWriter(file, hasher)
	buf := make([]byte, min(size, 4*1024*1024))
	for offset := int64(0); offset < size; {
		length := min(int64(len(buf)), size-offset)
		n, err := source.ReadContentInto(ctx, hash, offset, buf[:length], cache.ClientOptions{RoutingKey: hash})
		if err != nil {
			return fmt.Errorf("read image layer at %d: %w", offset, err)
		}
		if n != length {
			return io.ErrUnexpectedEOF
		}
		if _, err := writer.Write(buf[:n]); err != nil {
			return err
		}
		offset += n
	}
	if hex.EncodeToString(hasher.Sum(nil)) != hash {
		return fmt.Errorf("image layer source hash mismatch")
	}
	if err := file.Close(); err != nil {
		return err
	}
	actual, err := destination.StoreContentFromLocalFile(cache.LocalContentSource{Path: file.Name(), CachePath: imageLayerContentCachePath(hash)}, cache.StoreContentOptions{RoutingKey: hash, Lock: true})
	if err != nil {
		return fmt.Errorf("publish image layer: %w", err)
	}
	if actual != hash {
		return fmt.Errorf("image layer destination hash mismatch")
	}
	return nil
}
