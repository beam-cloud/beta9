package worker

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog/log"
)

// Image mounts belong to a worker, whereas archives are shared across the
// node. Retire idle mounts first so the owner's archive pruner can stop
// protecting image content that no recent stub or live container needs.
func (s *Worker) pruneIdleImageMounts(ctx context.Context) {
	if s.imageClient == nil || s.containerInstances == nil {
		return
	}
	seconds := s.config.Cache.Reconciliation.RecentStubTTLSeconds
	if seconds <= 0 {
		seconds = cache.DefaultReconcileRecentStubTTLS
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		cutoff := time.Now().Add(-time.Duration(seconds) * time.Second)
		inUse := func(imageID string) bool {
			if ctx.Err() != nil {
				return true
			}
			used := false
			s.containerInstances.Range(func(_ string, instance *ContainerInstance) bool {
				used = instance != nil && instance.Request != nil && instance.Request.ImageId == imageID
				return !used
			})
			return used
		}
		count := s.imageClient.pruneIdleMounts(cutoff, inUse, func(_ string, server *fuse.Server) error {
			return server.Unmount()
		})
		if count > 0 {
			log.Info().Int("unmounted", count).Msg("retired inactive worker image mounts")
		}
	}
}

func (c *ImageClient) pruneIdleMounts(cutoff time.Time, inUse func(string) bool, unmount func(string, *fuse.Server) error) int {
	if c == nil || c.imageMountPath == "" || c.mountedFuseServers == nil || cutoff.IsZero() || inUse == nil || unmount == nil {
		return 0
	}
	ids := []string{}
	c.mountedFuseServers.Range(func(id string, _ *fuse.Server) bool {
		ids = append(ids, id)
		return true
	})
	retired := 0
	for _, id := range ids {
		if id == "" || id == "." || id == ".." || filepath.Base(id) != id {
			continue
		}
		unlock := c.lockImageMount(id)
		func() {
			defer unlock()
			server, exists := c.mountedFuseServers.Get(id)
			if !exists {
				return
			}
			value, recorded := c.mountLastUsed.Load(id)
			if !recorded {
				// Older/injected mount records get a full grace period.
				c.mountLastUsed.Store(id, time.Now())
				return
			}
			if !value.(time.Time).Before(cutoff) || inUse(id) {
				return
			}
			// Do not force or lazily detach: a late reader must keep its mount.
			if err := unmount(id, server); err != nil {
				return
			}
			c.mountedFuseServers.Delete(id)
			c.mountLastUsed.Delete(id)
			// The directory itself protects its image in the shared pruner.
			// Remove only an empty mountpoint, never recursively remove content.
			if err := os.Remove(c.imageMountPoint(id)); err != nil && !os.IsNotExist(err) {
				log.Warn().Err(err).Str("image_id", id).Msg("could not remove retired image mountpoint")
			}
			retired++
		}()
	}
	return retired
}
