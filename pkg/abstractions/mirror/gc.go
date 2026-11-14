package mirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// expireGenerations marks the generations nothing needs: those older than
// the newest KeepGenerations that no VM, snapshot or template is pinned to.
// A generation something is pinned to after all is kept again. It returns
// every generation the mirror still has.
func (s *Service) expireGenerations(ctx context.Context, mirror *types.Mirror) ([]types.MirrorGeneration, error) {
	generations, err := s.repo.ListMirrorGenerations(ctx, mirror.ID)
	if err != nil {
		return nil, err
	}
	references, err := s.repo.ListMirrorReferences(ctx, mirror.WorkspaceID, mirror.ID)
	if err != nil {
		return nil, err
	}
	pinned := map[int64]bool{}
	for _, reference := range references {
		pinned[reference.Generation] = true
	}
	now := s.now()
	var expire, revive []int64
	for i := range generations {
		generation := &generations[i]
		keep := i < s.config.KeepGenerations || pinned[generation.Number]
		switch {
		case keep && generation.ExpiredAt != nil:
			revive = append(revive, generation.Number)
			generation.ExpiredAt = nil
		case !keep && generation.ExpiredAt == nil:
			expire = append(expire, generation.Number)
			generation.ExpiredAt = &now
		}
	}
	if err := s.repo.SetMirrorGenerationsExpired(ctx, mirror.ID, revive, false); err != nil {
		return nil, err
	}
	return generations, s.repo.SetMirrorGenerationsExpired(ctx, mirror.ID, expire, true)
}

// Sweep deletes the objects of a mirror that are older than the grace period
// and that no generation references, counting generations that expired
// within the grace period: a VM being created may have pinned one just
// before it expired. It deletes nothing unless every such manifest could be
// read, because an unreadable manifest would make its objects look
// unreferenced.
func (s *Service) Sweep(ctx context.Context, bucket ObjectStore, mirror *types.Mirror) (int, error) {
	unlock, err := s.repo.LockVM(ctx, mirror.ID)
	if err != nil {
		return 0, err
	}
	defer unlock()

	generations, err := s.expireGenerations(ctx, mirror)
	if err != nil {
		return 0, err
	}
	cutoff := s.now().Add(-s.config.SweepGrace)
	live := map[string]bool{}
	var dead []int64
	for _, generation := range generations {
		if generation.ExpiredAt != nil && generation.ExpiredAt.Before(cutoff) {
			dead = append(dead, generation.Number)
			continue
		}
		manifest, err := loadManifest(ctx, bucket, mirror.ID, generation.ManifestDigest)
		if err != nil {
			return 0, fmt.Errorf("read manifest of generation %d: %w", generation.Number, err)
		}
		live[generation.ManifestDigest] = true
		for i := range manifest.Objects() {
			live[manifest.Object(i).Digest.String()] = true
		}
	}
	// Forget dead generations before their objects go, so that nothing can
	// inherit a pin to a generation that no longer reads.
	if err := s.repo.DeleteMirrorGenerations(ctx, mirror.ID, dead); err != nil {
		return 0, err
	}
	objects, err := bucket.List(ctx, types.MirrorPrefix(mirror.ID))
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, object := range objects {
		if live[path.Base(object.Key)] || object.Modified.After(cutoff) {
			continue
		}
		if err := bucket.Delete(ctx, object.Key); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) deleteObjects(ctx context.Context, bucket ObjectStore, mirrorID string) error {
	objects, err := bucket.List(ctx, types.MirrorPrefix(mirrorID))
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := bucket.Delete(ctx, object.Key); err != nil {
			return err
		}
	}
	return s.repo.MarkMirrorObjectsDeleted(ctx, mirrorID)
}

func (s *Service) bucketFor(ctx context.Context, workspaceID uint) (ObjectStore, error) {
	workspace, err := s.workspaces.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	if workspace.Storage == nil && workspace.StorageId != nil {
		if workspace.Storage, err = s.workspaces.GetWorkspaceStorage(ctx, *workspace.StorageId); err != nil {
			return nil, err
		}
	}
	return s.store(ctx, workspace)
}

// SweepAll finishes interrupted purges and sweeps every mirror once.
func (s *Service) SweepAll(ctx context.Context) {
	purged, err := s.repo.ListPurgedMirrors(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("could not list purged mirrors")
	}
	for _, mirror := range purged {
		bucket, err := s.bucketFor(ctx, mirror.WorkspaceID)
		if err == nil {
			err = s.deleteObjects(ctx, bucket, mirror.ID)
		}
		if err != nil {
			log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("could not delete purged mirror objects")
		}
	}
	mirrors, err := s.repo.ListMirrorIDs(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("could not list mirrors to sweep")
		return
	}
	for _, mirror := range mirrors {
		if ctx.Err() != nil {
			return
		}
		bucket, err := s.bucketFor(ctx, mirror.WorkspaceID)
		if err != nil {
			log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("could not open mirror bucket")
			continue
		}
		deleted, err := s.Sweep(ctx, bucket, &mirror)
		if err != nil {
			log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("mirror sweep failed")
		} else if deleted > 0 {
			log.Info().Str("mirror_id", mirror.ID).Int("objects", deleted).Msg("swept unreferenced mirror objects")
		}
	}
}

func (s *Service) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.SweepAll(ctx)
		}
	}
}

type bucketStore struct {
	client *clients.WorkspaceStorageClient
}

func (b bucketStore) Head(ctx context.Context, key string) (int64, bool, error) {
	exists, object, err := b.client.Head(ctx, key)
	if err != nil || !exists || object == nil || object.ContentLength == nil {
		return 0, false, err
	}
	return *object.ContentLength, true, nil
}

func (b bucketStore) Download(ctx context.Context, key string, limit int64) ([]byte, error) {
	body, err := b.client.DownloadWithReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, io.LimitReader(body, limit+1)); err != nil {
		return nil, err
	}
	if int64(buffer.Len()) > limit {
		return nil, errors.New("object is larger than the limit")
	}
	return buffer.Bytes(), nil
}

func (b bucketStore) PresignPut(ctx context.Context, key string, expires time.Duration) (string, error) {
	return b.client.GeneratePresignedPutURL(ctx, key, int64(expires.Seconds()))
}

func (b bucketStore) PresignGet(ctx context.Context, key string, expires time.Duration) (string, error) {
	return b.client.GeneratePresignedGetURL(ctx, key, int64(expires.Seconds()))
}

func (b bucketStore) List(ctx context.Context, prefix string) ([]StoredObject, error) {
	listed, err := b.client.ListWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	objects := make([]StoredObject, 0, len(listed))
	for _, object := range listed {
		if object.Key == nil || object.LastModified == nil {
			continue
		}
		stored := StoredObject{Key: *object.Key, Modified: *object.LastModified}
		if object.Size != nil {
			stored.Size = *object.Size
		}
		objects = append(objects, stored)
	}
	return objects, nil
}

func (b bucketStore) Delete(ctx context.Context, key string) error {
	return b.client.Delete(ctx, key)
}
