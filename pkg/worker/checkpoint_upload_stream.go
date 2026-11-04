package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// The uploader follows a private archive as compression writes it. Only bytes
// from completed writes are readable; EOF is withheld until both producers and
// the compressed frame footer succeed. The file, not a RAM queue, buffers a
// slower origin. An upload failure cancels and joins the producer.
type checkpointArchiveTail struct {
	file     *os.File
	ctx      context.Context
	mu       sync.Mutex
	written  int64
	finished bool
	err      error
	changed  chan struct{}
	offset   int64 // one uploader reader
	readEOF  bool
}

func (t *checkpointArchiveTail) Write(p []byte) (int, error) {
	n, err := t.file.Write(p)
	t.mu.Lock()
	t.written += int64(n)
	t.mu.Unlock()
	t.notify()
	return n, err
}

func (t *checkpointArchiveTail) notify() {
	select {
	case t.changed <- struct{}{}:
	default:
	}
}

func (t *checkpointArchiveTail) finish(err error) {
	t.mu.Lock()
	t.finished, t.err = true, err
	t.mu.Unlock()
	t.notify()
}

func (t *checkpointArchiveTail) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if err := t.ctx.Err(); err != nil {
			return 0, err
		}
		t.mu.Lock()
		written, finished, err := t.written, t.finished, t.err
		t.mu.Unlock()
		if err != nil {
			return 0, err
		}
		if available := written - t.offset; available > 0 {
			n, err := t.file.ReadAt(p[:min(int64(len(p)), available)], t.offset)
			t.offset += int64(n)
			return n, err
		}
		if finished {
			t.readEOF = true
			return 0, io.EOF
		}
		select {
		case <-t.changed:
		case <-t.ctx.Done():
			return 0, t.ctx.Err()
		}
	}
}

func streamCheckpointArchive(ctx context.Context, source, archive string, progress func(int64), archiveReady func(), upload func(context.Context, io.Reader) error) (string, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	f, err := os.OpenFile(archive, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	tail := &checkpointArchiveTail{file: f, ctx: ctx, changed: make(chan struct{}, 1)}
	uploaded := make(chan error, 1)
	go func() {
		err := upload(ctx, tail)
		if err == nil && !tail.readEOF {
			err = errors.New("checkpoint upload returned before archive EOF")
		}
		if err != nil {
			cancel()
		}
		uploaded <- err
	}()
	hash := sha256.New()
	count := &countingWriter{progress: progress}
	producer := exec.CommandContext(ctx, "tar", append(tarSparseArgs(), "-cf", "-", "-C", filepath.Dir(source), filepath.Base(source))...)
	var stderr bytes.Buffer
	producer.Stderr = &stderr
	started := time.Now()
	err = writeCheckpointTar(ctx, producer, io.MultiWriter(tail, hash, count))
	common.RecordStartupSpan(ctx, "checkpoint.archive", started, time.Now())
	if err != nil {
		err = tarCommandError("stream checkpoint archive", err, stderr)
	}
	tail.finish(err)
	if err == nil && archiveReady != nil {
		archiveReady()
	}
	uploadErr := <-uploaded // join before the file may be closed or removed
	if err = errors.Join(err, uploadErr); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), count.n, nil
}

func (s *Worker) persistStreamingCheckpoint(ctx context.Context, request *types.ContainerRequest, checkpointID, checkpointPath, archivePath string, logger *slog.Logger, archiveReady func()) (*checkpointCacheMetadata, error) {
	client, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return nil, err
	}
	endUploadAdmission := common.StartupSpan(ctx, "checkpoint.upload_admission")
	releaseUpload, err := acquireCheckpointSlot(ctx, checkpointOriginUploadSlots)
	endUploadAdmission()
	if err != nil {
		return nil, err
	}
	defer releaseUpload()
	endArchiveAdmission := common.StartupSpan(ctx, "checkpoint.archive_admission")
	releaseArchive, err := acquireCheckpointSlot(ctx, checkpointArchiveSlots)
	endArchiveAdmission()
	if err != nil {
		return nil, err
	}
	defer releaseArchive()
	progress := newCheckpointPersistenceProgress(logger, "archive", 0)
	originKey := checkpointOriginKey(checkpointID)
	hash, size, err := streamCheckpointArchive(ctx, checkpointPath, archivePath, progress.update, func() {
		releaseArchive()
		if archiveReady != nil {
			archiveReady()
		}
	}, func(ctx context.Context, reader io.Reader) error {
		defer common.StartupSpan(ctx, "checkpoint.origin_upload")()
		return client.UploadWithReaderOptions(ctx, originKey, reader, clients.StorageUploadOptions{StreamingConcurrency: 2})
	})
	if err != nil {
		_ = os.Remove(archivePath)
		return nil, fmt.Errorf("persist streamed checkpoint: %w", err)
	}
	progress.finish(size)
	metadata := &checkpointCacheMetadata{hash: hash, sizeBytes: size, originKey: originKey, locality: s.cacheManager.locality, accelerator: checkpointAccelerator(request)}
	s.cacheCheckpointArchiveAsync(checkpointID, archivePath, originKey, hash, nil)
	return metadata, nil
}
