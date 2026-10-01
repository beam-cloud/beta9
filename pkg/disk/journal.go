package disk

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	journalLease           = 30 * time.Second
	journalTimeout         = 10 * time.Second
	journalWriteAttempts   = 5
	journalRetryDelay      = 100 * time.Millisecond
	journalMaxBytes        = 512 << 20
	journalCheckpointBytes = 128 << 20
	journalMaxSegments     = 16384
)

// JournalStore is the disk's existing object store. WriteVersion must provide
// atomic compare-and-set: an empty version creates, a nonempty version replaces.
type JournalStore interface {
	VerifyConditionalWrites(context.Context) error
	ReadVersion(context.Context, string) ([]byte, string, error)
	WriteVersion(context.Context, string, []byte, string) (string, error)
	Upload(context.Context, string, []byte) error
	Download(context.Context, string) ([]byte, error)
}

type journalSegment struct {
	Sequence uint64 `json:"sequence"`
	Digest   string `json:"digest"`
	Bytes    int    `json:"bytes"`
}

type journalHead struct {
	Formatted  bool             `json:"formatted"`
	Version    int              `json:"version"`
	Owner      string           `json:"owner"`
	Expires    time.Time        `json:"expires"`
	Size       int64            `json:"size"`
	Snapshot   string           `json:"snapshot,omitempty"`
	Checkpoint uint64           `json:"checkpoint"`
	Sequence   uint64           `json:"sequence"`
	Segments   []journalSegment `json:"segments,omitempty"`
}

// Journal commits block writes to object storage before a disk flush succeeds.
// A conditional head update is both the commit point and the ownership fence.
// Any uncertain update poisons this attachment; recovery reads the remote head.
type Journal struct {
	mu         sync.Mutex
	store      JournalStore
	prefix     string
	head       journalHead
	version    string
	failed     error
	cancel     context.CancelFunc
	done       chan struct{}
	checkpoint chan struct{}
}

func OpenJournal(ctx context.Context, store JournalStore, prefix, owner, snapshot string, size int64) (*Journal, error) {
	if size <= 0 || owner == "" || prefix == "" {
		return nil, fmt.Errorf("disk journal requires an owner, prefix, and positive capacity")
	}
	if err := store.VerifyConditionalWrites(ctx); err != nil {
		return nil, err
	}
	j := &Journal{
		store: store, prefix: prefix, done: make(chan struct{}), checkpoint: make(chan struct{}, 1),
		head: journalHead{Version: 1, Size: size, Snapshot: snapshot, Formatted: snapshot != ""},
	}
	if err := j.waitForReleasedHead(ctx); err != nil {
		return nil, err
	}
	if size < j.head.Size {
		return nil, fmt.Errorf("disk cannot shrink from %d to %d bytes", j.head.Size, size)
	}
	j.head.Owner, j.head.Size = owner, size
	if err := j.persist(ctx); err != nil {
		return nil, fmt.Errorf("acquire disk ownership: %w", err)
	}
	// A recovered journal can already exceed the checkpoint threshold.
	j.requestCheckpoint()

	leaseCtx, cancel := context.WithCancel(context.Background())
	j.cancel = cancel
	go j.renew(leaseCtx)
	return j, nil
}

// A replacement may arrive just before the failed owner's lease expires.
// Wait for that lease instead of counting a transient conflict as a failed
// container start. An owner that keeps renewing still fences this attachment.
func (j *Journal) waitForReleasedHead(ctx context.Context) error {
	deadline := time.Now().Add(journalLease)
	for {
		readCtx, cancel := context.WithTimeout(ctx, journalTimeout)
		data, version, err := j.store.ReadVersion(readCtx, j.headKey())
		cancel()
		if err != nil || version == "" {
			return err
		}
		var head journalHead
		if err := json.Unmarshal(data, &head); err != nil {
			return fmt.Errorf("decode disk journal: %w", err)
		}
		j.head, j.version = head, version
		if err := j.validate(); err != nil {
			return err
		}
		wait := time.Until(head.Expires)
		if wait <= 0 {
			return nil
		}
		if head.Expires.After(deadline) {
			return fmt.Errorf("disk is owned by %s until %s", head.Owner, head.Expires.Format(time.RFC3339))
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (j *Journal) validate() error {
	if j.head.Version != 1 || j.head.Size <= 0 || j.head.Sequence < j.head.Checkpoint {
		return fmt.Errorf("invalid disk journal header")
	}
	next := j.head.Checkpoint + 1
	pending := 0
	for _, segment := range j.head.Segments {
		digest, err := hex.DecodeString(segment.Digest)
		if segment.Sequence != next || err != nil || len(digest) != sha256.Size || segment.Bytes <= 0 {
			return fmt.Errorf("invalid disk journal segment %d", next)
		}
		if segment.Bytes > journalMaxBytes-pending {
			return fmt.Errorf("disk journal exceeds its recovery limit")
		}
		pending += segment.Bytes
		next++
	}
	if next-1 != j.head.Sequence || len(j.head.Segments) > journalMaxSegments {
		return fmt.Errorf("disk journal is incomplete")
	}
	return nil
}

func (j *Journal) headKey() string                 { return path.Join(j.prefix, "head.json") }
func (j *Journal) segmentKey(digest string) string { return path.Join(j.prefix, "segments", digest) }

func (j *Journal) persist(ctx context.Context) error {
	if j.failed != nil {
		return j.failed
	}
	j.head.Expires = time.Now().Add(journalLease)
	data, err := json.Marshal(j.head)
	if err == nil {
		previous := j.version
		for attempt := 0; attempt < journalWriteAttempts; attempt++ {
			var version string
			version, err = j.store.WriteVersion(ctx, j.headKey(), data, previous)
			if err == nil {
				j.version = version
				break
			}
			stored, version, readErr := j.store.ReadVersion(ctx, j.headKey())
			if readErr == nil && version != "" && bytes.Equal(stored, data) {
				j.version, err = version, nil
				break
			}
			var remote journalHead
			_ = json.Unmarshal(stored, &remote)
			log.Warn().Err(err).AnErr("read_error", readErr).
				Str("disk", j.prefix).Str("expected_version", previous).Str("stored_version", version).
				Str("owner", j.head.Owner).Str("stored_owner", remote.Owner).
				Uint64("sequence", j.head.Sequence).Uint64("stored_sequence", remote.Sequence).
				Int("attempt", attempt).Msg("disk journal conditional write rejected")
			if readErr != nil || remote.Owner != j.head.Owner || ctx.Err() != nil || attempt+1 == journalWriteAttempts {
				break
			}
			// Retry the identical conditional write; never adopt a conflicting version.
			timer := time.NewTimer(time.Duration(attempt+1) * journalRetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
	if err != nil {
		j.failed = fmt.Errorf("disk ownership or persistence lost: %w", err)
	}
	return j.failed
}

func (j *Journal) renew(ctx context.Context) {
	defer close(j.done)
	ticker := time.NewTicker(journalLease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.mu.Lock()
			updateCtx, cancel := context.WithTimeout(ctx, journalTimeout)
			err := j.persist(updateCtx)
			cancel()
			j.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (j *Journal) Check() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed == nil && time.Now().After(j.head.Expires) {
		j.failed = fmt.Errorf("disk ownership lease expired")
	}
	return j.failed
}

func (j *Journal) Fail(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed == nil {
		j.failed = err
	}
}

func (j *Journal) Initialized() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.head.Formatted
}

func (j *Journal) Initialize(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.head.Formatted = true
	return j.persist(ctx)
}

func (j *Journal) State() (snapshot string, sequence uint64, pendingBytes int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, segment := range j.head.Segments {
		pendingBytes += segment.Bytes
	}
	return j.head.Snapshot, j.head.Sequence, pendingBytes
}

// Checkpoints wakes the publisher after a burst of writes. The hard recovery
// limit leaves room for writes made while sealing and uploading a checkpoint.
func (j *Journal) Checkpoints() <-chan struct{} { return j.checkpoint }

func (j *Journal) NeedsCheckpoint() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.needsCheckpoint()
}

func (j *Journal) needsCheckpoint() bool {
	pending := 0
	for _, segment := range j.head.Segments {
		pending += segment.Bytes
	}
	return pending >= journalCheckpointBytes || len(j.head.Segments) >= 1024
}

func (j *Journal) requestCheckpoint() {
	if !j.needsCheckpoint() {
		return
	}
	select {
	case j.checkpoint <- struct{}{}:
	default:
	}
}

// Commit accepts an ordered sequence of (offset, length, bytes) records. The
// caller serializes disk requests and retains the bytes until this returns.
func (j *Journal) Commit(ctx context.Context, records []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if len(records) == 0 {
		return j.persist(ctx)
	}
	pending := len(records)
	for _, segment := range j.head.Segments {
		pending += segment.Bytes
	}
	if pending > journalMaxBytes || len(j.head.Segments) >= journalMaxSegments {
		j.failed = fmt.Errorf("disk checkpoint backlog exceeded its recovery limit")
		return j.failed
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write(records)
	if err == nil {
		err = writer.Close()
	}
	if err != nil {
		return err
	}
	digest := sha256.Sum256(compressed.Bytes())
	segment := journalSegment{Sequence: j.head.Sequence + 1, Digest: hex.EncodeToString(digest[:]), Bytes: len(records)}
	if err := j.store.Upload(ctx, j.segmentKey(segment.Digest), compressed.Bytes()); err != nil {
		j.failed = fmt.Errorf("persist disk writes: %w", err)
		return j.failed
	}
	j.head.Sequence = segment.Sequence
	j.head.Segments = append(j.head.Segments, segment)
	if err := j.persist(ctx); err != nil {
		return err
	}
	j.requestCheckpoint()
	return nil
}

func (j *Journal) Replay(ctx context.Context, write func(uint64, []byte) error) error {
	j.mu.Lock()
	segments := append([]journalSegment(nil), j.head.Segments...)
	size := j.head.Size
	j.mu.Unlock()
	for _, segment := range segments {
		if err := j.Check(); err != nil {
			return err
		}
		data, err := j.store.Download(ctx, j.segmentKey(segment.Digest))
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != segment.Digest {
			return fmt.Errorf("disk journal checksum mismatch at %d", segment.Sequence)
		}
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return err
		}
		records, err := io.ReadAll(io.LimitReader(reader, int64(segment.Bytes)+1))
		reader.Close()
		if err != nil || len(records) != segment.Bytes {
			return fmt.Errorf("invalid disk journal length at %d", segment.Sequence)
		}
		if err := replayRecords(records, uint64(size), write); err != nil {
			return err
		}
	}
	return nil
}

func replayRecords(records []byte, size uint64, write func(uint64, []byte) error) error {
	for len(records) > 0 {
		if len(records) < 12 {
			return fmt.Errorf("truncated disk journal record")
		}
		offset := binary.BigEndian.Uint64(records[:8])
		length := uint64(binary.BigEndian.Uint32(records[8:12]))
		records = records[12:]
		if length > uint64(len(records)) || offset > size || length > size-offset {
			return fmt.Errorf("disk journal record exceeds disk boundary")
		}
		if err := write(offset, records[:length]); err != nil {
			return err
		}
		records = records[length:]
	}
	return nil
}

// Checkpoint publishes the snapshot corresponding to a frozen journal position.
// New writes remain in the log; no segment is discarded before its snapshot is durable.
func (j *Journal) Checkpoint(ctx context.Context, sequence uint64, snapshot string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if sequence < j.head.Checkpoint || sequence > j.head.Sequence || snapshot == "" {
		return fmt.Errorf("invalid disk checkpoint position")
	}
	retained := make([]journalSegment, 0, len(j.head.Segments))
	for _, segment := range j.head.Segments {
		if segment.Sequence > sequence {
			retained = append(retained, segment)
		}
	}
	j.head.Snapshot, j.head.Checkpoint, j.head.Segments = snapshot, sequence, retained
	return j.persist(ctx)
}

func (j *Journal) Close() error {
	j.cancel()
	<-j.done
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	j.head.Expires = time.Time{}
	data, err := json.Marshal(j.head)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTimeout)
	defer cancel()
	_, err = j.store.WriteVersion(ctx, j.headKey(), data, j.version)
	j.failed = errors.New("disk journal is closed")
	return err
}
