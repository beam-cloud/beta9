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
	"golang.org/x/sync/errgroup"
)

// journalLease is the one fence. A dead owner blocks its replacement for at
// most this long, and a live owner keeps retrying a failing store for this
// long before it fences itself: every store operation here is idempotent, so
// until the lease lapses retrying is always safe and giving up never is. The
// kernel's NBD request timeout must outlast it (see connectNBDDevice). Tests
// shorten it.
var journalLease = 60 * time.Second

// journalRoomWait bounds how long a write waits for a checkpoint to make room
// in a full journal before the journal fails. Tests shorten it.
var journalRoomWait = 2 * time.Minute

const (
	journalTimeout       = 10 * time.Second // one store round trip
	journalRetryDelay    = 100 * time.Millisecond
	journalRetryMaxDelay = time.Second
	// Writes that would take the backlog past the max limits wait for a
	// checkpoint (see WaitForRoom).
	journalMaxBytes           = 512 << 20
	journalCheckpointBytes    = 128 << 20
	journalMaxSegments        = 16384
	journalCheckpointSegments = 4096
	journalReplayConcurrency  = 16
)

// errFenced ends retries early: another owner holds a newer lease on the disk.
var errFenced = errors.New("disk is owned by another journal")

// retry runs op until it succeeds, until passes, or ctx ends. Each attempt
// gets one round trip; attempts back off up to a second apart. A fenced
// error is final. The error returned is the last attempt's.
func retry(ctx context.Context, until time.Time, op func(context.Context, int) error) error {
	for attempt := 0; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, journalTimeout)
		err := op(attemptCtx, attempt)
		cancel()
		if err == nil || errors.Is(err, errFenced) {
			return err
		}
		delay := min(time.Duration(attempt+1)*journalRetryDelay, journalRetryMaxDelay)
		if ctx.Err() != nil || !time.Now().Add(delay).Before(until) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}

// retryUntil is how long a store operation may keep failing: until the lease
// the store currently records lapses. Without a live lease, while acquiring,
// nothing can be lost by trying for one lease period.
func retryUntil(committed time.Time) time.Time {
	if now := time.Now(); !committed.After(now) {
		return now.Add(journalLease)
	}
	return committed
}

// JournalStore is the disk's existing object store. WriteVersion must provide
// atomic compare-and-set against the latest committed version: an empty
// version creates, a nonempty version replaces. The caller is responsible for
// checking that a store honours those preconditions before trusting it with a
// journal; a store that ignores them cannot fence writers.
type JournalStore interface {
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
	held       bool // a head naming this owner has committed
	failed     error
	recovering bool
	sealing    bool
	waiting    int        // writes held by WaitForRoom
	room       *sync.Cond // wakes writers waiting for the backlog to shrink
	cancel     context.CancelFunc
	done       chan struct{}
	checkpoint chan struct{}
}

func OpenJournal(ctx context.Context, store JournalStore, prefix, owner, snapshot string, size int64) (*Journal, error) {
	if size <= 0 || owner == "" || prefix == "" {
		return nil, fmt.Errorf("disk journal requires an owner, prefix, and positive capacity")
	}
	j := &Journal{
		store: store, prefix: prefix, done: make(chan struct{}), checkpoint: make(chan struct{}, 1),
		head:       journalHead{Version: 1, Size: size, Snapshot: snapshot, Formatted: snapshot != ""},
		recovering: true,
	}
	j.room = sync.NewCond(&j.mu)
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
		head, version, err := j.readHead(ctx, deadline)
		if err != nil || version == "" {
			return err
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
	// A backlog of any size stays recoverable: refusing to open one would
	// strand every write it holds.
	next := j.head.Checkpoint + 1
	for _, segment := range j.head.Segments {
		digest, err := hex.DecodeString(segment.Digest)
		if segment.Sequence != next || err != nil || len(digest) != sha256.Size || segment.Bytes <= 0 {
			return fmt.Errorf("invalid disk journal segment %d", next)
		}
		next++
	}
	if next-1 != j.head.Sequence {
		return fmt.Errorf("disk journal is incomplete")
	}
	return nil
}

// readHead reads the stored head, retrying until the deadline. An empty
// version means no head has been written.
func (j *Journal) readHead(ctx context.Context, until time.Time) (journalHead, string, error) {
	var data []byte
	var version string
	err := retry(ctx, until, func(ctx context.Context, _ int) (err error) {
		data, version, err = j.store.ReadVersion(ctx, j.headKey())
		return err
	})
	var head journalHead
	if err != nil || version == "" {
		return head, version, err
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return head, version, fmt.Errorf("decode disk journal: %w", err)
	}
	return head, version, nil
}

func (j *Journal) headKey() string                 { return path.Join(j.prefix, "head.json") }
func (j *Journal) segmentKey(digest string) string { return path.Join(j.prefix, "segments", digest) }

// persist commits the head with a fresh lease. Failing means the lease lapsed
// or the disk was taken over, so the attachment is poisoned.
func (j *Journal) persist(ctx context.Context) error {
	if j.failed != nil {
		return j.failed
	}
	if err := j.writeHead(ctx, false); err != nil {
		return j.fail(fmt.Errorf("disk ownership or persistence lost: %w", err))
	}
	return nil
}

// fail records the journal's first failure, which every later write returns,
// and wakes the writers waiting for room to return it now. Callers hold j.mu.
func (j *Journal) fail(err error) error {
	if j.failed == nil {
		j.failed = err
	}
	j.room.Broadcast()
	return j.failed
}

// writeHead replaces the head conditionally on the version this journal last
// observed; that condition is the ownership fence. The store can reject a
// precondition that did hold, serve readbacks behind the latest write, or
// commit a write whose response was lost. Each attempt carries a fresh lease
// and remembers its digest, so a readback matching any attempt is this call's
// own committed write and is adopted. Once this journal has held the disk, a
// head from another owner whose lease outlives ours can only mean the disk was
// taken over after ours lapsed; that fails at once. While acquiring there is
// no lease of ours to compare, and a lagging readback can show any earlier
// owner's lease still running. Everything but a takeover is retried until our
// lease lapses: the unchanged conditional write can only succeed while the
// object is still at the observed version.
func (j *Journal) writeHead(ctx context.Context, release bool) error {
	previous, committed := j.version, j.head.Expires
	until := retryUntil(committed)
	attempts := make(map[[sha256.Size]byte]time.Time)
	return retry(ctx, until, func(ctx context.Context, attempt int) error {
		j.head.Expires = time.Time{}
		if !release {
			j.head.Expires = time.Now().Add(journalLease)
		}
		data, err := json.Marshal(j.head)
		if err != nil {
			return err
		}
		attempts[sha256.Sum256(data)] = j.head.Expires
		version, err := j.store.WriteVersion(ctx, j.headKey(), data, previous)
		if err == nil {
			j.version, j.held = version, true
			return nil
		}
		stored, version, readErr := j.store.ReadVersion(ctx, j.headKey())
		if expires, ours := attempts[sha256.Sum256(stored)]; readErr == nil && version != "" && ours {
			j.version, j.head.Expires, j.held = version, expires, true
			return nil
		}
		var remote journalHead
		_ = json.Unmarshal(stored, &remote)
		log.Warn().Err(err).AnErr("read_error", readErr).
			Str("disk", j.prefix).Str("expected_version", previous).Str("stored_version", version).
			Str("owner", j.head.Owner).Str("stored_owner", remote.Owner).
			Uint64("sequence", j.head.Sequence).Uint64("stored_sequence", remote.Sequence).
			Int("attempt", attempt).Time("retry_until", until).Msg("disk journal head write failed")
		if j.held && remote.Owner != "" && remote.Owner != j.head.Owner && remote.Expires.After(committed) {
			return fmt.Errorf("%w: %s until %s", errFenced, remote.Owner, remote.Expires.Format(time.RFC3339))
		}
		return err
	})
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
			err := j.persist(ctx)
			j.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (j *Journal) leaseEnd() time.Time {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.head.Expires
}

func (j *Journal) Check() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed == nil && time.Now().After(j.head.Expires) {
		j.fail(fmt.Errorf("disk ownership lease expired"))
	}
	return j.failed
}

func (j *Journal) Fail(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fail(err)
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
	pendingBytes, _ = j.backlogAfter(0)
	return j.head.Snapshot, j.head.Sequence, pendingBytes
}

// backlogAfter sums the uncheckpointed writes committed after sequence.
func (j *Journal) backlogAfter(sequence uint64) (pending, segments int) {
	after := j.after(sequence)
	for _, segment := range after {
		pending += segment.Bytes
	}
	return pending, len(after)
}

// after returns the uncheckpointed commits after sequence, oldest first.
// Commits append in sequence order, so they are a suffix of the log.
func (j *Journal) after(sequence uint64) []journalSegment {
	for i, segment := range j.head.Segments {
		if segment.Sequence > sequence {
			return j.head.Segments[i:]
		}
	}
	return nil
}

// Checkpoints wakes the publisher after a burst of writes.
func (j *Journal) Checkpoints() <-chan struct{} { return j.checkpoint }

func (j *Journal) NeedsCheckpoint() bool {
	return j.NeedsCheckpointAfter(0)
}

// NeedsCheckpointAfter reports whether the writes committed after sequence
// are enough to want a checkpoint of their own.
func (j *Journal) NeedsCheckpointAfter(sequence uint64) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.needsCheckpointAfter(sequence)
}

func (j *Journal) needsCheckpointAfter(sequence uint64) bool {
	pending, segments := j.backlogAfter(sequence)
	return pending >= journalCheckpointBytes || segments >= journalCheckpointSegments
}

// firstAfter returns the oldest uncheckpointed commit after sequence.
func (j *Journal) firstAfter(sequence uint64) (journalSegment, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if after := j.after(sequence); len(after) > 0 {
		return after[0], true
	}
	return journalSegment{}, false
}

// full reports whether a write of n bytes would take the backlog past its
// limits.
func (j *Journal) full(n int) bool {
	pending, segments := j.backlogAfter(0)
	return n+pending > journalMaxBytes || segments >= journalMaxSegments
}

// Waiting reports whether writes are held for a checkpoint to make room. They
// fail the journal if none does within journalRoomWait.
func (j *Journal) Waiting() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.waiting > 0
}

// WaitForRoom holds a write of n bytes for as long as it would take the
// backlog past its limits, until a checkpoint makes room. Writes no
// checkpoint could make room for are let through: those made while
// recovering (see Recovered), and those of a seal's freeze, which flushes
// every dirty page through the journal and cannot finish while they wait.
// What they add is bounded by the filesystem's dirty pages, and the seal
// publishes it. A journal still full after journalRoomWait fails.
func (j *Journal) WaitForRoom(n int) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil || j.recovering || j.sealing || !j.full(n) {
		return j.failed
	}
	j.waiting++
	defer func() { j.waiting-- }()
	j.requestCheckpoint()
	start, expired := time.Now(), false
	timer := time.AfterFunc(journalRoomWait, func() {
		j.mu.Lock()
		expired = true
		j.mu.Unlock()
		j.room.Broadcast()
	})
	defer timer.Stop()
	for j.failed == nil && !j.sealing && j.full(n) {
		if expired {
			j.fail(fmt.Errorf("disk checkpoints made no room in the journal for %s", journalRoomWait))
			break
		}
		j.room.Wait()
	}
	if j.failed == nil {
		log.Info().Str("disk", j.prefix).Dur("waited", time.Since(start)).
			Msg("disk writes waited for a checkpoint to shrink the journal")
	}
	return j.failed
}

// Sealing brackets a seal's freeze. While it lasts, writes past the limits
// proceed, since the freeze flushes the filesystem through the journal.
func (j *Journal) Sealing(active bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.sealing = active
	j.room.Broadcast()
}

// Recovered ends recovery; from then on writes wait for room. Until a
// recovered disk is mounted nothing can checkpoint it, and mounting it writes:
// a backlog left at the limit by the previous owner would otherwise fail every
// attach.
func (j *Journal) Recovered() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recovering = false
}

func (j *Journal) requestCheckpoint() {
	if !j.needsCheckpointAfter(0) {
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
	// Segments are content-addressed, so a repeated upload is harmless.
	err = retry(ctx, retryUntil(j.head.Expires), func(ctx context.Context, _ int) error {
		return j.store.Upload(ctx, j.segmentKey(segment.Digest), compressed.Bytes())
	})
	if err != nil {
		return j.fail(fmt.Errorf("persist disk writes: %w", err))
	}
	j.head.Sequence = segment.Sequence
	j.head.Segments = append(j.head.Segments, segment)
	if err := j.persist(ctx); err != nil {
		return err
	}
	j.requestCheckpoint()
	return nil
}

// Replay applies every committed segment in order. Segments are small and
// numerous, so they are fetched a window at a time; writes stay sequential.
func (j *Journal) Replay(ctx context.Context, write func(uint64, []byte) error) error {
	j.mu.Lock()
	segments := append([]journalSegment(nil), j.head.Segments...)
	size := j.head.Size
	j.mu.Unlock()
	for start := 0; start < len(segments); start += journalReplayConcurrency {
		window := segments[start:min(start+journalReplayConcurrency, len(segments))]
		fetched := make([][]byte, len(window))
		fetches, fetchCtx := errgroup.WithContext(ctx)
		for i, segment := range window {
			fetches.Go(func() error {
				var err error
				fetched[i], err = j.fetchSegment(fetchCtx, segment)
				return err
			})
		}
		if err := fetches.Wait(); err != nil {
			return err
		}
		for _, records := range fetched {
			if err := j.Check(); err != nil {
				return err
			}
			if err := replayRecords(records, uint64(size), write); err != nil {
				return err
			}
		}
	}
	return nil
}

func (j *Journal) fetchSegment(ctx context.Context, segment journalSegment) ([]byte, error) {
	var data []byte
	err := retry(ctx, retryUntil(j.leaseEnd()), func(ctx context.Context, _ int) error {
		var err error
		data, err = j.store.Download(ctx, j.segmentKey(segment.Digest))
		return err
	})
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != segment.Digest {
		return nil, fmt.Errorf("disk journal checksum mismatch at %d", segment.Sequence)
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	records, err := io.ReadAll(io.LimitReader(reader, int64(segment.Bytes)+1))
	reader.Close()
	if err != nil || len(records) != segment.Bytes {
		return nil, fmt.Errorf("invalid disk journal length at %d", segment.Sequence)
	}
	return records, nil
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
	retained := append([]journalSegment{}, j.after(sequence)...)
	j.head.Snapshot, j.head.Checkpoint, j.head.Segments = snapshot, sequence, retained
	defer j.room.Broadcast()
	return j.persist(ctx)
}

func (j *Journal) Close() error {
	j.cancel()
	<-j.done
	j.mu.Lock()
	defer j.mu.Unlock()
	// Releasing the lease lets the next owner start without waiting it out,
	// so the release gets the same retries as any other head write.
	ctx, cancel := context.WithTimeout(context.Background(), journalLease)
	defer cancel()
	if j.failed != nil {
		j.releaseCommitted(ctx)
		return j.failed
	}
	err := j.writeHead(ctx, true)
	j.failed = errors.New("disk journal is closed")
	j.room.Broadcast()
	return err
}

// releaseCommitted releases a failed journal's lease. Its in-memory head may
// hold writes or a checkpoint that never committed, so only the head it last
// committed is written back, unchanged but for the lease, and only while
// nothing has replaced that head and its lease still runs: a replacement
// starts writing only once the lease has lapsed. The read is retried for as
// long as ctx allows, which outlasts any lease the store can still hold.
func (j *Journal) releaseCommitted(ctx context.Context) {
	if j.version == "" {
		return
	}
	deadline, _ := ctx.Deadline()
	committed, version, err := j.readHead(ctx, deadline)
	if err != nil || version != j.version || committed.Owner != j.head.Owner ||
		!committed.Expires.After(time.Now()) {
		return
	}
	j.head = committed
	if err := j.writeHead(ctx, true); err != nil {
		log.Warn().Err(err).Str("disk", j.prefix).Msg("failed disk journal could not release its lease")
	}
}
