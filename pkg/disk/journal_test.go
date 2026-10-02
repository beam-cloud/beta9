package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retryJournalStore reproduces a lost PUT response: storage commits the first
// attempt, then the SDK retries with the old condition and receives HTTP 412.
// Its first readback lags that committed write, as observed in production.
type retryJournalStore struct {
	*clients.WorkspaceStorageClient
	retry        atomic.Bool
	reject       atomic.Bool
	prior        []byte
	priorVersion string
}

func (s *retryJournalStore) WriteVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if s.reject.CompareAndSwap(true, false) {
		return s.WorkspaceStorageClient.WriteVersion(ctx, key, data, "stale")
	}
	if s.retry.Load() {
		var err error
		s.prior, s.priorVersion, err = s.WorkspaceStorageClient.ReadVersion(ctx, key)
		if err != nil {
			return "", err
		}
	}
	next, err := s.WorkspaceStorageClient.WriteVersion(ctx, key, data, version)
	if err == nil && s.retry.CompareAndSwap(true, false) {
		return s.WorkspaceStorageClient.WriteVersion(ctx, key, data, version)
	}
	return next, err
}

func (s *retryJournalStore) ReadVersion(ctx context.Context, key string) ([]byte, string, error) {
	if s.prior != nil {
		data := s.prior
		s.prior = nil
		return data, s.priorVersion, nil
	}
	return s.WorkspaceStorageClient.ReadVersion(ctx, key)
}

// BEAM_TEST_STORAGE points to a WorkspaceStorage JSON file, or "-" for stdin.
// This test uses real conditional writes against a disposable object prefix.
func TestJournalConditionalRetry(t *testing.T) {
	configPath := os.Getenv("BEAM_TEST_STORAGE")
	if configPath == "" {
		t.Skip("set BEAM_TEST_STORAGE to exercise an S3-compatible store")
	}
	input := os.Stdin
	if configPath != "-" {
		var err error
		input, err = os.Open(configPath)
		require.NoError(t, err)
		defer input.Close()
	}
	var config types.WorkspaceStorage
	require.NoError(t, json.NewDecoder(input).Decode(&config))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := clients.NewWorkspaceStorageClient(ctx, "journal-test", &config)
	require.NoError(t, err)
	require.NoError(t, client.VerifyConditionalWrites(ctx), "the worker's once-per-store probe")
	store := &retryJournalStore{WorkspaceStorageClient: client}
	prefix := "durable-disks/conditional-probes/" + uuid.NewString()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		objects, err := client.ListWithPrefix(ctx, prefix+"/")
		if !assert.NoError(t, err) {
			return
		}
		for _, object := range objects {
			assert.NoError(t, client.Delete(ctx, *object.Key), "clean up %s", *object.Key)
		}
		remaining, err := client.ListWithPrefix(ctx, prefix+"/")
		assert.NoError(t, err)
		assert.Empty(t, remaining, "journal cleanup must remove nested segments")
	})

	journal, err := OpenJournal(ctx, store, prefix, "first-owner", "", 4096)
	require.NoError(t, err)
	defer journal.Close()
	store.retry.Store(true)
	require.NoError(t, journal.Commit(ctx, journalRecord(t, 0, "saved")))
	require.False(t, store.retry.Load(), "the retry must actually be exercised")
	require.NoError(t, journal.Commit(ctx, nil), "reconciliation must retain the new version")

	// A transient rejection leaves the previous head in place. Retrying must
	// keep its original condition, never replace it with an observed version.
	store.reject.Store(true)
	require.NoError(t, journal.Commit(ctx, nil))
	require.False(t, store.reject.Load(), "the rejected write must be exercised")
	require.NoError(t, journal.Commit(ctx, nil))

	// A replacement that acquired the disk holds a newer lease; that fences
	// the old writer at once.
	key := prefix + "/head.json"
	data, version, err := client.ReadVersion(ctx, key)
	require.NoError(t, err)
	var head map[string]any
	require.NoError(t, json.Unmarshal(data, &head))
	head["owner"], head["expires"] = "replacement-owner", time.Now().Add(2*journalLease)
	foreign, err := json.Marshal(head)
	require.NoError(t, err)
	_, err = client.WriteVersion(ctx, key, foreign, version)
	require.NoError(t, err)
	require.ErrorIs(t, journal.Commit(ctx, nil), errFenced)
	data, _, err = client.ReadVersion(ctx, key)
	require.NoError(t, err)
	require.JSONEq(t, string(foreign), string(data))

	// Once the replacement releases the disk, recovery must replay the
	// acknowledged data from the object store, and acquiring it must survive
	// the same lost response and lagging readback while the stored head still
	// names the previous owner.
	data, version, err = client.ReadVersion(ctx, key)
	require.NoError(t, err)
	_, err = client.WriteVersion(ctx, key, releasedHead(t, data), version)
	require.NoError(t, err)
	store.retry.Store(true)
	recovered, err := OpenJournal(ctx, store, prefix, "recovered-owner", "", 4096)
	require.NoError(t, err)
	defer recovered.Close()
	require.False(t, store.retry.Load(), "the acquisition retry must actually be exercised")
	require.Equal(t, "saved", replaySaved(t, recovered))
}

// releasedHead returns head with its lease released, as a clean Close leaves it.
func releasedHead(t *testing.T, data []byte) []byte {
	t.Helper()
	var head map[string]any
	require.NoError(t, json.Unmarshal(data, &head))
	head["expires"] = time.Time{}
	released, err := json.Marshal(head)
	require.NoError(t, err)
	return released
}

// memoryJournalStore is an in-memory JournalStore with compare-and-set heads
// and the store behaviours the journal must survive: a precondition rejected
// although it held, a committed write reported as rejected, readbacks that
// lag the latest write, and an outage during which every call fails.
type memoryJournalStore struct {
	mu            sync.Mutex
	objects       map[string][]byte
	current       map[string]memoryHead
	previous      map[string]memoryHead
	writes        int
	rejectWrites  int // reject without applying
	commitRejects int // apply, then report a rejection
	freshReads    int // reads served normally before the stale ones
	staleReads    int // reads served one write behind
	downUntil     time.Time
	failedCalls   int // calls refused during an outage
}

type memoryHead struct {
	data    []byte
	version string
}

var (
	errMemoryPrecondition = errors.New("api error PreconditionFailed")
	errMemoryUnavailable  = errors.New("api error ServiceUnavailable")
)

func newMemoryJournalStore() *memoryJournalStore {
	return &memoryJournalStore{objects: map[string][]byte{}, current: map[string]memoryHead{}, previous: map[string]memoryHead{}}
}

// outage makes every call fail for the given duration.
func (s *memoryJournalStore) outage(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.downUntil = time.Now().Add(d)
}

func (s *memoryJournalStore) down() bool {
	if time.Now().Before(s.downUntil) {
		s.failedCalls++
		return true
	}
	return false
}

func (s *memoryJournalStore) ReadVersion(_ context.Context, key string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down() {
		return nil, "", errMemoryUnavailable
	}
	heads := s.current
	if s.freshReads > 0 {
		s.freshReads--
	} else if s.staleReads > 0 {
		s.staleReads--
		heads = s.previous
	}
	head, ok := heads[key]
	if !ok {
		return nil, "", nil
	}
	return append([]byte(nil), head.data...), head.version, nil
}

func (s *memoryJournalStore) WriteVersion(_ context.Context, key string, data []byte, version string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down() {
		return "", errMemoryUnavailable
	}
	if s.rejectWrites > 0 {
		s.rejectWrites--
		return "", errMemoryPrecondition
	}
	head, exists := s.current[key]
	if (version == "" && exists) || (version != "" && head.version != version) {
		return "", errMemoryPrecondition
	}
	if exists {
		s.previous[key] = head
	} else {
		delete(s.previous, key)
	}
	s.writes++
	next := memoryHead{data: append([]byte(nil), data...), version: fmt.Sprintf("v%d", s.writes)}
	s.current[key] = next
	if s.commitRejects > 0 {
		s.commitRejects--
		return "", errMemoryPrecondition
	}
	return next.version, nil
}

func (s *memoryJournalStore) Upload(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down() {
		return errMemoryUnavailable
	}
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *memoryJournalStore) Download(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down() {
		return nil, errMemoryUnavailable
	}
	data, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("no such key %s", key)
	}
	return append([]byte(nil), data...), nil
}

func (s *memoryJournalStore) pendingFaults() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rejectWrites + s.commitRejects + s.freshReads + s.staleReads
}

func (s *memoryJournalStore) refused() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failedCalls
}

func (s *memoryJournalStore) head(t *testing.T, key string) (journalHead, string) {
	t.Helper()
	data, version, err := s.ReadVersion(context.Background(), key)
	require.NoError(t, err)
	var head journalHead
	require.NoError(t, json.Unmarshal(data, &head))
	return head, version
}

// replaceHead rewrites the stored head under another owner and lease, as a
// replacement or an earlier owner would, and returns what it stored.
func replaceHead(t *testing.T, store *memoryJournalStore, key, owner string, expires time.Time) ([]byte, string) {
	t.Helper()
	head, version := store.head(t, key)
	head.Owner, head.Expires = owner, expires
	data, err := json.Marshal(head)
	require.NoError(t, err)
	replaced, err := store.WriteVersion(context.Background(), key, data, version)
	require.NoError(t, err)
	return data, replaced
}

// seedJournalBacklog stores a released head whose segments, none of them
// checkpointed, add up to backlog bytes, as runs that never published leave
// it. The segments are never replayed, so their objects are not stored.
func seedJournalBacklog(t *testing.T, store *memoryJournalStore, prefix string, backlog int) {
	t.Helper()
	head := journalHead{Version: 1, Formatted: true, Size: 1 << 30, Snapshot: "snap-0"}
	for backlog > 0 {
		head.Sequence++
		digest := sha256.Sum256(binary.BigEndian.AppendUint64(nil, head.Sequence))
		segment := journalSegment{Sequence: head.Sequence, Digest: hex.EncodeToString(digest[:]), Bytes: min(backlog, 64<<20)}
		head.Segments = append(head.Segments, segment)
		backlog -= segment.Bytes
	}
	data, err := json.Marshal(head)
	require.NoError(t, err)
	_, err = store.WriteVersion(context.Background(), path.Join(prefix, "head.json"), data, "")
	require.NoError(t, err)
}

// shortLease shrinks the lease so lease-bounded behaviour fits in a test.
func shortLease(t *testing.T, lease time.Duration) {
	t.Helper()
	previous := journalLease
	journalLease = lease
	t.Cleanup(func() { journalLease = previous })
}

// shortRoomWait shrinks how long a full journal holds a write.
func shortRoomWait(t *testing.T, wait time.Duration) {
	t.Helper()
	previous := journalRoomWait
	journalRoomWait = wait
	t.Cleanup(func() { journalRoomWait = previous })
}

// A full journal holds writes until a checkpoint makes room rather than
// failing the disk, but never holds a seal: its freeze flushes through here.
func TestJournalFullWaitsForACheckpoint(t *testing.T) {
	ctx := context.Background()
	write := journalRecord(t, 0, "wal")
	store := newMemoryJournalStore()
	seedJournalBacklog(t, store, "disk", journalMaxBytes)
	journal, err := OpenJournal(ctx, store, "disk", "owner", "", 1<<30)
	require.NoError(t, err)
	defer journal.Close()
	journal.Recovered()

	waitForRoom := func() chan error {
		waited := make(chan error, 1)
		go func() { waited <- journal.WaitForRoom(len(write)) }()
		select {
		case err := <-waited:
			t.Fatalf("a write past the limit must wait, got %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		return waited
	}

	waited := waitForRoom()
	journal.Sealing(true)
	require.NoError(t, <-waited, "a seal must pass a full journal")
	require.NoError(t, journal.Commit(ctx, write))
	journal.Sealing(false)

	waited = waitForRoom()
	_, sequence, _ := journal.State()
	require.NoError(t, journal.Checkpoint(ctx, sequence, "snap-1"))
	require.NoError(t, <-waited)
	require.NoError(t, journal.Commit(ctx, write))
}

// A backlog of any size opens, and the writes that mount it pass until
// Recovered: nothing can checkpoint a disk before it is mounted.
func TestJournalRecoveryPassesAFullJournal(t *testing.T) {
	ctx := context.Background()
	write := journalRecord(t, 0, "wal")
	store := newMemoryJournalStore()
	seedJournalBacklog(t, store, "disk", 3*journalMaxBytes)
	journal, err := OpenJournal(ctx, store, "disk", "owner", "", 1<<30)
	require.NoError(t, err)
	defer journal.Close()

	require.NoError(t, journal.WaitForRoom(len(write)))
	require.NoError(t, journal.Commit(ctx, write))

	shortRoomWait(t, 20*time.Millisecond)
	journal.Recovered()
	require.Error(t, journal.WaitForRoom(len(write)), "a recovered journal must hold writes past its limits")
}

// Only writes after the given sequence count toward another checkpoint.
func TestJournalNeedsCheckpointAfter(t *testing.T) {
	store := newMemoryJournalStore()
	seedJournalBacklog(t, store, "disk", 3*journalCheckpointBytes)
	journal, err := OpenJournal(context.Background(), store, "disk", "owner", "", 1<<30)
	require.NoError(t, err)
	defer journal.Close()

	_, sequence, _ := journal.State()
	require.True(t, journal.NeedsCheckpoint())
	require.True(t, journal.NeedsCheckpointAfter(sequence-2), "the newest checkpoint's worth of writes")
	require.False(t, journal.NeedsCheckpointAfter(sequence-1))
	require.False(t, journal.NeedsCheckpointAfter(sequence))
}

func journalRecord(t *testing.T, offset uint64, payload string) []byte {
	t.Helper()
	var records bytes.Buffer
	require.NoError(t, binary.Write(&records, binary.BigEndian, offset))
	require.NoError(t, binary.Write(&records, binary.BigEndian, uint32(len(payload))))
	records.WriteString(payload)
	return records.Bytes()
}

// replaySaved replays a journal whose records all sit at offset zero and
// returns the replayed bytes in commit order.
func replaySaved(t *testing.T, journal *Journal) string {
	t.Helper()
	var restored []byte
	require.NoError(t, journal.Replay(context.Background(), func(offset uint64, data []byte) error {
		require.Zero(t, offset)
		restored = append(restored, data...)
		return nil
	}))
	return string(restored)
}

// A rejected head write is retried with its original condition: for the
// owner's commits and renewals, and for acquisition, where the stored head
// still names the previous owner, so a readback cannot tell a lagging store
// from a competitor.
func TestJournalRetriesRejectedHeadWrites(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)

	// Committed, reported as rejected, then read back one write behind.
	store.commitRejects, store.staleReads = 1, 1
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "saved")))
	// Rejected outright although the condition held.
	store.rejectWrites = 2
	require.NoError(t, first.Commit(ctx, nil))
	require.Zero(t, store.pendingFaults(), "every injected fault must be exercised")

	// The lease release is retried too; otherwise the next owner waits it out.
	store.rejectWrites = 1
	require.NoError(t, first.Close())
	released, _ := store.head(t, "disk/head.json")
	require.True(t, released.Expires.IsZero(), "close must release the lease")

	// The released head is read once, then the acquisition write is committed
	// but reported as rejected, and its readback still shows the old owner.
	store.freshReads, store.commitRejects, store.staleReads = 1, 1, 1
	second, err := OpenJournal(ctx, store, "disk", "second-owner", "", 4096)
	require.NoError(t, err, "acquisition must survive a lost response and a lagging readback")
	defer second.Close()
	require.Zero(t, store.pendingFaults(), "every injected fault must be exercised")
	require.NoError(t, second.Commit(ctx, nil), "the adopted version must be the committed one")
	require.Equal(t, "saved", replaySaved(t, second))
}

// While acquiring, a rejected write's readback can lag to the previous owner's
// head from before it released, whose lease is still running. That owner never
// writes again, so this is the store lagging, not a takeover.
func TestJournalAcquisitionRetriesLaggingPreviousOwner(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "saved")))
	require.NoError(t, first.Close())

	store.freshReads, store.rejectWrites, store.staleReads = 1, 1, 1
	second, err := OpenJournal(ctx, store, "disk", "second-owner", "", 4096)
	require.NoError(t, err, "a lagging readback of the released owner is not a takeover")
	defer second.Close()
	require.Zero(t, store.pendingFaults(), "every injected fault must be exercised")
	require.Equal(t, "saved", replaySaved(t, second))
}

// racingJournalStore lets another writer commit just before a head write.
type racingJournalStore struct {
	*memoryJournalStore
	race func()
}

func (s *racingJournalStore) WriteVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if race := s.race; race != nil {
		s.race = nil
		race()
	}
	return s.memoryJournalStore.WriteVersion(ctx, key, data, version)
}

// Retrying an acquisition never lets it overwrite a competitor that acquired
// the disk first; it fails once its lease period ends.
func TestJournalAcquisitionLosesToCompetitor(t *testing.T) {
	shortLease(t, time.Second)
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	var competitor *Journal
	racing := &racingJournalStore{memoryJournalStore: store, race: func() {
		competitor, err = OpenJournal(ctx, store, "disk", "competitor", "", 4096)
		require.NoError(t, err)
	}}
	_, lateErr := OpenJournal(ctx, racing, "disk", "late-owner", "", 4096)
	require.Error(t, lateErr)
	require.NotNil(t, competitor)
	defer competitor.Close()
	head, _ := store.head(t, "disk/head.json")
	require.Equal(t, "competitor", head.Owner)
	require.NoError(t, competitor.Commit(ctx, nil), "the competitor must still own the disk")
}

// A store outage shorter than the lease is absorbed everywhere the journal
// talks to the store: acquiring, committing (segment and head), renewing the
// lease in the background, and replaying. Nothing is poisoned and every
// acknowledged write is recovered. The outages last longer than a handful of
// backoff steps, so an attempt-capped retry would not pass.
func TestJournalSurvivesOutageShorterThanLease(t *testing.T) {
	shortLease(t, 4*time.Second)
	const outage = 1600 * time.Millisecond
	ctx := context.Background()
	store := newMemoryJournalStore()

	store.outage(outage)
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err, "acquisition must ride out a short outage")
	require.GreaterOrEqual(t, store.refused(), 1, "the outage must actually be hit")

	start := time.Now()
	store.outage(outage)
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "saved")))
	require.GreaterOrEqual(t, time.Since(start), outage, "the commit must wait for the store, not fail")
	require.NoError(t, first.Check())

	// Let the background renewal meet an outage on its own and recover.
	refused := store.refused()
	store.outage(outage)
	time.Sleep(outage + journalLease/3)
	require.Greater(t, store.refused(), refused, "a renewal must have been refused during the outage")
	require.NoError(t, first.Check(), "a renewal that recovers before the lease lapses must not fence")
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "again")))
	require.NoError(t, first.Close())

	second, err := OpenJournal(ctx, store, "disk", "second-owner", "", 4096)
	require.NoError(t, err)
	defer second.Close()
	store.outage(outage)
	require.Equal(t, "savedagain", replaySaved(t, second), "replay must retry segment downloads")
}

// An outage that outlasts the lease fences the owner, and only then. The
// write that could not be committed is reported as failed, never silently
// dropped, and a replacement acquires the disk with everything that was
// acknowledged before the outage.
func TestJournalOutageLongerThanLeaseFences(t *testing.T) {
	shortLease(t, time.Second)
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "saved")))
	committed, _ := store.head(t, "disk/head.json")

	store.outage(time.Minute)
	start := time.Now()
	require.Error(t, first.Commit(ctx, journalRecord(t, 0, "lost")))
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, store.refused(), 3, "the commit must keep retrying through the outage")
	require.Less(t, elapsed, 2*journalLease, "the commit must give up once the lease has lapsed")
	require.Error(t, first.Check(), "a journal that could not hold its lease is poisoned")
	require.Error(t, first.Close())

	store.outage(0)
	second, err := OpenJournal(ctx, store, "disk", "second-owner", "", 4096)
	require.NoError(t, err, "the replacement acquires once the lapsed lease has passed")
	defer second.Close()
	require.Equal(t, "saved", replaySaved(t, second), "only acknowledged writes are recovered")
	head, _ := store.head(t, "disk/head.json")
	require.Equal(t, committed.Sequence, head.Sequence, "the failed commit must not have reached the head")
}

// A head written by a replacement whose lease outlives ours can only mean a
// takeover; the old writer fails at once instead of waiting out its lease,
// and never overwrites the newer head.
func TestJournalTakeoverFencesImmediately(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	journal, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	defer journal.Close()

	key := "disk/head.json"
	foreign, foreignVersion := replaceHead(t, store, key, "replacement-owner", time.Now().Add(2*journalLease))

	start := time.Now()
	require.ErrorIs(t, journal.Commit(ctx, nil), errFenced)
	require.Less(t, time.Since(start), journalLease/2, "a takeover must not be retried until the lease lapses")
	require.Error(t, journal.Check(), "a fenced journal stays failed")
	stored, current, err := store.ReadVersion(ctx, key)
	require.NoError(t, err)
	require.Equal(t, foreignVersion, current)
	require.JSONEq(t, string(foreign), string(stored))
}

// A journal that fails while it still holds its lease releases the head it
// last committed on close, so the replacement neither waits out the lease nor
// sees the checkpoint that was attempted after the failure.
func TestJournalFailedCloseReleasesCommittedHead(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "saved")))
	committed, _ := store.head(t, "disk/head.json")

	first.Fail(errors.New("block device request failed"))
	require.Error(t, first.Checkpoint(ctx, committed.Sequence, "uncommitted-snapshot"))
	require.Error(t, first.Close())

	released, _ := store.head(t, "disk/head.json")
	require.True(t, released.Expires.IsZero(), "a failed owner must release its lease")
	committed.Expires = released.Expires
	require.Equal(t, committed, released, "only the lease may change")

	start := time.Now()
	second, err := OpenJournal(ctx, store, "disk", "second-owner", "", 4096)
	require.NoError(t, err)
	defer second.Close()
	require.Less(t, time.Since(start), journalLease/2, "the replacement must not wait out the failed lease")
	require.Equal(t, "saved", replaySaved(t, second))
}

// The release outlasts a store outage shorter than the lease; a single failed
// read would leave the replacement waiting out the whole lease.
func TestJournalFailedCloseReleasesAfterOutage(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	require.NoError(t, first.Commit(ctx, journalRecord(t, 0, "saved")))

	first.Fail(errors.New("block device request failed"))
	store.outage(500 * time.Millisecond)
	require.Error(t, first.Close())

	require.Positive(t, store.refused(), "the release must have met the outage")
	released, _ := store.head(t, "disk/head.json")
	require.True(t, released.Expires.IsZero(), "a failed owner must release its lease once the store is back")
}

// A failed journal whose head was replaced leaves the replacement's head alone.
func TestJournalFailedCloseLeavesReplacedHead(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	journal, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)

	key := "disk/head.json"
	foreign, _ := replaceHead(t, store, key, "replacement-owner", time.Now().Add(2*journalLease))

	require.ErrorIs(t, journal.Commit(ctx, nil), errFenced)
	require.Error(t, journal.Close())
	stored, _, err := store.ReadVersion(ctx, key)
	require.NoError(t, err)
	require.JSONEq(t, string(foreign), string(stored))
}

// A foreign head without a newer lease could be a lagging readback of an
// earlier owner, so it is retried like any other rejection, but it is never
// adopted and the retries stop when the lease lapses.
func TestJournalStaleForeignHeadIsRetriedNotAdopted(t *testing.T) {
	shortLease(t, 3*time.Second)
	ctx := context.Background()
	store := newMemoryJournalStore()
	journal, err := OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	defer journal.Close()

	key := "disk/head.json"
	foreign, foreignVersion := replaceHead(t, store, key, "earlier-owner", time.Time{})

	start := time.Now()
	err = journal.Commit(ctx, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, errFenced)
	require.GreaterOrEqual(t, time.Since(start), journalLease/2, "the write must be retried until the lease lapses")
	require.Error(t, journal.Check())
	stored, current, err := store.ReadVersion(ctx, key)
	require.NoError(t, err)
	require.Equal(t, foreignVersion, current)
	require.JSONEq(t, string(foreign), string(stored))
}

// Segments are fetched concurrently but must land in commit order, including
// across fetch windows and when a later segment overwrites an earlier one.
func TestJournalReplayPreservesCommitOrder(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	journal, err := OpenJournal(ctx, store, "disk", "owner", "", 4096)
	require.NoError(t, err)
	const segments = 40
	for i := 0; i < segments; i++ {
		require.NoError(t, journal.Commit(ctx, journalRecord(t, uint64(i%8)*512, fmt.Sprintf("segment-%02d", i))))
	}
	require.NoError(t, journal.Close())

	recovered, err := OpenJournal(ctx, store, "disk", "recovered", "", 4096)
	require.NoError(t, err)
	defer recovered.Close()
	var replayed []string
	image := make(map[uint64]string)
	require.NoError(t, recovered.Replay(ctx, func(offset uint64, data []byte) error {
		replayed = append(replayed, string(data))
		image[offset] = string(data)
		return nil
	}))
	require.Len(t, replayed, segments)
	for i, payload := range replayed {
		require.Equal(t, fmt.Sprintf("segment-%02d", i), payload)
	}
	require.Equal(t, "segment-39", image[7*512], "the last write to an offset must win")
}
