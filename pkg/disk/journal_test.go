package disk_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/disk"
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
	require.NoError(t, client.VerifyConditionalWrites(ctx), "the worker's once-per-bucket probe")
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

	journal, err := disk.OpenJournal(ctx, store, prefix, "first-owner", "", 4096)
	require.NoError(t, err)
	defer journal.Close()
	var records bytes.Buffer
	require.NoError(t, binary.Write(&records, binary.BigEndian, uint64(0)))
	require.NoError(t, binary.Write(&records, binary.BigEndian, uint32(5)))
	records.WriteString("saved")
	store.retry.Store(true)
	require.NoError(t, journal.Commit(ctx, records.Bytes()))
	require.False(t, store.retry.Load(), "the retry must actually be exercised")
	require.NoError(t, journal.Commit(ctx, nil), "reconciliation must retain the new version")

	// A transient rejection leaves the previous head in place. Retrying must
	// keep its original condition, never replace it with an observed version.
	store.reject.Store(true)
	require.NoError(t, journal.Commit(ctx, nil))
	require.False(t, store.reject.Load(), "the rejected write must be exercised")
	require.NoError(t, journal.Commit(ctx, nil))

	// A genuinely different head must still fence the old writer.
	key := prefix + "/head.json"
	data, version, err := client.ReadVersion(ctx, key)
	require.NoError(t, err)
	var head map[string]any
	require.NoError(t, json.Unmarshal(data, &head))
	head["owner"], head["expires"] = "replacement-owner", time.Time{}
	foreign, err := json.Marshal(head)
	require.NoError(t, err)
	_, err = client.WriteVersion(ctx, key, foreign, version)
	require.NoError(t, err)
	require.Error(t, journal.Commit(ctx, nil))
	data, _, err = client.ReadVersion(ctx, key)
	require.NoError(t, err)
	require.JSONEq(t, string(foreign), string(data))

	// Recovery must replay the acknowledged data from the object store, and
	// acquiring it must survive the same lost response and lagging readback
	// while the stored head still names the previous owner.
	store.retry.Store(true)
	recovered, err := disk.OpenJournal(ctx, store, prefix, "recovered-owner", "", 4096)
	require.NoError(t, err)
	defer recovered.Close()
	require.False(t, store.retry.Load(), "the acquisition retry must actually be exercised")
	var restored []byte
	require.NoError(t, recovered.Replay(ctx, func(offset uint64, data []byte) error {
		require.Zero(t, offset)
		restored = append(restored, data...)
		return nil
	}))
	require.Equal(t, "saved", string(restored))
}

// memoryJournalStore is an in-memory JournalStore with compare-and-set heads
// and the store behaviours the journal must survive: a precondition rejected
// although it held, a committed write reported as rejected, and readbacks
// that lag the latest write.
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
}

type memoryHead struct {
	data    []byte
	version string
}

var errMemoryPrecondition = errors.New("api error PreconditionFailed")

func newMemoryJournalStore() *memoryJournalStore {
	return &memoryJournalStore{objects: map[string][]byte{}, current: map[string]memoryHead{}, previous: map[string]memoryHead{}}
}

func (s *memoryJournalStore) ReadVersion(_ context.Context, key string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *memoryJournalStore) Download(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func journalRecord(t *testing.T, offset uint64, payload string) []byte {
	t.Helper()
	var records bytes.Buffer
	require.NoError(t, binary.Write(&records, binary.BigEndian, offset))
	require.NoError(t, binary.Write(&records, binary.BigEndian, uint32(len(payload))))
	records.WriteString(payload)
	return records.Bytes()
}

// A rejected head write is retried with its original condition: for the
// owner's commits and renewals, and for acquisition, where the stored head
// still names the previous owner, so a readback cannot tell a lagging store
// from a competitor.
func TestJournalRetriesRejectedHeadWrites(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	first, err := disk.OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
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
	released, _, err := store.ReadVersion(ctx, "disk/head.json")
	require.NoError(t, err)
	var head struct{ Expires time.Time }
	require.NoError(t, json.Unmarshal(released, &head))
	require.True(t, head.Expires.IsZero(), "close must release the lease")

	// The released head is read once, then the acquisition write is committed
	// but reported as rejected, and its readback still shows the old owner.
	store.freshReads, store.commitRejects, store.staleReads = 1, 1, 1
	second, err := disk.OpenJournal(ctx, store, "disk", "second-owner", "", 4096)
	require.NoError(t, err, "acquisition must survive a lost response and a lagging readback")
	defer second.Close()
	require.Zero(t, store.pendingFaults(), "every injected fault must be exercised")
	require.NoError(t, second.Commit(ctx, nil), "the adopted version must be the committed one")

	var restored []byte
	require.NoError(t, second.Replay(ctx, func(offset uint64, data []byte) error {
		require.Zero(t, offset)
		restored = append(restored, data...)
		return nil
	}))
	require.Equal(t, "saved", string(restored))
}

// Retrying never relaxes the fence: a head written by another owner keeps
// rejecting the original condition until the journal gives up and fails.
func TestJournalRetryNeverOverwritesForeignHead(t *testing.T) {
	ctx := context.Background()
	store := newMemoryJournalStore()
	journal, err := disk.OpenJournal(ctx, store, "disk", "first-owner", "", 4096)
	require.NoError(t, err)
	defer journal.Close()

	key := "disk/head.json"
	data, version, err := store.ReadVersion(ctx, key)
	require.NoError(t, err)
	var head map[string]any
	require.NoError(t, json.Unmarshal(data, &head))
	head["owner"] = "replacement-owner"
	foreign, err := json.Marshal(head)
	require.NoError(t, err)
	foreignVersion, err := store.WriteVersion(ctx, key, foreign, version)
	require.NoError(t, err)

	require.Error(t, journal.Commit(ctx, nil))
	require.Error(t, journal.Check(), "a fenced journal stays failed")
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
	journal, err := disk.OpenJournal(ctx, store, "disk", "owner", "", 4096)
	require.NoError(t, err)
	const segments = 40
	for i := 0; i < segments; i++ {
		require.NoError(t, journal.Commit(ctx, journalRecord(t, uint64(i%8)*512, fmt.Sprintf("segment-%02d", i))))
	}
	require.NoError(t, journal.Close())

	recovered, err := disk.OpenJournal(ctx, store, "disk", "recovered", "", 4096)
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
