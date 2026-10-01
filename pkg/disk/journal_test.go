package disk_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
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
type retryJournalStore struct {
	*clients.WorkspaceStorageClient
	retry atomic.Bool
}

func (s *retryJournalStore) WriteVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	next, err := s.WorkspaceStorageClient.WriteVersion(ctx, key, data, version)
	if err == nil && s.retry.CompareAndSwap(true, false) {
		return s.WorkspaceStorageClient.WriteVersion(ctx, key, data, version)
	}
	return next, err
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

	// Recovery must replay the acknowledged data from the object store.
	recovered, err := disk.OpenJournal(ctx, client, prefix, "recovered-owner", "", 4096)
	require.NoError(t, err)
	defer recovered.Close()
	var restored []byte
	require.NoError(t, recovered.Replay(ctx, func(offset uint64, data []byte) error {
		require.Zero(t, offset)
		restored = append(restored, data...)
		return nil
	}))
	require.Equal(t, "saved", string(restored))
}
