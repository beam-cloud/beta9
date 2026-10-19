package repository

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceReadsShareWorkWithoutSharingCancellationOrCredentials(t *testing.T) {
	r := &PostgresBackendRepository{}
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	secret := "original"
	source := types.Workspace{Id: 7, Storage: &types.WorkspaceStorage{SecretKey: &secret}}
	load := func(ctx context.Context) (types.Workspace, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return source, nil
		case <-ctx.Done():
			return types.Workspace{}, ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := r.readWorkspace(ctx, "id/7", load); first <- err }()
	<-started
	cancel()
	require.ErrorIs(t, <-first, context.Canceled)
	// A canceled original caller cannot cause peers to repeat the SQL or
	// inherit its cancellation. Waiting callers keep their own deadlines.
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, err := r.readWorkspace(ctx, "id/7", load)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	require.EqualValues(t, 1, calls.Load())
	close(release)
	// Join completion before checking subsequent reads; no timer is needed.
	_, err, _ := r.workspaceReads.Do("id/7", func() (any, error) { return nil, nil })
	require.NoError(t, err)
	workspace, err := r.readWorkspace(context.Background(), "id/7", func(context.Context) (types.Workspace, error) {
		calls.Add(1)
		return source, nil
	})
	require.NoError(t, err)
	require.Equal(t, source, workspace)
	*workspace.Storage.SecretKey = "changed by caller"
	require.Equal(t, "original", secret, "callers must not share nested credential pointers")

	// No result survives the overlapping reads: rotations and metadata changes
	// are visible to the next query, including after an earlier query failed.
	updated := func(context.Context) (types.Workspace, error) {
		calls.Add(1)
		return types.Workspace{Id: 7, Name: "updated"}, nil
	}
	workspace, err = r.readWorkspace(context.Background(), "id/7", updated)
	require.NoError(t, err)
	require.Equal(t, "updated", workspace.Name)
	require.EqualValues(t, 3, calls.Load())
}

func TestWorkspaceReadKeysAndFailuresAreIndependent(t *testing.T) {
	r := &PostgresBackendRepository{}
	started, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("database unavailable")
	first := make(chan error, 1)
	go func() {
		_, err := r.readWorkspace(context.Background(), "id/7", func(context.Context) (types.Workspace, error) {
			close(started)
			<-release
			return types.Workspace{}, failure
		})
		first <- err
	}()
	<-started
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	for _, key := range []string{"id/8", "external/7"} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		workspace, err := r.readWorkspace(ctx, key, func(context.Context) (types.Workspace, error) {
			return types.Workspace{Id: 8}, nil
		})
		cancel()
		require.NoError(t, err)
		require.EqualValues(t, 8, workspace.Id)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.readWorkspace(ctx, "id/9", func(context.Context) (types.Workspace, error) {
		t.Error("an already canceled call started a database query")
		return types.Workspace{}, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	// The blocked reader's error must not poison a subsequent read.
	close(release)
	require.ErrorIs(t, <-first, failure)
	_, err = r.readWorkspace(context.Background(), "id/7", func(context.Context) (types.Workspace, error) {
		return types.Workspace{}, nil
	})
	require.NoError(t, err)
}
