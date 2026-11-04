package repository

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func diskBatchRequest(name string, ws uint) *vmDiskRequest {
	return &vmDiskRequest{ctx: context.Background(), input: vmDiskInput{WorkspaceID: ws, Name: name,
		Size: "1Gi", Filesystem: "ext4", Driver: types.DurableDiskDriverQcow, MountPath: "/"}, done: make(chan vmDiskResult, 1)}
}

func TestDiskBurstBatchWaitsForDatabaseAndDeduplicates(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	first, duplicate := diskBatchRequest("first", 1), diskBatchRequest("first", 1)
	mock.ExpectQuery(`INSERT INTO disk`).WithArgs(sqlmock.AnyArg()).WillDelayFor(80 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "workspace_id", "name"}).AddRow(3, 1, "first"))
	finished := make(chan struct{})
	go func() {
		(&vmDiskBatcher{repo: repo.(*PostgresBackendRepository)}).flush([]*vmDiskRequest{first, duplicate})
		close(finished)
	}()
	select {
	case <-first.done:
		t.Fatal("acknowledged root disk before database result")
	case <-time.After(10 * time.Millisecond):
	}
	<-finished
	one, two := <-first.done, <-duplicate.done
	require.NoError(t, one.err)
	require.NoError(t, two.err)
	require.Equal(t, one.disk.Id, two.disk.Id)
	one.disk.Size = "changed by caller"
	require.NotEqual(t, one.disk.Size, two.disk.Size)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDiskBurstBatchDoesNotReplayAmbiguousFailure(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	first, second := diskBatchRequest("first", 1), diskBatchRequest("second", 1)
	mock.ExpectQuery(`INSERT INTO disk`).WillReturnError(context.DeadlineExceeded)
	(&vmDiskBatcher{repo: repo.(*PostgresBackendRepository)}).flush([]*vmDiskRequest{first, second})
	require.ErrorIs(t, (<-first.done).err, context.DeadlineExceeded)
	require.ErrorIs(t, (<-second.done).err, context.DeadlineExceeded)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDiskBurstBatchIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE disk (LIKE public.disk INCLUDING DEFAULTS INCLUDING INDEXES);
		CREATE TEMP SEQUENCE disk_burst_test_id;
		ALTER TABLE disk ALTER COLUMN id SET DEFAULT nextval('disk_burst_test_id');
		ALTER TABLE disk ADD CHECK (name <> 'reject')`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db, config: types.PostgresConfig{VMBurstBatchSize: 128}}
	batcher := &vmDiskBatcher{repo: repo}
	first, same, other := diskBatchRequest("first", 1), diskBatchRequest("first", 1), diskBatchRequest("first", 2)
	batcher.flush([]*vmDiskRequest{first, same, other})
	one, duplicate, separate := <-first.done, <-same.done, <-other.done
	require.NoError(t, one.err)
	require.NoError(t, duplicate.err)
	require.NoError(t, separate.err)
	require.Equal(t, one.disk.Id, duplicate.disk.Id)
	require.NotEqual(t, one.disk.Id, separate.disk.Id)
	// A concurrent creator's ON CONFLICT winner is returned by the normal
	// lookup, while other rows in the batch are committed independently.
	existing, fresh := diskBatchRequest("first", 1), diskBatchRequest("fresh", 1)
	batcher.flush([]*vmDiskRequest{existing, fresh})
	result := <-existing.done
	require.NoError(t, result.err)
	require.Equal(t, one.disk.Id, result.disk.Id)
	require.NoError(t, (<-fresh.done).err)
	valid, invalid := diskBatchRequest("valid", 1), diskBatchRequest("reject", 1)
	batcher.flush([]*vmDiskRequest{valid, invalid})
	require.NoError(t, (<-valid.done).err)
	require.Error(t, (<-invalid.done).err)
	ctx := ContextWithVMDiskBurst(context.Background())
	resized, err := repo.GetOrCreateDisk(ctx, 1, &types.Disk{Name: "first", Size: "2Gi", Driver: types.DurableDiskDriverQcow})
	require.NoError(t, err)
	require.Equal(t, "2Gi", resized.Size)
	_, err = repo.GetOrCreateDisk(ctx, 1, &types.Disk{Name: "first", Size: "1Gi"})
	require.ErrorContains(t, err, "cannot shrink")
	_, err = repo.GetOrCreateDisk(ctx, 1, &types.Disk{Name: "first", Driver: types.DurableDiskDriverSnapshot})
	require.ErrorContains(t, err, "changing drivers")
	var wg sync.WaitGroup
	gate := make(chan struct{})
	errors := make(chan error, 100)
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			_, err := repo.GetOrCreateDisk(ctx, 1, &types.Disk{Name: fmt.Sprintf("burst-%d", i), Size: "1Gi", Driver: types.DurableDiskDriverQcow})
			errors <- err
		}(i)
	}
	close(gate)
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM disk WHERE name LIKE 'burst-%'`))
	require.Equal(t, 100, count)
}
