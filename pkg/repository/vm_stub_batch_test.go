package repository

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func stubBatchRequest(ctx context.Context, name string, app uint) *vmStubRequest {
	return &vmStubRequest{ctx: ctx, input: stubUpsertInput{
		Name: name, Type: string(types.StubTypeVM), Config: json.RawMessage(`{"runtime":{"image_id":"batch-test"}}`),
		ObjectID: 1, WorkspaceID: 1, AppID: app,
	}, done: make(chan vmStubResult, 1)}
}

func TestVMStubBatchWaitsForDatabaseAndDeduplicates(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	batcher := &vmStubBatcher{repo: repo.(*PostgresBackendRepository)}
	first := stubBatchRequest(context.Background(), "same", 1)
	second := stubBatchRequest(context.Background(), "same", 1)
	first.input.Config = json.RawMessage(`{"runtime":{"image_id":"batch-test","cpu":9007199254740993},"env":{"a":"one","b":"two"}}`)
	second.input.Config = json.RawMessage(`{"env":{"b":"two","a":"one"},"runtime":{"cpu":9007199254740993,"image_id":"batch-test"}}`)
	mock.ExpectQuery(`WITH input AS MATERIALIZED`).WithArgs(sqlmock.AnyArg()).
		WillDelayFor(80 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"batch_index", "id", "name"}).AddRow(1, 7, "same"))
	finished := make(chan struct{})
	go func() { batcher.flush([]*vmStubRequest{first, second}); close(finished) }()
	select {
	case <-first.done:
		t.Fatal("published stub before database acknowledgement")
	case <-time.After(10 * time.Millisecond):
	}
	<-finished
	for _, request := range []*vmStubRequest{first, second} {
		result := <-request.done
		require.NoError(t, result.err)
		require.Equal(t, uint(7), result.stub.Id)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMStubBatchSkipsCanceledRequests(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := stubBatchRequest(ctx, "canceled", 1)
	(&vmStubBatcher{repo: repo.(*PostgresBackendRepository)}).flush([]*vmStubRequest{request})
	require.ErrorIs(t, (<-request.done).err, context.Canceled)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMStubBatchDoesNotReplayAmbiguousFailure(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	first := stubBatchRequest(context.Background(), "first", 1)
	second := stubBatchRequest(context.Background(), "second", 1)
	mock.ExpectQuery(`WITH input AS MATERIALIZED`).WillReturnError(context.DeadlineExceeded)
	(&vmStubBatcher{repo: repo.(*PostgresBackendRepository)}).flush([]*vmStubRequest{first, second})
	require.ErrorIs(t, (<-first.done).err, context.DeadlineExceeded)
	require.ErrorIs(t, (<-second.done).err, context.DeadlineExceeded)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMStubBatchIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE app (LIKE public.app INCLUDING DEFAULTS);
		CREATE TEMP TABLE stub (LIKE public.stub INCLUDING DEFAULTS);
		CREATE TEMP SEQUENCE stub_batch_test_id;
		ALTER TABLE stub ALTER COLUMN id SET DEFAULT nextval('stub_batch_test_id');
		ALTER TABLE stub ADD CHECK (name <> 'reject');
		INSERT INTO app (id, name, workspace_id, description, updated_at)
		VALUES (1, 'batch-test', 1, '', NOW()-INTERVAL '1 hour'), (2, 'other', 1, '', NOW()-INTERVAL '1 hour');`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db, config: types.PostgresConfig{VMBurstBatchSize: 128}}
	batcher := &vmStubBatcher{repo: repo}
	ctx := context.Background()
	first := stubBatchRequest(ctx, "first", 1)
	same := stubBatchRequest(ctx, "first", 1)
	otherApp := stubBatchRequest(ctx, "first", 2)
	batcher.flush([]*vmStubRequest{first, same, otherApp})
	created, duplicate, other := <-first.done, <-same.done, <-otherApp.done
	require.NoError(t, created.err)
	require.NoError(t, duplicate.err)
	require.NoError(t, other.err)
	require.Equal(t, created.stub.Id, duplicate.stub.Id)
	require.NotEqual(t, created.stub.Id, other.stub.Id)
	_, err = db.Exec(`UPDATE stub SET updated_at=NOW()-INTERVAL '1 hour'; UPDATE app SET updated_at=NOW()-INTERVAL '1 hour'`)
	require.NoError(t, err)
	reused, fresh := stubBatchRequest(ctx, "first", 1), stubBatchRequest(ctx, "fresh", 1)
	batcher.flush([]*vmStubRequest{reused, fresh})
	result := <-reused.done
	require.NoError(t, result.err)
	require.Equal(t, created.stub.Id, result.stub.Id)
	require.WithinDuration(t, time.Now(), result.stub.UpdatedAt.Time, time.Minute)
	require.NoError(t, (<-fresh.done).err)
	var active bool
	require.NoError(t, db.Get(&active, `SELECT updated_at > NOW()-INTERVAL '1 minute' FROM app WHERE id=1`))
	require.True(t, active)
	// A rejected row rolls the shared statement back. Independent retries
	// commit the valid row once and return the constraint failure to its owner.
	valid, invalid := stubBatchRequest(ctx, "valid", 1), stubBatchRequest(ctx, "reject", 1)
	batcher.flush([]*vmStubRequest{valid, invalid})
	require.NoError(t, (<-valid.done).err)
	require.Error(t, (<-invalid.done).err)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM stub WHERE name='valid'`))
	require.Equal(t, 1, count)
	// Exercise the actual collection queue with duplicate concurrent callers.
	var wg sync.WaitGroup
	results := make(chan vmStubResult, 100)
	gate := make(chan struct{})
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			stub, err := repo.getOrCreateVMStubBatched(ctx, stubBatchRequest(ctx, "burst", 1).input)
			results <- vmStubResult{stub: stub, err: err}
		}()
	}
	close(gate)
	wg.Wait()
	close(results)
	var id uint
	for result := range results {
		require.NoError(t, result.err)
		if id == 0 {
			id = result.stub.Id
		}
		require.Equal(t, id, result.stub.Id)
	}
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM stub WHERE name='burst'`))
	require.Equal(t, 1, count)
}

func TestStubInputCanonicalizationPreservesLargeIntegers(t *testing.T) {
	first := stubBatchRequest(context.Background(), "same", 1).input
	first.Config = json.RawMessage(`{"n":9007199254740993}`)
	second := first
	second.Config = json.RawMessage(`{"n":9007199254740992}`)
	a, err := canonicalStubInput(first)
	require.NoError(t, err)
	b, err := canonicalStubInput(second)
	require.NoError(t, err)
	require.NotEqual(t, string(a), string(b), "distinct JSONB integers must not be deduplicated")
}
