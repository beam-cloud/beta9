package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func newVMBatchIntegrationRepo(t *testing.T) *PostgresBackendRepository {
	t.Helper()
	repo := newVMIntegrationRepo(t)
	repo.client.SetMaxOpenConns(1)
	repo.client.SetMaxIdleConns(1)
	repo.config.VMBurstBatchSize = 128
	return repo
}

func testVMBatchFailure(t *testing.T, query string, flush func(*PostgresBackendRepository) []error) {
	t.Helper()
	for _, test := range []struct {
		name string
		err  error
	}{
		{"transport timeout", context.DeadlineExceeded},
		{"completion unknown", &pq.Error{Code: "40003"}},
		{"unrecognized rollback", &pq.Error{Code: "40XYZ"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, mock := NewBackendPostgresRepositoryForTest()
			mock.ExpectQuery(query).WillReturnError(test.err)
			for _, err := range flush(repo.(*PostgresBackendRepository)) {
				require.ErrorIs(t, err, test.err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestVMBatchRetriesOnlyKnownRollbackErrors(t *testing.T) {
	for _, code := range []pq.ErrorCode{"22003", "23505", "40000", "40001", "40002", "40P01"} {
		require.True(t, retryVMBatchIndividually(&pq.Error{Code: code}), string(code))
	}
	for _, err := range []error{nil, context.Canceled, &pq.Error{Code: "40003"}, &pq.Error{Code: "40XYZ"}, &pq.Error{Code: "08006"}} {
		require.False(t, retryVMBatchIndividually(err), "%v", err)
	}
}

func testVMBatchCancellation[T any](t *testing.T, mock sqlmock.Sqlmock, query string, rows *sqlmock.Rows, queue chan T, submit func(context.Context, string) error, flush func([]T)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	proceed := make(chan struct{})
	var once, release sync.Once
	unblock := func() { release.Do(func() { close(proceed) }) }
	t.Cleanup(unblock)
	mock.ExpectQuery(query).WithArgs(sqlArgumentMatcher(func(driver.Value) bool {
		once.Do(func() { close(started) })
		<-proceed
		return true
	})).WillReturnRows(rows)
	results := make(chan error, 2)
	for _, id := range []string{"first", "second"} {
		go func() { results <- submit(ctx, id) }()
	}
	batch := []T{<-queue, <-queue}
	go flush(batch)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch did not dispatch its database write")
	}
	cancel()
	select {
	case err := <-results:
		t.Fatalf("caller returned before its database outcome was known: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	for range 2 {
		require.NoError(t, <-results, "a committed write must not become a cancellation failure")
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMBatchCancellationAfterDispatch(t *testing.T) {
	t.Run("mutation", func(t *testing.T) {
		repo, mock := NewBackendPostgresRepositoryForTest()
		r := repo.(*PostgresBackendRepository)
		b := &vmMutationBatcher{repo: r, queue: make(chan *vmMutationRequest, 2)}
		r.vmMutationBatchOnce.Do(func() { r.vmMutationBatch = b })
		testVMBatchCancellation(t, mock, `WITH input AS MATERIALIZED`, sqlmock.NewRows([]string{"id"}).AddRow("first").AddRow("second"), b.queue, func(ctx context.Context, id string) error {
			return r.mutateVMBatched(ctx, mutationBatchRequest(id, true, `{}`).input)
		}, b.flush)
	})
	t.Run("stub", func(t *testing.T) {
		repo, mock := NewBackendPostgresRepositoryForTest()
		r := repo.(*PostgresBackendRepository)
		b := &vmStubBatcher{repo: r, queue: make(chan *vmStubRequest, 2)}
		r.vmStubBatchOnce.Do(func() { r.vmStubBatch = b })
		rows := sqlmock.NewRows([]string{"batch_index", "id"}).AddRow(1, 7).AddRow(2, 8)
		testVMBatchCancellation(t, mock, `WITH input AS MATERIALIZED`, rows, b.queue, func(ctx context.Context, id string) error {
			_, err := r.getOrCreateVMStubBatched(ctx, stubBatchRequest(ctx, id, 1).input)
			return err
		}, b.flush)
	})
	t.Run("disk", func(t *testing.T) {
		repo, mock := NewBackendPostgresRepositoryForTest()
		r := repo.(*PostgresBackendRepository)
		b := &vmDiskBatcher{repo: r, queue: make(chan *vmDiskRequest, 2)}
		r.vmDiskBatchOnce.Do(func() { r.vmDiskBatch = b })
		rows := sqlmock.NewRows([]string{"id", "workspace_id", "name"}).AddRow(1, 1, "first").AddRow(2, 1, "second")
		testVMBatchCancellation(t, mock, `INSERT INTO disk`, rows, b.queue, func(ctx context.Context, id string) error {
			_, err := r.createVMDiskBatched(ctx, diskBatchRequest(id, 1).input)
			return err
		}, b.flush)
	})
}

func mutationBatchRequest(id string, create bool, data string) *vmMutationRequest {
	return &vmMutationRequest{ctx: context.Background(), input: vmMutationInput{Create: create, Row: vmMutationRow{
		ID: id, WorkspaceID: 1, WorkspaceExternalID: "workspace", TokenID: "token", Name: id, Handle: id,
		Data: json.RawMessage(data), LastActiveAt: time.Now().UTC(),
	}}, done: make(chan error, 1)}
}

func TestVMMutationBatchWaitsForDatabaseAndReportsMissingRows(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	batcher := &vmMutationBatcher{repo: repo.(*PostgresBackendRepository)}
	first, second := mutationBatchRequest("first", false, `{}`), mutationBatchRequest("missing", false, `{}`)
	mock.ExpectQuery(`WITH input AS MATERIALIZED`).WithArgs(sqlmock.AnyArg()).
		WillDelayFor(80 * time.Millisecond).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("first"))
	finished := make(chan struct{})
	go func() { batcher.flush([]*vmMutationRequest{first, second}); close(finished) }()
	select {
	case <-first.done:
		t.Fatal("acknowledged VM before database result")
	case <-time.After(10 * time.Millisecond):
	}
	<-finished
	require.NoError(t, <-first.done)
	require.EqualError(t, <-second.done, "VM not found")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMMutationBatchDoesNotReplayAmbiguousFailure(t *testing.T) {
	testVMBatchFailure(t, `WITH input AS MATERIALIZED`, func(repo *PostgresBackendRepository) []error {
		first, second := mutationBatchRequest("first", true, `{}`), mutationBatchRequest("second", true, `{}`)
		(&vmMutationBatcher{repo: repo}).flush([]*vmMutationRequest{first, second})
		return []error{<-first.done, <-second.done}
	})
}

func TestVMMutationBatchIntegration(t *testing.T) {
	repo := newVMBatchIntegrationRepo(t)
	db := repo.client
	var err error
	_, err = db.Exec(`CREATE TEMP TABLE persistent_vm (LIKE public.persistent_vm INCLUDING DEFAULTS INCLUDING INDEXES)`)
	require.NoError(t, err)
	batcher := &vmMutationBatcher{repo: repo}
	first, second := mutationBatchRequest("first", true, `{"state":1}`), mutationBatchRequest("second", true, `{}`)
	first.input.Row.PublicIPv6 = nullableVMAddress("fd00::1")
	batcher.flush([]*vmMutationRequest{first, second})
	require.NoError(t, <-first.done)
	require.NoError(t, <-second.done)
	// Same-VM mutations must retain FIFO order, rather than letting UPDATE
	// FROM select an arbitrary state from duplicated identifiers.
	one, two := mutationBatchRequest("first", false, `{"state":2}`), mutationBatchRequest("first", false, `{"state":3}`)
	batcher.flush([]*vmMutationRequest{one, two})
	require.NoError(t, <-one.done)
	require.NoError(t, <-two.done)
	var state string
	require.NoError(t, db.Get(&state, `SELECT data->>'state' FROM persistent_vm WHERE id='first'`))
	require.Equal(t, "3", state)
	var address string
	require.NoError(t, db.Get(&address, `SELECT host(public_ipv6) FROM persistent_vm WHERE id='first'`))
	require.Equal(t, "fd00::1", address)
	// A nonexistent update does not roll back healthy peers or claim success.
	one, missing := mutationBatchRequest("first", false, `{"state":4}`), mutationBatchRequest("missing", false, `{}`)
	batcher.flush([]*vmMutationRequest{one, missing})
	require.NoError(t, <-one.done)
	require.EqualError(t, <-missing.done, "VM not found")
	// Constraint failures abort the whole statement before isolated retries.
	duplicate, healthy := mutationBatchRequest("first", true, `{}`), mutationBatchRequest("healthy", true, `{}`)
	batcher.flush([]*vmMutationRequest{duplicate, healthy})
	require.Error(t, <-duplicate.done)
	require.NoError(t, <-healthy.done)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM persistent_vm WHERE id='healthy'`))
	require.Equal(t, 1, count)
	// Exercise public CreateVM/SaveVM entrypoints under a real concurrent burst.
	var wg sync.WaitGroup
	gate := make(chan struct{})
	errors := make(chan error, 100)
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			id := fmt.Sprintf("burst-%d", i)
			vm := &types.VM{ID: id, WorkspaceID: 1, WorkspaceExternalID: "workspace", TokenID: "token", Name: id, Handle: id, LastActiveAt: time.Now().UTC()}
			if err := repo.CreateVM(context.Background(), vm); err != nil {
				errors <- err
				return
			}
			vm.Status = types.VMStateStarting
			errors <- repo.SaveVM(context.Background(), vm)
		}(i)
	}
	close(gate)
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM persistent_vm WHERE id LIKE 'burst-%'`))
	require.Equal(t, 100, count)
}

func stubBatchRequest(ctx context.Context, name string, app uint) *vmStubRequest {
	return &vmStubRequest{ctx: ctx, input: types.StubUpsertInput{
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
	testVMBatchFailure(t, `WITH input AS MATERIALIZED`, func(repo *PostgresBackendRepository) []error {
		first, second := stubBatchRequest(context.Background(), "first", 1), stubBatchRequest(context.Background(), "second", 1)
		(&vmStubBatcher{repo: repo}).flush([]*vmStubRequest{first, second})
		return []error{(<-first.done).err, (<-second.done).err}
	})
}

func TestVMStubBatchIntegration(t *testing.T) {
	repo := newVMBatchIntegrationRepo(t)
	db := repo.client
	var err error
	_, err = db.Exec(`CREATE TEMP TABLE app (LIKE public.app INCLUDING DEFAULTS);
		CREATE TEMP TABLE stub (LIKE public.stub INCLUDING DEFAULTS);
		CREATE TEMP SEQUENCE stub_batch_test_id;
		ALTER TABLE stub ALTER COLUMN id SET DEFAULT nextval('stub_batch_test_id');
		ALTER TABLE stub ADD CHECK (name <> 'reject');
		INSERT INTO app (id, name, workspace_id, description, updated_at)
		VALUES (1, 'batch-test', 1, '', NOW()-INTERVAL '1 hour'), (2, 'other', 1, '', NOW()-INTERVAL '1 hour');`)
	require.NoError(t, err)
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

func diskBatchRequest(name string, ws uint) *vmDiskRequest {
	return &vmDiskRequest{ctx: context.Background(), input: types.DiskCreateInput{WorkspaceID: ws, Name: name,
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
	testVMBatchFailure(t, `INSERT INTO disk`, func(repo *PostgresBackendRepository) []error {
		first, second := diskBatchRequest("first", 1), diskBatchRequest("second", 1)
		(&vmDiskBatcher{repo: repo}).flush([]*vmDiskRequest{first, second})
		return []error{(<-first.done).err, (<-second.done).err}
	})
}

func TestDiskBurstBatchIntegration(t *testing.T) {
	repo := newVMBatchIntegrationRepo(t)
	db := repo.client
	var err error
	_, err = db.Exec(`CREATE TEMP TABLE disk (LIKE public.disk INCLUDING DEFAULTS INCLUDING INDEXES);
		CREATE TEMP SEQUENCE disk_burst_test_id;
		ALTER TABLE disk ALTER COLUMN id SET DEFAULT nextval('disk_burst_test_id');
		ALTER TABLE disk ADD CHECK (name <> 'reject')`)
	require.NoError(t, err)
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
