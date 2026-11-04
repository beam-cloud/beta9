package repository

import (
	"context"
	"encoding/json"
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
	repo, mock := NewBackendPostgresRepositoryForTest()
	first, second := mutationBatchRequest("first", true, `{}`), mutationBatchRequest("second", true, `{}`)
	mock.ExpectQuery(`WITH input AS MATERIALIZED`).WillReturnError(context.DeadlineExceeded)
	(&vmMutationBatcher{repo: repo.(*PostgresBackendRepository)}).flush([]*vmMutationRequest{first, second})
	require.ErrorIs(t, <-first.done, context.DeadlineExceeded)
	require.ErrorIs(t, <-second.done, context.DeadlineExceeded)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMMutationBatchIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, err = db.Exec(`CREATE TEMP TABLE persistent_vm (LIKE public.persistent_vm INCLUDING DEFAULTS INCLUDING INDEXES)`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db, config: types.PostgresConfig{VMBurstBatchSize: 128}}
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
