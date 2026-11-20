package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newVMIntegrationRepo(t *testing.T) *PostgresBackendRepository {
	t.Helper()
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return &PostgresBackendRepository{client: db}
}

func vmIntegrationWorkspace(t *testing.T) uint64 {
	t.Helper()
	id, err := strconv.ParseUint(os.Getenv("BETA9_VM_TEST_WORKSPACE_ID"), 10, 32)
	require.NoError(t, err)
	return id
}

func TestVMLockQueueBatchesAndBoundsIndependentRequests(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	s := &vmLockSession{client: r.client, gate: make(chan struct{}, 1), batchRunning: true}
	var done sync.WaitGroup
	for i := range 300 {
		done.Add(1)
		go func() {
			defer done.Done()
			result := s.enqueue(context.Background(), fmt.Sprint(i))
			require.NoError(t, result.err)
			require.NotNil(t, result.busy)
		}()
	}
	require.Eventually(t, func() bool {
		s.batchMu.Lock()
		defer s.batchMu.Unlock()
		return len(s.pending) == 300
	}, time.Second, time.Millisecond)
	for offset := 0; offset < 300; offset += 128 {
		ids := []string{}
		rows := sqlmock.NewRows([]string{"id", "acquired"})
		for _, request := range s.pending[offset:min(offset+128, 300)] {
			ids = append(ids, request.id)
			rows.AddRow(request.id, false)
		}
		mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array(ids)).WillReturnRows(rows)
	}
	s.flushAcquires()
	done.Wait()
	require.Empty(t, s.pending)
	require.False(t, s.batchRunning)
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockQueueCancellationDoesNotWaitForAnotherBatch(t *testing.T) {
	s := &vmLockSession{batchRunning: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan vmLockResult, 1)
	go func() { done <- s.enqueue(ctx, "queued") }()
	require.Eventually(t, func() bool {
		s.batchMu.Lock()
		defer s.batchMu.Unlock()
		return len(s.pending) == 1
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case result := <-done:
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("a canceled queued request waited for another acquisition")
	}
	s.flushAcquires()
	require.Empty(t, s.pending)
	require.False(t, s.batchRunning)
}

func TestVMLocksLeaveConnectionsForLifecycleQueries(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	ctx := context.Background()

	lock := func(id string, acquired bool) {
		mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{id})).
			WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow(id, acquired))
	}
	unlock := func(id string) {
		mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(id).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	lock("a", true)
	a, err := r.LockVM(ctx, "a")
	require.NoError(t, err)
	lock("b", true)
	b, err := r.LockVM(ctx, "b")
	require.NoError(t, err)

	// Independent lifecycles share one lock session, leaving a connection for
	// queries even while both operations are in progress.
	mock.ExpectExec(`UPDATE persistent_vm SET last_active_at`).WithArgs("a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.TouchVM(ctx, "a"))
	_, err = r.TryLockVM(ctx, "a")
	require.ErrorContains(t, err, "operation already in progress")
	require.Equal(t, 1, r.client.Stats().InUse)

	unlock("a")
	a()
	// A competing gateway's lock must also return its local capacity.
	lock("c", false)
	_, err = r.TryLockVM(ctx, "c")
	require.ErrorContains(t, err, "operation already in progress")
	lock("c", true)
	c, err := r.LockVM(ctx, "c")
	require.NoError(t, err)
	unlock("b")
	b()
	unlock("c")
	c()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCanceledVMLockKeepsOtherSessionLocks(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	a, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b"})).
		WillDelayFor(20 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("b", true))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").
		WillReturnResult(sqlmock.NewResult(0, 1))
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = r.LockVM(ctx, "b")
	require.ErrorIs(t, err, context.Canceled)
	_, err = r.TryLockVM(context.Background(), "a")
	require.ErrorContains(t, err, "operation already in progress")
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	a()
	a() // Unlock callbacks are idempotent.
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockWaitLeavesSharedSessionAvailable(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(ctx, "a")
	require.NoError(t, err)

	type result struct {
		unlock func()
		err    error
	}
	waiting := make(chan result, 1)
	go func() { unlock, err := r.LockVM(ctx, "a"); waiting <- result{unlock, err} }()
	select {
	case <-waiting:
		t.Fatal("waiter acquired a held lifecycle lock")
	case <-time.After(20 * time.Millisecond):
	}
	// Waiting on a cannot hold its shared session gate or a pool connection.
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("b", true))
	other, err := r.TryLockVM(ctx, "b")
	require.NoError(t, err)
	mock.ExpectExec(`UPDATE persistent_vm SET last_active_at`).WithArgs("b").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.TouchVM(ctx, "b"))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	other()
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner()
	var acquired result
	select {
	case acquired = <-waiting:
	case <-ctx.Done():
		t.Fatal("waiter did not wake after release")
	}
	require.NoError(t, acquired.err)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	acquired.unlock()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockWaitCancellationKeepsOwner(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = r.LockVM(ctx, "a")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = r.TryLockVM(context.Background(), "a")
	require.ErrorContains(t, err, "operation already in progress")
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	owner()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockWaitsForAnotherGateway(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", false))
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	unlock, err := r.LockVM(ctx, "a")
	require.NoError(t, err)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	unlock()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockBatchPreservesIndependentOwners(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	defer owner()
	s := &r.vmLocks[0]
	requests := []*vmLockAcquire{{id: "a"}, {id: "b"}, {id: "b"}, {id: "foreign"}}
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b", "foreign"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("foreign", false).AddRow("b", true))
	results, err := s.acquire(context.Background(), requests)
	require.NoError(t, err)
	require.Equal(t, (<-chan struct{})(s.held["a"]), results[requests[0]].busy.released)
	require.NotNil(t, results[requests[1]].conn)
	require.Equal(t, (<-chan struct{})(s.held["b"]), results[requests[2]].busy.released)
	require.NotNil(t, results[requests[3]].busy)
	require.Nil(t, results[requests[3]].busy.released, "a remote owner must not become a local owner")
	require.Len(t, s.held, 2)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	s.gate <- struct{}{}
	s.release("b", results[requests[1]].conn)
	<-s.gate
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	owner()
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockBatchQuarantinesAmbiguousAcquisitionsUntilOwnersFinish(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	defer owner()
	s := &r.vmLocks[0]
	connection := s.conn
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b", "c"})).
		WillReturnError(errors.New("connection lost while acquiring locks"))
	_, err = s.acquire(context.Background(), []*vmLockAcquire{{id: "b"}, {id: "c"}})
	require.ErrorContains(t, err, "connection lost")
	require.Same(t, connection, s.conn, "a failed acquire must not drop the existing owner's PostgreSQL lock")
	require.NotNil(t, s.draining)
	require.Len(t, s.held, 1)
	request := &vmLockAcquire{id: "new"}
	results, err := s.acquire(context.Background(), []*vmLockAcquire{request})
	require.NoError(t, err)
	require.NotNil(t, results[request].busy, "the uncertain session cannot acquire more locks")
	drained := results[request].busy.released
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnResult(sqlmock.NewResult(0, 1))
	owner()
	select {
	case <-drained:
	default:
		t.Fatal("waiting acquisitions were not notified that the retired session drained")
	}
	require.Nil(t, s.conn, "the uncertain connection must be discarded after its last owner finishes")
	require.Empty(t, s.held)
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockUnlockFailurePreservesOtherOwners(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	owners := make([]func(), 0, 2)
	for _, id := range []string{"a", "b"} {
		mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{id})).
			WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow(id, true))
		unlock, err := r.LockVM(context.Background(), id)
		require.NoError(t, err)
		owners = append(owners, unlock)
	}
	s := &r.vmLocks[0]
	connection := s.conn
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("a").WillReturnError(&pq.Error{Code: "57014"})
	owners[0]()
	require.Same(t, connection, s.conn)
	require.NotNil(t, s.held["b"])
	require.NotNil(t, s.draining)
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs("b").WillReturnResult(sqlmock.NewResult(0, 1))
	owners[1]()
	require.Nil(t, s.conn)
	require.Empty(t, s.held)
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockSessionFailureIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	repo.client.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := uuid.NewString()
	owner, err := repo.LockVM(ctx, id)
	require.NoError(t, err)
	defer owner()
	_, err = repo.TryLockVM(ctx, id+"\x00") // A server-rejected text argument, not a lost connection.
	var pgErr *pq.Error
	require.ErrorAs(t, err, &pgErr)
	require.NotNil(t, repo.vmLocks[0].draining)
	peer := &PostgresBackendRepository{client: repo.client}
	_, err = peer.TryLockVM(ctx, id)
	require.ErrorContains(t, err, "operation already in progress", "a different gateway must still see the original PostgreSQL lock")
	owner()
	unlock, err := peer.LockVM(ctx, id)
	require.NoError(t, err)
	unlock()
	require.Zero(t, repo.client.Stats().InUse)
}

func TestVMLockPoolIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	var err error
	db.SetMaxOpenConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	var acquired atomic.Int32
	ready := make(chan struct{})
	started := time.Now()
	for range 100 {
		group.Go(func() error {
			unlock, err := repo.LockVM(ctx, uuid.NewString())
			if err != nil {
				return err
			}
			defer unlock()
			// All locks must coexist while ordinary queries still have capacity.
			if acquired.Add(1) == 100 {
				t.Logf("100 independent VM locks acquired in %s", time.Since(started))
				close(ready)
			}
			select {
			case <-ready:
			case <-ctx.Done():
				return ctx.Err()
			}
			var value int
			return db.GetContext(ctx, &value, `SELECT 1`)
		})
	}
	require.NoError(t, group.Wait())
	require.Zero(t, db.Stats().InUse)

	// Different gateways must still contend on the original PostgreSQL key.
	peer := &PostgresBackendRepository{client: db}
	ctx, peerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer peerCancel()
	id := uuid.NewString()
	unlock, err := repo.LockVM(ctx, id)
	require.NoError(t, err)
	_, err = peer.TryLockVM(ctx, id)
	require.ErrorContains(t, err, "operation already in progress")
	unlock()
	unlock, err = peer.LockVM(ctx, id)
	require.NoError(t, err)
	unlock()
	require.Zero(t, db.Stats().InUse)
}

// Run against a migrated development database only. Rows use random names and
// are removed on completion; no existing workspace data is changed.
func TestVMRepositoryIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	var err error
	ws := vmIntegrationWorkspace(t)
	ctx := context.Background()
	id := strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	now := time.Now().UTC().Add(-time.Hour)
	v := &types.VM{ID: id, WorkspaceID: uint(ws), WorkspaceExternalID: "test", TokenID: "test", Name: "vm-test-" + id[:8], Handle: "test-" + id, DesiredState: "running", Status: "running", CreatedAt: now, LastActiveAt: now}
	require.NoError(t, repo.CreateVM(ctx, v))
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM vm_artifact WHERE vm_id=$1`, id)
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=$1`, id)
	})

	unlock, err := repo.LockVM(ctx, id)
	require.NoError(t, err)
	_, err = repo.TryLockVM(ctx, id)
	require.ErrorContains(t, err, "operation already in progress")
	unlock()
	unlock, err = repo.LockVM(ctx, id)
	require.NoError(t, err)
	unlock()

	// Activity and lifecycle writes use different columns. A stale lifecycle
	// object must not overwrite a concurrent touch or trigger an idle stop.
	require.NoError(t, repo.TouchVM(ctx, id))
	v.Status = "starting"
	require.NoError(t, repo.SaveVM(ctx, v))
	loaded, err := repo.GetVM(ctx, uint(ws), id)
	require.NoError(t, err)
	require.True(t, loaded.LastActiveAt.After(now))
	claimed, err := repo.ClaimVMIdleStop(ctx, id, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	require.False(t, claimed)
	v.Spec.IdleAction = "pause"
	require.NoError(t, repo.SaveVM(ctx, v))
	claimed, err = repo.ClaimVMIdleStop(ctx, id, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)
	loaded, err = repo.GetVM(ctx, uint(ws), id)
	require.NoError(t, err)
	require.Equal(t, "paused", loaded.DesiredState)
	claimed, err = repo.ClaimVMIdleStop(ctx, id, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.False(t, claimed)

	a := &types.VMArtifact{ID: uuid.NewString(), VMID: id, Name: "template-" + id[:8], Kind: "template", RootSnapshotID: "committed-root", CreatedAt: time.Now()}
	require.NoError(t, repo.CreateVMArtifact(ctx, uint(ws), a))
	require.NoError(t, repo.CreateVMArtifact(ctx, uint(ws), a)) // crash retry
	v.DesiredState = "deleted"
	require.NoError(t, repo.SaveVM(ctx, v))
	child := *v
	child.ID, child.Handle, child.DesiredState = uuid.NewString(), "test-"+uuid.NewString(), "running"
	require.NoError(t, repo.CreateVM(ctx, &child)) // name reuse
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=$1`, child.ID) })
	loaded, err = repo.GetVM(ctx, uint(ws), v.Name)
	require.NoError(t, err)
	require.Equal(t, child.ID, loaded.ID)
	items, err := repo.ListVMArtifacts(ctx, uint(ws), "template")
	require.NoError(t, err)
	found := false
	for _, item := range items {
		if item.ID == a.ID {
			found = true
			require.Equal(t, a.RootSnapshotID, item.RootSnapshotID)
			require.Equal(t, v.Name, item.VMName)
			require.True(t, item.VMDeleted)
		}
	}
	require.True(t, found, "template must survive source deletion")

	// Public lookup retains source identity for the owner, but hides it from
	// recipients. Names and private artifacts remain scoped to the owner.
	_, err = repo.GetVMArtifact(ctx, uint(ws+1), a.Kind, a.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	a.ID, a.Name = uuid.NewString(), "public-"+id[:8]
	require.NoError(t, repo.CreateVMArtifact(ctx, uint(ws), a))
	_, err = db.Exec(`UPDATE vm_artifact SET data=data || '{"public":true}'::jsonb WHERE id=$1`, a.ID)
	require.NoError(t, err)
	owned, err := repo.GetVMArtifact(ctx, uint(ws), a.Kind, a.ID)
	require.NoError(t, err)
	require.Equal(t, id, owned.VMID)
	require.Equal(t, v.Name, owned.VMName)
	require.True(t, owned.VMDeleted)
	shared, err := repo.GetVMArtifact(ctx, uint(ws+1), a.Kind, a.ID)
	require.NoError(t, err)
	require.Empty(t, shared.VMID)
	require.Empty(t, shared.VMName)
	require.False(t, shared.VMDeleted)
	require.Equal(t, a.RootSnapshotID, shared.RootSnapshotID)
	_, err = repo.GetVMArtifact(ctx, uint(ws+1), a.Kind, a.Name)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = repo.GetVMArtifact(ctx, uint(ws+1), types.VMArtifactSnapshot, a.ID)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = db.Exec(`UPDATE vm_artifact SET kind=$2,data=data || jsonb_build_object('kind',$2::text) WHERE id=$1`, a.ID, types.VMArtifactSnapshot)
	require.NoError(t, err)
	_, err = repo.GetVMArtifact(ctx, uint(ws+1), types.VMArtifactSnapshot, a.ID)
	require.ErrorIs(t, err, sql.ErrNoRows, "public non-template rows remain workspace-local")
}

func TestVMPagesIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	var err error
	ws := vmIntegrationWorkspace(t)
	ctx := context.Background()
	prefix := "page-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	now := time.Now().UTC().Add(-time.Hour)
	var ids []string
	for i, state := range []struct{ desired, status string }{{"running", "running"}, {"running", "starting"}, {"paused", "paused"}, {"deleted", "deleted"}} {
		id := uuid.NewString()
		// Hold the lifecycle fence so a live development reconciler leaves the fixtures alone.
		unlock, err := repo.LockVM(ctx, id)
		require.NoError(t, err)
		t.Cleanup(unlock)
		v := &types.VM{ID: id, WorkspaceID: uint(ws), WorkspaceExternalID: "test", TokenID: "test", Name: fmt.Sprintf("%s-%d", prefix, i), Handle: "test-" + id, DesiredState: state.desired, Status: state.status, CreatedAt: now.Add(time.Duration(i) * time.Minute), LastActiveAt: now, Metadata: map[string]string{"suite": prefix}}
		if i == 0 {
			v.Metadata["team"] = "a"
		}
		require.NoError(t, repo.CreateVM(ctx, v))
		ids = append(ids, id)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM vm_artifact WHERE vm_id=ANY($1)`, pq.Array(ids))
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=ANY($1)`, pq.Array(ids))
	})

	names := func(page *types.VMPage) []string {
		result := []string{}
		for _, v := range page.VMs {
			result = append(result, v.Name)
		}
		return result
	}
	suite := map[string]string{"suite": prefix}
	page, err := repo.ListVMPage(ctx, uint(ws), types.VMFilter{Limit: 10, Metadata: suite})
	require.NoError(t, err)
	require.Equal(t, 3, page.Total)
	require.Equal(t, map[string]int{"running": 1, "starting": 1, "paused": 1}, page.Counts)
	require.Equal(t, []string{prefix + "-2", prefix + "-1", prefix + "-0"}, names(page))
	page, err = repo.ListVMPage(ctx, uint(ws), types.VMFilter{Limit: 1, Offset: 1, Metadata: suite, Statuses: []string{"running", "starting"}})
	require.NoError(t, err)
	require.Equal(t, 2, page.Total)
	require.Len(t, page.Counts, 3, "counts ignore the status filter")
	require.Equal(t, []string{prefix + "-0"}, names(page))
	page, err = repo.ListVMPage(ctx, uint(ws), types.VMFilter{Limit: 10, Metadata: suite, Search: strings.ToUpper(prefix + "-1")})
	require.NoError(t, err)
	require.Equal(t, 1, page.Total)
	require.Len(t, page.Counts, 3, "counts ignore the search")
	require.Equal(t, []string{prefix + "-1"}, names(page))
	page, err = repo.ListVMPage(ctx, uint(ws), types.VMFilter{Limit: 10, Metadata: map[string]string{"suite": prefix, "team": "a"}})
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "-0"}, names(page))
	page, err = repo.ListVMPage(ctx, uint(ws), types.VMFilter{Limit: 10, Metadata: suite, Search: prefix + "_0"})
	require.NoError(t, err)
	require.Zero(t, page.Total, "search is literal, not a LIKE pattern")
	page, err = repo.ListVMPage(ctx, uint(ws), types.VMFilter{Metadata: suite, All: true})
	require.NoError(t, err)
	require.Equal(t, 4, page.Total)
	require.Equal(t, 1, page.Counts["deleted"])
	require.Empty(t, page.VMs, "a zero limit returns only counts")

	var artifacts []*types.VMArtifact
	for i, vm := range []string{ids[0], ids[0], ids[1]} {
		a := &types.VMArtifact{ID: uuid.NewString(), VMID: vm, Name: fmt.Sprintf("%s-snapshot-%d", prefix, i), Kind: "snapshot", RootSnapshotID: "root", CreatedAt: now.Add(time.Duration(i) * time.Minute)}
		require.NoError(t, repo.CreateVMArtifact(ctx, uint(ws), a))
		artifacts = append(artifacts, a)
	}
	items, total, err := repo.ListVMArtifactPage(ctx, uint(ws), "snapshot", types.VMArtifactFilter{Limit: 1, VMID: ids[0]})
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, items, 1)
	require.Equal(t, artifacts[1].ID, items[0].ID, "newest first")
	require.Equal(t, prefix+"-0", items[0].VMName)
	items, _, err = repo.ListVMArtifactPage(ctx, uint(ws), "snapshot", types.VMArtifactFilter{Limit: 1, Offset: 1, VMID: ids[0]})
	require.NoError(t, err)
	require.Equal(t, artifacts[0].ID, items[0].ID)
	_, total, err = repo.ListVMArtifactPage(ctx, uint(ws), "template", types.VMArtifactFilter{VMID: ids[0]})
	require.NoError(t, err)
	require.Zero(t, total)

	for _, name := range []string{artifacts[2].ID, artifacts[2].Name} {
		found, err := repo.GetVMArtifact(ctx, uint(ws), "snapshot", name)
		require.NoError(t, err)
		require.Equal(t, artifacts[2].ID, found.ID)
	}
	_, err = repo.GetVMArtifact(ctx, uint(ws), "template", artifacts[2].Name)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestVMReconciliationCandidatesIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	var err error
	ws := vmIntegrationWorkspace(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := uuid.NewString()
	// Hold the normal lifecycle fence so a live development reconciler cannot
	// act on this fixture while its selection states are being exercised.
	unlock, err := repo.LockVM(ctx, id)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE id=$1`, id)
		unlock()
	})
	now := time.Now().UTC()
	v := &types.VM{ID: id, WorkspaceID: uint(ws), WorkspaceExternalID: "test", TokenID: "test", Name: "vm-query-" + id[:8], Handle: "test-" + id, CreatedAt: now, LastActiveAt: now}
	require.NoError(t, repo.CreateVM(ctx, v))
	for _, tt := range []struct {
		desired, status  string
		normal, backfill bool
	}{
		{"running", "running", true, true},
		{"running", "starting", true, true},
		{"stopped", "stopped", false, true},
		{"paused", "paused", false, true},
		{"deleted", "stopped", true, true},
		{"deleted", "deleted", false, false},
	} {
		v.DesiredState, v.Status = tt.desired, tt.status
		require.NoError(t, repo.SaveVM(ctx, v))
		for _, backfill := range []bool{false, true} {
			rows, err := repo.ListVMsToReconcile(ctx, backfill)
			require.NoError(t, err)
			found := false
			for _, row := range rows {
				found = found || row.ID == id
			}
			expected := tt.normal
			if backfill {
				expected = tt.backfill
			}
			require.Equal(t, expected, found, "desired=%s status=%s backfill=%v", tt.desired, tt.status, backfill)
		}
	}
	v.DesiredState, v.Status = "paused", "paused"
	require.NoError(t, repo.SaveVM(ctx, v))
	_, err = repo.AssignVMPublicIPv6(ctx, id, "2001:db8:"+id[:4]+":"+id[4:8]+"::1")
	require.NoError(t, err)
	rows, err := repo.ListVMsToReconcile(ctx, true)
	require.NoError(t, err)
	for _, row := range rows {
		require.NotEqual(t, id, row.ID, "settled VM with an address needs no backfill")
	}
}

func TestPublicTemplateLookupRequiresImmutableIDAndTemplateKind(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectQuery(`a.kind=\$4 AND a.id::text=\$3 AND a.data->'public'='true'::jsonb`).
		WithArgs(uint(8), types.VMArtifactTemplate, "template-id", types.VMArtifactTemplate).
		WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow([]byte(`{"id":"template-id","kind":"template","public":true}`)))
	a, err := repo.(*PostgresBackendRepository).GetVMArtifact(context.Background(), 8, types.VMArtifactTemplate, "template-id")
	require.NoError(t, err)
	require.True(t, a.Public)
	require.NoError(t, mock.ExpectationsWereMet())
}

// statement matches a statement that has each of the parts, in their order,
// whatever is between them.
func statement(parts ...string) string {
	for i, part := range parts {
		parts[i] = regexp.QuoteMeta(part)
	}
	return strings.Join(parts, ".*")
}

func TestGitHubRunnerJobIsClaimedWithinTheLimit(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx := context.Background()

	// A claim holds the job for a minute and starts its runner's times over.
	// It counts the workspace's runners, those being started among them, and
	// the jobs of the account that have been due longer. No more runners
	// are counted than the limit, in the order of their names.
	claim := statement(
		`SET vm = $3, attempts = attempts + 1, provision_at = NOW() + INTERVAL '1 minute', waiting = '', registered_at = NULL, vm_ready_at = NULL`,
		`WHERE id = $1 AND attempts = $2 AND provision_at <= NOW()`,
		`AND NOT EXISTS ( SELECT 1 FROM persistent_vm v WHERE v.workspace_id = j.workspace_id AND v.name = j.vm AND v.data->>'desired_state' <> 'deleted' )`,
		`AND EXISTS ( SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id AND NOT i.paused )`,
		`AND ($4 <= 0 OR (`,
		`v.workspace_id = j.workspace_id AND v.data->>'desired_state' <> 'deleted'`,
		`r.workspace_id = v.workspace_id AND r.vm = v.name AND r.vm <> '' LIMIT 1 ) ORDER BY v.name LIMIT $4 ) runners ) + (`,
		`r.workspace_id = j.workspace_id AND r.provision_at > NOW() AND r.vm <> '' AND r.waiting = ''`,
		`AND NOT EXISTS (SELECT 1 FROM persistent_vm v WHERE v.workspace_id = r.workspace_id AND v.name = r.vm) ) ) + (`,
		`d.workspace_id = j.workspace_id AND d.installation_id = j.installation_id`,
		`d.provision_at <= NOW() AND (d.provision_at, d.id) < (j.provision_at, j.id) ) < $4);`)
	// The limit is what the workspace of the job may have: the one the claim
	// is given, unless one is set for it. It is read first, and the statement
	// has it as a number.
	limit := statement(`SELECT COALESCE((SELECT runner_limit FROM github_runner_limit WHERE workspace_id = $1), $2);`)
	limited := func(most int) *sqlmock.Rows { return sqlmock.NewRows([]string{"limit"}).AddRow(most) }
	job := types.GitHubRunnerJob{ID: 5, WorkspaceID: 8, Attempts: 2}
	mock.ExpectQuery(limit).WithArgs(uint(8), 100).WillReturnRows(limited(100))
	mock.ExpectExec(claim).WithArgs(int64(5), 2, "ghr-a", 100).WillReturnResult(sqlmock.NewResult(0, 1))
	claimed, err := r.ClaimGitHubRunnerJob(ctx, &job, "ghr-a", 100)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, types.GitHubRunnerJob{ID: 5, WorkspaceID: 8, Attempts: 3, VM: "ghr-a"}, job)

	// A job that is not claimed, as one past the limit, stays as it is.
	mock.ExpectQuery(limit).WithArgs(uint(8), 100).WillReturnRows(limited(250))
	mock.ExpectExec(claim).WithArgs(int64(5), 3, "ghr-b", 250).WillReturnResult(sqlmock.NewResult(0, 0))
	claimed, err = r.ClaimGitHubRunnerJob(ctx, &job, "ghr-b", 100)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, types.GitHubRunnerJob{ID: 5, WorkspaceID: 8, Attempts: 3, VM: "ghr-a"}, job)

	// Nor is one claimed when what its workspace may have cannot be read.
	mock.ExpectQuery(limit).WithArgs(uint(8), 100).WillReturnError(sql.ErrConnDone)
	claimed, err = r.ClaimGitHubRunnerJob(ctx, &job, "ghr-b", 100)
	require.ErrorIs(t, err, sql.ErrConnDone)
	require.False(t, claimed)

	// The jobs of a workspace that is full are not listed as due, and the
	// others are, the job due longest first. With no limit set for any
	// workspace, that is one statement, of every workspace.
	received := time.Date(2026, 11, 16, 9, 0, 0, 123_000_000, time.UTC)
	columns := strings.Split(strings.Join(strings.Fields(githubRunnerJobColumns), ""), ",")
	limits := statement(`SELECT $1::int UNION SELECT runner_limit FROM github_runner_limit ORDER BY 1;`)
	listed := statement(`WITH due AS (`, `WHERE provision_at <= NOW()`,
		`AND COALESCE((SELECT l.runner_limit FROM github_runner_limit l WHERE l.workspace_id = j.workspace_id), $2) = $1`, `AND NOT i.paused`,
		`WHERE $1 <= 0 OR workspace_id NOT IN (`, `FROM (SELECT DISTINCT workspace_id FROM due) j WHERE (`,
		`ORDER BY v.name LIMIT $1 ) runners ) + (`, `) >= $1 )`, `ORDER BY provision_at, id LIMIT 100;`)
	row := func(id int64, workspace uint, due time.Time) []driver.Value {
		return []driver.Value{id, int64(77), int64(42), int64(workspace), "acme/repo", "CI", "test", "", "beam-4vcpu-ubuntu-2404",
			"queued", "", "", received, nil, nil, "ghr-a", 3, due, "boom", "Retrying", received, received.Add(time.Second), nil, nil, true}
	}
	mock.ExpectQuery(limits).WithArgs(100).WillReturnRows(limited(100))
	mock.ExpectQuery(listed).WithArgs(100, 100).WillReturnRows(sqlmock.NewRows(columns).AddRow(row(5, 8, received)...))
	due, err := r.ListDueGitHubRunnerJobs(ctx, 100)
	require.NoError(t, err)
	registered := received.Add(time.Second)
	require.Equal(t, []types.GitHubRunnerJob{{ID: 5, RunID: 77, InstallationID: 42, WorkspaceID: 8, Repository: "acme/repo", Workflow: "CI",
		Name: "test", Label: "beam-4vcpu-ubuntu-2404", Status: "queued", CreatedAt: received, VM: "ghr-a", Attempts: 3, ProvisionAt: &received,
		Error: "boom", Waiting: "Retrying", ReceivedAt: &received, RegisteredAt: &registered, DefaultBranch: true}}, due,
		"a due job says whether it is on a default branch, which decides what its runner keeps")

	// The workspaces of each limit are asked for together, and the limit is
	// a number in their statement. The jobs are still the hundred due
	// longest.
	mock.ExpectQuery(limits).WithArgs(100).WillReturnRows(limited(0).AddRow(100).AddRow(250))
	others := sqlmock.NewRows(columns)
	for id := int64(100); id < 200; id++ {
		others.AddRow(row(id, 7, received.Add(time.Duration(id)*time.Second))...)
	}
	mock.ExpectQuery(listed).WithArgs(0, 100).WillReturnRows(sqlmock.NewRows(columns).AddRow(row(6, 11, received.Add(150*time.Second))...))
	mock.ExpectQuery(listed).WithArgs(100, 100).WillReturnRows(others)
	mock.ExpectQuery(listed).WithArgs(250, 100).WillReturnRows(sqlmock.NewRows(columns).
		AddRow(row(7, 9, received)...).AddRow(row(4, 8, received)...))
	due, err = r.ListDueGitHubRunnerJobs(ctx, 100)
	require.NoError(t, err)
	require.Len(t, due, 100)
	ids := []int64{}
	for _, job := range due {
		ids = append(ids, job.ID)
	}
	require.Equal(t, []int64{4, 7, 100, 101}, ids[:4], "jobs due at the same time are in the order of their IDs")
	require.Equal(t, []int64{148, 149, 6, 150}, ids[50:54])
	require.Equal(t, int64(196), ids[99], "the three due last are past the hundred")

	// Every runner of a workspace is counted for its status.
	mock.ExpectQuery(statement(`SELECT (`, `v.workspace_id = j.workspace_id`, `ORDER BY v.name LIMIT NULL ) runners ) + (`,
		`) FROM (SELECT $1::bigint AS workspace_id) j;`)).
		WithArgs(uint(8)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	running, err := r.CountGitHubRunners(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 3, running)

	// A workspace has a limit of its own once one is set for it, and until
	// it is taken away again.
	mock.ExpectExec(statement(`INSERT INTO github_runner_limit (workspace_id, runner_limit) VALUES ($1, $2)`,
		`ON CONFLICT (workspace_id) DO UPDATE SET runner_limit = EXCLUDED.runner_limit, updated_at = NOW();`)).
		WithArgs(uint(8), 250).WillReturnResult(sqlmock.NewResult(0, 1))
	most := 250
	require.NoError(t, r.SetGitHubRunnerLimit(ctx, 8, &most))
	mock.ExpectExec(statement(`DELETE FROM github_runner_limit WHERE workspace_id = $1;`)).
		WithArgs(uint(8)).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.SetGitHubRunnerLimit(ctx, 8, nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGitHubRunnerJobWaitsToBeTriedAgain(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx := context.Background()
	const retry = `provision_at = NOW() + LEAST(60, 5 * POWER(2, LEAST(attempts, 5) - 1)) * INTERVAL '1 second'`

	// A failed attempt is made again after a time that grows to a minute,
	// unless no runner is started for the job any more.
	mock.ExpectExec(statement(`UPDATE github_runner_job SET `+retry+`, error = $2, waiting = $3`, `WHERE id = $1 AND provision_at IS NOT NULL;`)).
		WithArgs(int64(5), "boom", "Retrying").WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.FailGitHubRunnerJob(ctx, 5, "boom", "Retrying"))

	// So is a runner that stopped before it took a job replaced.
	mock.ExpectExec(statement(`SET data = jsonb_set(data, '{desired_state}', '"deleted"')`, `UPDATE github_runner_job SET `+retry+`, error = $3, waiting = $4`,
		`WHERE workspace_id = $1 AND vm = $2 AND vm <> '';`)).
		WithArgs(uint(8), "ghr-a", "stopped", "Waiting for capacity").WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.ReplaceGitHubRunner(ctx, 8, "ghr-a", "stopped", "Waiting for capacity"))

	// A job that waits without an attempt keeps its attempts, and is only
	// made to wait while it is due.
	until := time.Now().Add(time.Hour)
	mock.ExpectExec(statement(`UPDATE github_runner_job SET provision_at = $3, waiting = $2 WHERE id = $1 AND provision_at <= NOW();`)).
		WithArgs(int64(5), "GitHub is limiting requests", until).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.WaitGitHubRunnerJob(ctx, 5, "GitHub is limiting requests", until))

	// A job waits for nothing once no runner is started for it any more.
	mock.ExpectExec(statement(`UPDATE github_runner_job SET provision_at = NULL, error = $2, waiting = '' WHERE id = $1;`)).
		WithArgs(int64(5), "").WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.FinishGitHubRunnerJob(ctx, 5, ""))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGitHubRunnerJobTimelineIsStored(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx := context.Background()

	// A job is received when it is first seen, and no later delivery of it
	// changes that, or the branch it was first said to be on.
	created := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	mock.ExpectExec(statement(`completed_at, run_id, default_branch, provision_at, received_at )`, `$15, $16, NOW(), NOW())`, `ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status,`,
		`run_id = COALESCE(NULLIF(EXCLUDED.run_id, 0), github_runner_job.run_id) WHERE`)).
		WithArgs(int64(5), int64(42), uint(8), "acme/repo", "CI", "test", "", "beam-4vcpu-ubuntu-2404", "queued", "", "", created, nil, nil, int64(77), true).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.SaveGitHubRunnerJob(ctx, &types.GitHubRunnerJob{ID: 5, RunID: 77, InstallationID: 42, WorkspaceID: 8, Repository: "acme/repo",
		Workflow: "CI", Name: "test", Label: "beam-4vcpu-ubuntu-2404", Status: "queued", CreatedAt: created, DefaultBranch: true}))

	mock.ExpectExec(statement(`UPDATE github_runner_job SET registered_at = NOW() WHERE id = $1;`)).
		WithArgs(int64(5)).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.MarkGitHubRunnerRegistered(ctx, 5))

	// What is found of a runner's VM is its job's to know. The time it was
	// first found ready stays, and none is given until it is.
	observed := statement(`UPDATE github_runner_job SET waiting = $3, vm_ready_at = COALESCE(vm_ready_at, $4), runner_ready_at = COALESCE(runner_ready_at, $4) WHERE workspace_id = $1 AND vm = $2 AND vm <> '';`)
	ready := created.Add(7 * time.Second)
	mock.ExpectExec(observed).WithArgs(uint(8), "ghr-a", "Waiting for capacity", nil).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.ObserveGitHubRunner(ctx, 8, "ghr-a", "Waiting for capacity", nil))
	mock.ExpectExec(observed).WithArgs(uint(8), "ghr-a", "", ready).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.ObserveGitHubRunner(ctx, 8, "ghr-a", "", &ready))
	mock.ExpectQuery(statement(`AS job_status, j.waiting, j.vm_ready_at IS NOT NULL AS ready, j.status = 'queued' AS queued FROM github_runner_job j WHERE j.workspace_id = $1 AND j.vm = $2 AND j.vm <> ''`)).
		WithArgs(uint(8), "ghr-a").
		WillReturnRows(sqlmock.NewRows([]string{"installation_id", "repository", "connected", "job_status", "waiting", "ready", "queued"}).
			AddRow(int64(42), "acme/repo", true, "", "Waiting for capacity", true, true))
	runner, err := r.GetGitHubRunner(ctx, 8, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunner{InstallationID: 42, Repository: "acme/repo", Connected: true, Waiting: "Waiting for capacity", Ready: true, Queued: true}, runner)

	// The times are returned with the job.
	received, registered := created.Add(time.Second+123*time.Millisecond), created.Add(2*time.Second)
	columns := strings.Split(strings.Join(strings.Fields(githubRunnerJobColumns), ""), ",")
	mock.ExpectQuery(statement(`error, waiting, received_at, registered_at, vm_ready_at, runner_ready_at, default_branch FROM github_runner_job j WHERE workspace_id = $1 AND id = $2`)).
		WithArgs(uint(8), int64(5)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(int64(5), int64(77), int64(42), 8, "acme/repo", "CI", "test", "", "beam-4vcpu-ubuntu-2404",
			"queued", "", "", created, nil, nil, "ghr-a", 1, nil, "", "", received, registered, ready, ready, false))
	job, err := r.GetGitHubRunnerJob(ctx, 8, 5)
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunnerJob{ID: 5, RunID: 77, InstallationID: 42, WorkspaceID: 8, Repository: "acme/repo", Workflow: "CI", Name: "test",
		Label: "beam-4vcpu-ubuntu-2404", Status: "queued", CreatedAt: created, VM: "ghr-a", Attempts: 1,
		ReceivedAt: &received, RegisteredAt: &registered, VMReadyAt: &ready, RunnerReadyAt: &ready}, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGitHubRunnerIsSurplusPastTheQueuedJobs(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx := context.Background()

	// The queued jobs of the runner's repository and label are compared with
	// the idle runners that come before it: those that are up, and that
	// GitHub has given no job.
	surplus := statement(
		`q.workspace_id = j.workspace_id AND q.repository = j.repository AND q.label = j.label AND q.status = 'queued' ) <= (`,
		`WHERE v.workspace_id = j.workspace_id AND v.name < j.vm`,
		`AND v.data->>'desired_state' <> 'deleted' AND v.data->>'status' = 'running'`,
		`SELECT o.repository = j.repository AND o.label = j.label FROM github_runner_job o`,
		`WHERE o.workspace_id = v.workspace_id AND o.vm = v.name AND o.vm <> '' LIMIT 1 )`,
		`AND COALESCE((`, `WHERE a.workspace_id = v.workspace_id AND a.runner_name = v.name AND a.runner_name <> '' LIMIT 1 ), '') = ''`,
		`ORDER BY v.name ) idle )`,
		`FROM github_runner_job WHERE workspace_id = $1 AND vm = $2 AND vm <> '' LIMIT 1 ) j;`)
	for _, answer := range []bool{true, false} {
		mock.ExpectQuery(surplus).WithArgs(uint(8), "ghr-b").WillReturnRows(sqlmock.NewRows([]string{"surplus"}).AddRow(answer))
		is, err := r.IsSurplusGitHubRunner(ctx, 8, "ghr-b")
		require.NoError(t, err)
		require.Equal(t, answer, is)
	}

	// A VM that no job was started a runner for is no runner.
	mock.ExpectQuery(surplus).WithArgs(uint(8), "dev").WillReturnRows(sqlmock.NewRows([]string{"surplus"}))
	is, err := r.IsSurplusGitHubRunner(ctx, 8, "dev")
	require.NoError(t, err)
	require.False(t, is)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGitHubRunnerQueueRearmsStrandedJobs(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db, ctx := repo.client, context.Background()
	var workspace uint
	require.NoError(t, db.Get(&workspace, `INSERT INTO workspace (name) VALUES ($1) RETURNING id`, "github-runner-test-"+uuid.NewString()))
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM github_runner_job WHERE workspace_id=$1`, workspace)
		_, _ = db.Exec(`DELETE FROM github_runner_installation WHERE workspace_id=$1`, workspace)
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE workspace_id=$1`, workspace)
		_, _ = db.Exec(`DELETE FROM workspace WHERE id=$1`, workspace)
	})

	now := time.Now().UTC()
	base := now.UnixNano()
	installation := types.GitHubRunnerInstallation{ID: base, WorkspaceID: workspace, TokenID: "token", Account: "acme"}
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	job := func(n int64, label, status, runner, vm string, finish bool) {
		t.Helper()
		require.NoError(t, repo.SaveGitHubRunnerJob(ctx, &types.GitHubRunnerJob{ID: base + n, InstallationID: installation.ID,
			WorkspaceID: workspace, Label: label, Repository: "acme/runner", Status: status, RunnerName: runner,
			CreatedAt: now.Add(time.Duration(n) * time.Second)}))
		if vm != "" {
			_, err := db.Exec(`UPDATE github_runner_job SET vm = $2, attempts = 1 WHERE id = $1`, base+n, vm)
			require.NoError(t, err)
		}
		if finish {
			require.NoError(t, repo.FinishGitHubRunnerJob(ctx, base+n, ""))
		}
	}
	vm := func(name, desired, status string) *types.VM {
		t.Helper()
		v := &types.VM{ID: uuid.NewString(), WorkspaceID: workspace, WorkspaceExternalID: "test", TokenID: "token", Name: name,
			Handle: uuid.NewString(), DesiredState: desired, Status: status, LastActiveAt: now}
		require.NoError(t, repo.CreateVM(ctx, v))
		return v
	}
	stored := func(n int64) types.GitHubRunnerJob {
		t.Helper()
		j, err := repo.GetGitHubRunnerJob(ctx, workspace, base+n)
		require.NoError(t, err)
		require.NotNil(t, j)
		return *j
	}
	provisioning := func(n int64) bool {
		return stored(n).ProvisionAt != nil
	}
	const label, held = "beam-4vcpu-ubuntu-2404", "Held by GitHub"
	job(1, label, types.GitHubJobQueued, "", "ghr-ran-a-different-job", true)
	job(2, label, types.GitHubJobQueued, "", "ghr-expired-idle", true)
	job(3, label, types.GitHubJobQueued, "", "ghr-missing-vm", true)
	job(4, label, types.GitHubJobQueued, "", "ghr-live", true)
	job(5, label, types.GitHubJobCompleted, "", "ghr-spare", true)
	job(6, label, types.GitHubJobCompleted, "ghr-ran-a-different-job", "ghr-claim", false)
	job(7, "beam-8vcpu-ubuntu-2404", types.GitHubJobCompleted, "", "ghr-wrong-label", true)
	job(8, label, types.GitHubJobQueued, "", "ghr-permanent-error", true)
	job(9, label, types.GitHubJobQueued, "", "ghr-expired-job", true)
	job(10, label, types.GitHubJobQueued, "", "ghr-disconnected", true)
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, base+8, "registration refused"))
	_, err := db.Exec(`UPDATE github_runner_job SET received_at = NOW() - INTERVAL '25 hours' WHERE id = $1`, base+9)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE github_runner_job SET installation_id = $2 WHERE id = $1`, base+10, installation.ID+1)
	require.NoError(t, err)
	vm("ghr-ran-a-different-job", types.VMStateDeleted, types.VMStateDeleted)
	vm("ghr-expired-idle", types.VMStateDeleted, types.VMStateDeleted)
	vm("ghr-live", types.VMStateRunning, types.VMStateRunning)
	spare := vm("ghr-spare", types.VMStateRunning, types.VMStateStarting)
	vm("ghr-wrong-label", types.VMStateRunning, types.VMStateRunning)

	_, err = repo.PauseGitHubRunnerInstallation(ctx, workspace, true)
	require.NoError(t, err)
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	require.False(t, provisioning(1), "a paused installation does not start replacements")
	_, err = repo.PauseGitHubRunnerInstallation(ctx, workspace, false)
	require.NoError(t, err)

	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	require.True(t, provisioning(1), "the oldest stranded job gets one missing runner")
	require.False(t, provisioning(2), "a compatible VM that is starting can take this job")
	require.False(t, provisioning(3), "an existing claim can cover this job")
	require.False(t, provisioning(4), "the job's own live runner stays tracked")
	require.False(t, provisioning(5), "completed jobs stay completed")
	require.False(t, provisioning(8), "an explicit refusal stays final")
	require.False(t, provisioning(9), "GitHub has expired the job's queue window")

	spare.DesiredState = types.VMStateDeleted
	require.NoError(t, repo.SaveVM(ctx, spare))
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	require.True(t, provisioning(2), "a job whose idle runner was deleted can get a replacement")
	require.False(t, provisioning(3))
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, base+6, ""))
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	require.True(t, provisioning(3), "a different runner label does not cover this job")
	var disconnectedScheduled bool
	require.NoError(t, db.Get(&disconnectedScheduled, `SELECT provision_at IS NOT NULL FROM github_runner_job WHERE id = $1`, base+10))
	require.False(t, disconnectedScheduled)

	// A live VM may appear between rearming its job and the next claim.
	orphan := stored(3)
	vm(orphan.VM, types.VMStateRunning, types.VMStateStarting)
	claimed, err := repo.ClaimGitHubRunnerJob(ctx, &orphan, "ghr-replacement", 0)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, "ghr-missing-vm", stored(3).VM)

	old := stored(1)
	claimed, err = repo.ClaimGitHubRunnerJob(ctx, &old, "ghr-allowed", 0)
	require.NoError(t, err)
	require.True(t, claimed, "a deleted VM does not prevent a replacement")
}

// Every row belongs to a workspace made for the test, and is removed on
// completion.
func TestVMGitHubRunnersIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	ctx := context.Background()
	newWorkspace := func() uint {
		var id uint
		require.NoError(t, db.Get(&id, `INSERT INTO workspace (name) VALUES ($1) RETURNING id`, "github-runner-test-"+uuid.NewString()))
		t.Cleanup(func() {
			_, _ = db.Exec(`DELETE FROM github_runner_job WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM github_runner_installation WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM persistent_vm WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM workspace WHERE id=$1`, id)
		})
		return id
	}
	ws, other := newWorkspace(), newWorkspace()

	// GitHub's IDs are far below the clock's, so these collide with nothing.
	id := time.Now().UnixNano()
	none, err := repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.Nil(t, none)
	none, err = repo.GetGitHubRunnerInstallationById(ctx, id)
	require.NoError(t, err)
	require.Nil(t, none)

	// A workspace connects one installation, and an installation one workspace.
	installation := types.GitHubRunnerInstallation{ID: id, WorkspaceID: ws, TokenID: "token", Account: "acme", AccountType: "Organization", EnhancedCache: true}
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	installation.TokenID = "new-token"
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation), "connecting again refreshes the token")
	moved, second := installation, installation
	moved.WorkspaceID, second.ID = other, id+1
	require.ErrorIs(t, repo.SaveGitHubRunnerInstallation(ctx, &moved), ErrGitHubRunnerConflict)
	require.ErrorIs(t, repo.SaveGitHubRunnerInstallation(ctx, &second), ErrGitHubRunnerConflict)
	saved, err := repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), saved.CreatedAt, time.Minute)
	installation.CreatedAt = saved.CreatedAt
	require.Equal(t, &installation, saved)
	saved, err = repo.GetGitHubRunnerInstallationById(ctx, id)
	require.NoError(t, err)
	require.Equal(t, &installation, saved)

	job := func(id int64, status, runner string) *types.GitHubRunnerJob {
		return &types.GitHubRunnerJob{ID: id, InstallationID: installation.ID, WorkspaceID: ws, Repository: "acme/repo", Workflow: "CI", Name: "test",
			URL: "https://github.com/acme/repo/actions/runs/1/job/1", Label: "beam-4vcpu-ubuntu-2404", Status: status, RunnerName: runner,
			CreatedAt: time.Now().UTC().Truncate(time.Second)}
	}
	list := func(ws uint, filter types.GitHubRunnerJobFilter) ([]types.GitHubRunnerJob, int) {
		page, err := repo.ListGitHubRunnerJobs(ctx, ws, filter)
		require.NoError(t, err)
		return page.Jobs, page.Total
	}
	stored := func(id int64) types.GitHubRunnerJob {
		jobs, _ := list(ws, types.GitHubRunnerJobFilter{Limit: 100})
		index := slices.IndexFunc(jobs, func(job types.GitHubRunnerJob) bool { return job.ID == id })
		require.GreaterOrEqual(t, index, 0)
		return jobs[index]
	}
	isDue := func(id int64) bool {
		due, err := repo.ListDueGitHubRunnerJobs(ctx, 0)
		require.NoError(t, err)
		return slices.ContainsFunc(due, func(job types.GitHubRunnerJob) bool { return job.ID == id })
	}
	// comeDue passes the time that a job waits to be tried again.
	comeDue := func(id int64) {
		_, err := db.Exec(`UPDATE github_runner_job SET provision_at = NOW() WHERE id = $1`, id)
		require.NoError(t, err)
	}

	// A claim gives a due job to one caller, and holds it for a minute. A
	// job says when it was received.
	queued := job(id, types.GitHubJobQueued, "")
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, queued))
	require.True(t, isDue(id))
	first := stored(id)
	require.Equal(t, queued.CreatedAt, first.CreatedAt.UTC())
	require.WithinDuration(t, time.Now(), *first.ReceivedAt, time.Minute)
	received := *first.ReceivedAt
	first.CreatedAt, first.ProvisionAt, first.ReceivedAt = queued.CreatedAt, nil, nil
	require.Equal(t, *queued, first)
	stale := first
	claimed, err := repo.ClaimGitHubRunnerJob(ctx, &first, "ghr-a", 0)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, "ghr-a", first.VM)
	require.Equal(t, 1, first.Attempts)
	require.WithinDuration(t, time.Now().Add(time.Minute), *stored(id).ProvisionAt, 10*time.Second, "an attempt that never finishes is made again")
	claimed, err = repo.ClaimGitHubRunnerJob(ctx, &stale, "ghr-b", 0)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Empty(t, stale.VM)
	require.False(t, isDue(id))

	// The runner's steps are timed, and a failed attempt is made again
	// sooner than one that never finished.
	require.NoError(t, repo.MarkGitHubRunnerRegistered(ctx, id))
	require.WithinDuration(t, time.Now(), *stored(id).RegisteredAt, time.Minute)
	require.NoError(t, repo.FailGitHubRunnerJob(ctx, id, "boom", "Retrying"))
	failed := stored(id)
	require.Equal(t, "boom", failed.Error)
	require.Equal(t, "Retrying", failed.Waiting)
	require.WithinDuration(t, time.Now().Add(5*time.Second), *failed.ProvisionAt, 4*time.Second, "a failed attempt is made again soon")
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, id, ""))
	finished := stored(id)
	require.Nil(t, finished.ProvisionAt)
	require.Empty(t, finished.Error)
	require.Empty(t, finished.Waiting)
	require.Equal(t, "ghr-a", finished.VM)
	require.Equal(t, received, *finished.ReceivedAt)
	require.NoError(t, repo.FailGitHubRunnerJob(ctx, id, "late", "Retrying"))
	require.Equal(t, finished, stored(id), "a job that no runner is started for any more stays so")

	// A job is found in its workspace only. Its run is recorded by the first
	// delivery that names it, and kept by one that does not.
	found, err := repo.GetGitHubRunnerJob(ctx, ws, id)
	require.NoError(t, err)
	require.Equal(t, finished, *found)
	for _, missing := range []struct {
		workspace uint
		job       int64
	}{{other, id}, {ws, id + 1}} {
		found, err = repo.GetGitHubRunnerJob(ctx, missing.workspace, missing.job)
		require.NoError(t, err)
		require.Nil(t, found)
	}
	for _, run := range []int64{70, 0} {
		queued.RunID = run
		require.NoError(t, repo.SaveGitHubRunnerJob(ctx, queued))
		require.EqualValues(t, 70, stored(id).RunID)
	}
	require.Equal(t, "ghr-a", stored(id).VM)

	// GitHub ran another job on this job's runner. That job is due when it
	// is first seen, whatever its status, and a late delivery changes nothing.
	ran := job(id+1, types.GitHubJobInProgress, "ghr-a")
	ran.RunID = 71
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, ran))
	require.True(t, isDue(id+1))
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, id+1, ""))
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(id+1, types.GitHubJobQueued, "")))
	require.False(t, isDue(id+1))
	require.Equal(t, types.GitHubJobInProgress, stored(id+1).Status, "a job's status never moves backwards")
	require.Equal(t, "ghr-a", stored(id+1).RunnerName)
	stolen := job(id+1, types.GitHubJobCompleted, "ghr-a")
	stolen.WorkspaceID = other
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, stolen))
	require.Equal(t, types.GitHubJobInProgress, stored(id+1).Status, "a job stays in its workspace")

	runner, err := repo.GetGitHubRunner(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunner{InstallationID: id, Repository: "acme/repo", Connected: true, JobStatus: types.GitHubJobInProgress, Queued: true}, runner)
	for _, missing := range []struct {
		workspace uint
		vm        string
	}{{other, "ghr-a"}, {ws, "ghr-b"}, {ws, ""}} {
		runner, err = repo.GetGitHubRunner(ctx, missing.workspace, missing.vm)
		require.NoError(t, err)
		require.Nil(t, runner)
	}

	// What is found of a runner's VM is told to the job it was started for.
	// The first time it was found ready stays.
	vm := &types.VM{ID: uuid.NewString(), WorkspaceID: ws, WorkspaceExternalID: "test", TokenID: "test", Name: "ghr-a",
		Handle: uuid.NewString(), DesiredState: types.VMStateRunning, Status: types.VMStateRunning, LastActiveAt: time.Now()}
	require.NoError(t, repo.CreateVM(ctx, vm))
	ready := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	later := ready.Add(time.Second)
	require.NoError(t, repo.ObserveGitHubRunner(ctx, ws, "ghr-a", "Waiting for capacity", nil))
	require.Equal(t, "Waiting for capacity", stored(id).Waiting)
	require.Nil(t, stored(id).VMReadyAt)
	require.NoError(t, repo.ObserveGitHubRunner(ctx, ws, "ghr-a", "", &ready))
	require.NoError(t, repo.ObserveGitHubRunner(ctx, ws, "ghr-a", "", &later))
	require.Empty(t, stored(id).Waiting)
	require.Equal(t, ready, stored(id).VMReadyAt.UTC())
	runner, err = repo.GetGitHubRunner(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.True(t, runner.Ready)

	// Replacing a runner deletes its VM and makes its job due again, after
	// the time that a job waits to be tried again.
	require.NoError(t, repo.ReplaceGitHubRunner(ctx, ws, "ghr-a", "stopped", "Retrying"))
	require.False(t, isDue(id))
	replaced := stored(id)
	require.Equal(t, "stopped", replaced.Error)
	require.Equal(t, "Retrying", replaced.Waiting)
	require.WithinDuration(t, time.Now().Add(5*time.Second), *replaced.ProvisionAt, 4*time.Second)
	comeDue(id)
	require.True(t, isDue(id))
	vm, err = repo.GetVM(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, types.VMStateDeleted, vm.DesiredState)
	require.Equal(t, types.VMStateRunning, vm.Status)

	// The jobs of a paused account stay as they are, and none is listed as
	// due or claimed until it resumes. Connecting it again does not resume it.
	none, err = repo.PauseGitHubRunnerInstallation(ctx, other, true)
	require.NoError(t, err)
	require.Nil(t, none)
	saved, err = repo.PauseGitHubRunnerInstallation(ctx, ws, true)
	require.NoError(t, err)
	installation.Paused = true
	require.Equal(t, &installation, saved)
	require.False(t, isDue(id))
	waiting := stored(id)
	claimed, err = repo.ClaimGitHubRunnerJob(ctx, &waiting, "ghr-b", 0)
	require.NoError(t, err)
	require.False(t, claimed)
	require.Equal(t, waiting, stored(id))
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	saved, err = repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.True(t, saved.Paused)
	saved, err = repo.PauseGitHubRunnerInstallation(ctx, ws, false)
	require.NoError(t, err)
	installation.Paused = false
	require.Equal(t, &installation, saved)
	require.True(t, isDue(id))
	require.Equal(t, waiting, stored(id))

	// History is newest first.
	jobs, total := list(ws, types.GitHubRunnerJobFilter{Limit: 1})
	require.Equal(t, 2, total)
	require.Len(t, jobs, 1)
	require.Equal(t, id+1, jobs[0].ID)
	jobs, _ = list(ws, types.GitHubRunnerJobFilter{Limit: 1, Offset: 1})
	require.Equal(t, id, jobs[0].ID)
	for _, page := range [][2]int{{0, 0}, {1, 2}} {
		jobs, total = list(ws, types.GitHubRunnerJobFilter{Limit: page[0], Offset: page[1]})
		require.Equal(t, 2, total)
		require.Empty(t, jobs)
	}
	_, total = list(other, types.GitHubRunnerJobFilter{Limit: 1})
	require.Zero(t, total)

	// Disconnecting starts nothing more. Nothing reports how the jobs it
	// leaves unfinished end, so the history is the ones that finished, and
	// no other is found.
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(id+1, types.GitHubJobCompleted, "ghr-a")))
	require.EqualValues(t, 71, stored(id+1).RunID)
	_, err = repo.PauseGitHubRunnerInstallation(ctx, ws, true)
	require.NoError(t, err)
	require.NoError(t, repo.DeleteGitHubRunnerInstallation(ctx, id))
	require.False(t, isDue(id))
	none, err = repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.Nil(t, none)
	runner, err = repo.GetGitHubRunner(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.False(t, runner.Connected, "its runners are still known, to be deleted")
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(id+2, types.GitHubJobQueued, "")))
	require.False(t, isDue(id+2), "jobs of a disconnected account are not due")
	jobs, total = list(ws, types.GitHubRunnerJobFilter{Limit: 100})
	require.Equal(t, 1, total)
	require.Len(t, jobs, 1)
	require.Equal(t, id+1, jobs[0].ID)
	for jobID, listed := range map[int64]bool{id: false, id + 1: true} {
		found, err = repo.GetGitHubRunnerJob(ctx, ws, jobID)
		require.NoError(t, err)
		require.Equal(t, listed, found != nil)
	}

	// Connecting again shows them, without starting what was given up. The
	// account was paused when it disconnected, and is not when it is back.
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	saved, err = repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.False(t, saved.Paused)
	_, total = list(ws, types.GitHubRunnerJobFilter{})
	require.Equal(t, 3, total)
	require.Nil(t, stored(id).ProvisionAt)
	require.False(t, isDue(id))

	// The filter narrows the jobs and their total, and never the
	// repositories to choose from. The jobs so far are of acme/repo: id is
	// queued, id+1 completed and id+2 queued.
	deploy := job(id+3, types.GitHubJobCompleted, "ghr-c")
	deploy.Repository, deploy.Workflow, deploy.Name, deploy.Conclusion = "acme/100%_site", "Deploy", "build", "failure"
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, deploy))
	for _, tc := range []struct {
		filter types.GitHubRunnerJobFilter
		ids    []int64
	}{
		{types.GitHubRunnerJobFilter{}, []int64{id + 3, id + 2, id + 1, id}},
		{types.GitHubRunnerJobFilter{Repository: "acme/repo"}, []int64{id + 2, id + 1, id}},
		{types.GitHubRunnerJobFilter{Repository: "acme"}, []int64{}},
		{types.GitHubRunnerJobFilter{Search: "DEPLOY"}, []int64{id + 3}},
		{types.GitHubRunnerJobFilter{Search: "0%_s"}, []int64{id + 3}},
		{types.GitHubRunnerJobFilter{Search: "%"}, []int64{id + 3}},
		{types.GitHubRunnerJobFilter{Search: "_"}, []int64{id + 3}},
		{types.GitHubRunnerJobFilter{Search: "tes"}, []int64{id + 2, id + 1, id}},
		{types.GitHubRunnerJobFilter{Search: "acme/"}, []int64{id + 3, id + 2, id + 1, id}},
		{types.GitHubRunnerJobFilter{Statuses: []string{types.GitHubJobQueued}}, []int64{id + 2, id}},
		{types.GitHubRunnerJobFilter{Statuses: []string{types.GitHubJobQueued, types.GitHubJobInProgress}, Repository: "acme/100%_site"}, []int64{}},
		{types.GitHubRunnerJobFilter{Conclusions: []string{"failure", "timed_out"}}, []int64{id + 3}},
		{types.GitHubRunnerJobFilter{Statuses: []string{types.GitHubJobCompleted}, Search: "ci"}, []int64{id + 1}},
	} {
		tc.filter.Limit = 100
		page, err := repo.ListGitHubRunnerJobs(ctx, ws, tc.filter)
		require.NoError(t, err)
		listed := []int64{}
		for _, job := range page.Jobs {
			listed = append(listed, job.ID)
		}
		require.Equal(t, tc.ids, listed, "%+v", tc.filter)
		require.Equal(t, len(tc.ids), page.Total, "%+v", tc.filter)
		require.Equal(t, []string{"acme/100%_site", "acme/repo"}, page.Repositories, "%+v", tc.filter)
	}
	page, err := repo.ListGitHubRunnerJobs(ctx, ws, types.GitHubRunnerJobFilter{Limit: 1, Offset: 1, Repository: "acme/repo"})
	require.NoError(t, err)
	require.Equal(t, 3, page.Total)
	require.Len(t, page.Jobs, 1)
	require.Equal(t, id+1, page.Jobs[0].ID)

	// The runs to cancel are those of the jobs that have not finished, each
	// of them once, in one repository or in all of them. A job recorded
	// without its run has none: id+1 completed, and id+2 names no run.
	again, elsewhere := job(id+4, types.GitHubJobInProgress, "ghr-d"), job(id+5, types.GitHubJobQueued, "")
	again.RunID, elsewhere.RunID, elsewhere.Repository = 70, 72, "acme/100%_site"
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, again))
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, elsewhere))
	for repository, runs := range map[string][]types.GitHubRunnerRun{
		"":               {{InstallationID: id, Repository: "acme/repo", RunID: 70}, {InstallationID: id, Repository: "acme/100%_site", RunID: 72}},
		"acme/100%_site": {{InstallationID: id, Repository: "acme/100%_site", RunID: 72}},
		"acme":           {},
	} {
		active, total, err := repo.ListActiveGitHubRunnerRuns(ctx, ws, repository)
		require.NoError(t, err)
		require.Equal(t, runs, active, repository)
		require.Equal(t, len(runs), total, repository)
	}

	// A hundred of them at most are returned, the oldest, with how many
	// there are.
	for run := int64(100); run < 200; run++ {
		more := job(id+run, types.GitHubJobQueued, "")
		more.RunID = run
		require.NoError(t, repo.SaveGitHubRunnerJob(ctx, more))
	}
	active, total, err := repo.ListActiveGitHubRunnerRuns(ctx, ws, "")
	require.NoError(t, err)
	require.Equal(t, 102, total)
	require.Len(t, active, 100)
	require.EqualValues(t, 70, active[0].RunID)
	require.EqualValues(t, 197, active[99].RunID)

	active, total, err = repo.ListActiveGitHubRunnerRuns(ctx, other, "")
	require.NoError(t, err)
	require.Empty(t, active)
	require.Zero(t, total)
	require.NoError(t, repo.DeleteGitHubRunnerInstallation(ctx, id))
	active, total, err = repo.ListActiveGitHubRunnerRuns(ctx, ws, "")
	require.NoError(t, err)
	require.Empty(t, active, "the runs of a disconnected account are not the workspace's to cancel")
	require.Zero(t, total)
}

// Every row belongs to a workspace made for the test, and is removed on
// completion.
func TestVMGitHubRunnerQueueIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	ctx := context.Background()
	newWorkspace := func() uint {
		var id uint
		require.NoError(t, db.Get(&id, `INSERT INTO workspace (name) VALUES ($1) RETURNING id`, "github-runner-test-"+uuid.NewString()))
		t.Cleanup(func() {
			_, _ = db.Exec(`DELETE FROM github_runner_job WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM github_runner_installation WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM github_runner_limit WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM persistent_vm WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM workspace WHERE id=$1`, id)
		})
		return id
	}
	ws, other := newWorkspace(), newWorkspace()

	// GitHub's IDs are far below the clock's, so these collide with nothing.
	// Jobs are named by how far their ID is past this one.
	base := time.Now().UnixNano()
	installations := map[uint]int64{ws: base, other: base + 1}
	for workspace, installation := range installations {
		require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &types.GitHubRunnerInstallation{ID: installation, WorkspaceID: workspace,
			TokenID: "token", Account: "acme-" + fmt.Sprint(workspace), AccountType: "Organization"}))
	}
	job := func(workspace uint, n int64, status, runner string) *types.GitHubRunnerJob {
		return &types.GitHubRunnerJob{ID: base + n, InstallationID: installations[workspace], WorkspaceID: workspace, Repository: "acme/repo",
			Label: "beam-4vcpu-ubuntu-2404", Status: status, RunnerName: runner, CreatedAt: time.Now().UTC().Truncate(time.Second)}
	}
	// queue records a job that has been due for some seconds.
	queue := func(workspace uint, n int64, seconds int) {
		t.Helper()
		require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(workspace, n, types.GitHubJobQueued, "")))
		_, err := db.Exec(`UPDATE github_runner_job SET provision_at = NOW() - $2 * INTERVAL '1 second' WHERE id = $1`, base+n, seconds)
		require.NoError(t, err)
	}
	stored := func(workspace uint, n int64) types.GitHubRunnerJob {
		t.Helper()
		found, err := repo.GetGitHubRunnerJob(ctx, workspace, base+n)
		require.NoError(t, err)
		require.NotNil(t, found)
		return *found
	}
	due := func(limit int) []int64 {
		t.Helper()
		jobs, err := repo.ListDueGitHubRunnerJobs(ctx, limit)
		require.NoError(t, err)
		due := []int64{}
		for _, job := range jobs {
			if job.WorkspaceID == ws || job.WorkspaceID == other {
				due = append(due, job.ID-base)
			}
		}
		return due
	}
	claim := func(workspace uint, n int64, vm string, limit int) bool {
		t.Helper()
		job := stored(workspace, n)
		claimed, err := repo.ClaimGitHubRunnerJob(ctx, &job, vm, limit)
		require.NoError(t, err)
		return claimed
	}
	runners := func(workspace uint) int {
		t.Helper()
		count, err := repo.CountGitHubRunners(ctx, workspace)
		require.NoError(t, err)
		return count
	}
	runnerVM := func(workspace uint, name, status string) *types.VM {
		t.Helper()
		vm := &types.VM{ID: uuid.NewString(), WorkspaceID: workspace, WorkspaceExternalID: "test", TokenID: "test", Name: name,
			Handle: uuid.NewString(), DesiredState: types.VMStateRunning, Status: status, LastActiveAt: time.Now()}
		require.NoError(t, repo.CreateVM(ctx, vm))
		return vm
	}

	// A workspace may have two runners. In whatever order its jobs are
	// claimed, the two that have been due longest get them, and the others
	// stay as they are.
	for n := range int64(4) {
		queue(ws, n, 40-int(n))
	}
	queue(other, 10, 10)
	require.Equal(t, []int64{0, 1, 2, 3, 10}, due(2))
	require.False(t, claim(ws, 3, "ghr-q3", 2))
	require.False(t, claim(ws, 2, "ghr-q2", 2))
	require.True(t, claim(ws, 1, "ghr-q1", 2))
	require.True(t, claim(ws, 0, "ghr-q0", 2))
	require.Zero(t, stored(ws, 2).Attempts)
	require.Empty(t, stored(ws, 2).VM)
	require.Equal(t, 2, runners(ws), "a runner counts from the claim that names it")
	require.Zero(t, runners(other))
	require.Equal(t, []int64{10}, due(2), "the jobs of a full workspace are not due for a runner")
	require.Equal(t, []int64{2, 3, 10}, due(3))
	require.Equal(t, []int64{2, 3, 10}, due(0))
	require.False(t, claim(ws, 2, "ghr-q2", 2))
	require.True(t, claim(other, 10, "ghr-o0", 2), "each workspace has runners of its own")

	// A runner counts once: as a claim, then as its VM.
	first := runnerVM(ws, "ghr-q0", types.VMStateStarting)
	require.Equal(t, 2, runners(ws))
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, base, ""))
	require.Equal(t, 2, runners(ws))
	runnerVM(ws, "dev", types.VMStateRunning)
	require.Equal(t, 2, runners(ws), "a VM that no job was started a runner for is none")

	// A job that waits to be tried again is starting no runner, and the
	// place is the job's that has been due longest.
	require.NoError(t, repo.FailGitHubRunnerJob(ctx, base+1, "boom", "Retrying"))
	require.Equal(t, 1, runners(ws))
	require.Equal(t, []int64{2, 3}, due(2))
	require.False(t, claim(ws, 3, "ghr-q3", 2))
	require.True(t, claim(ws, 2, "ghr-q2", 2))
	require.Empty(t, due(2))

	// A job that waits for something else than a runner keeps its attempts,
	// and holds no place either. One that is not due is another gateway's.
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.WaitGitHubRunnerJob(ctx, base+3, "GitHub is limiting requests", until))
	waiting := stored(ws, 3)
	require.Equal(t, until, waiting.ProvisionAt.UTC())
	require.Equal(t, "GitHub is limiting requests", waiting.Waiting)
	require.Zero(t, waiting.Attempts)
	require.Equal(t, 2, runners(ws))
	held := stored(ws, 2)
	require.NoError(t, repo.WaitGitHubRunnerJob(ctx, base+2, "GitHub is limiting requests", until))
	require.Equal(t, held, stored(ws, 2))

	// A runner that is gone leaves its place.
	first.DesiredState = types.VMStateDeleted
	require.NoError(t, repo.SaveVM(ctx, first))
	require.Equal(t, 1, runners(ws))

	// The times of a runner go when another is started for its job. When
	// the job was received stays.
	require.NoError(t, repo.MarkGitHubRunnerRegistered(ctx, base+2))
	runnerVM(ws, "ghr-q2", types.VMStateRunning)
	ready := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.ObserveGitHubRunner(ctx, ws, "ghr-q2", "", &ready))
	timed := stored(ws, 2)
	require.NotNil(t, timed.ReceivedAt)
	require.NotNil(t, timed.RegisteredAt)
	require.Equal(t, ready, timed.VMReadyAt.UTC())
	require.NoError(t, repo.ReplaceGitHubRunner(ctx, ws, "ghr-q2", "stopped", "Retrying"))
	require.WithinDuration(t, time.Now().Add(5*time.Second), *stored(ws, 2).ProvisionAt, 4*time.Second)
	_, err := db.Exec(`UPDATE github_runner_job SET provision_at = NOW() WHERE id = $1`, base+2)
	require.NoError(t, err)
	require.True(t, claim(ws, 2, "ghr-q2b", 2))
	again := stored(ws, 2)
	require.Equal(t, 2, again.Attempts)
	require.Empty(t, again.Waiting)
	require.Nil(t, again.RegisteredAt)
	require.Nil(t, again.VMReadyAt)
	require.Equal(t, timed.ReceivedAt, again.ReceivedAt)
	require.NoError(t, repo.FailGitHubRunnerJob(ctx, base+2, "boom", "Retrying"))
	require.WithinDuration(t, time.Now().Add(10*time.Second), *stored(ws, 2).ProvisionAt, 4*time.Second, "the time between attempts grows")

	// However many gateways claim at once, a workspace gets the runners
	// there is room for, and the jobs that have been due longest get them.
	// This one has two runners, and may have nine.
	crowded := newWorkspace()
	installations[crowded] = base + 2
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &types.GitHubRunnerInstallation{ID: base + 2, WorkspaceID: crowded,
		TokenID: "token", Account: "crowded", AccountType: "Organization"}))
	for n, name := range map[int64]string{98: "ghr-c0", 99: "ghr-c1"} {
		queue(crowded, n, 100)
		require.True(t, claim(crowded, n, name, 0))
		runnerVM(crowded, name, types.VMStateRunning)
		require.NoError(t, repo.FinishGitHubRunnerJob(ctx, base+n, ""))
	}
	listed := make([]types.GitHubRunnerJob, 40)
	for i := range listed {
		queue(crowded, 100+int64(i), 90-i)
		listed[i] = stored(crowded, 100+int64(i))
	}
	claimed := make([]bool, len(listed))
	var claims errgroup.Group
	for i := range listed {
		claims.Go(func() (err error) {
			claimed[i], err = repo.ClaimGitHubRunnerJob(ctx, &listed[len(listed)-1-i], "ghr-c"+fmt.Sprint(100+i), 9)
			return err
		})
	}
	require.NoError(t, claims.Wait())
	slices.Reverse(claimed)
	for i, got := range claimed {
		require.Equal(t, i < 7, got, "job %d", i)
	}
	require.Equal(t, 9, runners(crowded))

	// A workspace has the limit that is set for it, whatever the others
	// have. This one has nine runners and 33 jobs that wait for one.
	limit := func(workspace uint) int {
		t.Helper()
		most, err := repo.GetGitHubRunnerLimit(ctx, workspace, 9)
		require.NoError(t, err)
		return most
	}
	set := func(workspace uint, runners int) {
		t.Helper()
		require.NoError(t, repo.SetGitHubRunnerLimit(ctx, workspace, &runners))
		require.Equal(t, runners, limit(workspace))
	}
	left := func() []int64 {
		t.Helper()
		jobs, err := repo.ListDueGitHubRunnerJobs(ctx, 9)
		require.NoError(t, err)
		due := []int64{}
		for _, job := range jobs {
			if job.WorkspaceID == crowded {
				due = append(due, job.ID-base)
			}
		}
		return due
	}
	require.Equal(t, 9, limit(crowded))
	require.Empty(t, left(), "full at what every workspace is allowed")
	set(crowded, 12)
	require.Len(t, left(), 33)
	require.Equal(t, int64(107), left()[0], "the job that has been due longest is first")

	// However many gateways claim at once, the three that have waited
	// longest get the room there now is, and the workspace is full again.
	late := make([]types.GitHubRunnerJob, 33)
	for i := range late {
		late[i] = stored(crowded, 107+int64(i))
	}
	claimed = make([]bool, len(late))
	claims = errgroup.Group{}
	for i := range late {
		claims.Go(func() (err error) {
			claimed[i], err = repo.ClaimGitHubRunnerJob(ctx, &late[len(late)-1-i], "ghr-d"+fmt.Sprint(100+i), 9)
			return err
		})
	}
	require.NoError(t, claims.Wait())
	slices.Reverse(claimed)
	for i, got := range claimed {
		require.Equal(t, i < 3, got, "job %d", i)
	}
	require.Equal(t, 12, runners(crowded))
	require.Empty(t, left())

	// Fewer than it has: it keeps its runners, and gets no other.
	set(crowded, 5)
	require.Equal(t, 12, runners(crowded))
	require.Empty(t, left())
	require.False(t, claim(crowded, 110, "ghr-d200", 9))

	// Zero is any number, also where every workspace is allowed some.
	set(crowded, 0)
	require.Len(t, left(), 30)
	require.True(t, claim(crowded, 139, "ghr-d201", 9), "nor does a job wait for those due longer to get theirs")
	require.Equal(t, 13, runners(crowded))

	// Another workspace is allowed what every workspace is, as before.
	plain := newWorkspace()
	installations[plain] = base + 3
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &types.GitHubRunnerInstallation{ID: base + 3, WorkspaceID: plain,
		TokenID: "token", Account: "plain", AccountType: "Organization"}))
	require.Equal(t, 9, limit(plain))
	queue(plain, 200, 20)
	queue(plain, 201, 10)
	require.True(t, claim(plain, 200, "ghr-p0", 1))
	require.False(t, claim(plain, 201, "ghr-p1", 1))
	require.Equal(t, 1, runners(plain))

	// Without a limit of its own again, it is full at nine, as it was.
	require.NoError(t, repo.SetGitHubRunnerLimit(ctx, crowded, nil))
	require.Equal(t, 9, limit(crowded))
	require.Empty(t, left())
	require.False(t, claim(crowded, 110, "ghr-d202", 9))

	// Three jobs of a repository are queued, and each has a runner that is
	// up and idle, so none is one too many.
	surplus := func(name string) bool {
		t.Helper()
		is, err := repo.IsSurplusGitHubRunner(ctx, other, name)
		require.NoError(t, err)
		return is
	}
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(other, 10, types.GitHubJobCompleted, "")))
	vms := map[string]*types.VM{}
	for n, name := range map[int64]string{20: "ghr-s0", 21: "ghr-s1", 22: "ghr-s2"} {
		queue(other, n, 10)
		_, err := db.Exec(`UPDATE github_runner_job SET vm = $2, provision_at = NULL WHERE id = $1`, base+n, name)
		require.NoError(t, err)
		vms[name] = runnerVM(other, name, types.VMStateRunning)
	}
	surpluses := func() []bool { return []bool{surplus("ghr-s0"), surplus("ghr-s1"), surplus("ghr-s2")} }
	require.Equal(t, []bool{false, false, false}, surpluses())
	require.False(t, surplus("ghr-none"))

	// One of the jobs is cancelled. Runners are interchangeable, so the one
	// that goes is the last by name, whichever job it was started for.
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(other, 20, types.GitHubJobCompleted, "")))
	require.Equal(t, []bool{false, false, true}, surpluses())

	// A runner that GitHub gave a job is not idle, so the first idle runner
	// is the one for the job that is still queued.
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(other, 21, types.GitHubJobInProgress, "ghr-s0")))
	require.False(t, surplus("ghr-s1"))
	require.True(t, surplus("ghr-s2"))

	// Nor is a runner idle whose VM is not up, so the one after it is needed.
	vms["ghr-s1"].Status = types.VMStateStarting
	require.NoError(t, repo.SaveVM(ctx, vms["ghr-s1"]))
	require.False(t, surplus("ghr-s2"))
	vms["ghr-s1"].Status = types.VMStateRunning
	require.NoError(t, repo.SaveVM(ctx, vms["ghr-s1"]))
	require.True(t, surplus("ghr-s2"))

	// A job of another repository, or with another label, is not one that
	// these runners take. Another job of theirs is.
	elsewhere, larger := job(other, 23, types.GitHubJobQueued, ""), job(other, 24, types.GitHubJobQueued, "")
	elsewhere.Repository, larger.Label = "acme/site", "beam-8vcpu-ubuntu-2404"
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, elsewhere))
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, larger))
	require.True(t, surplus("ghr-s2"))
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(other, 25, types.GitHubJobQueued, "")))
	require.False(t, surplus("ghr-s2"))
}

// The statements that the cache of a workspace's runners is kept with. They
// say which disk they are for, and those that withdraw or delete what a
// worker published only do so for a cache disk.
func TestGitHubRunnerCacheStatements(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	ctx := context.Background()
	const disk, layers = "root-layer-0123456789abcdef", "root-layer-%"
	created := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	columns := strings.Split(strings.ReplaceAll(githubRunnerCacheColumns, " ", ""), ",")

	// A repository's cache is the one it has, or a new one.
	mock.ExpectQuery(statement(`INSERT INTO github_runner_cache (workspace_id, repository, disk_name) VALUES ($1, $2, $3)`, `ON CONFLICT DO NOTHING`,
		`UNION ALL`, `WHERE workspace_id = $1 AND repository = $2 AND retired_at IS NULL`, `LIMIT 1;`)).
		WithArgs(uint(8), "acme/repo", disk).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(8, "acme/repo", disk, "generation-1", created, nil, nil))
	cache, err := r.CurrentGitHubRunnerCache(ctx, 8, "acme/repo", disk)
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunnerCache{WorkspaceID: 8, Repository: "acme/repo", DiskName: disk, SnapshotID: "generation-1", CreatedAt: created}, cache)

	// Another gateway made the repository's disk in the meantime.
	mock.ExpectQuery(statement(`INSERT INTO github_runner_cache`)).WithArgs(uint(8), "acme/repo", disk).WillReturnRows(sqlmock.NewRows(columns))
	cache, err = r.CurrentGitHubRunnerCache(ctx, 8, "acme/repo", disk)
	require.NoError(t, err)
	require.Nil(t, cache)

	// One runner at a time is started to add to the cache.
	for _, claimed := range []bool{true, false} {
		affected := int64(0)
		if claimed {
			affected = 1
		}
		mock.ExpectExec(statement(`UPDATE github_runner_cache SET publisher_at = NOW()`, `WHERE workspace_id = $1 AND disk_name = $2 AND retired_at IS NULL`,
			`AND (publisher_at IS NULL OR publisher_at <= NOW() - $3 * INTERVAL '1 second');`)).
			WithArgs(uint(8), disk, float64(900)).WillReturnResult(sqlmock.NewResult(0, affected))
		got, err := r.ClaimGitHubRunnerCachePublisher(ctx, 8, disk, 15*time.Minute)
		require.NoError(t, err)
		require.Equal(t, claimed, got)
	}

	// A candidate is a generation that the VM it was published for started
	// on what runners get now, as the VM's own record says.
	mock.ExpectQuery(statement(`SELECT c.workspace_id, c.repository, c.disk_name`, `FROM github_runner_cache c`, `JOIN disk_snapshot s ON s.workspace_id = c.workspace_id AND s.disk_name = c.disk_name`,
		`s.status = 'available' AND s.deleted_at IS NULL AND s.created_at > NOW() - INTERVAL '1 hour'`,
		`JOIN stub st ON st.id = s.stub_id AND st.name LIKE 'vm/%'`,
		`JOIN persistent_vm v ON v.workspace_id = c.workspace_id AND v.id = SUBSTRING(st.name FROM 4)`,
		`WHERE c.retired_at IS NULL AND s.external_id::text <> c.snapshot_id`,
		`AND v.data->'spec'->'root_layer'->>'disk_name' = c.disk_name`,
		`AND COALESCE(v.data->'spec'->'root_layer'->>'snapshot_id', '') = c.snapshot_id`, `ORDER BY s.created_at`)).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "repository", "disk_name", "parent", "snapshot_id", "runner", "created_at"}).
			AddRow(8, "acme/repo", disk, "generation-1", "generation-2", "ghr-a", created))
	candidates, err := r.ListGitHubRunnerCacheCandidates(ctx)
	require.NoError(t, err)
	candidate := types.GitHubRunnerCacheCandidate{WorkspaceID: 8, Repository: "acme/repo", DiskName: disk, Parent: "generation-1", SnapshotID: "generation-2", Runner: "ghr-a", CreatedAt: created}
	require.Equal(t, []types.GitHubRunnerCacheCandidate{candidate}, candidates)

	// It takes the place of the generation it was made from, and of no other.
	for _, promoted := range []bool{true, false} {
		affected := int64(0)
		if promoted {
			affected = 1
		}
		mock.ExpectExec(statement(`UPDATE github_runner_cache SET snapshot_id = $4, publisher_at = NULL`,
			`WHERE workspace_id = $1 AND disk_name = $2 AND snapshot_id = $3 AND retired_at IS NULL;`)).
			WithArgs(uint(8), disk, "generation-1", "generation-2").WillReturnResult(sqlmock.NewResult(0, affected))
		got, err := r.PromoteGitHubRunnerCache(ctx, candidate)
		require.NoError(t, err)
		require.Equal(t, promoted, got)
	}

	mock.ExpectExec(statement(`UPDATE disk_snapshot SET status = 'failed', reason = $4`,
		`WHERE workspace_id = $1 AND disk_name = $2 AND external_id::text = $3 AND disk_name LIKE $5`,
		`UPDATE github_runner_cache c SET publisher_at = NULL`)).
		WithArgs(uint(8), disk, "generation-2", "the job of the runner did not succeed", layers).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.RejectGitHubRunnerCacheCandidate(ctx, candidate, "the job of the runner did not succeed"))

	// A cache is retired when it is old enough, and deleted once it has
	// been retired for long enough.
	mock.ExpectExec(statement(`UPDATE github_runner_cache SET retired_at = NOW()`,
		`WHERE workspace_id = $1 AND retired_at IS NULL AND created_at <= NOW() - $2 * INTERVAL '1 second';`)).
		WithArgs(uint(8), float64(7*24*3600)).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, r.RetireGitHubRunnerCache(ctx, 8, 7*24*time.Hour))

	retiredAt := created.Add(time.Hour)
	mock.ExpectQuery(statement(`FROM github_runner_cache`, `WHERE retired_at <= NOW() - $1 * INTERVAL '1 second'`, `ORDER BY retired_at`)).
		WithArgs(float64(25 * 3600)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(8, "acme/repo", disk, "generation-1", created, nil, retiredAt))
	retired, err := r.ListRetiredGitHubRunnerCaches(ctx, 25*time.Hour)
	require.NoError(t, err)
	require.Equal(t, []types.GitHubRunnerCache{{WorkspaceID: 8, Repository: "acme/repo", DiskName: disk, SnapshotID: "generation-1", CreatedAt: created, RetiredAt: &retiredAt}}, retired)

	mock.ExpectExec(statement(`DELETE FROM github_runner_cache`,
		`WHERE workspace_id = $1 AND disk_name = $2 AND retired_at IS NOT NULL AND disk_name LIKE $3`,
		`DELETE FROM disk_snapshot s USING retired`, `WHERE s.workspace_id = retired.workspace_id AND s.disk_name = retired.disk_name;`)).
		WithArgs(uint(8), disk, layers).WillReturnResult(sqlmock.NewResult(0, 3))
	require.NoError(t, r.DeleteGitHubRunnerCache(ctx, 8, disk))

	// The setting is the account's, and the job that ran on a runner is
	// found by the runner's name.
	installation := strings.Split(strings.ReplaceAll(githubRunnerInstallationColumns, " ", ""), ",")
	mock.ExpectQuery(statement(`UPDATE github_runner_installation SET enhanced_cache = $2 WHERE workspace_id = $1`, `RETURNING`)).
		WithArgs(uint(8), false).
		WillReturnRows(sqlmock.NewRows(installation).AddRow(int64(42), 8, "token", "acme", "Organization", false, false, created))
	account, err := r.SetGitHubRunnerEnhancedCache(ctx, 8, false)
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunnerInstallation{ID: 42, WorkspaceID: 8, TokenID: "token", Account: "acme", AccountType: "Organization", CreatedAt: created}, account)

	mock.ExpectQuery(statement(`FROM github_runner_job`, `WHERE workspace_id = $1 AND runner_name = $2 AND runner_name <> ''`)).
		WithArgs(uint(8), "ghr-a").WillReturnRows(sqlmock.NewRows(strings.Split(strings.Join(strings.Fields(githubRunnerJobColumns), ""), ",")))
	job, err := r.GetGitHubRunnerJobByRunner(ctx, 8, "ghr-a")
	require.NoError(t, err)
	require.Nil(t, job)
	require.NoError(t, mock.ExpectationsWereMet())
}

// GitHub holds back the queued jobs of a workflow that runs only so many at
// once. The jobs are told so, and share a few spare runners: as many as run
// their jobs, and the reserve at least. The runner that GitHub gives a job
// becomes that job's, so that the job a runner was started for refers to the
// runner it gave in exchange, and the job keeps when a runner was first ready
// for it.
func TestGitHubRunnerHeldJobsIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db, ctx := repo.client, context.Background()
	var workspace uint
	require.NoError(t, db.Get(&workspace, `INSERT INTO workspace (name) VALUES ($1) RETURNING id`, "github-runner-test-"+uuid.NewString()))
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM github_runner_job WHERE workspace_id=$1`, workspace)
		_, _ = db.Exec(`DELETE FROM github_runner_installation WHERE workspace_id=$1`, workspace)
		_, _ = db.Exec(`DELETE FROM persistent_vm WHERE workspace_id=$1`, workspace)
		_, _ = db.Exec(`DELETE FROM workspace WHERE id=$1`, workspace)
	})

	now := time.Now().UTC()
	base := now.UnixNano()
	installation := types.GitHubRunnerInstallation{ID: base, WorkspaceID: workspace, TokenID: "token", Account: "acme"}
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	const label, held = "beam-4vcpu-ubuntu-2404", "Held by GitHub"
	vms := map[string]*types.VM{}
	// Job n was started runner ghr-n, which runs.
	for n := int64(1); n <= 8; n++ {
		require.NoError(t, repo.SaveGitHubRunnerJob(ctx, &types.GitHubRunnerJob{ID: base + n, InstallationID: installation.ID,
			WorkspaceID: workspace, Label: label, Repository: "acme/runner", Status: types.GitHubJobQueued,
			CreatedAt: now.Add(time.Duration(n) * time.Second)}))
		name := fmt.Sprintf("ghr-%02d", n)
		_, err := db.Exec(`UPDATE github_runner_job SET vm = $2, attempts = 1, provision_at = NULL, registered_at = $3 WHERE id = $1`,
			base+n, name, now)
		require.NoError(t, err)
		v := &types.VM{ID: uuid.NewString(), WorkspaceID: workspace, WorkspaceExternalID: "test", TokenID: "token", Name: name,
			Handle: uuid.NewString(), DesiredState: types.VMStateRunning, Status: types.VMStateRunning, LastActiveAt: now}
		require.NoError(t, repo.CreateVM(ctx, v))
		vms[name] = v
		require.NoError(t, repo.ObserveGitHubRunner(ctx, workspace, name, "", &now))
	}
	stored := func(n int64) types.GitHubRunnerJob {
		t.Helper()
		j, err := repo.GetGitHubRunnerJob(ctx, workspace, base+n)
		require.NoError(t, err)
		return *j
	}

	pool, err := repo.GetGitHubRunnerPool(ctx, workspace, "ghr-05")
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunnerPool{Queued: 8, Unheld: 8, Busy: 0, Ahead: []string{"ghr-01", "ghr-02", "ghr-03", "ghr-04"}}, pool)
	pool, err = repo.GetGitHubRunnerPool(ctx, workspace, "dev")
	require.NoError(t, err)
	require.Nil(t, pool, "a VM that no job was started a runner for is no runner")

	// GitHub gives job 1 the runner started for job 3, and job 2 its own.
	start := func(n int64, runner string) {
		t.Helper()
		require.NoError(t, repo.SaveGitHubRunnerJob(ctx, &types.GitHubRunnerJob{ID: base + n, InstallationID: installation.ID,
			WorkspaceID: workspace, Label: label, Repository: "acme/runner", Status: types.GitHubJobInProgress, RunnerName: runner}))
		require.NoError(t, repo.AdoptGitHubRunner(ctx, workspace, base+n, runner))
	}
	start(1, "ghr-03")
	start(1, "ghr-03")
	start(2, "ghr-02")
	require.Equal(t, "ghr-03", stored(1).VM, "the runner GitHub gave a job is the job's")
	require.Equal(t, "ghr-01", stored(3).VM, "and the job it was started for has the other one")
	require.Equal(t, "ghr-02", stored(2).VM)
	require.NotNil(t, stored(1).RunnerReadyAt)

	// The free runners wait for GitHub, which holds the other jobs back.
	freeSince := now.Add(-time.Minute)
	require.NoError(t, repo.HoldGitHubRunnerJobs(ctx, workspace, "ghr-05", held, freeSince))
	for n := int64(3); n <= 8; n++ {
		require.Equal(t, held, stored(n).Waiting, n)
		require.True(t, stored(n).RunnerReadyAt.Equal(now), "a runner was ready for it before: %d", n)
	}
	require.Empty(t, stored(1).Waiting, "a job that runs is held by nothing")
	pool, err = repo.GetGitHubRunnerPool(ctx, workspace, "ghr-08")
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunnerPool{Queued: 6, Unheld: 0, Busy: 2, Ahead: []string{"ghr-01", "ghr-04", "ghr-05", "ghr-06", "ghr-07"}}, pool)

	// Of the six free runners, the gateway removes four, which leaves the
	// reserve of two. The jobs whose runners went are not due again: the
	// pool is not short of a runner.
	for _, name := range []string{"ghr-05", "ghr-06", "ghr-07", "ghr-08"} {
		vms[name].DesiredState = types.VMStateDeleted
		require.NoError(t, repo.SaveVM(ctx, vms[name]))
	}
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	for n := int64(3); n <= 8; n++ {
		require.Nil(t, stored(n).ProvisionAt, n)
	}

	// GitHub gives a spare runner the next job. Three run now, which is how
	// many spare runners are kept from now on, so the oldest jobs that have
	// no runner get two, and only once.
	start(4, "ghr-04")
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	require.NoError(t, repo.RearmGitHubRunnerJobs(ctx, 2, held))
	due := []int64{}
	for n := int64(3); n <= 8; n++ {
		if stored(n).ProvisionAt != nil {
			due = append(due, n)
		}
	}
	require.Equal(t, []int64{5, 6}, due)
	require.True(t, stored(5).RunnerReadyAt.Equal(now), "the job still waits for GitHub, not for Beam")

	// A runner that stops before it takes a job was ready for none.
	require.NoError(t, repo.ReplaceGitHubRunner(ctx, workspace, "ghr-01", "stopped", "Retrying"))
	require.Nil(t, stored(3).RunnerReadyAt)
}
