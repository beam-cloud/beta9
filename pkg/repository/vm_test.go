package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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

func TestVMLockBatchDiscardsAmbiguousAcquisitions(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	r := repo.(*PostgresBackendRepository)
	r.client.SetMaxOpenConns(2)
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"a"})).
		WillReturnRows(sqlmock.NewRows([]string{"id", "acquired"}).AddRow("a", true))
	owner, err := r.LockVM(context.Background(), "a")
	require.NoError(t, err)
	defer owner()
	s := &r.vmLocks[0]
	mock.ExpectQuery(`SELECT id, pg_try_advisory_lock`).WithArgs(pq.Array([]string{"b", "c"})).
		WillReturnError(errors.New("connection lost while acquiring locks"))
	_, err = s.acquire(context.Background(), []*vmLockAcquire{{id: "b"}, {id: "c"}})
	require.ErrorContains(t, err, "connection lost")
	require.Nil(t, s.conn, "possibly acquired locks must never return to the connection pool")
	require.Len(t, s.held, 1, "the existing caller keeps its local fence until it finishes")
	owner()
	require.Empty(t, s.held)
	require.Zero(t, r.client.Stats().InUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVMLockPoolIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(4)
	repo := &PostgresBackendRepository{client: db}
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
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	ws, err := strconv.ParseUint(os.Getenv("BETA9_VM_TEST_WORKSPACE_ID"), 10, 32)
	require.NoError(t, err)
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := &PostgresBackendRepository{client: db}
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
}

func TestVMPagesIntegration(t *testing.T) {
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	ws, err := strconv.ParseUint(os.Getenv("BETA9_VM_TEST_WORKSPACE_ID"), 10, 32)
	require.NoError(t, err)
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := &PostgresBackendRepository{client: db}
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
	dsn := os.Getenv("BETA9_VM_TEST_DSN")
	if dsn == "" {
		t.Skip("set BETA9_VM_TEST_DSN for a development database")
	}
	ws, err := strconv.ParseUint(os.Getenv("BETA9_VM_TEST_WORKSPACE_ID"), 10, 32)
	require.NoError(t, err)
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	repo := &PostgresBackendRepository{client: db}
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
