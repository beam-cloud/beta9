package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type VMRepository interface {
	CreateVM(context.Context, *types.VM) error
	SaveVM(context.Context, *types.VM) error
	GetVM(context.Context, uint, string) (*types.VM, error)
	GetVMByHandle(context.Context, string) (*types.VM, error)
	ListVMs(context.Context, uint) ([]*types.VM, error)
	ListVMPage(context.Context, uint, types.VMFilter) (*types.VMPage, error)
	ListVMsToReconcile(context.Context, bool) ([]*types.VM, error)
	LockVM(context.Context, string) (func(), error)
	TryLockVM(context.Context, string) (func(), error)
	TouchVM(context.Context, string) error
	ClaimVMIdleStop(context.Context, string, time.Time) (bool, error)
	CreateVMArtifact(context.Context, uint, *types.VMArtifact) error
	GetVMArtifact(context.Context, uint, string, string) (*types.VMArtifact, error)
	ListVMArtifacts(context.Context, uint, string) ([]types.VMArtifact, error)
	ListVMArtifactPage(context.Context, uint, string, types.VMArtifactFilter) ([]types.VMArtifact, int, error)
	DeleteVMArtifact(context.Context, uint, string, string) error
}

// PublicVMRepository is required only when public VM networking is enabled.
type PublicVMRepository interface {
	GetVMByPublicIPv6(context.Context, string) (*types.VM, error)
	AssignVMPublicIPv6(context.Context, string, string) (string, error)
}

func (r *PostgresBackendRepository) CreateVM(ctx context.Context, vm *types.VM) error {
	data, err := json.Marshal(vm)
	if err != nil {
		return err
	}

	if r.config.VMBurstBatchSize > 1 {
		return r.mutateVMBatched(ctx, vmMutationInput{Create: true, Row: vmMutationRow{
			ID: vm.ID, WorkspaceID: vm.WorkspaceID, WorkspaceExternalID: vm.WorkspaceExternalID,
			TokenID: vm.TokenID, Name: vm.Name, Handle: vm.Handle, Data: data,
			LastActiveAt: vm.LastActiveAt, PublicIPv6: nullableVMAddress(vm.PublicIPv6),
		}})
	}
	_, err = r.client.ExecContext(ctx, `INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,'')::inet)`, vm.ID, vm.WorkspaceID, vm.WorkspaceExternalID, vm.TokenID, vm.Name, vm.Handle, data, vm.LastActiveAt, vm.PublicIPv6)
	return err
}

func (r *PostgresBackendRepository) SaveVM(ctx context.Context, vm *types.VM) error {
	vm.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(vm)
	if err != nil {
		return err
	}

	if r.config.VMBurstBatchSize > 1 {
		return r.mutateVMBatched(ctx, vmMutationInput{Row: vmMutationRow{ID: vm.ID, Data: data, TokenID: vm.TokenID}})
	}
	result, err := r.client.ExecContext(ctx, `UPDATE persistent_vm SET data=$2,token_id=$3 WHERE id=$1`, vm.ID, data, vm.TokenID)
	if err != nil {
		return err
	}

	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return fmt.Errorf("VM not found")
	}

	return err
}

type vmRow struct {
	WorkspaceID         uint           `db:"workspace_id"`
	WorkspaceExternalID string         `db:"workspace_external_id"`
	TokenID             string         `db:"token_id"`
	Data                []byte         `db:"data"`
	LastActiveAt        time.Time      `db:"last_active_at"`
	PublicIPv6          sql.NullString `db:"public_ipv6"`
}

const selectVM = `SELECT workspace_id,workspace_external_id,token_id,data,last_active_at,host(public_ipv6) AS public_ipv6 FROM persistent_vm `

func (row vmRow) vm() (*types.VM, error) {
	var vm types.VM
	if err := json.Unmarshal(row.Data, &vm); err != nil {
		return nil, err
	}

	vm.WorkspaceID = row.WorkspaceID
	vm.WorkspaceExternalID = row.WorkspaceExternalID
	vm.TokenID = row.TokenID
	vm.LastActiveAt = row.LastActiveAt
	vm.PublicIPv6 = row.PublicIPv6.String
	return &vm, nil
}

func (r *PostgresBackendRepository) GetVM(ctx context.Context, ws uint, name string) (*types.VM, error) {
	return r.getVM(ctx, `WHERE workspace_id=$1 AND (name=$2 OR id=$2) ORDER BY (data->>'desired_state'='deleted') LIMIT 1`, ws, name)
}

func (r *PostgresBackendRepository) GetVMByHandle(ctx context.Context, handle string) (*types.VM, error) {
	return r.getVM(ctx, `WHERE handle=$1`, handle)
}

func (r *PostgresBackendRepository) GetVMByPublicIPv6(ctx context.Context, address string) (*types.VM, error) {
	return r.getVM(ctx, `WHERE public_ipv6=$1::inet AND data->>'desired_state'<>'deleted'`, address)
}

// Separate ownership from launch JSON: a stale lifecycle save cannot release
// an address, and retrying an ambiguous allocation returns the same reservation.
func (r *PostgresBackendRepository) AssignVMPublicIPv6(ctx context.Context, id, address string) (string, error) {
	var assigned string
	err := r.client.GetContext(ctx, &assigned, `UPDATE persistent_vm SET public_ipv6=COALESCE(public_ipv6,$2::inet) WHERE id=$1 AND data->>'desired_state'<>'deleted' RETURNING host(public_ipv6)`, id, address)
	return assigned, err
}

func (r *PostgresBackendRepository) getVM(ctx context.Context, filter string, args ...any) (*types.VM, error) {
	var row vmRow
	err := r.client.GetContext(ctx, &row, selectVM+filter, args...)
	if err != nil {
		return nil, err
	}

	return row.vm()
}

func (r *PostgresBackendRepository) ListVMs(ctx context.Context, ws uint) ([]*types.VM, error) {
	return r.listVMs(ctx, `WHERE ($1=0 OR workspace_id=$1)`, ws)
}

// Each condition is inert at its zero value, so one statement covers every
// combination of filters.
const (
	vmPageScope  = `WHERE workspace_id=$1 AND ($2 OR data->>'desired_state'<>'deleted') AND COALESCE(data->'metadata','{}')@>$3::jsonb`
	vmPageSearch = `($4='' OR name ILIKE $4 OR id ILIKE $4)`
)

var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

func (r *PostgresBackendRepository) ListVMPage(ctx context.Context, ws uint, filter types.VMFilter) (*types.VMPage, error) {
	metadata, err := json.Marshal(filter.Metadata)
	if err != nil {
		return nil, err
	}
	if filter.Metadata == nil {
		metadata = []byte("{}")
	}
	search := ""
	if filter.Search != "" {
		search = "%" + likeEscaper.Replace(filter.Search) + "%"
	}
	statuses := pq.StringArray(filter.Statuses)
	if statuses == nil {
		statuses = pq.StringArray{}
	}
	args := []any{ws, filter.All, string(metadata), search}

	var counts []struct {
		Status  string `db:"status"`
		Count   int    `db:"count"`
		Matches int    `db:"matches"`
	}
	if err := r.client.SelectContext(ctx, &counts, `SELECT COALESCE(data->>'status','') AS status,COUNT(*) AS count,COUNT(*) FILTER (WHERE `+vmPageSearch+`) AS matches FROM persistent_vm `+vmPageScope+` GROUP BY 1`, args...); err != nil {
		return nil, err
	}

	page := &types.VMPage{VMs: []*types.VM{}, Counts: map[string]int{}}
	for _, row := range counts {
		page.Counts[row.Status] = row.Count
		if len(statuses) == 0 || slices.Contains(statuses, row.Status) {
			page.Total += row.Matches
		}
	}
	if filter.Limit == 0 || filter.Offset >= page.Total {
		return page, nil
	}

	rows := []vmRow{}
	err = r.client.SelectContext(ctx, &rows, selectVM+vmPageScope+` AND `+vmPageSearch+` AND (cardinality($5::text[])=0 OR data->>'status'=ANY($5)) ORDER BY (data->>'created_at')::timestamptz DESC,id DESC LIMIT $6 OFFSET $7`, append(args, statuses, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		vm, err := row.vm()
		if err != nil {
			return nil, err
		}

		page.VMs = append(page.VMs, vm)
	}

	return page, nil
}

// Keep settled history out of the reconciler's recurring database reads.
// Stopped/paused VMs are candidates only while public IPv6 needs backfilling.
func (r *PostgresBackendRepository) ListVMsToReconcile(ctx context.Context, backfillIPv6 bool) ([]*types.VM, error) {
	filter := `data->>'desired_state' IS DISTINCT FROM data->>'status' OR data->>'desired_state'='running'`
	if backfillIPv6 {
		filter += ` OR (public_ipv6 IS NULL AND data->>'desired_state' IN ('stopped','paused'))`
	}
	return r.listVMs(ctx, "WHERE "+filter)
}

func (r *PostgresBackendRepository) listVMs(ctx context.Context, filter string, args ...any) ([]*types.VM, error) {
	rows := []vmRow{}
	err := r.client.SelectContext(ctx, &rows, selectVM+filter+` ORDER BY (data->>'created_at')::timestamptz`, args...)
	if err != nil {
		return nil, err
	}

	result := []*types.VM{}
	for _, row := range rows {
		vm, err := row.vm()
		if err != nil {
			return nil, err
		}

		result = append(result, vm)
	}

	return result, nil
}

func (r *PostgresBackendRepository) TouchVM(ctx context.Context, id string) error {
	_, err := r.client.ExecContext(ctx, `UPDATE persistent_vm SET last_active_at=NOW() WHERE id=$1`, id)
	return err
}

func (r *PostgresBackendRepository) ClaimVMIdleStop(ctx context.Context, id string, cutoff time.Time) (bool, error) {
	result, err := r.client.ExecContext(ctx, `UPDATE persistent_vm SET data=jsonb_set(data,'{desired_state}',to_jsonb(CASE WHEN data#>>'{spec,idle_action}'='pause' THEN 'paused' ELSE 'stopped' END)) WHERE id=$1 AND data->>'desired_state'='running' AND last_active_at <= $2`, id, cutoff)
	if err != nil {
		return false, err
	}

	count, err := result.RowsAffected()
	return count == 1, err
}

func (r *PostgresBackendRepository) CreateVMArtifact(ctx context.Context, ws uint, a *types.VMArtifact) error {
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}

	_, err = r.client.ExecContext(ctx, `INSERT INTO vm_artifact(id,workspace_id,name,kind,vm_id,data,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`, a.ID, ws, a.Name, a.Kind, a.VMID, data, a.CreatedAt)
	return err
}

// Source VMs are soft deleted; retain their identity even after the live
// VM list stops returning them. The captured name also survives a purge.
const selectVMArtifact = `SELECT a.data || jsonb_build_object('vm_name',COALESCE(v.data->>'name',a.data->>'vm_name'),'vm_deleted',COALESCE(v.data->>'desired_state'='deleted',false)) FROM vm_artifact a LEFT JOIN persistent_vm v ON v.id=a.vm_id AND v.workspace_id=a.workspace_id `

func (r *PostgresBackendRepository) GetVMArtifact(ctx context.Context, ws uint, kind, name string) (*types.VMArtifact, error) {
	items, err := r.listVMArtifacts(ctx, `WHERE a.workspace_id=$1 AND a.kind=$2 AND (a.id::text=$3 OR a.name=$3) ORDER BY a.id::text=$3 DESC LIMIT 1`, ws, kind, name)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, sql.ErrNoRows
	}

	return &items[0], nil
}

func (r *PostgresBackendRepository) ListVMArtifacts(ctx context.Context, ws uint, kind string) ([]types.VMArtifact, error) {
	return r.listVMArtifacts(ctx, `WHERE a.workspace_id=$1 AND a.kind=$2 ORDER BY a.created_at,a.id`, ws, kind)
}

func (r *PostgresBackendRepository) ListVMArtifactPage(ctx context.Context, ws uint, kind string, filter types.VMArtifactFilter) ([]types.VMArtifact, int, error) {
	const scope = `WHERE a.workspace_id=$1 AND a.kind=$2 AND ($3='' OR a.vm_id=$3)`
	var total int
	if err := r.client.GetContext(ctx, &total, `SELECT COUNT(*) FROM vm_artifact a `+scope, ws, kind, filter.VMID); err != nil {
		return nil, 0, err
	}
	if filter.Limit == 0 || filter.Offset >= total {
		return []types.VMArtifact{}, total, nil
	}

	items, err := r.listVMArtifacts(ctx, scope+` ORDER BY a.created_at DESC,a.id DESC LIMIT $4 OFFSET $5`, ws, kind, filter.VMID, filter.Limit, filter.Offset)
	return items, total, err
}

func (r *PostgresBackendRepository) listVMArtifacts(ctx context.Context, filter string, args ...any) ([]types.VMArtifact, error) {
	var rows [][]byte
	err := r.client.SelectContext(ctx, &rows, selectVMArtifact+filter, args...)
	if err != nil {
		return nil, err
	}

	result := []types.VMArtifact{}
	for _, data := range rows {
		var a types.VMArtifact
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, err
		}
		if a.Kind == "snapshot" && len(a.ID) >= 8 && a.Name == "stop-"+a.ID[:8] {
			a.Name = a.ID
		}

		result = append(result, a)
	}

	return result, nil
}

func (r *PostgresBackendRepository) DeleteVMArtifact(ctx context.Context, ws uint, kind, name string) error {
	_, err := r.client.ExecContext(ctx, `DELETE FROM vm_artifact WHERE workspace_id=$1 AND kind=$2 AND (name=$3 OR id::text=$3)`, ws, kind, name)
	return err
}

// Each session can hold many independent advisory locks. Keep their short
// acquire/release queries separate from the lifecycle work: a hundred ongoing
// snapshots must not consume a hundred connections or block unrelated starts.
// The local set prevents PostgreSQL's same-session, reentrant lock acquisition.
// A failed query quarantines the entire session until its owners finish; new
// IDs on that shard wait too, potentially for a long lifecycle operation. Other
// shards remain available. This failure path favors existing fences over admission.
type vmLockSession struct {
	gate     chan struct{}
	client   *sqlx.DB
	conn     *sqlx.Conn
	held     map[string]chan struct{}
	draining chan struct{} // Quarantine failed sessions until existing owners release.

	batchMu      sync.Mutex
	batchRunning bool
	pending      []*vmLockAcquire
}

type vmLockAcquire struct {
	id     string
	ctx    context.Context
	result chan vmLockResult
}

type vmLockResult struct {
	conn *sqlx.Conn
	busy *vmOperationBusy
	err  error
}

func (s *vmLockSession) enqueue(ctx context.Context, id string) vmLockResult {
	if err := ctx.Err(); err != nil {
		return vmLockResult{err: err}
	}
	request := &vmLockAcquire{id: id, ctx: ctx, result: make(chan vmLockResult, 1)}
	s.batchMu.Lock()
	s.pending = append(s.pending, request)
	if !s.batchRunning {
		s.batchRunning = true
		time.AfterFunc(time.Millisecond, s.flushAcquires)
	}
	s.batchMu.Unlock()
	select {
	case result := <-request.result:
		return result
	case <-ctx.Done():
		s.batchMu.Lock()
		for i, queued := range s.pending {
			if queued == request {
				copy(s.pending[i:], s.pending[i+1:])
				s.pending[len(s.pending)-1] = nil
				s.pending = s.pending[:len(s.pending)-1]
				s.batchMu.Unlock()
				return vmLockResult{err: ctx.Err()}
			}
		}
		s.batchMu.Unlock()
		// An executing acquisition must settle before its caller can release
		// the lock. Cancellation cannot leave an ownerless PostgreSQL lock.
		return <-request.result
	}
}

func (s *vmLockSession) flushAcquires() {
	for {
		s.batchMu.Lock()
		if len(s.pending) == 0 {
			s.pending = nil
			s.batchRunning = false
			s.batchMu.Unlock()
			return
		}
		n := min(len(s.pending), vmBurstBatchLimit)
		requests := s.pending[:n:n]
		s.pending = s.pending[n:]
		s.batchMu.Unlock()

		ctx, cancel := context.WithTimeout(context.WithoutCancel(requests[0].ctx), vmBatchOperationTimeout)
		results, err := s.acquire(ctx, requests)
		cancel()
		for _, request := range requests {
			result := results[request]
			result.err = err
			request.result <- result
		}
	}
}

// Concurrent lifecycles acquire their independent locks in one round trip per
// session. The local set still rejects duplicate keys before PostgreSQL's
// reentrant advisory locks can grant the same lock to two callers.
func (s *vmLockSession) acquire(ctx context.Context, requests []*vmLockAcquire) (map[*vmLockAcquire]vmLockResult, error) {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.gate }()
	if s.draining != nil {
		results := make(map[*vmLockAcquire]vmLockResult, len(requests))
		for _, request := range requests {
			results[request] = vmLockResult{busy: &vmOperationBusy{released: s.draining}}
		}
		return results, nil
	}
	ids := make([]string, 0, len(requests))
	acquired := make(map[string]bool, len(requests))
	for _, request := range requests {
		if _, seen := acquired[request.id]; s.held[request.id] == nil && !seen {
			ids = append(ids, request.id)
			acquired[request.id] = false
		}
	}
	if len(ids) > 0 {
		if s.conn == nil {
			conn, err := s.client.Connx(ctx)
			if err != nil {
				return nil, err
			}
			s.conn = conn
		}
		// The batch context outlives individual callers. Canceling one HTTP
		// request must not close a session holding another VM's locks.
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var rows []struct {
			ID       string `db:"id"`
			Acquired bool   `db:"acquired"`
		}
		err := s.conn.SelectContext(queryCtx, &rows, `SELECT id, pg_try_advisory_lock(hashtextextended(id, 947)) AS acquired FROM unnest($1::text[]) AS locks(id)`, pq.Array(ids))
		for _, row := range rows {
			acquired[row.ID] = row.Acquired
		}

		if err != nil {
			s.retire()
			return nil, err
		}
	}
	results := make(map[*vmLockAcquire]vmLockResult, len(requests))
	for _, request := range requests {
		if released := s.held[request.id]; released != nil {
			results[request] = vmLockResult{busy: &vmOperationBusy{released: released}}
		} else if acquired[request.id] {
			s.held[request.id] = make(chan struct{})
			results[request] = vmLockResult{conn: s.conn}
		} else {
			results[request] = vmLockResult{busy: &vmOperationBusy{}}
		}
	}
	if len(s.held) == 0 && s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	return results, nil
}

func (s *vmLockSession) discard() {
	if s.conn != nil {
		_ = s.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = s.conn.Close()
		s.conn = nil
	}
}

// Failed lock queries may have acquired or retained unknown locks. Quarantine
// the session, but do not drop another operation's advisory lock to clean it up.
// Once all known owners finish, discard the connection and any unknown locks.
func (s *vmLockSession) retire() {
	if s.draining == nil {
		s.draining = make(chan struct{})
	}
	if len(s.held) != 0 {
		return
	}
	s.discard()
	close(s.draining)
	s.draining = nil
}

// release runs with the session gate held. A callback from a lost session
// cannot unlock a lock belonging to its replacement connection.
func (s *vmLockSession) release(id string, conn *sqlx.Conn) {
	if s.conn == conn {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := conn.ExecContext(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1, 947))`, id)
		cancel()
		if err != nil {
			s.retire()
		}
	}
	close(s.held[id])
	delete(s.held, id)
	if len(s.held) == 0 {
		if s.draining != nil {
			s.retire()
		} else if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
	}
}

type vmOperationBusy struct {
	released <-chan struct{}
}

func (*vmOperationBusy) Error() string { return "VM operation already in progress" }

// LockVM queues user operations behind an existing lifecycle operation without
// holding a connection or blocking other VMs on the shared lock session.
func (r *PostgresBackendRepository) LockVM(ctx context.Context, id string) (func(), error) {
	for {
		unlock, err := r.TryLockVM(ctx, id)
		var busy *vmOperationBusy
		if !errors.As(err, &busy) {
			return unlock, err
		}
		if busy.released != nil {
			select {
			case <-busy.released:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		// Another gateway owns the PostgreSQL lock. A blocking SQL lock on
		// this multiplexed session would also prevent its owners unlocking.
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

// TryLockVM lets background reconciliation skip busy VMs. Session advisory
// locks fence lifecycle operations across gateway replicas. Sessions close
// when their last lock is released; PostgreSQL also releases all
// their locks when a gateway dies. Persisted intent remains recoverable.
func (r *PostgresBackendRepository) TryLockVM(ctx context.Context, id string) (func(), error) {
	r.vmLocksOnce.Do(func() {
		count := 4
		if max := r.client.Stats().MaxOpenConnections; max > 0 {
			count = min(count, max/2)
		}
		r.vmLocks = make([]vmLockSession, count)
		for i := range r.vmLocks {
			r.vmLocks[i] = vmLockSession{client: r.client, gate: make(chan struct{}, 1), held: make(map[string]chan struct{})}
		}
	})
	if len(r.vmLocks) == 0 {
		return nil, errors.New("VM lifecycle requires at least two database connections")
	}
	s := &r.vmLocks[crc32.ChecksumIEEE([]byte(id))%uint32(len(r.vmLocks))]
	result := s.enqueue(ctx, id)
	if result.err != nil {
		return nil, result.err
	}
	if result.busy != nil {
		return nil, result.busy
	}
	conn := result.conn
	var once sync.Once
	release := func() {
		once.Do(func() {
			s.gate <- struct{}{}
			defer func() { <-s.gate }()
			s.release(id, conn)
		})
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
