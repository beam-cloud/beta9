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
	"github.com/google/uuid"
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

	// GitHub runners: the accounts that workspaces have connected, and the
	// workflow jobs that need a runner VM. These lookups return nil when
	// nothing matches.
	GetGitHubRunnerInstallation(context.Context, uint) (*types.GitHubRunnerInstallation, error)
	GetGitHubRunnerInstallationById(context.Context, int64) (*types.GitHubRunnerInstallation, error)
	SaveGitHubRunnerInstallation(context.Context, *types.GitHubRunnerInstallation) error
	DeleteGitHubRunnerInstallation(context.Context, int64) error
	SaveGitHubRunnerJob(context.Context, *types.GitHubRunnerJob) error
	ListGitHubRunnerJobs(context.Context, uint, int, int) ([]types.GitHubRunnerJob, int, error)
	ListDueGitHubRunnerJobs(context.Context) ([]types.GitHubRunnerJob, error)
	ClaimGitHubRunnerJob(context.Context, *types.GitHubRunnerJob, string) (bool, error)
	FinishGitHubRunnerJob(context.Context, int64, string) error
	FailGitHubRunnerJob(context.Context, int64, string) error
	GetGitHubRunner(context.Context, uint, string) (*types.GitHubRunner, error)
	ReplaceGitHubRunner(context.Context, uint, string, string) error

	// Mirrors: trees of a local machine that VMs, snapshots and templates
	// pin by generation. LockVM, keyed by mirror ID, serializes their
	// retention and deletion.

	// EnsureMirror returns the workspace's mirror of that name, creating it
	// when absent, and whether it was created.
	EnsureMirror(ctx context.Context, workspaceID uint, name string) (*types.Mirror, bool, error)
	// GetMirror returns sql.ErrNoRows for an unknown or purged mirror.
	GetMirror(ctx context.Context, workspaceID uint, name string) (*types.Mirror, error)
	ListMirrors(ctx context.Context, workspaceID uint) ([]types.Mirror, error)
	// CommitMirrorGeneration stores generation as number parent+1, or
	// returns ErrMirrorGenerationConflict.
	CommitMirrorGeneration(ctx context.Context, generation *types.MirrorGeneration) error
	// GetMirrorGeneration returns the latest generation for number zero.
	GetMirrorGeneration(ctx context.Context, mirrorID string, number int64) (*types.MirrorGeneration, error)
	ListMirrorGenerations(ctx context.Context, mirrorID string) ([]types.MirrorGeneration, error)
	// ListMirrorReferences lists the VMs, snapshots and templates pinned to
	// a generation of the mirror.
	ListMirrorReferences(ctx context.Context, workspaceID uint, mirrorID string) ([]types.MirrorReference, error)
	// SetMirrorGenerationsExpired marks generations as no longer kept, or
	// as kept again. Expired generations stay readable until deleted.
	SetMirrorGenerationsExpired(ctx context.Context, mirrorID string, numbers []int64, expired bool) error
	// DeleteMirrorGenerations forgets expired generations.
	DeleteMirrorGenerations(ctx context.Context, mirrorID string, numbers []int64) error
	// PurgeMirror forgets the mirror and its generations and frees its
	// name. Its objects are deleted next; until MarkMirrorObjectsDeleted
	// records that, ListPurgedMirrors keeps returning it.
	PurgeMirror(ctx context.Context, mirrorID string) error
	// MarkMirrorObjectsDeleted completes cleanup only for a purge old
	// enough that a final listing could not miss an outstanding upload.
	MarkMirrorObjectsDeleted(ctx context.Context, mirrorID string, purgedBefore time.Time) error
	ListPurgedMirrors(ctx context.Context) ([]types.Mirror, error)
	// ListMirrorIDs lists every live mirror with its workspace, for sweeps.
	ListMirrorIDs(ctx context.Context) ([]types.Mirror, error)
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

	var db sqlx.ExecerContext = r.client
	if a.Public {
		if a.Kind != types.VMArtifactTemplate {
			return fmt.Errorf("only templates can be published")
		}
		tx, err := r.client.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := publishDiskSnapshot(ctx, tx, ws, a.RootSnapshotID); err != nil {
			return err
		}
		db = tx
	}
	_, err = db.ExecContext(ctx, `INSERT INTO vm_artifact(id,workspace_id,name,kind,vm_id,data,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`, a.ID, ws, a.Name, a.Kind, a.VMID, data, a.CreatedAt)
	if err != nil {
		return err
	}
	if tx, ok := db.(*sqlx.Tx); ok {
		return tx.Commit()
	}
	return nil
}

// Source VMs are soft deleted; retain their identity even after the live
// VM list stops returning them. The captured name also survives a purge.
// Cross-workspace readers receive the template without its source VM identity.
const selectVMArtifact = `SELECT a.data || CASE WHEN a.workspace_id=$1 THEN
 jsonb_build_object('vm_name',COALESCE(v.data->>'name',a.data->>'vm_name'),'vm_deleted',COALESCE(v.data->>'desired_state'='deleted',false))
 ELSE jsonb_build_object('vm_id','','vm_name','','vm_deleted',false) END
 FROM vm_artifact a LEFT JOIN persistent_vm v ON v.id=a.vm_id AND v.workspace_id=a.workspace_id `

func (r *PostgresBackendRepository) GetVMArtifact(ctx context.Context, ws uint, kind, name string) (*types.VMArtifact, error) {
	// Public templates are addressed by immutable ID across workspaces. Names
	// remain workspace-local and snapshots never inherit template visibility.
	items, err := r.listVMArtifacts(ctx, `WHERE a.kind=$2 AND
	 ((a.workspace_id=$1 AND (a.id::text=$3 OR a.name=$3)) OR
	  (a.kind=$4 AND a.id::text=$3 AND a.data->'public'='true'::jsonb))
	 ORDER BY a.workspace_id=$1 DESC,a.id::text=$3 DESC LIMIT 1`, ws, kind, name, types.VMArtifactTemplate)
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

// ErrGitHubRunnerConflict means the installation is connected to another
// workspace, or the workspace to another installation.
var ErrGitHubRunnerConflict = errors.New("GitHub runner connection conflict")

const (
	githubRunnerInstallationColumns = `id, workspace_id, token_id, account, account_type, created_at`
	githubRunnerJobColumns          = `id, installation_id, workspace_id, repository, workflow, name, url, label, status,
	conclusion, runner_name, created_at, started_at, completed_at, vm, attempts, provision_at, error`
)

func (r *PostgresBackendRepository) GetGitHubRunnerInstallation(ctx context.Context, workspaceId uint) (*types.GitHubRunnerInstallation, error) {
	query := `SELECT ` + githubRunnerInstallationColumns + ` FROM github_runner_installation WHERE workspace_id = $1;`
	return r.getGitHubRunnerInstallation(ctx, query, workspaceId)
}

func (r *PostgresBackendRepository) GetGitHubRunnerInstallationById(ctx context.Context, installationId int64) (*types.GitHubRunnerInstallation, error) {
	query := `SELECT ` + githubRunnerInstallationColumns + ` FROM github_runner_installation WHERE id = $1;`
	return r.getGitHubRunnerInstallation(ctx, query, installationId)
}

func (r *PostgresBackendRepository) getGitHubRunnerInstallation(ctx context.Context, query string, args ...any) (*types.GitHubRunnerInstallation, error) {
	var installation types.GitHubRunnerInstallation
	err := r.client.GetContext(ctx, &installation, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &installation, nil
}

// SaveGitHubRunnerInstallation connects an installation to a workspace.
// Connecting it again refreshes the token its runners launch with; an
// installation never moves to another workspace.
func (r *PostgresBackendRepository) SaveGitHubRunnerInstallation(ctx context.Context, installation *types.GitHubRunnerInstallation) error {
	query := `
	INSERT INTO github_runner_installation (id, workspace_id, token_id, account, account_type)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (id) DO UPDATE
	SET token_id = EXCLUDED.token_id, account = EXCLUDED.account, account_type = EXCLUDED.account_type
	WHERE github_runner_installation.workspace_id = EXCLUDED.workspace_id;
	`

	result, err := r.client.ExecContext(ctx, query, installation.ID, installation.WorkspaceID, installation.TokenID,
		installation.Account, installation.AccountType)
	if err != nil {
		// The workspace is connected to another installation.
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code.Name() == "unique_violation" {
			return ErrGitHubRunnerConflict
		}
		return err
	}

	// The installation is connected to another workspace.
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrGitHubRunnerConflict
	}

	return nil
}

// DeleteGitHubRunnerInstallation disconnects an installation. Its jobs are
// kept, but the runners they still wait for are not started.
func (r *PostgresBackendRepository) DeleteGitHubRunnerInstallation(ctx context.Context, installationId int64) error {
	query := `
	WITH disconnected AS (
		DELETE FROM github_runner_installation WHERE id = $1
	)
	UPDATE github_runner_job
	SET provision_at = NULL
	WHERE installation_id = $1 AND provision_at IS NOT NULL;
	`

	_, err := r.client.ExecContext(ctx, query, installationId)
	return err
}

// SaveGitHubRunnerJob records a delivery of a workflow job. A job is due for
// a runner when it is first seen. Deliveries repeat and arrive out of order,
// so a job's status never moves backwards.
func (r *PostgresBackendRepository) SaveGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob) error {
	query := `
	INSERT INTO github_runner_job (
		id, installation_id, workspace_id, repository, workflow, name, url, label, status,
		conclusion, runner_name, created_at, started_at, completed_at, provision_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
	ON CONFLICT (id) DO UPDATE
	SET status = EXCLUDED.status, conclusion = EXCLUDED.conclusion, runner_name = EXCLUDED.runner_name,
		started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at
	WHERE github_runner_job.workspace_id = EXCLUDED.workspace_id
		AND array_position(ARRAY['queued', 'in_progress', 'completed'], github_runner_job.status)
			<= array_position(ARRAY['queued', 'in_progress', 'completed'], EXCLUDED.status);
	`

	_, err := r.client.ExecContext(ctx, query, job.ID, job.InstallationID, job.WorkspaceID, job.Repository, job.Workflow,
		job.Name, job.URL, job.Label, job.Status, job.Conclusion, job.RunnerName, job.CreatedAt, job.StartedAt, job.CompletedAt)
	return err
}

// ListGitHubRunnerJobs returns a page of a workspace's jobs, newest first,
// and how many it has. The jobs that a disconnected account left unfinished
// are not among them, because nothing reports how they end. They are back if
// the account connects again.
func (r *PostgresBackendRepository) ListGitHubRunnerJobs(ctx context.Context, workspaceId uint, limit, offset int) ([]types.GitHubRunnerJob, int, error) {
	const history = `
	FROM github_runner_job j
	WHERE workspace_id = $1 AND (
		status = 'completed'
		OR EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	)
	`

	var total int
	if err := r.client.GetContext(ctx, &total, `SELECT COUNT(*)`+history, workspaceId); err != nil {
		return nil, 0, err
	}

	jobs := []types.GitHubRunnerJob{}
	if limit == 0 || offset >= total {
		return jobs, total, nil
	}

	query := `
	SELECT ` + githubRunnerJobColumns + history + `
	ORDER BY id DESC
	LIMIT $2 OFFSET $3;
	`

	if err := r.client.SelectContext(ctx, &jobs, query, workspaceId, limit, offset); err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}

// ListDueGitHubRunnerJobs returns the jobs of connected accounts that are due
// for a runner, longest due first.
func (r *PostgresBackendRepository) ListDueGitHubRunnerJobs(ctx context.Context) ([]types.GitHubRunnerJob, error) {
	query := `
	SELECT ` + githubRunnerJobColumns + `
	FROM github_runner_job j
	WHERE provision_at <= NOW()
		AND EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	ORDER BY provision_at
	LIMIT 100;
	`

	jobs := []types.GitHubRunnerJob{}
	if err := r.client.SelectContext(ctx, &jobs, query); err != nil {
		return nil, err
	}

	return jobs, nil
}

// ClaimGitHubRunnerJob gives a due job to one caller, to start its runner.
// The claim names that runner before it exists, so that a crash cannot leave
// one no job refers to, and makes the job due again later, for an attempt
// that fails or never finishes.
func (r *PostgresBackendRepository) ClaimGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob, vmName string) (bool, error) {
	query := `
	UPDATE github_runner_job
	SET vm = $3, attempts = attempts + 1, provision_at = NOW() + (attempts + 1) * INTERVAL '1 minute'
	WHERE id = $1 AND attempts = $2 AND provision_at <= NOW();
	`

	result, err := r.client.ExecContext(ctx, query, job.ID, job.Attempts, vmName)
	if err != nil {
		return false, err
	}

	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return false, err
	}

	job.VM, job.Attempts = vmName, job.Attempts+1
	return true, nil
}

// FinishGitHubRunnerJob stops starting a runner for a job: it has one, or it
// will never get one, for the reason in message.
func (r *PostgresBackendRepository) FinishGitHubRunnerJob(ctx context.Context, jobId int64, message string) error {
	query := `UPDATE github_runner_job SET provision_at = NULL, error = $2 WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, jobId, message)
	return err
}

// FailGitHubRunnerJob records why an attempt to start a job's runner failed.
// The job stays due when its claim said it would be.
func (r *PostgresBackendRepository) FailGitHubRunnerJob(ctx context.Context, jobId int64, message string) error {
	query := `UPDATE github_runner_job SET error = $2 WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, jobId, message)
	return err
}

// GetGitHubRunner describes the runner that a VM was started as, or returns
// nil for a VM that is not a runner. The conditions on empty names are those
// of the partial indexes, which the planner must see to use them.
func (r *PostgresBackendRepository) GetGitHubRunner(ctx context.Context, workspaceId uint, vmName string) (*types.GitHubRunner, error) {
	query := `
	SELECT j.installation_id, j.repository,
		EXISTS (
			SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id
		) AS connected,
		COALESCE((
			SELECT a.status FROM github_runner_job a
			WHERE a.workspace_id = j.workspace_id AND a.runner_name = j.vm AND a.runner_name <> ''
			LIMIT 1
		), '') AS job_status
	FROM github_runner_job j
	WHERE j.workspace_id = $1 AND j.vm = $2 AND j.vm <> ''
	LIMIT 1;
	`

	var runner types.GitHubRunner
	err := r.client.GetContext(ctx, &runner, query, workspaceId, vmName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &runner, nil
}

// ReplaceGitHubRunner deletes a runner VM that stopped before GitHub gave it
// a job, and makes the job it was started for due again, as some queued job
// is still counting on a runner. One statement does both, so that a crash
// cannot do only one of them.
func (r *PostgresBackendRepository) ReplaceGitHubRunner(ctx context.Context, workspaceId uint, vmName, message string) error {
	query := `
	WITH deleted AS (
		UPDATE persistent_vm
		SET data = jsonb_set(data, '{desired_state}', '"deleted"')
		WHERE workspace_id = $1 AND name = $2 AND data->>'desired_state' <> 'deleted'
	)
	UPDATE github_runner_job
	SET provision_at = NOW(), error = $3
	WHERE workspace_id = $1 AND vm = $2 AND vm <> '';
	`

	_, err := r.client.ExecContext(ctx, query, workspaceId, vmName, message)
	return err
}

// ErrMirrorGenerationConflict means the generation a commit was based on is
// no longer the mirror's latest: another writer committed first.
var ErrMirrorGenerationConflict = errors.New("mirror generation conflict")

const mirrorGenerationColumns = `mirror_id,number,parent,manifest_digest,manifest_size,entries,objects,total_size,home,token_id,created_at,expired_at`
const selectMirrorGeneration = `SELECT ` + mirrorGenerationColumns + ` FROM mirror_generation `

type mirrorGenerationRow struct {
	MirrorID       string     `db:"mirror_id"`
	Number         int64      `db:"number"`
	Parent         int64      `db:"parent"`
	ManifestDigest string     `db:"manifest_digest"`
	ManifestSize   int64      `db:"manifest_size"`
	Entries        int64      `db:"entries"`
	Objects        int64      `db:"objects"`
	TotalSize      int64      `db:"total_size"`
	Home           string     `db:"home"`
	TokenID        string     `db:"token_id"`
	CreatedAt      time.Time  `db:"created_at"`
	ExpiredAt      *time.Time `db:"expired_at"`
}

func (row mirrorGenerationRow) generation() types.MirrorGeneration {
	return types.MirrorGeneration{
		MirrorID: row.MirrorID, Number: row.Number, Parent: row.Parent,
		ManifestDigest: row.ManifestDigest, ManifestSize: row.ManifestSize,
		Entries: row.Entries, Objects: row.Objects, TotalSize: row.TotalSize,
		Home: row.Home, TokenID: row.TokenID, CreatedAt: row.CreatedAt, ExpiredAt: row.ExpiredAt,
	}
}

type mirrorRow struct {
	ID          string    `db:"id"`
	WorkspaceID uint      `db:"workspace_id"`
	Name        string    `db:"name"`
	CreatedAt   time.Time `db:"created_at"`
}

func (r *PostgresBackendRepository) mirror(ctx context.Context, row mirrorRow) (*types.Mirror, error) {
	mirror := &types.Mirror{ID: row.ID, WorkspaceID: row.WorkspaceID, Name: row.Name, CreatedAt: row.CreatedAt}
	latest, err := r.GetMirrorGeneration(ctx, row.ID, 0)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	mirror.Latest = latest
	return mirror, nil
}

func (r *PostgresBackendRepository) EnsureMirror(ctx context.Context, workspaceID uint, name string) (*types.Mirror, bool, error) {
	result, err := r.client.ExecContext(ctx, `INSERT INTO mirror (id,workspace_id,name) VALUES ($1,$2,$3) ON CONFLICT (workspace_id,name) WHERE purged_at IS NULL DO NOTHING`, uuid.NewString(), workspaceID, name)
	if err != nil {
		return nil, false, err
	}
	created, err := result.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	mirror, err := r.GetMirror(ctx, workspaceID, name)
	return mirror, created == 1, err
}

func (r *PostgresBackendRepository) GetMirror(ctx context.Context, workspaceID uint, name string) (*types.Mirror, error) {
	var row mirrorRow
	if err := r.client.GetContext(ctx, &row, `SELECT id,workspace_id,name,created_at FROM mirror WHERE workspace_id=$1 AND name=$2 AND purged_at IS NULL`, workspaceID, name); err != nil {
		return nil, err
	}
	return r.mirror(ctx, row)
}

func (r *PostgresBackendRepository) ListMirrors(ctx context.Context, workspaceID uint) ([]types.Mirror, error) {
	var rows []mirrorRow
	if err := r.client.SelectContext(ctx, &rows, `SELECT id,workspace_id,name,created_at FROM mirror WHERE workspace_id=$1 AND purged_at IS NULL ORDER BY name`, workspaceID); err != nil {
		return nil, err
	}
	mirrors := make([]types.Mirror, 0, len(rows))
	for _, row := range rows {
		mirror, err := r.mirror(ctx, row)
		if err != nil {
			return nil, err
		}
		mirrors = append(mirrors, *mirror)
	}
	return mirrors, nil
}

func (r *PostgresBackendRepository) ListMirrorIDs(ctx context.Context) ([]types.Mirror, error) {
	var rows []mirrorRow
	if err := r.client.SelectContext(ctx, &rows, `SELECT id,workspace_id,name,created_at FROM mirror WHERE purged_at IS NULL ORDER BY id`); err != nil {
		return nil, err
	}
	mirrors := make([]types.Mirror, 0, len(rows))
	for _, row := range rows {
		mirrors = append(mirrors, types.Mirror{ID: row.ID, WorkspaceID: row.WorkspaceID, Name: row.Name, CreatedAt: row.CreatedAt})
	}
	return mirrors, nil
}

// The insert succeeds only on top of the generation it names as parent: a
// missing parent inserts nothing and a parent that already has a child
// collides on the primary key.
func (r *PostgresBackendRepository) CommitMirrorGeneration(ctx context.Context, generation *types.MirrorGeneration) error {
	var row mirrorGenerationRow
	err := r.client.GetContext(ctx, &row, `
INSERT INTO mirror_generation (mirror_id,number,parent,manifest_digest,manifest_size,entries,objects,total_size,token_id,home)
SELECT m.id,$2+1,$2,$3,$4,$5,$6,$7,$8,$9 FROM mirror m
WHERE m.id=$1 AND m.purged_at IS NULL
 AND COALESCE((SELECT MAX(number) FROM mirror_generation WHERE mirror_id=m.id),0)=$2
RETURNING `+mirrorGenerationColumns,
		generation.MirrorID, generation.Parent, generation.ManifestDigest, generation.ManifestSize,
		generation.Entries, generation.Objects, generation.TotalSize, generation.TokenID, generation.Home)
	var pg *pq.Error
	if errors.Is(err, sql.ErrNoRows) || errors.As(err, &pg) && pg.Code == "23505" {
		return ErrMirrorGenerationConflict
	}
	if err != nil {
		return err
	}
	*generation = row.generation()
	return nil
}

func (r *PostgresBackendRepository) GetMirrorGeneration(ctx context.Context, mirrorID string, number int64) (*types.MirrorGeneration, error) {
	var row mirrorGenerationRow
	var err error
	if number == 0 {
		err = r.client.GetContext(ctx, &row, selectMirrorGeneration+`WHERE mirror_id=$1 ORDER BY number DESC LIMIT 1`, mirrorID)
	} else {
		err = r.client.GetContext(ctx, &row, selectMirrorGeneration+`WHERE mirror_id=$1 AND number=$2`, mirrorID, number)
	}
	if err != nil {
		return nil, err
	}
	generation := row.generation()
	return &generation, nil
}

func (r *PostgresBackendRepository) ListMirrorGenerations(ctx context.Context, mirrorID string) ([]types.MirrorGeneration, error) {
	var rows []mirrorGenerationRow
	if err := r.client.SelectContext(ctx, &rows, selectMirrorGeneration+`WHERE mirror_id=$1 ORDER BY number DESC`, mirrorID); err != nil {
		return nil, err
	}
	generations := make([]types.MirrorGeneration, 0, len(rows))
	for _, row := range rows {
		generations = append(generations, row.generation())
	}
	return generations, nil
}

func (r *PostgresBackendRepository) ListMirrorReferences(ctx context.Context, workspaceID uint, mirrorID string) ([]types.MirrorReference, error) {
	references := []types.MirrorReference{}
	err := r.client.SelectContext(ctx, &references, `
SELECT 'vm' AS kind, name, COALESCE((data->'spec'->'mirror'->>'generation')::bigint,0) AS generation FROM persistent_vm
 WHERE workspace_id=$1 AND data->>'desired_state' <> 'deleted' AND data->'spec'->'mirror'->>'id'=$2
UNION ALL
SELECT kind, name, COALESCE((data->'spec'->'mirror'->>'generation')::bigint,0) FROM vm_artifact
 WHERE workspace_id=$1 AND data->'spec'->'mirror'->>'id'=$2
ORDER BY kind, name`, workspaceID, mirrorID)
	return references, err
}

func (r *PostgresBackendRepository) SetMirrorGenerationsExpired(ctx context.Context, mirrorID string, numbers []int64, expired bool) error {
	if len(numbers) == 0 {
		return nil
	}
	query := `UPDATE mirror_generation SET expired_at=NULL WHERE mirror_id=$1 AND number=ANY($2)`
	if expired {
		query = `UPDATE mirror_generation SET expired_at=NOW() WHERE mirror_id=$1 AND number=ANY($2) AND expired_at IS NULL`
	}
	_, err := r.client.ExecContext(ctx, query, mirrorID, pq.Array(numbers))
	return err
}

func (r *PostgresBackendRepository) DeleteMirrorGenerations(ctx context.Context, mirrorID string, numbers []int64) error {
	if len(numbers) == 0 {
		return nil
	}
	_, err := r.client.ExecContext(ctx, `DELETE FROM mirror_generation WHERE mirror_id=$1 AND number=ANY($2) AND expired_at IS NOT NULL`, mirrorID, pq.Array(numbers))
	return err
}

func (r *PostgresBackendRepository) PurgeMirror(ctx context.Context, mirrorID string) error {
	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM mirror_generation WHERE mirror_id=$1`, mirrorID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE mirror SET purged_at=NOW() WHERE id=$1 AND purged_at IS NULL`, mirrorID); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *PostgresBackendRepository) MarkMirrorObjectsDeleted(ctx context.Context, mirrorID string, purgedBefore time.Time) error {
	_, err := r.client.ExecContext(ctx, `UPDATE mirror SET objects_deleted_at=NOW() WHERE id=$1 AND purged_at <= $2 AND objects_deleted_at IS NULL`, mirrorID, purgedBefore)
	return err
}

func (r *PostgresBackendRepository) ListPurgedMirrors(ctx context.Context) ([]types.Mirror, error) {
	var rows []mirrorRow
	if err := r.client.SelectContext(ctx, &rows, `SELECT id,workspace_id,name,created_at FROM mirror WHERE purged_at IS NOT NULL AND objects_deleted_at IS NULL ORDER BY id`); err != nil {
		return nil, err
	}
	mirrors := make([]types.Mirror, 0, len(rows))
	for _, row := range rows {
		mirrors = append(mirrors, types.Mirror{ID: row.ID, WorkspaceID: row.WorkspaceID, Name: row.Name, CreatedAt: row.CreatedAt})
	}
	return mirrors, nil
}
