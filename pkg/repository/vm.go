package repository

import (
	"cmp"
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

	// GitHub runners: the accounts that workspaces have connected, and the
	// workflow jobs that need a runner VM. These lookups return nil when
	// nothing matches.
	GetGitHubRunnerInstallation(context.Context, uint) (*types.GitHubRunnerInstallation, error)
	GetGitHubRunnerInstallationById(context.Context, int64) (*types.GitHubRunnerInstallation, error)
	SaveGitHubRunnerInstallation(context.Context, *types.GitHubRunnerInstallation) error
	PauseGitHubRunnerInstallation(context.Context, uint, bool) (*types.GitHubRunnerInstallation, error)
	DeleteGitHubRunnerInstallation(context.Context, int64) error
	SaveGitHubRunnerJob(context.Context, *types.GitHubRunnerJob) error
	GetGitHubRunnerJob(context.Context, uint, int64) (*types.GitHubRunnerJob, error)
	ListGitHubRunnerJobs(context.Context, uint, types.GitHubRunnerJobFilter) (*types.GitHubRunnerJobPage, error)
	ListActiveGitHubRunnerRuns(context.Context, uint, string) ([]types.GitHubRunnerRun, int, error)
	RearmGitHubRunnerJobs(context.Context, int, string) error
	ListDueGitHubRunnerJobs(context.Context, int) ([]types.GitHubRunnerJob, error)
	ClaimGitHubRunnerJob(context.Context, *types.GitHubRunnerJob, string, int) (bool, error)
	FinishGitHubRunnerJob(context.Context, int64, string) error
	FailGitHubRunnerJob(context.Context, int64, string, string) error
	WaitGitHubRunnerJob(context.Context, int64, string, time.Time) error
	MarkGitHubRunnerRegistered(context.Context, int64) error
	CountGitHubRunners(context.Context, uint) (int, error)
	GetGitHubRunnerLimit(context.Context, uint, int) (int, error)
	SetGitHubRunnerLimit(context.Context, uint, *int) error
	GetGitHubRunner(context.Context, uint, string) (*types.GitHubRunner, error)
	ObserveGitHubRunner(context.Context, uint, string, string, *time.Time) error
	IsSurplusGitHubRunner(context.Context, uint, string) (bool, error)
	GetGitHubRunnerPool(context.Context, uint, string) (*types.GitHubRunnerPool, error)
	HoldGitHubRunnerJobs(context.Context, uint, string, string, time.Time) error
	AdoptGitHubRunner(context.Context, uint, int64, string) error
	ReplaceGitHubRunner(context.Context, uint, string, string, string) error

	// The caches of a workspace's runners: the disk that the runners of
	// each repository start on, and the generations of it that jobs leave.
	SetGitHubRunnerEnhancedCache(context.Context, uint, bool) (*types.GitHubRunnerInstallation, error)
	CurrentGitHubRunnerCache(context.Context, uint, string, string) (*types.GitHubRunnerCache, error)
	ClaimGitHubRunnerCachePublisher(context.Context, uint, string, time.Duration) (bool, error)
	RetireGitHubRunnerCache(context.Context, uint, time.Duration) error
	ListGitHubRunnerCacheCandidates(context.Context) ([]types.GitHubRunnerCacheCandidate, error)
	PromoteGitHubRunnerCache(context.Context, types.GitHubRunnerCacheCandidate) (bool, error)
	RejectGitHubRunnerCacheCandidate(context.Context, types.GitHubRunnerCacheCandidate, string) error
	ListRetiredGitHubRunnerCaches(context.Context, time.Duration) ([]types.GitHubRunnerCache, error)
	DeleteGitHubRunnerCache(context.Context, uint, string) error
	GetGitHubRunnerJobByRunner(context.Context, uint, string) (*types.GitHubRunnerJob, error)
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
	githubRunnerInstallationColumns = `id, workspace_id, token_id, account, account_type, paused, enhanced_cache, created_at`
	githubRunnerJobColumns          = `id, run_id, installation_id, workspace_id, repository, workflow, name, url, label, status,
	conclusion, runner_name, created_at, started_at, completed_at, vm, attempts, provision_at, error, waiting,
	received_at, registered_at, vm_ready_at, runner_ready_at, default_branch`

	// githubRunnerRetry is when a job is due again after its runner could not
	// be started, or stopped before it took a job: five seconds later at
	// first, twice as long with each attempt, and never more than a minute,
	// for as long as it takes.
	githubRunnerRetry = `NOW() + LEAST(60, 5 * POWER(2, LEAST(attempts, 5) - 1)) * INTERVAL '1 second'`
)

// githubRunnerCount is the SQL that counts the runners of the workspace of a
// row j: the runner VMs that exist, and the runners that a claim has named
// and is starting, which have no VM yet. A job that waits to try again says
// what for, and is starting none. No more VMs are counted than most, which
// is the placeholder of a number, or NULL for all of them.
//
// Nothing tells the planner how few of a workspace's VMs exist, and on its
// own it reads every VM the workspace ever had, or the name of every runner.
// Asked for the VMs in the order of their names, it reads them from the
// index of those that exist, and whether each is a runner is asked of each.
func githubRunnerCount(most string) string {
	return `(
		(
			SELECT COUNT(*) FROM (
				SELECT v.name FROM persistent_vm v
				WHERE v.workspace_id = j.workspace_id AND v.data->>'desired_state' <> 'deleted'
					AND (
						SELECT TRUE FROM github_runner_job r
						WHERE r.workspace_id = v.workspace_id AND r.vm = v.name AND r.vm <> ''
						LIMIT 1
					)
				ORDER BY v.name
				LIMIT ` + most + `
			) runners
		) + (
			SELECT COUNT(*) FROM github_runner_job r
			WHERE r.workspace_id = j.workspace_id AND r.provision_at > NOW() AND r.vm <> '' AND r.waiting = ''
				AND NOT EXISTS (SELECT 1 FROM persistent_vm v WHERE v.workspace_id = r.workspace_id AND v.name = r.vm)
		)
	)`
}

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
// installation never moves to another workspace. A new connection is not
// paused, and connecting again leaves a paused one paused, as whoever paused
// it has not asked for its runners to start again.
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

// PauseGitHubRunnerInstallation pauses or resumes the account a workspace has
// connected, and returns it as it then is.
func (r *PostgresBackendRepository) PauseGitHubRunnerInstallation(ctx context.Context, workspaceId uint, paused bool) (*types.GitHubRunnerInstallation, error) {
	query := `
	UPDATE github_runner_installation SET paused = $2 WHERE workspace_id = $1
	RETURNING ` + githubRunnerInstallationColumns + `;`
	return r.getGitHubRunnerInstallation(ctx, query, workspaceId, paused)
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
// a runner when it is first seen, which is also when it was received.
// Deliveries repeat and arrive out of order, so a job's status never moves
// backwards, and a delivery that does not name the job's run leaves the one
// that is known.
func (r *PostgresBackendRepository) SaveGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob) error {
	query := `
	INSERT INTO github_runner_job (
		id, installation_id, workspace_id, repository, workflow, name, url, label, status,
		conclusion, runner_name, created_at, started_at, completed_at, run_id, default_branch, provision_at, received_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW(), NOW())
	ON CONFLICT (id) DO UPDATE
	SET status = EXCLUDED.status, conclusion = EXCLUDED.conclusion, runner_name = EXCLUDED.runner_name,
		started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at,
		run_id = COALESCE(NULLIF(EXCLUDED.run_id, 0), github_runner_job.run_id)
	WHERE github_runner_job.workspace_id = EXCLUDED.workspace_id
		AND array_position(ARRAY['queued', 'in_progress', 'completed'], github_runner_job.status)
			<= array_position(ARRAY['queued', 'in_progress', 'completed'], EXCLUDED.status);
	`

	_, err := r.client.ExecContext(ctx, query, job.ID, job.InstallationID, job.WorkspaceID, job.Repository, job.Workflow,
		job.Name, job.URL, job.Label, job.Status, job.Conclusion, job.RunnerName, job.CreatedAt, job.StartedAt, job.CompletedAt, job.RunID,
		job.DefaultBranch)
	return err
}

// GetGitHubRunnerJob returns a job of a workspace that its history shows, so
// not one that a disconnected account left unfinished: an unfinished job is
// always of the account the workspace has connected.
func (r *PostgresBackendRepository) GetGitHubRunnerJob(ctx context.Context, workspaceId uint, jobId int64) (*types.GitHubRunnerJob, error) {
	query := `
	SELECT ` + githubRunnerJobColumns + `
	FROM github_runner_job j
	WHERE workspace_id = $1 AND id = $2 AND (
		status = 'completed'
		OR EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	);
	`

	var job types.GitHubRunnerJob
	err := r.client.GetContext(ctx, &job, query, workspaceId, jobId)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &job, nil
}

// ListGitHubRunnerJobs returns a page of a workspace's jobs, newest first,
// with how many match the filter and the repositories it has jobs of. The
// jobs that a disconnected account left unfinished are not among them,
// because nothing reports how they end. They are back if the account
// connects again.
func (r *PostgresBackendRepository) ListGitHubRunnerJobs(ctx context.Context, workspaceId uint, filter types.GitHubRunnerJobFilter) (*types.GitHubRunnerJobPage, error) {
	// Each condition of the filter is inert at its zero value, so one
	// statement covers every combination of them.
	const (
		history = `
	FROM github_runner_job j
	WHERE workspace_id = $1 AND (
		status = 'completed'
		OR EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	)
	`
		matches = `($2 = '' OR repository = $2)
		AND ($3 = '' OR name ILIKE $3 OR workflow ILIKE $3 OR repository ILIKE $3)
		AND (cardinality($4::text[]) = 0 OR status = ANY($4))
		AND (cardinality($5::text[]) = 0 OR conclusion = ANY($5))`
	)

	search := ""
	if filter.Search != "" {
		search = "%" + likeEscaper.Replace(filter.Search) + "%"
	}
	args := []any{workspaceId, filter.Repository, search, pq.Array(append([]string{}, filter.Statuses...)), pq.Array(append([]string{}, filter.Conclusions...))}

	var repositories []struct {
		Repository string `db:"repository"`
		Matches    int    `db:"matches"`
	}
	query := `SELECT repository, COUNT(*) FILTER (WHERE ` + matches + `) AS matches` + history + `GROUP BY repository ORDER BY repository;`
	if err := r.client.SelectContext(ctx, &repositories, query, args...); err != nil {
		return nil, err
	}

	page := &types.GitHubRunnerJobPage{Jobs: []types.GitHubRunnerJob{}, Repositories: []string{}}
	for _, row := range repositories {
		page.Repositories = append(page.Repositories, row.Repository)
		page.Total += row.Matches
	}
	if filter.Limit == 0 || filter.Offset >= page.Total {
		return page, nil
	}

	query = `
	SELECT ` + githubRunnerJobColumns + history + `AND ` + matches + `
	ORDER BY id DESC
	LIMIT $6 OFFSET $7;
	`

	if err := r.client.SelectContext(ctx, &page.Jobs, query, append(args, filter.Limit, filter.Offset)...); err != nil {
		return nil, err
	}

	return page, nil
}

// ListActiveGitHubRunnerRuns returns the workflow runs that have a queued or
// running job of the account a workspace has connected, in one repository
// or, given none, in all of them. A job recorded before runs were is in none
// of them. It returns the oldest hundred, as GitHub limits how much an
// account changes in a minute, with how many there are in all.
func (r *PostgresBackendRepository) ListActiveGitHubRunnerRuns(ctx context.Context, workspaceId uint, repository string) ([]types.GitHubRunnerRun, int, error) {
	query := `
	SELECT installation_id, repository, run_id, COUNT(*) OVER () AS total
	FROM github_runner_job j
	WHERE workspace_id = $1 AND status IN ('queued', 'in_progress') AND run_id <> 0
		AND ($2 = '' OR repository = $2)
		AND EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	GROUP BY installation_id, repository, run_id
	ORDER BY run_id
	LIMIT 100;
	`

	var rows []struct {
		types.GitHubRunnerRun
		Total int `db:"total"`
	}
	if err := r.client.SelectContext(ctx, &rows, query, workspaceId, repository); err != nil {
		return nil, 0, err
	}

	runs, total := []types.GitHubRunnerRun{}, 0
	for _, row := range rows {
		runs, total = append(runs, row.GitHubRunnerRun), row.Total
	}

	return runs, total, nil
}

// RearmGitHubRunnerJobs makes queued jobs due again after their runner went
// away. A runner is not tied to the job it was started for. Another runner
// might still cover that job, so recover only as many jobs as there are
// missing runners for that installation, repository and label. Count both
// live unassigned VMs (including those still starting) and attempts that
// can start a new one. GitHub gives self-hosted jobs 24 hours in the queue,
// even if we miss their final webhook.
//
// Every queued job is missing a runner until GitHub is found to hold jobs
// back, which a job is told in the words of held (HoldGitHubRunnerJobs). A
// runner started for a job that GitHub holds only waits, so from then on
// the jobs share a few spare runners: as many as their runners are running
// jobs, which is how many GitHub starts at a time, and reserve at least.
// Each spare runner that GitHub gives a job is one missing, so the next job
// that GitHub lets go finds one ready, and the jobs are due again oldest
// first until none is held.
func (r *PostgresBackendRepository) RearmGitHubRunnerJobs(ctx context.Context, reserve int, held string) error {
	query := `
	WITH orphaned AS (
		SELECT j.id, j.workspace_id, j.installation_id, j.repository, j.label, j.created_at, j.vm
		FROM github_runner_installation i
		JOIN github_runner_job j ON j.workspace_id = i.workspace_id AND j.installation_id = i.id
		WHERE NOT i.paused AND j.status = 'queued' AND j.runner_name = ''
			AND j.provision_at IS NULL AND j.error = '' AND j.vm <> ''
			AND COALESCE(j.received_at, j.created_at) > NOW() - INTERVAL '24 hours'
			AND NOT EXISTS (
				SELECT 1 FROM persistent_vm v
				WHERE v.workspace_id = j.workspace_id AND v.name = j.vm AND v.data->>'desired_state' <> 'deleted'
			)
	), pools AS (
		SELECT DISTINCT workspace_id, installation_id, repository, label FROM orphaned
	), counted AS MATERIALIZED (
		SELECT p.*,
			(SELECT COUNT(*) FROM github_runner_job q
				WHERE q.workspace_id = p.workspace_id AND q.installation_id = p.installation_id
					AND q.repository = p.repository AND q.label = p.label AND q.status = 'queued'
					AND q.runner_name = '' AND (q.error = '' OR q.provision_at IS NOT NULL)
					AND COALESCE(q.received_at, q.created_at) > NOW() - INTERVAL '24 hours') AS queued,
			(SELECT COUNT(*) FROM github_runner_job h
				WHERE h.workspace_id = p.workspace_id AND h.installation_id = p.installation_id
					AND h.repository = p.repository AND h.label = p.label AND h.status = 'queued'
					AND h.runner_name = '' AND h.waiting = $2
					AND COALESCE(h.received_at, h.created_at) > NOW() - INTERVAL '24 hours') AS held,
			(SELECT COUNT(*) FROM github_runner_job a
				WHERE a.workspace_id = p.workspace_id AND a.installation_id = p.installation_id
					AND a.repository = p.repository AND a.label = p.label AND a.status = 'in_progress'
					AND a.runner_name <> '' AND EXISTS (
						SELECT 1 FROM persistent_vm v
						WHERE v.workspace_id = a.workspace_id AND v.name = a.runner_name AND v.data->>'desired_state' <> 'deleted'
					)) AS busy,
			(SELECT COUNT(*) FROM persistent_vm v
				WHERE v.workspace_id = p.workspace_id AND v.data->>'desired_state' <> 'deleted'
					AND v.data->>'desired_state' = 'running' AND (
						SELECT TRUE FROM github_runner_job o
						WHERE o.workspace_id = v.workspace_id AND o.vm = v.name AND o.vm <> ''
							AND o.installation_id = p.installation_id AND o.repository = p.repository AND o.label = p.label
						LIMIT 1
					) AND NOT EXISTS (
						SELECT 1 FROM github_runner_job a
						WHERE a.workspace_id = v.workspace_id AND a.runner_name = v.name AND a.runner_name <> ''
					)) AS live,
			(SELECT COUNT(*) FROM github_runner_job o
				WHERE o.workspace_id = p.workspace_id AND o.installation_id = p.installation_id
					AND o.repository = p.repository AND o.label = p.label AND o.provision_at IS NOT NULL
					AND COALESCE(o.received_at, o.created_at) > NOW() - INTERVAL '24 hours'
					AND (o.status = 'queued' OR o.runner_name LIKE 'ghr-%') AND NOT EXISTS (
						SELECT 1 FROM persistent_vm v
						WHERE v.workspace_id = o.workspace_id AND v.name = o.vm AND v.data->>'desired_state' <> 'deleted'
					)) AS pending
		FROM pools p
	), shortages AS (
		SELECT c.*,
			LEAST(c.queued, CASE WHEN c.held > 0 THEN GREATEST($1::bigint, c.busy) ELSE c.queued END)
				- c.live - c.pending AS shortage
		FROM counted c
	), ranked AS (
		SELECT o.*, ROW_NUMBER() OVER (
			PARTITION BY workspace_id, installation_id, repository, label ORDER BY created_at, id
		) AS position
		FROM orphaned o
	), due AS (
		SELECT r.id, r.vm FROM ranked r JOIN shortages s USING (workspace_id, installation_id, repository, label)
		WHERE r.position <= s.shortage
		ORDER BY r.created_at, r.id
		LIMIT 100
	)
	UPDATE github_runner_job j SET provision_at = NOW(), waiting = ''
	FROM due d
	WHERE j.id = d.id AND j.vm = d.vm AND j.status = 'queued' AND j.runner_name = ''
		AND j.provision_at IS NULL AND j.error = ''
		AND EXISTS (
			SELECT 1 FROM github_runner_installation i
			WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id AND NOT i.paused
		);
	`
	_, err := r.client.ExecContext(ctx, query, reserve, held)
	return err
}

// ListDueGitHubRunnerJobs returns the jobs of connected accounts that are due
// for a runner, longest due first. The jobs of a paused account stay as they
// are, so that they are due, in the same order, once it resumes. So do those
// of a workspace that has as many runners as it may (GetGitHubRunnerLimit),
// until one of its runners is gone.
//
// The workspaces of each limit are asked for together, with the limit as a
// number. One that the statement looked up for each workspace would be
// unknown to the planner, which then expects to count a tenth of the VMs a
// workspace ever had, and where that is many, compiles the statement each
// time it runs, for longer than it takes to run.
func (r *PostgresBackendRepository) ListDueGitHubRunnerJobs(ctx context.Context, limit int) ([]types.GitHubRunnerJob, error) {
	limits := []int{}
	if err := r.client.SelectContext(ctx, &limits, `SELECT $1::int UNION SELECT runner_limit FROM github_runner_limit ORDER BY 1;`, limit); err != nil {
		return nil, err
	}

	query := `
	WITH due AS (
		SELECT ` + githubRunnerJobColumns + `
		FROM github_runner_job j
		WHERE provision_at <= NOW()
			AND COALESCE((SELECT l.runner_limit FROM github_runner_limit l WHERE l.workspace_id = j.workspace_id), $2) = $1
			AND EXISTS (
				SELECT 1 FROM github_runner_installation i
				WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id AND NOT i.paused
			)
	)
	SELECT * FROM due
	WHERE $1 <= 0 OR workspace_id NOT IN (
		SELECT j.workspace_id FROM (SELECT DISTINCT workspace_id FROM due) j WHERE ` + githubRunnerCount("$1") + ` >= $1
	)
	ORDER BY provision_at, id
	LIMIT 100;
	`

	jobs := []types.GitHubRunnerJob{}
	for _, most := range limits {
		page := []types.GitHubRunnerJob{}
		if err := r.client.SelectContext(ctx, &page, query, most, limit); err != nil {
			return nil, err
		}

		jobs = append(jobs, page...)
	}

	slices.SortFunc(jobs, func(a, b types.GitHubRunnerJob) int {
		return cmp.Or(a.ProvisionAt.Compare(*b.ProvisionAt), cmp.Compare(a.ID, b.ID))
	})
	return jobs[:min(len(jobs), 100)], nil
}

// ClaimGitHubRunnerJob gives a due job to one caller, to start its runner.
// The claim names that runner before it exists, so that a crash cannot leave
// one no job refers to, and makes the job due again in a minute, for an
// attempt that never finishes. The job waits for nothing while its runner
// is started, and the times of the runner it had before are not this one's.
// A job must still be on a connected and unpaused account at claim time.
// Its previous runner must be gone before its VM name may be replaced.
//
// Nor is a job claimed that would be one runner too many for its workspace
// (GetGitHubRunnerLimit), which is read first, so that the claim has it as a
// number, as the planner needs (see ListDueGitHubRunnerJobs). The jobs of
// its account that have been due longer count as runners too: however many
// gateways claim at once, the jobs that have waited longest get the runners
// there is room for, and no other job gets one. A job that is not claimed
// stays as it is, so it has spent no attempt.
func (r *PostgresBackendRepository) ClaimGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob, vmName string, limit int) (bool, error) {
	limit, err := r.GetGitHubRunnerLimit(ctx, job.WorkspaceID, limit)
	if err != nil {
		return false, err
	}

	query := `
	UPDATE github_runner_job j
	SET vm = $3, attempts = attempts + 1, provision_at = NOW() + INTERVAL '1 minute', waiting = '',
		registered_at = NULL, vm_ready_at = NULL
	WHERE id = $1 AND attempts = $2 AND provision_at <= NOW()
		AND NOT EXISTS (
			SELECT 1 FROM persistent_vm v
			WHERE v.workspace_id = j.workspace_id AND v.name = j.vm AND v.data->>'desired_state' <> 'deleted'
		)
		AND EXISTS (
			SELECT 1 FROM github_runner_installation i
			WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id AND NOT i.paused
		)
		AND ($4 <= 0 OR ` + githubRunnerCount("$4") + ` + (
			SELECT COUNT(*) FROM github_runner_job d
			WHERE d.workspace_id = j.workspace_id AND d.installation_id = j.installation_id
				AND d.provision_at <= NOW() AND (d.provision_at, d.id) < (j.provision_at, j.id)
		) < $4);
	`

	result, err := r.client.ExecContext(ctx, query, job.ID, job.Attempts, vmName, limit)
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
	query := `UPDATE github_runner_job SET provision_at = NULL, error = $2, waiting = '' WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, jobId, message)
	return err
}

// FailGitHubRunnerJob records why an attempt to start a job's runner failed,
// and what the job waits for until the next one, in words a user is shown.
// The job is due again after a time that grows with its attempts. A job that
// is no longer to get a runner, as one of an account that disconnected
// during the attempt, stays so.
func (r *PostgresBackendRepository) FailGitHubRunnerJob(ctx context.Context, jobId int64, message, waiting string) error {
	query := `
	UPDATE github_runner_job SET provision_at = ` + githubRunnerRetry + `, error = $2, waiting = $3
	WHERE id = $1 AND provision_at IS NOT NULL;
	`
	_, err := r.client.ExecContext(ctx, query, jobId, message, waiting)
	return err
}

// WaitGitHubRunnerJob makes a job that is due for a runner due at a later
// time instead, and says what it waits for until then. Nothing was tried, so
// the job has spent no attempt.
func (r *PostgresBackendRepository) WaitGitHubRunnerJob(ctx context.Context, jobId int64, waiting string, until time.Time) error {
	query := `UPDATE github_runner_job SET provision_at = $3, waiting = $2 WHERE id = $1 AND provision_at <= NOW();`
	_, err := r.client.ExecContext(ctx, query, jobId, waiting, until)
	return err
}

// MarkGitHubRunnerRegistered records that the runner a job has claimed was
// registered with GitHub, now.
func (r *PostgresBackendRepository) MarkGitHubRunnerRegistered(ctx context.Context, jobId int64) error {
	query := `UPDATE github_runner_job SET registered_at = NOW() WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, jobId)
	return err
}

// CountGitHubRunners returns how many runners a workspace has: those that
// exist, and those being started.
func (r *PostgresBackendRepository) CountGitHubRunners(ctx context.Context, workspaceId uint) (int, error) {
	query := `SELECT ` + githubRunnerCount("NULL") + ` FROM (SELECT $1::bigint AS workspace_id) j;`

	var count int
	err := r.client.GetContext(ctx, &count, query, workspaceId)
	return count, err
}

// GetGitHubRunnerLimit returns the most runners a workspace has at once:
// what is set for it (SetGitHubRunnerLimit), and limit where nothing is.
// Zero or less is any number.
func (r *PostgresBackendRepository) GetGitHubRunnerLimit(ctx context.Context, workspaceId uint, limit int) (int, error) {
	query := `SELECT COALESCE((SELECT runner_limit FROM github_runner_limit WHERE workspace_id = $1), $2);`
	err := r.client.GetContext(ctx, &limit, query, workspaceId, limit)
	return limit, err
}

// SetGitHubRunnerLimit sets the most runners a workspace has at once, zero
// for any number, or with nil what every workspace is allowed again. The
// runners that a workspace has beyond a lower limit stay, and it gets no
// other until enough of them are gone.
func (r *PostgresBackendRepository) SetGitHubRunnerLimit(ctx context.Context, workspaceId uint, limit *int) error {
	if limit == nil {
		_, err := r.client.ExecContext(ctx, `DELETE FROM github_runner_limit WHERE workspace_id = $1;`, workspaceId)
		return err
	}

	query := `
	INSERT INTO github_runner_limit (workspace_id, runner_limit) VALUES ($1, $2)
	ON CONFLICT (workspace_id) DO UPDATE SET runner_limit = EXCLUDED.runner_limit, updated_at = NOW();
	`
	_, err := r.client.ExecContext(ctx, query, workspaceId, *limit)
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
		), '') AS job_status,
		j.waiting, j.vm_ready_at IS NOT NULL AS ready, j.status = 'queued' AS queued
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

// ObserveGitHubRunner records, on the job a runner was started for, what the
// runner's VM was found to be doing: what the job waits for meanwhile, and
// when the VM was ready, if it is. The first time it was found ready stays.
func (r *PostgresBackendRepository) ObserveGitHubRunner(ctx context.Context, workspaceId uint, vmName, waiting string, readyAt *time.Time) error {
	query := `
	UPDATE github_runner_job
	SET waiting = $3, vm_ready_at = COALESCE(vm_ready_at, $4), runner_ready_at = COALESCE(runner_ready_at, $4)
	WHERE workspace_id = $1 AND vm = $2 AND vm <> '';
	`
	_, err := r.client.ExecContext(ctx, query, workspaceId, vmName, waiting, readyAt)
	return err
}

// IsSurplusGitHubRunner reports whether an idle runner is one more than the
// queued jobs of its repository and label can take. Each such job has a
// runner in the idle ones that come first by name, so that of several idle
// runners, each asking for itself, the same ones are left for the jobs.
// Idle means that the runner's VM is up and that GitHub has given it no job.
// The VMs that exist are read in the order of their names, and what each is
// a runner of is asked of each, for the reason githubRunnerCount gives.
func (r *PostgresBackendRepository) IsSurplusGitHubRunner(ctx context.Context, workspaceId uint, vmName string) (bool, error) {
	query := `
	SELECT (
		SELECT COUNT(*) FROM github_runner_job q
		WHERE q.workspace_id = j.workspace_id AND q.repository = j.repository AND q.label = j.label AND q.status = 'queued'
	) <= (
		SELECT COUNT(*) FROM (
			SELECT v.name FROM persistent_vm v
			WHERE v.workspace_id = j.workspace_id AND v.name < j.vm
				AND v.data->>'desired_state' <> 'deleted' AND v.data->>'status' = 'running'
				AND (
					SELECT o.repository = j.repository AND o.label = j.label FROM github_runner_job o
					WHERE o.workspace_id = v.workspace_id AND o.vm = v.name AND o.vm <> ''
					LIMIT 1
				)
				AND COALESCE((
					SELECT a.status FROM github_runner_job a
					WHERE a.workspace_id = v.workspace_id AND a.runner_name = v.name AND a.runner_name <> ''
					LIMIT 1
				), '') = ''
			ORDER BY v.name
		) idle
	)
	FROM (
		SELECT workspace_id, repository, label, vm FROM github_runner_job
		WHERE workspace_id = $1 AND vm = $2 AND vm <> ''
		LIMIT 1
	) j;
	`

	var surplus bool
	err := r.client.GetContext(ctx, &surplus, query, workspaceId, vmName)
	if err == sql.ErrNoRows {
		return false, nil
	}

	return surplus, err
}

// GetGitHubRunnerPool returns what an idle runner is weighed against once
// GitHub has had the time to give it a job: the jobs of its repository and
// label, and the runners that could take them instead. It returns nil for a
// VM that is not a runner. The idle runners are found as
// IsSurplusGitHubRunner finds them.
func (r *PostgresBackendRepository) GetGitHubRunnerPool(ctx context.Context, workspaceId uint, vmName string) (*types.GitHubRunnerPool, error) {
	query := `
	SELECT
		(SELECT COUNT(*) FROM github_runner_job q
			WHERE q.workspace_id = j.workspace_id AND q.repository = j.repository AND q.label = j.label
				AND q.status = 'queued') AS queued,
		(SELECT COUNT(*) FROM github_runner_job q
			WHERE q.workspace_id = j.workspace_id AND q.repository = j.repository AND q.label = j.label
				AND q.status = 'queued' AND q.runner_name = '' AND q.provision_at IS NULL
				AND q.error = '' AND q.waiting = '') AS unheld,
		(SELECT COUNT(*) FROM github_runner_job a
			WHERE a.workspace_id = j.workspace_id AND a.repository = j.repository AND a.label = j.label
				AND a.status = 'in_progress' AND a.runner_name <> '' AND EXISTS (
					SELECT 1 FROM persistent_vm v
					WHERE v.workspace_id = a.workspace_id AND v.name = a.runner_name AND v.data->>'desired_state' <> 'deleted'
				)) AS busy,
		COALESCE((
			SELECT array_agg(idle.name ORDER BY idle.name) FROM (
				SELECT v.name FROM persistent_vm v
				WHERE v.workspace_id = j.workspace_id AND v.name < j.vm
					AND v.data->>'desired_state' <> 'deleted' AND v.data->>'status' = 'running'
					AND (
						SELECT o.repository = j.repository AND o.label = j.label FROM github_runner_job o
						WHERE o.workspace_id = v.workspace_id AND o.vm = v.name AND o.vm <> ''
						LIMIT 1
					)
					AND COALESCE((
						SELECT a.status FROM github_runner_job a
						WHERE a.workspace_id = v.workspace_id AND a.runner_name = v.name AND a.runner_name <> ''
						LIMIT 1
					), '') = ''
			) idle
		), '{}') AS ahead
	FROM (
		SELECT workspace_id, repository, label, vm FROM github_runner_job
		WHERE workspace_id = $1 AND vm = $2 AND vm <> ''
		LIMIT 1
	) j;
	`

	var pool struct {
		Queued int            `db:"queued"`
		Unheld int            `db:"unheld"`
		Busy   int            `db:"busy"`
		Ahead  pq.StringArray `db:"ahead"`
	}
	err := r.client.GetContext(ctx, &pool, query, workspaceId, vmName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &types.GitHubRunnerPool{Queued: pool.Queued, Unheld: pool.Unheld, Busy: pool.Busy, Ahead: pool.Ahead}, nil
}

// HoldGitHubRunnerJobs tells the queued jobs that a runner could take what
// they wait for, in the words of waiting: GitHub, which has the runner and
// gives it none of them. A job that waits for something else, or that a
// runner is being started for, is told nothing: that comes first. A runner
// was ready for each of the jobs since the runner was free, or since the job
// was received, if that was later.
func (r *PostgresBackendRepository) HoldGitHubRunnerJobs(ctx context.Context, workspaceId uint, vmName, waiting string, freeSince time.Time) error {
	query := `
	UPDATE github_runner_job q
	SET waiting = $3, runner_ready_at = COALESCE(q.runner_ready_at, GREATEST(COALESCE(q.received_at, q.created_at), $4))
	FROM (
		SELECT workspace_id, repository, label FROM github_runner_job
		WHERE workspace_id = $1 AND vm = $2 AND vm <> ''
		LIMIT 1
	) j
	WHERE q.workspace_id = j.workspace_id AND q.repository = j.repository AND q.label = j.label
		AND q.status = 'queued' AND q.runner_name = '' AND q.provision_at IS NULL
		AND q.error = '' AND q.waiting = '';
	`
	_, err := r.client.ExecContext(ctx, query, workspaceId, vmName, waiting, freeSince)
	return err
}

// AdoptGitHubRunner makes the runner that GitHub gave a job the job's own.
// GitHub gives a job any runner that is free, which was started for another
// job, and that one gets the runner of the job in exchange, with the times
// that are the runner's. A queued job then refers to a runner that can still
// take it, or to one that is gone, which RearmGitHubRunnerJobs replaces,
// and never to one that is busy with another job until that job ends.
// A job that no runner was ready for before was ready for this one when it
// was ready, or when the job was received, if that was later.
//
// A job that has no runner to give leaves the other one its own: a runner
// may yet be started in the job's name (provision), and exchanges then.
// Deliveries repeat, and the second one finds nothing to exchange. The two
// jobs are locked in the order of their ids, as two exchanges at once could
// otherwise each wait for the other.
func (r *PostgresBackendRepository) AdoptGitHubRunner(ctx context.Context, workspaceId uint, jobId int64, runnerName string) error {
	query := `
	WITH pair AS (
		SELECT id, vm, registered_at, vm_ready_at FROM github_runner_job
		WHERE workspace_id = $1 AND id IN (
			SELECT $2::bigint
			UNION
			SELECT id FROM github_runner_job WHERE workspace_id = $1 AND vm = $3 AND vm <> ''
		)
		ORDER BY id
		FOR UPDATE
	)
	UPDATE github_runner_job j
	SET vm = o.vm, registered_at = o.registered_at, vm_ready_at = o.vm_ready_at,
		runner_ready_at = CASE WHEN j.id = $2 AND o.vm_ready_at IS NOT NULL
			THEN COALESCE(j.runner_ready_at, GREATEST(COALESCE(j.received_at, j.created_at), o.vm_ready_at))
			ELSE j.runner_ready_at END
	FROM pair s JOIN pair o ON o.id <> s.id
	WHERE j.id = s.id
		AND (SELECT COUNT(*) FROM pair) = 2
		AND EXISTS (SELECT 1 FROM pair taken WHERE taken.id = $2 AND taken.vm <> '' AND taken.vm <> $3)
		AND EXISTS (SELECT 1 FROM pair started WHERE started.id <> $2 AND started.vm = $3);
	`
	_, err := r.client.ExecContext(ctx, query, workspaceId, jobId, runnerName)
	return err
}

// ReplaceGitHubRunner deletes a runner VM that stopped before GitHub gave it
// a job, and makes the job it was started for due again, as some queued job
// is still counting on a runner. One statement does both, so that a crash
// cannot do only one of them. The job is due after a time that grows with
// its attempts, as a runner that stops every time would otherwise be started
// without a pause, and waits for what waiting says until then.
func (r *PostgresBackendRepository) ReplaceGitHubRunner(ctx context.Context, workspaceId uint, vmName, message, waiting string) error {
	query := `
	WITH deleted AS (
		UPDATE persistent_vm
		SET data = jsonb_set(data, '{desired_state}', '"deleted"')
		WHERE workspace_id = $1 AND name = $2 AND data->>'desired_state' <> 'deleted'
	)
	UPDATE github_runner_job
	SET provision_at = ` + githubRunnerRetry + `, error = $3, waiting = $4, runner_ready_at = NULL
	WHERE workspace_id = $1 AND vm = $2 AND vm <> '';
	`

	_, err := r.client.ExecContext(ctx, query, workspaceId, vmName, message, waiting)
	return err
}

const githubRunnerCacheColumns = `workspace_id, repository, disk_name, snapshot_id, created_at, publisher_at, retired_at`

// SetGitHubRunnerEnhancedCache turns the cache of the account a workspace
// has connected on or off, and returns the account as it then is.
func (r *PostgresBackendRepository) SetGitHubRunnerEnhancedCache(ctx context.Context, workspaceId uint, enabled bool) (*types.GitHubRunnerInstallation, error) {
	query := `
	UPDATE github_runner_installation SET enhanced_cache = $2 WHERE workspace_id = $1
	RETURNING ` + githubRunnerInstallationColumns + `;`
	return r.getGitHubRunnerInstallation(ctx, query, workspaceId, enabled)
}

// CurrentGitHubRunnerCache returns the cache disk that the runners of one
// repository of a workspace start on, which is a new one, named diskName,
// if the repository has none. It returns nil when another gateway made the
// repository's disk in the meantime.
func (r *PostgresBackendRepository) CurrentGitHubRunnerCache(ctx context.Context, workspaceId uint, repository, diskName string) (*types.GitHubRunnerCache, error) {
	query := `
	WITH created AS (
		INSERT INTO github_runner_cache (workspace_id, repository, disk_name) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING
		RETURNING ` + githubRunnerCacheColumns + `
	)
	SELECT ` + githubRunnerCacheColumns + ` FROM created
	UNION ALL
	SELECT ` + githubRunnerCacheColumns + ` FROM github_runner_cache WHERE workspace_id = $1 AND repository = $2 AND retired_at IS NULL
	LIMIT 1;
	`

	var cache types.GitHubRunnerCache
	err := r.client.GetContext(ctx, &cache, query, workspaceId, repository, diskName)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &cache, nil
}

// ClaimGitHubRunnerCachePublisher reports whether a runner that starts now
// on a repository's cache disk may be the one to leave its next generation:
// no other was started to since the disk's last generation, or the last one
// that was is given up on, as it was started longer than wait ago.
func (r *PostgresBackendRepository) ClaimGitHubRunnerCachePublisher(ctx context.Context, workspaceId uint, diskName string, wait time.Duration) (bool, error) {
	query := `
	UPDATE github_runner_cache SET publisher_at = NOW()
	WHERE workspace_id = $1 AND disk_name = $2 AND retired_at IS NULL
		AND (publisher_at IS NULL OR publisher_at <= NOW() - $3 * INTERVAL '1 second');
	`
	result, err := r.client.ExecContext(ctx, query, workspaceId, diskName, wait.Seconds())
	if err != nil {
		return false, err
	}

	count, err := result.RowsAffected()
	return count == 1, err
}

// RetireGitHubRunnerCache retires each cache disk of a workspace that is at
// least as old as age, so that no runner starts on it again and it is
// deleted.
func (r *PostgresBackendRepository) RetireGitHubRunnerCache(ctx context.Context, workspaceId uint, age time.Duration) error {
	query := `
	UPDATE github_runner_cache SET retired_at = NOW()
	WHERE workspace_id = $1 AND retired_at IS NULL AND created_at <= NOW() - $2 * INTERVAL '1 second';
	`
	_, err := r.client.ExecContext(ctx, query, workspaceId, age.Seconds())
	return err
}

// ListGitHubRunnerCacheCandidates returns the generations of the last hour
// that runners left of the generation their repository's runners get now,
// oldest first. A generation is published for the stub of its VM, which is
// named after the VM, and the VM says which generation it started on.
func (r *PostgresBackendRepository) ListGitHubRunnerCacheCandidates(ctx context.Context) ([]types.GitHubRunnerCacheCandidate, error) {
	query := `
	SELECT c.workspace_id, c.repository, c.disk_name, c.snapshot_id AS parent, s.external_id::text AS snapshot_id, v.name AS runner, s.created_at
	FROM github_runner_cache c
	JOIN disk_snapshot s ON s.workspace_id = c.workspace_id AND s.disk_name = c.disk_name
		AND s.status = 'available' AND s.deleted_at IS NULL AND s.created_at > NOW() - INTERVAL '1 hour'
	JOIN stub st ON st.id = s.stub_id AND st.name LIKE 'vm/%'
	JOIN persistent_vm v ON v.workspace_id = c.workspace_id AND v.id = SUBSTRING(st.name FROM 4)
	WHERE c.retired_at IS NULL AND s.external_id::text <> c.snapshot_id
		AND v.data->'spec'->'root_layer'->>'disk_name' = c.disk_name
		AND COALESCE(v.data->'spec'->'root_layer'->>'snapshot_id', '') = c.snapshot_id
	ORDER BY s.created_at
	LIMIT 50;
	`

	candidates := []types.GitHubRunnerCacheCandidate{}
	err := r.client.SelectContext(ctx, &candidates, query)
	return candidates, err
}

// PromoteGitHubRunnerCache makes a candidate the generation its repository's
// runners get, if they still get the one it was made from, and lets the
// next runner be started to leave a generation. It reports whether it did.
func (r *PostgresBackendRepository) PromoteGitHubRunnerCache(ctx context.Context, candidate types.GitHubRunnerCacheCandidate) (bool, error) {
	query := `
	UPDATE github_runner_cache SET snapshot_id = $4, publisher_at = NULL
	WHERE workspace_id = $1 AND disk_name = $2 AND snapshot_id = $3 AND retired_at IS NULL;
	`
	result, err := r.client.ExecContext(ctx, query, candidate.WorkspaceID, candidate.DiskName, candidate.Parent, candidate.SnapshotID)
	if err != nil {
		return false, err
	}

	count, err := result.RowsAffected()
	return count == 1, err
}

// RejectGitHubRunnerCacheCandidate withdraws a candidate for a reason, so
// that it is not considered again, and lets the next runner be started to
// leave a generation. Only a generation of a cache disk is withdrawn.
func (r *PostgresBackendRepository) RejectGitHubRunnerCacheCandidate(ctx context.Context, candidate types.GitHubRunnerCacheCandidate, reason string) error {
	query := `
	WITH rejected AS (
		UPDATE disk_snapshot SET status = 'failed', reason = $4, updated_at = NOW()
		WHERE workspace_id = $1 AND disk_name = $2 AND external_id::text = $3 AND disk_name LIKE $5
		RETURNING workspace_id, disk_name
	)
	UPDATE github_runner_cache c SET publisher_at = NULL
	FROM rejected
	WHERE c.workspace_id = rejected.workspace_id AND c.disk_name = rejected.disk_name;
	`
	_, err := r.client.ExecContext(ctx, query, candidate.WorkspaceID, candidate.DiskName, candidate.SnapshotID, reason, types.RootLayerDiskPrefix+"%")
	return err
}

// ListRetiredGitHubRunnerCaches returns the cache disks that were retired
// at least age ago, by when no runner still runs on one.
func (r *PostgresBackendRepository) ListRetiredGitHubRunnerCaches(ctx context.Context, age time.Duration) ([]types.GitHubRunnerCache, error) {
	query := `
	SELECT ` + githubRunnerCacheColumns + ` FROM github_runner_cache
	WHERE retired_at <= NOW() - $1 * INTERVAL '1 second'
	ORDER BY retired_at
	LIMIT 20;
	`

	caches := []types.GitHubRunnerCache{}
	err := r.client.SelectContext(ctx, &caches, query, age.Seconds())
	return caches, err
}

// DeleteGitHubRunnerCache forgets a retired cache disk and its generations,
// once what was stored for them is gone. Nothing else deletes the rows of a
// disk's generations, and these are deleted only for a cache disk.
func (r *PostgresBackendRepository) DeleteGitHubRunnerCache(ctx context.Context, workspaceId uint, diskName string) error {
	query := `
	WITH retired AS (
		DELETE FROM github_runner_cache
		WHERE workspace_id = $1 AND disk_name = $2 AND retired_at IS NOT NULL AND disk_name LIKE $3
		RETURNING workspace_id, disk_name
	)
	DELETE FROM disk_snapshot s USING retired
	WHERE s.workspace_id = retired.workspace_id AND s.disk_name = retired.disk_name;
	`
	_, err := r.client.ExecContext(ctx, query, workspaceId, diskName, types.RootLayerDiskPrefix+"%")
	return err
}

// GetGitHubRunnerJobByRunner returns the job that GitHub gave a runner, or
// nil if it has reported none.
func (r *PostgresBackendRepository) GetGitHubRunnerJobByRunner(ctx context.Context, workspaceId uint, runner string) (*types.GitHubRunnerJob, error) {
	query := `
	SELECT ` + githubRunnerJobColumns + ` FROM github_runner_job
	WHERE workspace_id = $1 AND runner_name = $2 AND runner_name <> ''
	ORDER BY created_at DESC
	LIMIT 1;
	`

	var job types.GitHubRunnerJob
	err := r.client.GetContext(ctx, &job, query, workspaceId, runner)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &job, nil
}
