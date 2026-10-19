package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
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

	_, err = r.client.ExecContext(ctx, `INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,'')::inet)`, vm.ID, vm.WorkspaceID, vm.WorkspaceExternalID, vm.TokenID, vm.Name, vm.Handle, data, vm.LastActiveAt, vm.PublicIPv6)
	return err
}

func (r *PostgresBackendRepository) SaveVM(ctx context.Context, vm *types.VM) error {
	vm.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(vm)
	if err != nil {
		return err
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
