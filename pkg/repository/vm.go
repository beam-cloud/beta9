package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type VMRepository interface {
	CreateVM(context.Context, *types.VM) error
	CreateVMWithStub(context.Context, *types.VM, types.Stub) error
	SaveVM(context.Context, *types.VM) error
	GetVM(context.Context, uint, string) (*types.VM, error)
	GetVMByHandle(context.Context, string) (*types.VM, error)
	ListVMs(context.Context, uint) ([]*types.VM, error)
	ListVMsToReconcile(context.Context, bool) ([]*types.VM, error)
	LockVM(context.Context, string) (func(), error)
	TryLockVM(context.Context, string) (func(), error)
	TouchVM(context.Context, string) error
	ClaimVMIdleStop(context.Context, string, time.Time) (bool, error)
	CreateVMArtifact(context.Context, uint, *types.VMArtifact) error
	ListVMArtifacts(context.Context, uint, string) ([]types.VMArtifact, error)
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

// CreateVMWithStub publishes a complete first launch in one commit. A failed
// name, address or stub write must not leave a root disk or a partial VM.
// VM activity belongs to persistent_vm; touching their shared hidden app would
// serialize otherwise independent launches on the same app row.
func (r *PostgresBackendRepository) CreateVMWithStub(ctx context.Context, vm *types.VM, stub types.Stub) error {
	config, err := stub.UnmarshalConfig()
	if err != nil {
		return err
	}
	if config == nil || stub.Type != types.StubType(types.StubTypeVM) || stub.WorkspaceId != vm.WorkspaceID || stub.ExternalId != vm.StubID || vm.ContainerID == "" {
		return fmt.Errorf("VM and stub launch identities do not match")
	}
	var root *types.Disk
	for _, disk := range config.Disks {
		if disk == nil || disk.MountPath != types.DurableDiskRootMountPath {
			continue
		}
		if root != nil || disk.Name != vm.ID || disk.Driver != types.DurableDiskDriverQcow || disk.Filesystem != "ext4" || disk.ReadOnly {
			return fmt.Errorf("VM requires its own writable qcow root disk")
		}
		if _, err := configuredDiskBytes(disk.Size); err != nil {
			return err
		}
		root = &types.Disk{Name: disk.Name, Size: disk.Size, Driver: disk.Driver, Filesystem: disk.Filesystem}
	}
	if root == nil {
		return fmt.Errorf("VM root disk is missing")
	}
	data, err := json.Marshal(vm)
	if err != nil {
		return err
	}
	_, err = r.client.ExecContext(ctx, `
	WITH root AS (
		INSERT INTO disk (workspace_id,name,size,filesystem,driver,mount_path)
		VALUES ($2,$1,$10,'ext4','qcow','/') RETURNING id
	), launch_stub AS (
		INSERT INTO stub (external_id,name,type,config,object_id,workspace_id,app_id)
		SELECT $11,$12,'vm',$13::json,$14,$2,$15 FROM root RETURNING id
	)
	INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6)
	SELECT $1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,'')::inet FROM launch_stub`,
		vm.ID, vm.WorkspaceID, vm.WorkspaceExternalID, vm.TokenID, vm.Name, vm.Handle, data, vm.LastActiveAt, vm.PublicIPv6,
		root.Size, stub.ExternalId, stub.Name, stub.Config, stub.ObjectId, stub.AppId)
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

	_, err = r.client.ExecContext(ctx, `INSERT INTO vm_artifact(id,workspace_id,name,kind,vm_id,data) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING`, a.ID, ws, a.Name, a.Kind, a.VMID, data)
	return err
}

func (r *PostgresBackendRepository) ListVMArtifacts(ctx context.Context, ws uint, kind string) ([]types.VMArtifact, error) {
	var rows [][]byte
	// Source VMs are soft deleted; retain their identity even after the live
	// VM list stops returning them. The captured name also survives a purge.
	err := r.client.SelectContext(ctx, &rows, `SELECT a.data || jsonb_build_object('vm_name',COALESCE(v.data->>'name',a.data->>'vm_name'),'vm_deleted',COALESCE(v.data->>'desired_state'='deleted',false)) FROM vm_artifact a LEFT JOIN persistent_vm v ON v.id=a.vm_id AND v.workspace_id=a.workspace_id WHERE a.workspace_id=$1 AND a.kind=$2 ORDER BY (a.data->>'created_at')::timestamptz`, ws, kind)
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
