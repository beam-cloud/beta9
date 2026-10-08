package repository

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/types"
	"time"
)

type VMRepository interface {
	CreateVM(context.Context, *types.VM) error
	SaveVM(context.Context, *types.VM) error
	GetVM(context.Context, uint, string) (*types.VM, error)
	GetVMByHandle(context.Context, string) (*types.VM, error)
	ListVMs(context.Context, uint) ([]*types.VM, error)
	LockVM(context.Context, string) (func(), error)
	TouchVM(context.Context, string) error
	ClaimVMIdleStop(context.Context, string, time.Time) (bool, error)
	CreateVMArtifact(context.Context, uint, *types.VMArtifact) error
	ListVMArtifacts(context.Context, uint, string) ([]types.VMArtifact, error)
	DeleteVMArtifact(context.Context, uint, string, string) error
}

func (r *PostgresBackendRepository) CreateVM(ctx context.Context, vm *types.VM) error {
	data, err := json.Marshal(vm)
	if err != nil {
		return err
	}
	_, err = r.client.ExecContext(ctx, `INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, vm.ID, vm.WorkspaceID, vm.WorkspaceExternalID, vm.TokenID, vm.Name, vm.Handle, data, vm.LastActiveAt)
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
	WorkspaceID         uint      `db:"workspace_id"`
	WorkspaceExternalID string    `db:"workspace_external_id"`
	TokenID             string    `db:"token_id"`
	Data                []byte    `db:"data"`
	LastActiveAt        time.Time `db:"last_active_at"`
}

func (row vmRow) vm() (*types.VM, error) {
	var vm types.VM
	if err := json.Unmarshal(row.Data, &vm); err != nil {
		return nil, err
	}
	vm.WorkspaceID = row.WorkspaceID
	vm.WorkspaceExternalID = row.WorkspaceExternalID
	vm.TokenID = row.TokenID
	vm.LastActiveAt = row.LastActiveAt
	return &vm, nil
}

func (r *PostgresBackendRepository) GetVM(ctx context.Context, ws uint, name string) (*types.VM, error) {
	var row vmRow
	err := r.client.GetContext(ctx, &row, `SELECT workspace_id,workspace_external_id,token_id,data,last_active_at FROM persistent_vm WHERE workspace_id=$1 AND (name=$2 OR id::text=$2) ORDER BY (data->>'desired_state'='deleted') LIMIT 1`, ws, name)
	if err != nil {
		return nil, err
	}
	return row.vm()
}

func (r *PostgresBackendRepository) GetVMByHandle(ctx context.Context, handle string) (*types.VM, error) {
	var row vmRow
	err := r.client.GetContext(ctx, &row, `SELECT workspace_id,workspace_external_id,token_id,data,last_active_at FROM persistent_vm WHERE handle=$1`, handle)
	if err != nil {
		return nil, err
	}
	return row.vm()
}

func (r *PostgresBackendRepository) ListVMs(ctx context.Context, ws uint) ([]*types.VM, error) {
	rows := []vmRow{}
	err := r.client.SelectContext(ctx, &rows, `SELECT workspace_id,workspace_external_id,token_id,data,last_active_at FROM persistent_vm WHERE ($1=0 OR workspace_id=$1) ORDER BY data->>'created_at'`, ws)
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

// A session advisory lock fences lifecycle operations across gateway replicas.
// Writes are committed before external side effects, so a replacement gateway
// can resume the persisted intent after a crash. Closing the session releases
// the lock, including when a gateway dies during a snapshot.
func (r *PostgresBackendRepository) LockVM(ctx context.Context, id string) (func(), error) {
	conn, err := r.client.Connx(ctx)
	if err != nil {
		return nil, err
	}
	var acquired bool
	err = conn.GetContext(ctx, &acquired, `SELECT pg_try_advisory_lock(hashtextextended($1, 947))`, id)
	if err != nil || !acquired {
		if err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		conn.Close()
		if err == nil {
			err = fmt.Errorf("VM operation already in progress")
		}
		return nil, err
	}
	return func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1, 947))`, id); err != nil {
			// A failed unlock must not return a locked session to the pool.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		conn.Close()
	}, nil
}

func (r *PostgresBackendRepository) TouchVM(ctx context.Context, id string) error {
	_, err := r.client.ExecContext(ctx, `UPDATE persistent_vm SET last_active_at=NOW() WHERE id=$1`, id)
	return err
}

func (r *PostgresBackendRepository) ClaimVMIdleStop(ctx context.Context, id string, cutoff time.Time) (bool, error) {
	result, err := r.client.ExecContext(ctx, `UPDATE persistent_vm SET data=jsonb_set(data,'{desired_state}','"stopped"') WHERE id=$1 AND data->>'desired_state'='running' AND last_active_at <= $2`, id, cutoff)
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
	err := r.client.SelectContext(ctx, &rows, `SELECT data FROM vm_artifact WHERE workspace_id=$1 AND kind=$2 ORDER BY data->>'created_at'`, ws, kind)
	if err != nil {
		return nil, err
	}
	result := []types.VMArtifact{}
	for _, data := range rows {
		var a types.VMArtifact
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, nil
}

func (r *PostgresBackendRepository) DeleteVMArtifact(ctx context.Context, ws uint, kind, name string) error {
	_, err := r.client.ExecContext(ctx, `DELETE FROM vm_artifact WHERE workspace_id=$1 AND kind=$2 AND (name=$3 OR id::text=$3)`, ws, kind, name)
	return err
}
