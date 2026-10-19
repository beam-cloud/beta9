package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
)

// CreateVMWithStub publishes a complete first launch in one commit. A failed
// name, address or stub write must not leave a root disk or a partial VM.
// VM activity belongs to persistent_vm; touching their shared hidden app would
// serialize otherwise independent launches on the same app row.
func (r *PostgresBackendRepository) CreateVMWithStub(ctx context.Context, vm *types.VM, stub types.Stub) error {
	launch, err := newVMLaunchInsert(vm, stub)
	if err != nil {
		return err
	}
	result, err := r.vmCreates.commit(ctx, launch, r.createVMLaunches)
	if err != nil {
		return err
	}
	return result
}

func newVMLaunchInsert(vm *types.VM, stub types.Stub) (*vmLaunchInsert, error) {
	config, err := stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if config == nil || stub.Type != types.StubType(types.StubTypeVM) || stub.WorkspaceId != vm.WorkspaceID || stub.ExternalId != vm.StubID || vm.ContainerID == "" {
		return nil, fmt.Errorf("VM and stub launch identities do not match")
	}
	var root *types.Disk
	for _, disk := range config.Disks {
		if disk == nil || disk.MountPath != types.DurableDiskRootMountPath {
			continue
		}
		if root != nil || disk.Name != vm.ID || disk.Driver != types.DurableDiskDriverQcow || disk.Filesystem != "ext4" || disk.ReadOnly {
			return nil, fmt.Errorf("VM requires its own writable qcow root disk")
		}
		if _, err := configuredDiskBytes(disk.Size); err != nil {
			return nil, err
		}
		root = &types.Disk{Name: disk.Name, Size: disk.Size, Driver: disk.Driver, Filesystem: disk.Filesystem}
	}
	if root == nil {
		return nil, fmt.Errorf("VM root disk is missing")
	}
	data, err := json.Marshal(vm)
	if err != nil {
		return nil, err
	}
	return &vmLaunchInsert{
		ID: vm.ID, WorkspaceID: vm.WorkspaceID, WorkspaceExternalID: vm.WorkspaceExternalID,
		TokenID: vm.TokenID, Name: vm.Name, Handle: vm.Handle, Data: data,
		LastActiveAt: vm.LastActiveAt, PublicIPv6: vm.PublicIPv6, Size: root.Size,
		StubID: stub.ExternalId, StubName: stub.Name, StubConfig: json.RawMessage(stub.Config),
		ObjectID: stub.ObjectId, AppID: stub.AppId,
	}, nil
}

// Marshal launch values before enqueueing; callers may reuse their VM after
// the commit completes, and no queued write may retain mutable launch state.
type vmLaunchInsert struct {
	ID                  string          `json:"id"`
	WorkspaceID         uint            `json:"workspace_id"`
	WorkspaceExternalID string          `json:"workspace_external_id"`
	TokenID             string          `json:"token_id"`
	Name                string          `json:"name"`
	Handle              string          `json:"handle"`
	Data                json.RawMessage `json:"data"`
	LastActiveAt        time.Time       `json:"last_active_at"`
	PublicIPv6          string          `json:"public_ipv6"`
	Size                string          `json:"size"`
	StubID              string          `json:"stub_id"`
	StubName            string          `json:"stub_name"`
	StubConfig          json.RawMessage `json:"stub_config"`
	ObjectID            uint            `json:"object_id"`
	AppID               uint            `json:"app_id"`
}

func (r *PostgresBackendRepository) createVMLaunches(ctx context.Context, launches []*vmLaunchInsert) (map[*vmLaunchInsert]error, error) {
	data, err := json.Marshal(launches)
	if err != nil {
		return nil, err
	}
	_, err = r.client.ExecContext(ctx, `
	WITH launches AS (
		SELECT * FROM jsonb_to_recordset($1::jsonb) AS launch(
			id text, workspace_id bigint, workspace_external_id text, token_id text,
			name text, handle text, data jsonb, last_active_at timestamptz,
			public_ipv6 text, size text, stub_id uuid, stub_name text,
			stub_config json, object_id bigint, app_id bigint)
	), roots AS (
		INSERT INTO disk (workspace_id,name,size,filesystem,driver,mount_path)
		SELECT workspace_id,id,size,'ext4','qcow','/' FROM launches
		RETURNING workspace_id,name
	), launch_stubs AS (
		INSERT INTO stub (external_id,name,type,config,object_id,workspace_id,app_id)
		SELECT l.stub_id,l.stub_name,'vm',l.stub_config,l.object_id,l.workspace_id,l.app_id
		FROM launches l JOIN roots r ON r.workspace_id=l.workspace_id AND r.name=l.id
		RETURNING external_id
	)
	INSERT INTO persistent_vm (id,workspace_id,workspace_external_id,token_id,name,handle,data,last_active_at,public_ipv6)
	SELECT l.id,l.workspace_id,l.workspace_external_id,l.token_id,l.name,l.handle,l.data,l.last_active_at,NULLIF(l.public_ipv6,'')::inet
	FROM launches l JOIN launch_stubs s ON s.external_id=l.stub_id`, data)
	if err == nil {
		return nil, nil
	}
	var rejected *pq.Error
	if len(launches) == 1 || !errors.As(err, &rejected) || (rejected.Code.Class() != "22" && rejected.Code.Class() != "23") {
		return nil, err
	}
	// A data or constraint failure rolls back the statement. Isolate its rows
	// so a name/address conflict cannot reject unrelated launches. Connection
	// failures and ambiguous commits are never replayed here.
	results := make(map[*vmLaunchInsert]error, len(launches))
	for _, launch := range launches {
		_, results[launch] = r.createVMLaunches(ctx, []*vmLaunchInsert{launch})
	}
	return results, nil
}
