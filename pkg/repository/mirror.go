package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// ErrMirrorGenerationConflict means the generation a commit was based on is
// no longer the mirror's latest: another writer committed first.
var ErrMirrorGenerationConflict = errors.New("mirror generation conflict")

type MirrorRepository interface {
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
	MarkMirrorObjectsDeleted(ctx context.Context, mirrorID string) error
	ListPurgedMirrors(ctx context.Context) ([]types.Mirror, error)
	// ListMirrorIDs lists every live mirror with its workspace, for sweeps.
	ListMirrorIDs(ctx context.Context) ([]types.Mirror, error)
	// LockVM is the gateway-wide advisory lock, keyed here by mirror ID.
	LockVM(context.Context, string) (func(), error)
}

const mirrorGenerationColumns = `mirror_id,number,parent,manifest_digest,manifest_size,entries,objects,total_size,token_id,created_at,expired_at`
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
	TokenID        string     `db:"token_id"`
	CreatedAt      time.Time  `db:"created_at"`
	ExpiredAt      *time.Time `db:"expired_at"`
}

func (row mirrorGenerationRow) generation() types.MirrorGeneration {
	return types.MirrorGeneration{
		MirrorID: row.MirrorID, Number: row.Number, Parent: row.Parent,
		ManifestDigest: row.ManifestDigest, ManifestSize: row.ManifestSize,
		Entries: row.Entries, Objects: row.Objects, TotalSize: row.TotalSize,
		TokenID: row.TokenID, CreatedAt: row.CreatedAt, ExpiredAt: row.ExpiredAt,
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
INSERT INTO mirror_generation (mirror_id,number,parent,manifest_digest,manifest_size,entries,objects,total_size,token_id)
SELECT m.id,$2+1,$2,$3,$4,$5,$6,$7,$8 FROM mirror m
WHERE m.id=$1 AND m.purged_at IS NULL
 AND COALESCE((SELECT MAX(number) FROM mirror_generation WHERE mirror_id=m.id),0)=$2
RETURNING `+mirrorGenerationColumns,
		generation.MirrorID, generation.Parent, generation.ManifestDigest, generation.ManifestSize,
		generation.Entries, generation.Objects, generation.TotalSize, generation.TokenID)
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

func (r *PostgresBackendRepository) MarkMirrorObjectsDeleted(ctx context.Context, mirrorID string) error {
	_, err := r.client.ExecContext(ctx, `UPDATE mirror SET objects_deleted_at=NOW() WHERE id=$1 AND purged_at IS NOT NULL`, mirrorID)
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
