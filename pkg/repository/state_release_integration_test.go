//go:build statevolume_integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	stateMigrations "github.com/beam-cloud/beta9/pkg/repository/backend_postgres_migrations"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func stateReleaseIntegrationDatabase(t *testing.T) *sql.DB {
	t.Helper()
	adminDSN := strings.TrimSpace(os.Getenv("BETA9_STATE_MIGRATION_TEST_ADMIN_DSN"))
	if adminDSN == "" {
		t.Fatal("BETA9_STATE_MIGRATION_TEST_ADMIN_DSN is required for the privileged release-intent suite")
	}
	parsed, err := url.Parse(adminDSN)
	require.NoError(t, err)
	admin, err := sql.Open("postgres", adminDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	name := "beta9_state_release_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(`CREATE DATABASE "` + name + `"`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec(`DROP DATABASE IF EXISTS "` + name + `" WITH (FORCE)`) })
	parsed.Path = "/" + name
	db, err := sql.Open("postgres", parsed.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE TABLE workspace (id SERIAL PRIMARY KEY, external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL, name TEXT NOT NULL UNIQUE)`,
		`CREATE TABLE stub (id SERIAL PRIMARY KEY, external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL, workspace_id INT REFERENCES workspace(id))`,
	} {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
	require.NoError(t, stateMigrations.ApplyStateVolumeCutoverForIntegration(context.Background(), db))
	_, err = db.Exec(`INSERT INTO workspace(id, name) VALUES (1, 'release-test');
		INSERT INTO stub(id, workspace_id) VALUES (1, 1);`)
	require.NoError(t, err)
	return db
}

func seedReleaseIntegrationAttachment(t *testing.T, db *sql.DB, containerID string, fence int64,
	expiresAt time.Time,
) (types.StateVolumeLease, []types.StateVolumeReleaseMember) {
	t.Helper()
	volumeID, planID, attachmentToken := uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err := db.Exec(`INSERT INTO state_volume_attachment_plan
		(plan_id, workspace_id, container_id, request_hash, expected_writable_members)
		VALUES ($1,1,$2,$3,1);`, planID, containerID, strings.Repeat("c", 64))
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO state_volume(external_id, workspace_id, name, size, mount_path, next_fencing_token)
		VALUES ($1,1,$2,'4Gi',$3,$4);`, volumeID, "data-"+containerID, "/data", fence)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO state_volume_attachment(state_volume_id, attachment_plan_id, workspace_id,
		 container_id, initialize, attachment_token, fencing_token, owner_worker_id,
		 owner_worker_instance_id, storage_node_id, expires_at)
		SELECT id,$1,1,$2,TRUE,$3,$4,'worker-source','epoch-source','node-1',$5
		FROM state_volume WHERE external_id=$6;`, planID, containerID, attachmentToken, fence, expiresAt, volumeID)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE state_volume_attachment_plan SET admitted_at=CURRENT_TIMESTAMP
		WHERE plan_id=$1`, planID)
	require.NoError(t, err)
	return types.StateVolumeLease{VolumeId: volumeID, AttachmentToken: attachmentToken, FencingToken: fence},
		[]types.StateVolumeReleaseMember{{VolumeId: volumeID, FencingToken: fence}}
}

func TestStateVolumeReleaseIntentRealPostgresBeginReleaseAndRecoveryHandoff(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres")}
	ctx := context.Background()
	digest := "sha256:" + strings.Repeat("d", 64)

	lease, members := seedReleaseIntegrationAttachment(t, db, "released-before-crash", 7, time.Now().Add(time.Minute))
	intent, err := repo.BeginStateVolumeReleaseIntent(ctx, 1, "released-before-crash",
		"worker-source", "epoch-source", "node-1", digest, members)
	require.NoError(t, err)
	require.EqualValues(t, 0, intent.ClaimGeneration)
	require.Equal(t, "source", intent.Phase)
	require.NoError(t, repo.ReleaseStateVolumeAttachments(ctx, 1, "released-before-crash",
		"worker-source", "epoch-source", "node-1", []types.StateVolumeLease{lease}))
	completed, err := repo.ClaimStateVolumeRelease(ctx, 1, "released-before-crash",
		"worker-source", "epoch-source", "node-1", "worker-recovery", "epoch-recovery",
		digest, 0, members)
	require.NoError(t, err)
	require.True(t, completed.Completed)
	require.EqualValues(t, 0, completed.ClaimGeneration)

	lease, members = seedReleaseIntegrationAttachment(t, db, "recovered-release", 11,
		time.Now().Add(-31*time.Second))
	intent, err = repo.BeginStateVolumeReleaseIntent(ctx, 1, "recovered-release",
		"worker-source", "epoch-source", "node-1", digest, members)
	require.NoError(t, err)
	claim, err := repo.ClaimStateVolumeRelease(ctx, 1, "recovered-release",
		"worker-source", "epoch-source", "node-1", "worker-recovery", "epoch-recovery",
		digest, 0, members)
	require.NoError(t, err)
	require.EqualValues(t, 1, claim.ClaimGeneration)
	require.False(t, claim.Completed)
	require.NoError(t, repo.CompleteClaimedStateVolumeRelease(ctx, 1, "recovered-release",
		claim.ExternalId, "worker-recovery", "epoch-recovery", "node-1", claim.ClaimGeneration))
	stored, err := repo.GetStateVolumeReleaseClaim(ctx, 1, "recovered-release")
	require.NoError(t, err)
	require.True(t, stored.Completed)
	var attachments int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM state_volume_attachment WHERE container_id=$1`,
		"recovered-release").Scan(&attachments))
	require.Zero(t, attachments, fmt.Sprintf("attachment survived claimed completion for %s", lease.VolumeId))

	lease, members = seedReleaseIntegrationAttachment(t, db, "terminal-with-intent", 15, time.Now().Add(time.Minute))
	intent, err = repo.BeginStateVolumeReleaseIntent(ctx, 1, "terminal-with-intent",
		"worker-source", "epoch-source", "node-1", digest, members)
	require.NoError(t, err)
	tx, err := repo.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	require.NoError(t, err)
	require.NoError(t, releaseCommittedTerminalStateLeases(ctx, tx, 1, "terminal-with-intent",
		"worker-source", "epoch-source", "node-1", map[string]types.StateVolumeLease{lease.VolumeId: lease}))
	require.NoError(t, tx.Commit())
	stored, err = repo.GetStateVolumeReleaseClaim(ctx, 1, "terminal-with-intent")
	require.NoError(t, err)
	require.True(t, stored.Completed)
	require.Equal(t, "completed", stored.Phase)

	lease, _ = seedReleaseIntegrationAttachment(t, db, "terminal-without-intent", 19, time.Now().Add(time.Minute))
	tx, err = repo.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	require.NoError(t, err)
	require.NoError(t, releaseCommittedTerminalStateLeases(ctx, tx, 1, "terminal-without-intent",
		"worker-source", "epoch-source", "node-1", map[string]types.StateVolumeLease{lease.VolumeId: lease}))
	require.NoError(t, tx.Commit())

	lease, members = seedReleaseIntegrationAttachment(t, db, "crash-before-begin", 23,
		time.Now().Add(-31*time.Second))
	claim, err = repo.ClaimStateVolumeRelease(ctx, 1, "crash-before-begin",
		"worker-source", "epoch-source", "node-1", "worker-recovery", "epoch-recovery",
		"sha256:"+strings.Repeat("e", 64), 0, members)
	require.NoError(t, err)
	require.EqualValues(t, 1, claim.ClaimGeneration)
	require.Equal(t, "claimed", claim.Phase)
	require.NoError(t, repo.CompleteClaimedStateVolumeRelease(ctx, 1, "crash-before-begin",
		claim.ExternalId, "worker-recovery", "epoch-recovery", "node-1", claim.ClaimGeneration))
}
