//go:build statevolume_integration

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"github.com/stretchr/testify/require"
)

type stateCacheIntegrationSink struct {
	mu             sync.Mutex
	state          *stubCacheRequiredContentState
	beforeAppend   func(types.EventStubCacheRequiredContentSchema) error
	writtenRecords []types.EventStubCacheRequiredContentSchema
}

func newStateCacheIntegrationEventRepo(t *testing.T, sink *stateCacheIntegrationSink) *EventClientRepo {
	t.Helper()
	s2Sink := &S2EventRepository{
		streamPrefix: "events",
		appendRecords: func(_ s2.StreamName, records []s2.AppendRecord) error {
			for _, record := range records {
				var envelope struct {
					Data types.EventStubCacheRequiredContentSchema `json:"data"`
				}
				if err := json.Unmarshal(record.Body, &envelope); err != nil {
					return err
				}
				if sink.beforeAppend != nil {
					if err := sink.beforeAppend(envelope.Data); err != nil {
						return err
					}
				}
				sink.mu.Lock()
				sink.writtenRecords = append(sink.writtenRecords, envelope.Data)
				mergeStubCacheRequiredContentRecordIntoState(sink.state, record.Body)
				sink.mu.Unlock()
			}
			return nil
		},
	}
	return &EventClientRepo{storageSinks: []eventSink{s2Sink}}
}

func seedStateCacheGeneration(t *testing.T, db *sql.DB, volumeID, generationID, name string,
	generation int64, parentID, cloneParentID string,
) {
	t.Helper()
	var parent, clone any
	if parentID != "" {
		parent = parentID
	}
	if cloneParentID != "" {
		clone = cloneParentID
	}
	_, err := db.Exec(`INSERT INTO volume_generation
		(external_id, workspace_id, stub_id, volume_id, name, parent_generation_id,
		 clone_parent_generation_id, generation, status, manifest_key, manifest_digest,
		 manifest_size_bytes, chunk_count, logical_size_bytes, stored_size_bytes,
		 bucket_name, object_prefix, completed_at)
		VALUES ($1,1,1,$2,$3,$4,$5,$6,'available','manifest.json',$7,1,1,4096,4096,
		        'state-origin',$8,CURRENT_TIMESTAMP);`, generationID, volumeID, name, parent, clone,
		generation, "sha256:"+fmt.Sprintf("%064d", generation), "state-volumes/"+generationID)
	require.NoError(t, err)
}

func seedAvailableStateCacheSnapshot(t *testing.T, db *sql.DB, sourceStubID, volumeID,
	generationID, containerID string, generation int64,
) (uint, string) {
	t.Helper()
	snapshotID := uuid.NewString()
	operationID := "cache-retire-" + uuid.NewString()
	var databaseID uint
	err := db.QueryRow(`INSERT INTO state_snapshot
		(external_id, operation_id, workspace_id, stub_id, source_stub_external_id,
		 source_stub_name, source_stub_type, source_container_id, source_worker_id,
		 source_worker_instance_id, storage_node_id, mode, include_memory, visible,
		 status, image_id, image_digest, runtime_profile)
		VALUES ($1,$2,1,1,$3,'cache-test','pod',$4,'worker','worker-epoch','node',
		        'terminal',FALSE,TRUE,'pending','image','sha256:image','cpu') RETURNING id;`,
		snapshotID, operationID, sourceStubID, containerID).Scan(&databaseID)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO state_snapshot_generation
		(state_snapshot_id, volume_generation_id, volume_id, name, mount_path, read_only, is_root, generation)
		SELECT $1,id,$2,'root','/',FALSE,TRUE,$4 FROM volume_generation WHERE external_id=$3;`,
		databaseID, volumeID, generationID, generation)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE state_snapshot SET status='available', armed_at=CURRENT_TIMESTAMP,
		completed_at=CURRENT_TIMESTAMP WHERE id=$1;`, databaseID)
	require.NoError(t, err)
	return databaseID, snapshotID
}

func stateCacheIntegrationIdentity(t *testing.T, db *sql.DB) (string, string) {
	t.Helper()
	var workspaceID, stubID string
	require.NoError(t, db.QueryRow(`SELECT w.external_id::text, s.external_id::text
		FROM workspace w JOIN stub s ON s.workspace_id=w.id WHERE w.id=1 AND s.id=1;`).Scan(&workspaceID, &stubID))
	return workspaceID, stubID
}

func publishStateCacheScope(t *testing.T, eventRepo *EventClientRepo, workspaceID, stubID,
	volumeID, generationID, hash string,
) {
	t.Helper()
	records, err := types.BuildScopedCacheRequiredContentRevision(workspaceID, stubID, "node", volumeID,
		1, generationID, []types.CacheRequiredContentItem{{
			Kind: types.CacheContentKindStateManifest, Hash: hash, RoutingKey: hash,
			ExpectedHash: hash, Source: "state-volumes/" + generationID + "/manifest.json",
			VolumeID: volumeID, GenerationID: generationID, SizeBytes: 1,
		}}, false)
	require.NoError(t, err)
	for _, record := range records {
		require.NoError(t, eventRepo.PushStubCacheRequiredContent(record))
	}
}

func backdateStateCacheRetirement(t *testing.T, db *sql.DB, snapshotID uint) {
	t.Helper()
	_, err := db.Exec(`UPDATE state_snapshot SET cache_retire_after=CURRENT_TIMESTAMP-INTERVAL '1 second'
		WHERE id=$1;`, snapshotID)
	require.NoError(t, err)
}

func TestStateCacheRetirementRealPostgresDefersLiveHeadAndPreservesForkCloneParent(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	workspaceExternalID, stubExternalID := stateCacheIntegrationIdentity(t, db)
	sink := &stateCacheIntegrationSink{state: &stubCacheRequiredContentState{
		items: map[string]types.CacheRequiredContentItem{}, scopes: map[string]*stubCacheRequiredContentScope{},
	}}
	eventRepo := newStateCacheIntegrationEventRepo(t, sink)
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres"), eventRepo: eventRepo}
	ctx := context.Background()

	// A currently-headed lineage keeps its complete state group retryable. It
	// creates no partial tombstone and does not lose the retirement deadline.
	liveVolumeID, liveGenerationID := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, liveVolumeID, liveGenerationID, "root", 1, "", "")
	liveSnapshotDBID, liveSnapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		liveVolumeID, liveGenerationID, "live-head", 1)
	_, err := db.Exec(`INSERT INTO state_branch_lineage
		(workspace_id, stub_external_id, member_name, mount_path, is_root, volume_id, size, current_generation_id)
		VALUES (1,$1,'root','/',TRUE,$2,'4Gi',$3);`, stubExternalID, liveVolumeID, liveGenerationID)
	require.NoError(t, err)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, liveSnapshotID, "machine", "machine:live-head:"+liveSnapshotID)
	require.NoError(t, err)
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, liveSnapshotID, "machine", "machine:live-head:"+liveSnapshotID)
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, liveSnapshotDBID)
	processed, err := repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Zero(t, processed)
	var outboxes int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM state_cache_retirement_outbox
		WHERE state_snapshot_id=$1 AND status='pending';`, liveSnapshotDBID).Scan(&outboxes))
	require.Zero(t, outboxes)
	var retryArmed bool
	require.NoError(t, db.QueryRow(`SELECT cache_retire_after IS NOT NULL FROM state_snapshot WHERE id=$1`,
		liveSnapshotDBID).Scan(&retryArmed))
	require.True(t, retryArmed)

	// Once the authoritative head is cleared, the reconciler redrives without
	// a teardown-specific callback and publishes the exact scoped tombstone.
	_, err = db.Exec(`DELETE FROM state_branch_lineage WHERE volume_id=$1`, liveVolumeID)
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, liveSnapshotDBID)
	publishStateCacheScope(t, eventRepo, workspaceExternalID, stubExternalID, liveVolumeID,
		liveGenerationID, "sha256:"+fmt.Sprintf("%064x", 1))
	processed, err = repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	// A fork scope is independent, while its cross-volume physical clone
	// parent remains reachable in PostgreSQL after the source scope retires.
	sourceVolumeID, sourceGenerationID := uuid.NewString(), uuid.NewString()
	forkVolumeID, forkGenerationID := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, sourceVolumeID, sourceGenerationID, "root", 1, "", "")
	seedStateCacheGeneration(t, db, forkVolumeID, forkGenerationID, "root", 1, "", sourceGenerationID)
	sourceSnapshotDBID, sourceSnapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		sourceVolumeID, sourceGenerationID, "source", 1)
	forkSnapshotDBID, forkSnapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		forkVolumeID, forkGenerationID, "fork", 1)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, sourceSnapshotID, "snapshot", "snapshot:source")
	require.NoError(t, err)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, forkSnapshotID, "machine", "machine:fork")
	require.NoError(t, err)
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, sourceSnapshotID, "snapshot", "snapshot:source")
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, sourceSnapshotDBID)
	publishStateCacheScope(t, eventRepo, workspaceExternalID, stubExternalID, sourceVolumeID,
		sourceGenerationID, "sha256:"+fmt.Sprintf("%064x", 2))
	publishStateCacheScope(t, eventRepo, workspaceExternalID, stubExternalID, forkVolumeID,
		forkGenerationID, "sha256:"+fmt.Sprintf("%064x", 3))
	processed, err = repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	var sourceGenerationCount, forkSnapshotCount int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM volume_generation WHERE external_id=$1`,
		sourceGenerationID).Scan(&sourceGenerationCount))
	require.Equal(t, 1, sourceGenerationCount, "fork clone parent was pruned with its source scope")
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM state_snapshot WHERE external_id=$1`,
		forkSnapshotID).Scan(&forkSnapshotCount))
	require.Equal(t, 1, forkSnapshotCount)
	// The released source reference tombstone survives metadata GC, so a
	// response-loss retry remains byte-for-byte idempotent.
	replayedRelease, err := repo.ReleaseStateSnapshotReference(ctx, 1, sourceSnapshotID,
		"snapshot", "snapshot:source")
	require.NoError(t, err)
	require.True(t, replayedRelease.Released)

	sink.mu.Lock()
	items := sink.state.requiredContentItems()
	sink.mu.Unlock()
	hashes := map[string]bool{}
	for _, item := range items {
		hashes[item.Hash] = true
	}
	require.False(t, hashes["sha256:"+fmt.Sprintf("%064x", 2)], "retired source scope remained visible")
	require.True(t, hashes["sha256:"+fmt.Sprintf("%064x", 3)], "independent fork scope was tombstoned")

	// Retiring the fork revisits its exact physical Parent/Clone ancestry. The
	// fork row is deleted first and its now-unreachable source-volume parent is
	// then eligible for bounded pruning in the same transaction.
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, forkSnapshotID, "machine", "machine:fork")
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, forkSnapshotDBID)
	processed, err = repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	var remainingPhysicalGenerations int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM volume_generation
		WHERE external_id IN ($1,$2)`, sourceGenerationID, forkGenerationID).Scan(&remainingPhysicalGenerations))
	require.Zero(t, remainingPhysicalGenerations)
	sink.mu.Lock()
	items = sink.state.requiredContentItems()
	sink.mu.Unlock()
	require.Empty(t, items, "fork retirement retained a cache scope after both references were released")
}

func TestStateCacheRetirementRealPostgresFencesActiveDeleteAndSerializesRetainAgainstTombstone(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	_, stubExternalID := stateCacheIntegrationIdentity(t, db)
	volumeID, generationID := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, volumeID, generationID, "root", 1, "", "")
	snapshotDBID, snapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		volumeID, generationID, "retain-race", 1)

	started, allowAppend := make(chan struct{}), make(chan struct{})
	var startOnce sync.Once
	sink := &stateCacheIntegrationSink{state: &stubCacheRequiredContentState{
		items: map[string]types.CacheRequiredContentItem{}, scopes: map[string]*stubCacheRequiredContentScope{},
	}}
	sink.beforeAppend = func(schema types.EventStubCacheRequiredContentSchema) error {
		if schema.Tombstone {
			startOnce.Do(func() { close(started) })
			<-allowAppend
		}
		return nil
	}
	eventRepo := newStateCacheIntegrationEventRepo(t, sink)
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres"), eventRepo: eventRepo}
	ctx := context.Background()

	_, err := repo.RetainStateSnapshotReference(ctx, 1, snapshotID, "machine", "machine:retain-race")
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM state_snapshot WHERE id=$1`, snapshotDBID)
	require.ErrorContains(t, err, "active authoritative references")
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, snapshotID, "machine", "machine:retain-race")
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, snapshotDBID)
	require.NoError(t, repo.planStateCacheRetirements(ctx, 8))

	processResult := make(chan error, 1)
	go func() {
		_, processErr := repo.ProcessStateCacheRetirements(ctx, 8)
		processResult <- processErr
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not reach synchronous S2 tombstone")
	}

	_, err = repo.RetainStateSnapshotReference(ctx, 1, snapshotID, "machine", "machine:late-retain")
	require.ErrorContains(t, err, "irreversibly authorized")
	close(allowAppend)
	require.NoError(t, <-processResult)
	var snapshots int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM state_snapshot WHERE id=$1`, snapshotDBID).Scan(&snapshots))
	require.Zero(t, snapshots)
}

func TestStateCacheRetirementRealPostgresPostWriteFailureNeverReopensRetain(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	_, stubExternalID := stateCacheIntegrationIdentity(t, db)
	volumeID, generationID := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, volumeID, generationID, "root", 1, "", "")
	snapshotDBID, snapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		volumeID, generationID, "post-write-failure", 1)
	sink := &stateCacheIntegrationSink{state: &stubCacheRequiredContentState{
		items: map[string]types.CacheRequiredContentItem{}, scopes: map[string]*stubCacheRequiredContentScope{},
	}}
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres"),
		eventRepo: newStateCacheIntegrationEventRepo(t, sink)}
	ctx := context.Background()
	_, err := repo.RetainStateSnapshotReference(ctx, 1, snapshotID, "machine",
		"machine:post-write:"+snapshotID)
	require.NoError(t, err)
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, snapshotID, "machine",
		"machine:post-write:"+snapshotID)
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, snapshotDBID)

	injected := true
	repo.stateCacheAfterWrite = func(types.StateCacheRetirement) error {
		if injected {
			injected = false
			return fmt.Errorf("injected post-S2 PostgreSQL boundary failure")
		}
		return nil
	}
	processed, err := repo.ProcessStateCacheRetirements(ctx, 8)
	require.Zero(t, processed)
	require.ErrorContains(t, err, "injected post-S2")
	_, err = repo.RetainStateSnapshotReference(ctx, 1, snapshotID, "machine",
		"machine:cannot-resurrect:"+snapshotID)
	require.ErrorContains(t, err, "irreversibly authorized")
	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM state_cache_retirement_outbox
		WHERE state_snapshot_id=$1`, snapshotDBID).Scan(&status))
	require.Equal(t, "delivering", status)
	_, err = db.Exec(`UPDATE state_cache_retirement_outbox SET next_attempt_at=CURRENT_TIMESTAMP
		WHERE state_snapshot_id=$1`, snapshotDBID)
	require.NoError(t, err)
	repo.stateCacheAfterWrite = nil
	processed, err = repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 1, processed)

	sink.mu.Lock()
	var tombstoneRevisionIDs []string
	for _, record := range sink.writtenRecords {
		if record.Tombstone {
			tombstoneRevisionIDs = append(tombstoneRevisionIDs, record.RevisionID)
		}
	}
	sink.mu.Unlock()
	require.Len(t, tombstoneRevisionIDs, 2)
	require.Equal(t, tombstoneRevisionIDs[0], tombstoneRevisionIDs[1],
		"post-write ambiguity did not replay the same immutable revision")
	var snapshots int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM state_snapshot WHERE id=$1`, snapshotDBID).Scan(&snapshots))
	require.Zero(t, snapshots)
}

func TestStateCacheRetirementRealPostgresRepeatedMachinePinsAreAdditive(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	_, stubExternalID := stateCacheIntegrationIdentity(t, db)
	volumeID := uuid.NewString()
	generationIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	seedStateCacheGeneration(t, db, volumeID, generationIDs[0], "root", 1, "", "")
	seedStateCacheGeneration(t, db, volumeID, generationIDs[1], "root", 2, generationIDs[0], "")
	seedStateCacheGeneration(t, db, volumeID, generationIDs[2], "root", 3, generationIDs[1], "")
	snapshotDatabaseIDs := make([]uint, 3)
	snapshotIDs := make([]string, 3)
	for index := range snapshotIDs {
		snapshotDatabaseIDs[index], snapshotIDs[index] = seedAvailableStateCacheSnapshot(t, db,
			stubExternalID, volumeID, generationIDs[index], fmt.Sprintf("machine-s%d", index+1), int64(index+1))
	}
	sink := &stateCacheIntegrationSink{state: &stubCacheRequiredContentState{
		items: map[string]types.CacheRequiredContentItem{}, scopes: map[string]*stubCacheRequiredContentScope{},
	}}
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres"),
		eventRepo: newStateCacheIntegrationEventRepo(t, sink)}
	ctx := context.Background()
	referenceID := func(index int) string {
		return "machine:machine-1:" + snapshotIDs[index]
	}

	// Each advance first adds an exact new state pin, then durably releases
	// the old identity. At no point is the machine unpinned, and immutable
	// references never need a mutable rebind operation.
	_, err := repo.RetainStateSnapshotReference(ctx, 1, snapshotIDs[0], "machine", referenceID(0))
	require.NoError(t, err)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, snapshotIDs[1], "machine", referenceID(1))
	require.NoError(t, err)
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, snapshotIDs[0], "machine", referenceID(0))
	require.NoError(t, err)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, snapshotIDs[2], "machine", referenceID(2))
	require.NoError(t, err)
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, snapshotIDs[1], "machine", referenceID(1))
	require.NoError(t, err)
	backdateStateCacheRetirement(t, db, snapshotDatabaseIDs[0])
	backdateStateCacheRetirement(t, db, snapshotDatabaseIDs[1])
	processed, err := repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Zero(t, processed, "an older state scope retired while the latest machine pin was active")

	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, snapshotIDs[2], "machine", referenceID(2))
	require.NoError(t, err)
	for _, snapshotDatabaseID := range snapshotDatabaseIDs {
		backdateStateCacheRetirement(t, db, snapshotDatabaseID)
	}
	processed, err = repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 1, processed, "one monotonic tombstone should retire the shared lineage scope")
	// The other exact snapshots observe the committed scope retirement and
	// remove only metadata; they cannot publish conflicting revisions.
	backdateStateCacheRetirement(t, db, snapshotDatabaseIDs[1])
	backdateStateCacheRetirement(t, db, snapshotDatabaseIDs[2])
	processed, err = repo.ProcessStateCacheRetirements(ctx, 8)
	require.NoError(t, err)
	require.Zero(t, processed)

	var remainingSnapshots, remainingGenerations int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM state_snapshot WHERE external_id::text=ANY($1)`,
		pq.Array(snapshotIDs)).Scan(&remainingSnapshots))
	require.Zero(t, remainingSnapshots)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM volume_generation WHERE external_id::text=ANY($1)`,
		pq.Array(generationIDs)).Scan(&remainingGenerations))
	require.Zero(t, remainingGenerations)
	for index := range snapshotIDs {
		replayed, err := repo.ReleaseStateSnapshotReference(ctx, 1, snapshotIDs[index], "machine", referenceID(index))
		require.NoError(t, err)
		require.True(t, replayed.Released)
	}
}

func TestStateCacheCompactionRealPostgresRejectsRetainedHistoricalGenerationAndIsolatesForkScope(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	_, stubExternalID := stateCacheIntegrationIdentity(t, db)
	ctx := context.Background()
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres")}

	sourceVolumeID := uuid.NewString()
	sourceS10, sourceS17 := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, sourceVolumeID, sourceS10, "root", 1, "", "")
	seedStateCacheGeneration(t, db, sourceVolumeID, sourceS17, "root", 2, sourceS10, "")
	_, snapshotS10 := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		sourceVolumeID, sourceS10, "source-s10", 1)
	_, snapshotS17 := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		sourceVolumeID, sourceS17, "source-s17", 2)

	forkVolumeID, forkGenerationID := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, forkVolumeID, forkGenerationID, "root", 1, "", sourceS10)
	_, forkSnapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		forkVolumeID, forkGenerationID, "fork-from-s10", 1)

	_, err := repo.RetainStateSnapshotReference(ctx, 1, snapshotS10, "snapshot", "snapshot:s10")
	require.NoError(t, err)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, snapshotS17, "machine", "machine:s17")
	require.NoError(t, err)
	_, err = repo.RetainStateSnapshotReference(ctx, 1, forkSnapshotID, "machine", "machine:fork-s10")
	require.NoError(t, err)

	compactions := map[string]types.StateGenerationCompaction{sourceVolumeID: {
		VolumeId: sourceVolumeID, GenerationId: uuid.NewString(), SourceGenerationId: sourceS17,
	}}
	tx, err := repo.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	require.NoError(t, err)
	err = preventCompactionFromDroppingRetainedState(ctx, tx, 1, stubExternalID, compactions)
	require.ErrorContains(t, err, "retained historical state snapshot")
	require.NoError(t, tx.Rollback())

	// Once the historical source pin is durably released, the exact current
	// source pin may advance to the new parentless anchor. A retained fork uses
	// its own volume scope and does not block the source lineage compaction.
	_, err = repo.ReleaseStateSnapshotReference(ctx, 1, snapshotS10, "snapshot", "snapshot:s10")
	require.NoError(t, err)
	tx, err = repo.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	require.NoError(t, err)
	require.NoError(t, preventCompactionFromDroppingRetainedState(ctx, tx, 1, stubExternalID, compactions))
	require.NoError(t, tx.Rollback())
}

func TestStateCacheRetirementRealPostgresReleaseBeforeRetainCreatesTerminalExactTombstone(t *testing.T) {
	db := stateReleaseIntegrationDatabase(t)
	_, stubExternalID := stateCacheIntegrationIdentity(t, db)
	volumeID, generationID := uuid.NewString(), uuid.NewString()
	seedStateCacheGeneration(t, db, volumeID, generationID, "root", 1, "", "")
	_, snapshotID := seedAvailableStateCacheSnapshot(t, db, stubExternalID,
		volumeID, generationID, "release-before-retain", 1)
	repo := &PostgresBackendRepository{client: sqlx.NewDb(db, "postgres")}
	ctx := context.Background()
	referenceID := "machine:lost-retain:" + snapshotID

	released, err := repo.ReleaseStateSnapshotReference(ctx, 1, snapshotID, "machine", referenceID)
	require.NoError(t, err)
	require.True(t, released.Released)
	require.Equal(t, snapshotID, released.SnapshotExternalId)
	replayed, err := repo.ReleaseStateSnapshotReference(ctx, 1, snapshotID, "machine", referenceID)
	require.NoError(t, err)
	require.Equal(t, released.ExternalId, replayed.ExternalId)
	require.True(t, replayed.Released)

	_, err = repo.RetainStateSnapshotReference(ctx, 1, snapshotID, "machine", referenceID)
	require.ErrorContains(t, err, "already released reference")
}
