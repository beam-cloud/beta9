package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upStateSnapshots, downStateSnapshots)
}

// This is the destructive state-volume cutover. Superseded machine-state
// tables are dropped before the exact state-snapshot schema is created. User,
// workspace, token, credit, and usage tables are intentionally untouched.
func upStateSnapshots(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`DROP TABLE IF EXISTS disk_snapshot;`,
		`DROP TABLE IF EXISTS disk;`,
		`DROP TABLE IF EXISTS checkpoint;`,
		`CREATE TABLE state_volume (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			size TEXT NOT NULL,
			mount_path TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			deleted_at TIMESTAMP WITH TIME ZONE
		);`,
		`CREATE UNIQUE INDEX idx_state_volume_workspace_name ON state_volume(workspace_id, name) WHERE deleted_at IS NULL;`,
		`CREATE TABLE volume_generation (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			stub_id INT REFERENCES stub(id) ON DELETE SET NULL,
			volume_id TEXT NOT NULL,
			name TEXT NOT NULL,
			parent_generation_id UUID REFERENCES volume_generation(external_id) ON DELETE RESTRICT,
			clone_parent_generation_id UUID REFERENCES volume_generation(external_id) ON DELETE RESTRICT,
			generation BIGINT NOT NULL CHECK (generation > 0),
			status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'available', 'failed')),
			reason TEXT NOT NULL DEFAULT '',
			manifest_key TEXT NOT NULL DEFAULT '',
			manifest_digest TEXT NOT NULL DEFAULT '',
			manifest_size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (manifest_size_bytes >= 0),
			chunk_count BIGINT NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
			logical_size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (logical_size_bytes >= 0),
			stored_size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (stored_size_bytes >= 0),
			bucket_name TEXT NOT NULL DEFAULT '',
			object_prefix TEXT NOT NULL DEFAULT '',
			public BOOLEAN NOT NULL DEFAULT FALSE CHECK (public = FALSE),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP WITH TIME ZONE,
			CHECK (status <> 'available' OR
				(manifest_key <> '' AND manifest_digest <> '' AND manifest_size_bytes > 0 AND
				 logical_size_bytes > 0 AND bucket_name <> '' AND object_prefix <> '')),
			CHECK (status <> 'failed' OR reason <> ''),
			CHECK ((generation = 1 AND parent_generation_id IS NULL) OR
			       (generation > 1 AND clone_parent_generation_id IS NULL)),
			UNIQUE(workspace_id, volume_id, generation)
		);`,
		`CREATE INDEX idx_volume_generation_latest ON volume_generation(workspace_id, volume_id, generation DESC) WHERE status = 'available';`,
		`CREATE TABLE state_snapshot (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			operation_id TEXT NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			stub_id INT REFERENCES stub(id) ON DELETE SET NULL,
			source_stub_external_id TEXT NOT NULL CHECK (source_stub_external_id <> ''),
			source_stub_name TEXT NOT NULL CHECK (source_stub_name <> ''),
			source_stub_type TEXT NOT NULL CHECK (source_stub_type <> ''),
			source_container_id TEXT NOT NULL,
			source_worker_id TEXT NOT NULL CHECK (source_worker_id <> ''),
			source_worker_instance_id TEXT NOT NULL CHECK (source_worker_instance_id <> ''),
			storage_node_id TEXT NOT NULL CHECK (storage_node_id <> ''),
			armed_at TIMESTAMP WITH TIME ZONE,
			mode TEXT NOT NULL CHECK (mode IN ('live', 'terminal')),
			include_memory BOOLEAN NOT NULL,
			visible BOOLEAN NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'available', 'failed')),
			reason TEXT NOT NULL DEFAULT '',
			image_id TEXT NOT NULL CHECK (image_id <> ''),
			image_digest TEXT NOT NULL CHECK (image_digest <> ''),
			runtime_profile TEXT NOT NULL CHECK (runtime_profile <> ''),
			checkpoint_id TEXT NOT NULL DEFAULT '',
			checkpoint_digest TEXT NOT NULL DEFAULT '',
			checkpoint_cache_hash TEXT NOT NULL DEFAULT '',
			checkpoint_size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (checkpoint_size_bytes >= 0),
			checkpoint_origin_key TEXT NOT NULL DEFAULT '',
			checkpoint_accelerator TEXT NOT NULL DEFAULT '',
			checkpoint_locality TEXT NOT NULL DEFAULT '',
			restore_mode TEXT NOT NULL DEFAULT 'cold_state' CHECK (restore_mode IN ('memory', 'cold_state')),
			fallback_reason TEXT NOT NULL DEFAULT '',
			public BOOLEAN NOT NULL DEFAULT FALSE CHECK (public = FALSE),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP WITH TIME ZONE,
			CHECK ((restore_mode = 'cold_state' AND checkpoint_id = '' AND checkpoint_digest = '' AND
				checkpoint_cache_hash = '' AND checkpoint_size_bytes = 0 AND checkpoint_origin_key = '' AND
				checkpoint_accelerator = '' AND checkpoint_locality = '') OR
			       (restore_mode = 'memory' AND checkpoint_id <> '' AND checkpoint_digest <> '' AND
				checkpoint_cache_hash <> '' AND checkpoint_size_bytes > 0 AND checkpoint_origin_key <> '' AND
				fallback_reason = '')),
			CHECK (mode <> 'live' OR include_memory = FALSE),
			CHECK (status <> 'available' OR armed_at IS NOT NULL),
			CHECK (status <> 'failed' OR reason <> ''),
			UNIQUE(workspace_id, source_container_id, operation_id),
			UNIQUE(source_container_id, operation_id)
		);`,
		`CREATE UNIQUE INDEX idx_state_snapshot_one_pending_container
			ON state_snapshot(source_container_id) WHERE status = 'pending';`,
		`CREATE TABLE state_snapshot_generation (
			state_snapshot_id INT NOT NULL REFERENCES state_snapshot(id) ON DELETE CASCADE,
			volume_generation_id INT NOT NULL REFERENCES volume_generation(id) ON DELETE RESTRICT,
			volume_id TEXT NOT NULL,
			name TEXT NOT NULL,
			mount_path TEXT NOT NULL,
			read_only BOOLEAN NOT NULL DEFAULT FALSE,
			is_root BOOLEAN NOT NULL DEFAULT FALSE,
			generation BIGINT NOT NULL,
			PRIMARY KEY(state_snapshot_id, volume_id),
			UNIQUE(state_snapshot_id, volume_generation_id),
			UNIQUE(state_snapshot_id, name),
			UNIQUE(state_snapshot_id, mount_path),
			CHECK (left(mount_path, 1) = '/'),
			CHECK ((is_root AND name = 'root' AND mount_path = '/' AND read_only = FALSE) OR
			       (is_root = FALSE AND name <> 'root' AND mount_path <> '/'))
		);`,
		`CREATE FUNCTION enforce_volume_generation_transition() RETURNS trigger AS $$
		DECLARE parent_count BIGINT; clone_parent_count BIGINT; compaction_plan_count BIGINT;
		BEGIN
			IF TG_OP = 'UPDATE' THEN
				IF OLD.status <> 'pending' AND NEW IS DISTINCT FROM OLD THEN
					RAISE EXCEPTION 'terminal volume generation is immutable';
				END IF;
				IF OLD.external_id IS DISTINCT FROM NEW.external_id OR
				   OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR
				   OLD.stub_id IS DISTINCT FROM NEW.stub_id OR
				   OLD.volume_id IS DISTINCT FROM NEW.volume_id OR
				   OLD.name IS DISTINCT FROM NEW.name OR
				   OLD.parent_generation_id IS DISTINCT FROM NEW.parent_generation_id OR
				   OLD.clone_parent_generation_id IS DISTINCT FROM NEW.clone_parent_generation_id OR
				   OLD.generation IS DISTINCT FROM NEW.generation THEN
					RAISE EXCEPTION 'volume generation identity is immutable';
				END IF;
			END IF;
			IF NEW.parent_generation_id IS NOT NULL THEN
				SELECT count(*) INTO parent_count FROM volume_generation parent
				WHERE parent.external_id = NEW.parent_generation_id
				  AND parent.workspace_id = NEW.workspace_id
				  AND parent.volume_id = NEW.volume_id
				  AND parent.generation = NEW.generation - 1
				  AND parent.status = 'available';
				IF parent_count <> 1 THEN
					RAISE EXCEPTION 'volume generation parent must be the available previous generation';
				END IF;
			END IF;
			IF NEW.clone_parent_generation_id IS NOT NULL THEN
				SELECT count(*) INTO clone_parent_count FROM volume_generation source
				WHERE source.external_id = NEW.clone_parent_generation_id
				  AND source.workspace_id = NEW.workspace_id
				  AND source.volume_id <> NEW.volume_id
				  AND source.status = 'available';
				IF clone_parent_count <> 1 OR NEW.parent_generation_id IS NOT NULL OR NEW.generation <> 1 THEN
					RAISE EXCEPTION 'clone parent must be an available cross-volume generation for generation one';
				END IF;
			END IF;
			IF NEW.generation > 1 AND NEW.parent_generation_id IS NULL THEN
				SELECT count(*) INTO compaction_plan_count
				FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
				JOIN volume_generation source ON source.external_id = p.compaction_source_generation_id
				WHERE p.compaction = TRUE AND p.generation_id = NEW.external_id
				  AND p.volume_id::text = NEW.volume_id AND p.generation = NEW.generation
				  AND s.workspace_id = NEW.workspace_id AND s.status = 'pending'
				  AND source.workspace_id = NEW.workspace_id AND source.volume_id = NEW.volume_id
				  AND source.generation = NEW.generation - 1 AND source.status = 'available';
				IF compaction_plan_count <> 1 THEN
					RAISE EXCEPTION 'parentless generation requires its exact pending compaction authorization';
				END IF;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER volume_generation_transition BEFORE INSERT OR UPDATE ON volume_generation
			FOR EACH ROW EXECUTE FUNCTION enforce_volume_generation_transition();`,
		`CREATE FUNCTION enforce_state_snapshot_transition() RETURNS trigger AS $$
		DECLARE members BIGINT; unavailable BIGINT; roots BIGINT;
		BEGIN
			IF TG_OP = 'UPDATE' AND OLD.status <> 'pending' AND NEW IS DISTINCT FROM OLD THEN
				RAISE EXCEPTION 'terminal state snapshot is immutable';
			END IF;
			IF TG_OP = 'UPDATE' AND
			   (OLD.external_id IS DISTINCT FROM NEW.external_id OR
			    OLD.operation_id IS DISTINCT FROM NEW.operation_id OR
			    OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR
			    OLD.source_container_id IS DISTINCT FROM NEW.source_container_id OR
			    OLD.source_worker_id IS DISTINCT FROM NEW.source_worker_id OR
			    OLD.source_worker_instance_id IS DISTINCT FROM NEW.source_worker_instance_id OR
			    OLD.storage_node_id IS DISTINCT FROM NEW.storage_node_id OR
			    OLD.mode IS DISTINCT FROM NEW.mode OR
			    OLD.include_memory IS DISTINCT FROM NEW.include_memory OR
			    OLD.visible IS DISTINCT FROM NEW.visible OR
			    OLD.source_stub_external_id IS DISTINCT FROM NEW.source_stub_external_id OR
			    OLD.source_stub_name IS DISTINCT FROM NEW.source_stub_name OR
			    OLD.source_stub_type IS DISTINCT FROM NEW.source_stub_type OR
			    OLD.image_id IS DISTINCT FROM NEW.image_id OR
			    OLD.image_digest IS DISTINCT FROM NEW.image_digest OR
			    OLD.runtime_profile IS DISTINCT FROM NEW.runtime_profile) THEN
				RAISE EXCEPTION 'state snapshot identity is immutable';
			END IF;
			IF TG_OP = 'UPDATE' AND OLD.armed_at IS NOT NULL AND NEW.armed_at IS DISTINCT FROM OLD.armed_at THEN
				RAISE EXCEPTION 'armed state snapshot ownership is immutable';
			END IF;
			IF NEW.armed_at IS NOT NULL AND NEW.status = 'pending' AND
			   (NEW.source_worker_id = '' OR NEW.source_worker_instance_id = '' OR NEW.storage_node_id = '') THEN
				RAISE EXCEPTION 'armed state snapshot requires an exact worker and storage node owner';
			END IF;
			IF NEW.status = 'available' THEN
				SELECT count(*), count(*) FILTER (WHERE vg.status <> 'available'),
					count(*) FILTER (WHERE ssg.is_root)
				INTO members, unavailable, roots
				FROM state_snapshot_generation ssg
				JOIN volume_generation vg ON vg.id = ssg.volume_generation_id
				WHERE ssg.state_snapshot_id = NEW.id;
				IF members = 0 OR unavailable <> 0 OR roots <> 1 THEN
					RAISE EXCEPTION 'available state snapshot requires all available members and exactly one root';
				END IF;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_snapshot_transition BEFORE INSERT OR UPDATE ON state_snapshot
			FOR EACH ROW EXECUTE FUNCTION enforce_state_snapshot_transition();`,
		`CREATE FUNCTION enforce_state_snapshot_membership() RETURNS trigger AS $$
		DECLARE snapshot_status TEXT; snapshot_workspace_id INT; recorded_workspace_id INT;
			recorded_volume_id TEXT; recorded_name TEXT; recorded_generation BIGINT;
		BEGIN
			IF TG_OP = 'DELETE' THEN
				SELECT status INTO snapshot_status FROM state_snapshot WHERE id = OLD.state_snapshot_id FOR UPDATE;
				IF NOT FOUND OR pg_trigger_depth() > 1 THEN
					RETURN OLD;
				END IF;
				IF snapshot_status <> 'pending' THEN
					RAISE EXCEPTION 'terminal state snapshot membership is immutable';
				END IF;
				RETURN OLD;
			END IF;
			SELECT status, workspace_id INTO snapshot_status, snapshot_workspace_id
			FROM state_snapshot WHERE id = NEW.state_snapshot_id FOR UPDATE;
			IF snapshot_status <> 'pending' THEN
				RAISE EXCEPTION 'terminal state snapshot membership is immutable';
			END IF;
			SELECT workspace_id, volume_id, name, generation
			INTO recorded_workspace_id, recorded_volume_id, recorded_name, recorded_generation
			FROM volume_generation WHERE id = NEW.volume_generation_id;
			IF recorded_workspace_id IS DISTINCT FROM snapshot_workspace_id OR
			   recorded_volume_id IS DISTINCT FROM NEW.volume_id OR recorded_name IS DISTINCT FROM NEW.name OR
			   recorded_generation IS DISTINCT FROM NEW.generation THEN
				RAISE EXCEPTION 'state membership must match its recorded volume generation';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_snapshot_membership_immutable BEFORE INSERT OR UPDATE OR DELETE ON state_snapshot_generation
			FOR EACH ROW EXECUTE FUNCTION enforce_state_snapshot_membership();`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downStateSnapshots(ctx context.Context, tx *sql.Tx) error {
	// Rollback removes only the new schema. The destructive cutover never
	// recreates superseded machine-state tables.
	statements := []string{
		`DROP TABLE IF EXISTS state_snapshot_generation;`,
		`DROP TABLE IF EXISTS state_snapshot;`,
		`DROP TABLE IF EXISTS volume_generation;`,
		`DROP TABLE IF EXISTS state_volume;`,
		`DROP FUNCTION IF EXISTS enforce_state_snapshot_membership();`,
		`DROP FUNCTION IF EXISTS enforce_state_snapshot_transition();`,
		`DROP FUNCTION IF EXISTS enforce_volume_generation_transition();`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
