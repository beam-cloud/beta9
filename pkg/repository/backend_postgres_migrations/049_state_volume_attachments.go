package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upStateVolumeAttachments, downStateVolumeAttachments)
}

func upStateVolumeAttachments(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE state_snapshot_recovery_claim (
			state_snapshot_id INT PRIMARY KEY REFERENCES state_snapshot(id) ON DELETE CASCADE,
			worker_id TEXT NOT NULL CHECK (worker_id <> ''),
			worker_instance_id TEXT NOT NULL CHECK (worker_instance_id <> ''),
			storage_node_id TEXT NOT NULL CHECK (storage_node_id <> ''),
			claim_generation BIGINT NOT NULL CHECK (claim_generation > 0),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,
		`ALTER TABLE state_volume
			ADD COLUMN next_fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (next_fencing_token >= 0),
			ADD COLUMN current_generation_id UUID REFERENCES volume_generation(external_id) ON DELETE RESTRICT;`,
		`CREATE TABLE state_volume_attachment_plan (
			plan_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL CHECK (container_id <> ''),
			request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
			expected_writable_members INT NOT NULL CHECK (expected_writable_members > 0),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			admitted_at TIMESTAMP WITH TIME ZONE,
			enqueued_at TIMESTAMP WITH TIME ZONE,
			aborted_at TIMESTAMP WITH TIME ZONE,
			abort_reason TEXT NOT NULL DEFAULT '',
			CHECK (enqueued_at IS NULL OR admitted_at IS NOT NULL),
			CHECK (aborted_at IS NULL OR abort_reason <> ''),
			PRIMARY KEY(workspace_id, container_id)
		);`,
		`CREATE INDEX idx_state_volume_attachment_plan_created
			ON state_volume_attachment_plan(created_at);`,
		`CREATE TABLE state_volume_attachment (
			id SERIAL PRIMARY KEY,
			state_volume_id INT NOT NULL REFERENCES state_volume(id) ON DELETE CASCADE,
			attachment_plan_id UUID NOT NULL REFERENCES state_volume_attachment_plan(plan_id) ON DELETE RESTRICT,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL CHECK (container_id <> ''),
			source_generation_id UUID REFERENCES volume_generation(external_id) ON DELETE RESTRICT,
			initialize BOOLEAN NOT NULL DEFAULT FALSE,
			attachment_token UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
			owner_worker_id TEXT NOT NULL DEFAULT '',
			owner_worker_instance_id TEXT NOT NULL DEFAULT '',
			storage_node_id TEXT NOT NULL DEFAULT '',
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(state_volume_id, container_id),
			CHECK (NOT initialize OR source_generation_id IS NULL),
			CHECK ((owner_worker_id = '' AND owner_worker_instance_id = '' AND storage_node_id = '') OR
			       (owner_worker_id <> '' AND owner_worker_instance_id <> '' AND storage_node_id <> ''))
		);`,
		`CREATE UNIQUE INDEX idx_state_volume_single_writer ON state_volume_attachment(state_volume_id);`,
		`CREATE INDEX idx_state_volume_attachment_container ON state_volume_attachment(workspace_id, container_id);`,
		`CREATE TABLE state_branch_lineage (
			id SERIAL PRIMARY KEY,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			stub_external_id TEXT NOT NULL CHECK (stub_external_id <> ''),
			member_name TEXT NOT NULL CHECK (member_name <> ''),
			mount_path TEXT NOT NULL CHECK (left(mount_path, 1) = '/'),
			is_root BOOLEAN NOT NULL DEFAULT FALSE,
			volume_id UUID UNIQUE NOT NULL,
			size TEXT NOT NULL CHECK (size <> ''),
			current_generation_id UUID REFERENCES volume_generation(external_id) ON DELETE RESTRICT,
			next_fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (next_fencing_token >= 0),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, stub_external_id, member_name),
			UNIQUE(workspace_id, stub_external_id, mount_path),
			CHECK ((is_root AND member_name = 'root' AND mount_path = '/') OR
			       (is_root = FALSE AND member_name <> 'root' AND mount_path <> '/'))
		);`,
		`CREATE TABLE state_branch_attachment (
			id SERIAL PRIMARY KEY,
			lineage_id INT NOT NULL UNIQUE REFERENCES state_branch_lineage(id) ON DELETE CASCADE,
			attachment_plan_id UUID NOT NULL REFERENCES state_volume_attachment_plan(plan_id) ON DELETE RESTRICT,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL CHECK (container_id <> ''),
			source_generation_id UUID REFERENCES volume_generation(external_id) ON DELETE RESTRICT,
			initialize BOOLEAN NOT NULL DEFAULT FALSE,
			clone_source BOOLEAN NOT NULL DEFAULT FALSE,
			attachment_token UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
			owner_worker_id TEXT NOT NULL DEFAULT '',
			owner_worker_instance_id TEXT NOT NULL DEFAULT '',
			storage_node_id TEXT NOT NULL DEFAULT '',
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CHECK ((initialize AND source_generation_id IS NULL AND clone_source = FALSE) OR
			       (initialize = FALSE AND source_generation_id IS NOT NULL)),
			CHECK ((owner_worker_id = '' AND owner_worker_instance_id = '' AND storage_node_id = '') OR
			       (owner_worker_id <> '' AND owner_worker_instance_id <> '' AND storage_node_id <> ''))
		);`,
		`CREATE INDEX idx_state_branch_attachment_container ON state_branch_attachment(workspace_id, container_id);`,
		`CREATE TABLE state_read_only_attachment (
			id SERIAL PRIMARY KEY,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL CHECK (container_id <> ''),
			volume_id UUID NOT NULL,
			source_generation_id UUID NOT NULL REFERENCES volume_generation(external_id) ON DELETE RESTRICT,
			name TEXT NOT NULL CHECK (name <> ''),
			mount_path TEXT NOT NULL CHECK (left(mount_path, 1) = '/'),
			is_root BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, container_id, volume_id),
			CHECK ((is_root AND name = 'root' AND mount_path = '/') OR
			       (is_root = FALSE AND name <> 'root' AND mount_path <> '/'))
		);`,
		`CREATE FUNCTION enforce_state_read_only_attachment_scope() RETURNS trigger AS $$
		DECLARE exact_sources BIGINT; exact_roles BIGINT;
		BEGIN
			IF TG_OP = 'UPDATE' AND ROW(OLD.workspace_id, OLD.container_id, OLD.volume_id,
				OLD.source_generation_id, OLD.name, OLD.mount_path, OLD.is_root) IS DISTINCT FROM
				ROW(NEW.workspace_id, NEW.container_id, NEW.volume_id,
				NEW.source_generation_id, NEW.name, NEW.mount_path, NEW.is_root) THEN
				RAISE EXCEPTION 'read-only state attachment identity is immutable';
			END IF;
			SELECT count(*) INTO exact_sources FROM volume_generation
			WHERE external_id = NEW.source_generation_id AND workspace_id = NEW.workspace_id
			  AND volume_id = NEW.volume_id::text AND name = NEW.name AND status = 'available';
			SELECT
			  (SELECT count(*) FROM state_volume v WHERE v.workspace_id = NEW.workspace_id
			   AND v.external_id = NEW.volume_id AND v.name = NEW.name AND v.mount_path = NEW.mount_path
			   AND NEW.is_root = FALSE AND v.deleted_at IS NULL) +
			  (SELECT count(*) FROM state_branch_lineage l WHERE l.workspace_id = NEW.workspace_id
			   AND l.volume_id = NEW.volume_id AND l.member_name = NEW.name AND l.mount_path = NEW.mount_path
			   AND l.is_root = NEW.is_root)
			INTO exact_roles;
			IF exact_sources <> 1 OR exact_roles <> 1 THEN
				RAISE EXCEPTION 'read-only state attachment does not match its exact generation and member role';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_read_only_attachment_scope BEFORE INSERT OR UPDATE ON state_read_only_attachment
			FOR EACH ROW EXECUTE FUNCTION enforce_state_read_only_attachment_scope();`,
		`CREATE TABLE state_snapshot_member_plan (
			state_snapshot_id INT NOT NULL REFERENCES state_snapshot(id) ON DELETE CASCADE,
			volume_id UUID NOT NULL,
			generation_id UUID NOT NULL,
			parent_generation_id UUID,
			clone_parent_generation_id UUID,
			compaction BOOLEAN NOT NULL DEFAULT FALSE,
			compaction_source_generation_id UUID,
			generation BIGINT NOT NULL CHECK (generation > 0),
			name TEXT NOT NULL CHECK (name <> ''),
			mount_path TEXT NOT NULL CHECK (left(mount_path, 1) = '/'),
			read_only BOOLEAN NOT NULL DEFAULT FALSE,
			is_root BOOLEAN NOT NULL DEFAULT FALSE,
			attachment_token UUID,
			fencing_token BIGINT,
			PRIMARY KEY(state_snapshot_id, volume_id),
			UNIQUE(state_snapshot_id, name),
			UNIQUE(state_snapshot_id, mount_path),
			CHECK ((read_only AND attachment_token IS NULL AND fencing_token IS NULL) OR
			       (read_only = FALSE AND attachment_token IS NOT NULL AND fencing_token > 0)),
			CHECK ((is_root AND name = 'root' AND mount_path = '/' AND read_only = FALSE) OR
			       (is_root = FALSE AND name <> 'root' AND mount_path <> '/')),
			CHECK ((read_only AND compaction = FALSE AND compaction_source_generation_id IS NULL) OR
			       (read_only = FALSE AND compaction AND compaction_source_generation_id IS NOT NULL AND generation > 1 AND
			        parent_generation_id IS NULL AND clone_parent_generation_id IS NULL) OR
			       (read_only = FALSE AND compaction = FALSE AND compaction_source_generation_id IS NULL AND
			        ((generation = 1 AND parent_generation_id IS NULL) OR
			         (generation > 1 AND parent_generation_id IS NOT NULL AND clone_parent_generation_id IS NULL))))
		);`,
		`CREATE FUNCTION enforce_state_volume_attachment_scope() RETURNS trigger AS $$
		DECLARE scoped_volume_id TEXT; source_count BIGINT; plan_count BIGINT;
		BEGIN
			IF TG_OP = 'UPDATE' AND
			   (OLD.state_volume_id IS DISTINCT FROM NEW.state_volume_id OR
			    OLD.attachment_plan_id IS DISTINCT FROM NEW.attachment_plan_id OR
			    OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR
			    OLD.container_id IS DISTINCT FROM NEW.container_id OR
			    OLD.source_generation_id IS DISTINCT FROM NEW.source_generation_id OR
			    OLD.initialize IS DISTINCT FROM NEW.initialize OR
			    OLD.attachment_token IS DISTINCT FROM NEW.attachment_token OR
			    OLD.fencing_token IS DISTINCT FROM NEW.fencing_token) THEN
				RAISE EXCEPTION 'state-volume attachment identity is immutable';
			END IF;
			IF TG_OP = 'UPDATE' AND OLD.owner_worker_id <> '' AND
			   (OLD.owner_worker_id IS DISTINCT FROM NEW.owner_worker_id OR
			    OLD.owner_worker_instance_id IS DISTINCT FROM NEW.owner_worker_instance_id OR
			    OLD.storage_node_id IS DISTINCT FROM NEW.storage_node_id) THEN
				RAISE EXCEPTION 'state-volume attachment worker epoch is immutable';
			END IF;
			SELECT external_id::text INTO scoped_volume_id FROM state_volume
			WHERE id = NEW.state_volume_id AND workspace_id = NEW.workspace_id AND deleted_at IS NULL;
			IF scoped_volume_id IS NULL THEN
				RAISE EXCEPTION 'state-volume attachment workspace does not match volume';
			END IF;
			IF TG_OP = 'INSERT' THEN
				SELECT count(*) INTO plan_count FROM state_volume_attachment_plan
				WHERE plan_id = NEW.attachment_plan_id AND workspace_id = NEW.workspace_id
				  AND container_id = NEW.container_id AND admitted_at IS NULL AND aborted_at IS NULL;
				IF plan_count <> 1 THEN
					RAISE EXCEPTION 'state-volume attachment requires its exact active scheduler plan';
				END IF;
			END IF;
			IF NEW.source_generation_id IS NOT NULL THEN
				SELECT count(*) INTO source_count FROM volume_generation
				WHERE external_id = NEW.source_generation_id AND workspace_id = NEW.workspace_id
				  AND volume_id = scoped_volume_id AND status = 'available';
				IF source_count <> 1 THEN
					RAISE EXCEPTION 'state-volume attachment source generation does not match volume';
				END IF;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_volume_attachment_scope BEFORE INSERT OR UPDATE ON state_volume_attachment
			FOR EACH ROW EXECUTE FUNCTION enforce_state_volume_attachment_scope();`,
		`CREATE FUNCTION enforce_state_branch_attachment_scope() RETURNS trigger AS $$
		DECLARE lineage_workspace_id INT; lineage_volume_id UUID; lineage_head UUID; source_count BIGINT; plan_count BIGINT;
		BEGIN
			IF TG_OP = 'UPDATE' AND
			   (OLD.lineage_id IS DISTINCT FROM NEW.lineage_id OR
			    OLD.attachment_plan_id IS DISTINCT FROM NEW.attachment_plan_id OR
			    OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR
			    OLD.container_id IS DISTINCT FROM NEW.container_id OR
			    OLD.source_generation_id IS DISTINCT FROM NEW.source_generation_id OR
			    OLD.initialize IS DISTINCT FROM NEW.initialize OR
			    OLD.clone_source IS DISTINCT FROM NEW.clone_source OR
			    OLD.attachment_token IS DISTINCT FROM NEW.attachment_token OR
			    OLD.fencing_token IS DISTINCT FROM NEW.fencing_token) THEN
				RAISE EXCEPTION 'root-state attachment identity is immutable';
			END IF;
			IF TG_OP = 'UPDATE' AND OLD.owner_worker_id <> '' AND
			   (OLD.owner_worker_id IS DISTINCT FROM NEW.owner_worker_id OR
			    OLD.owner_worker_instance_id IS DISTINCT FROM NEW.owner_worker_instance_id OR
			    OLD.storage_node_id IS DISTINCT FROM NEW.storage_node_id) THEN
				RAISE EXCEPTION 'root-state attachment worker epoch is immutable';
			END IF;
			SELECT workspace_id, volume_id, current_generation_id
			INTO lineage_workspace_id, lineage_volume_id, lineage_head
			FROM state_branch_lineage WHERE id = NEW.lineage_id;
			IF lineage_workspace_id IS DISTINCT FROM NEW.workspace_id THEN
				RAISE EXCEPTION 'root-state attachment workspace does not match lineage';
			END IF;
			IF TG_OP = 'INSERT' THEN
				SELECT count(*) INTO plan_count FROM state_volume_attachment_plan
				WHERE plan_id = NEW.attachment_plan_id AND workspace_id = NEW.workspace_id
				  AND container_id = NEW.container_id AND admitted_at IS NULL AND aborted_at IS NULL;
				IF plan_count <> 1 THEN
					RAISE EXCEPTION 'root-state attachment requires its exact active scheduler plan';
				END IF;
			END IF;
			IF NEW.initialize THEN
				IF lineage_head IS NOT NULL THEN
					RAISE EXCEPTION 'initialized root lineage already has a head';
				END IF;
			ELSIF NEW.clone_source THEN
				SELECT count(*) INTO source_count FROM volume_generation
				WHERE external_id = NEW.source_generation_id AND workspace_id = NEW.workspace_id
				  AND volume_id <> lineage_volume_id::text AND status = 'available';
				IF lineage_head IS NOT NULL OR source_count <> 1 THEN
					RAISE EXCEPTION 'root clone source must be an available cross-volume generation';
				END IF;
			ELSIF NEW.source_generation_id IS DISTINCT FROM lineage_head THEN
				RAISE EXCEPTION 'root resume source must equal the current lineage head';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_branch_attachment_scope BEFORE INSERT OR UPDATE ON state_branch_attachment
			FOR EACH ROW EXECUTE FUNCTION enforce_state_branch_attachment_scope();`,
		`CREATE FUNCTION enforce_state_snapshot_member_plan() RETURNS trigger AS $$
		DECLARE snapshot_status TEXT; snapshot_workspace_id INT; snapshot_container_id TEXT; active_leases BIGINT;
			attachment_source UUID; attachment_initialize BOOLEAN; attachment_clone BOOLEAN; attachment_head UUID;
			attachment_name TEXT; attachment_mount_path TEXT; attachment_root BOOLEAN; read_only_sources BIGINT;
			compaction_sources BIGINT;
		BEGIN
			IF TG_OP = 'DELETE' THEN
				SELECT status INTO snapshot_status FROM state_snapshot WHERE id = OLD.state_snapshot_id FOR UPDATE;
				IF NOT FOUND OR pg_trigger_depth() > 1 THEN
					RETURN OLD;
				END IF;
				IF snapshot_status = 'failed' THEN
					RETURN OLD;
				END IF;
				RAISE EXCEPTION 'state snapshot member plan is immutable';
			END IF;
			IF TG_OP = 'UPDATE' THEN
				RAISE EXCEPTION 'state snapshot member plan is immutable';
			END IF;
			SELECT status, workspace_id, source_container_id
			INTO snapshot_status, snapshot_workspace_id, snapshot_container_id
			FROM state_snapshot WHERE id = NEW.state_snapshot_id FOR UPDATE;
			IF snapshot_status <> 'pending' THEN
				RAISE EXCEPTION 'state snapshot member plan requires a pending operation';
			END IF;
			IF NEW.read_only THEN
				IF NEW.compaction THEN
					RAISE EXCEPTION 'read-only state member cannot request compaction';
				END IF;
				SELECT count(*) INTO read_only_sources FROM state_read_only_attachment
				WHERE workspace_id = snapshot_workspace_id AND container_id = snapshot_container_id
				  AND volume_id = NEW.volume_id AND source_generation_id = NEW.generation_id
				  AND name = NEW.name AND mount_path = NEW.mount_path AND is_root = NEW.is_root;
				IF read_only_sources <> 1 THEN
					RAISE EXCEPTION 'snapshot read-only member requires its exact authorized attachment source';
				END IF;
			ELSE
				SELECT
				  (SELECT count(*) FROM state_volume_attachment a JOIN state_volume v ON v.id = a.state_volume_id
				   WHERE v.external_id = NEW.volume_id AND a.workspace_id = snapshot_workspace_id
				     AND a.container_id = snapshot_container_id AND a.attachment_token = NEW.attachment_token
				     AND a.fencing_token = NEW.fencing_token AND a.expires_at > CURRENT_TIMESTAMP) +
				  (SELECT count(*) FROM state_branch_attachment a JOIN state_branch_lineage l ON l.id = a.lineage_id
				   WHERE l.volume_id = NEW.volume_id AND a.workspace_id = snapshot_workspace_id
				     AND a.container_id = snapshot_container_id AND a.attachment_token = NEW.attachment_token
				     AND a.fencing_token = NEW.fencing_token AND a.expires_at > CURRENT_TIMESTAMP)
				INTO active_leases;
				IF active_leases <> 1 THEN
					RAISE EXCEPTION 'snapshot member plan requires its exact active writer lease';
				END IF;
				SELECT source_generation_id, initialize, clone_source, current_generation_id,
					member_name, member_mount_path, member_root
				INTO attachment_source, attachment_initialize, attachment_clone, attachment_head,
					attachment_name, attachment_mount_path, attachment_root
				FROM (
					SELECT a.source_generation_id, a.initialize, FALSE AS clone_source, v.current_generation_id,
						v.name AS member_name, v.mount_path AS member_mount_path, FALSE AS member_root
					FROM state_volume_attachment a JOIN state_volume v ON v.id = a.state_volume_id
					WHERE v.external_id = NEW.volume_id AND a.workspace_id = snapshot_workspace_id
					  AND a.container_id = snapshot_container_id AND a.attachment_token = NEW.attachment_token
					  AND a.fencing_token = NEW.fencing_token AND a.expires_at > CURRENT_TIMESTAMP
					UNION ALL
					SELECT a.source_generation_id, a.initialize, a.clone_source, l.current_generation_id,
						l.member_name, l.mount_path AS member_mount_path, l.is_root AS member_root
					FROM state_branch_attachment a JOIN state_branch_lineage l ON l.id = a.lineage_id
					WHERE l.volume_id = NEW.volume_id AND a.workspace_id = snapshot_workspace_id
					  AND a.container_id = snapshot_container_id AND a.attachment_token = NEW.attachment_token
					  AND a.fencing_token = NEW.fencing_token AND a.expires_at > CURRENT_TIMESTAMP
				) exact_attachment;
				IF attachment_name IS DISTINCT FROM NEW.name OR attachment_mount_path IS DISTINCT FROM NEW.mount_path OR
				   attachment_root IS DISTINCT FROM NEW.is_root THEN
					RAISE EXCEPTION 'snapshot member role does not match its exact attachment volume';
				END IF;
				IF attachment_head IS NOT NULL THEN
					IF NEW.compaction THEN
						SELECT count(*) INTO compaction_sources FROM volume_generation
						WHERE external_id = NEW.compaction_source_generation_id
						  AND workspace_id = snapshot_workspace_id AND volume_id = NEW.volume_id::text
						  AND generation = NEW.generation - 1 AND status = 'available';
						IF NEW.compaction_source_generation_id IS DISTINCT FROM attachment_head OR
						   NEW.parent_generation_id IS NOT NULL OR NEW.clone_parent_generation_id IS NOT NULL OR
						   NEW.generation <= 1 OR compaction_sources <> 1 THEN
							RAISE EXCEPTION 'snapshot compaction must replace its exact available attachment head';
						END IF;
					ELSIF NEW.clone_parent_generation_id IS NOT NULL OR
					      NEW.parent_generation_id IS DISTINCT FROM attachment_head THEN
						RAISE EXCEPTION 'snapshot member generation does not advance its exact attachment head';
					END IF;
				ELSIF attachment_initialize THEN
					IF attachment_source IS NOT NULL OR attachment_clone OR NEW.generation <> 1 OR
					   NEW.parent_generation_id IS NOT NULL OR NEW.clone_parent_generation_id IS NOT NULL OR NEW.compaction THEN
						RAISE EXCEPTION 'snapshot member initial generation does not match its attachment';
					END IF;
				ELSIF attachment_clone THEN
					IF attachment_source IS NULL OR NEW.generation <> 1 OR
					   NEW.clone_parent_generation_id IS DISTINCT FROM attachment_source OR
					   NEW.parent_generation_id IS NOT NULL OR NEW.compaction THEN
						RAISE EXCEPTION 'snapshot member clone parent does not match its attachment source';
					END IF;
				ELSE
					RAISE EXCEPTION 'snapshot member has no valid initial or clone attachment lineage';
				END IF;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_snapshot_member_plan_immutable BEFORE INSERT OR UPDATE OR DELETE ON state_snapshot_member_plan
			FOR EACH ROW EXECUTE FUNCTION enforce_state_snapshot_member_plan();`,
		`CREATE FUNCTION prevent_state_volume_attachment_release() RETURNS trigger AS $$
		DECLARE pending_operations BIGINT;
		BEGIN
			SELECT count(*) INTO pending_operations
			FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
			JOIN state_volume v ON v.external_id = p.volume_id
			WHERE s.status = 'pending' AND s.workspace_id = OLD.workspace_id
			  AND s.source_container_id = OLD.container_id AND v.id = OLD.state_volume_id
			  AND p.attachment_token = OLD.attachment_token AND p.fencing_token = OLD.fencing_token;
			IF pending_operations <> 0 THEN
				RAISE EXCEPTION 'state-volume attachment is held by a pending snapshot operation';
			END IF;
			RETURN OLD;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_volume_attachment_pending_snapshot BEFORE DELETE ON state_volume_attachment
			FOR EACH ROW EXECUTE FUNCTION prevent_state_volume_attachment_release();`,
		`CREATE FUNCTION prevent_state_branch_attachment_release() RETURNS trigger AS $$
		DECLARE pending_operations BIGINT;
		BEGIN
			SELECT count(*) INTO pending_operations
			FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
			JOIN state_branch_lineage l ON l.volume_id = p.volume_id
			WHERE s.status = 'pending' AND s.workspace_id = OLD.workspace_id
			  AND s.source_container_id = OLD.container_id AND l.id = OLD.lineage_id
			  AND p.attachment_token = OLD.attachment_token AND p.fencing_token = OLD.fencing_token;
			IF pending_operations <> 0 THEN
				RAISE EXCEPTION 'state-branch attachment is held by a pending snapshot operation';
			END IF;
			RETURN OLD;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_branch_attachment_pending_snapshot BEFORE DELETE ON state_branch_attachment
			FOR EACH ROW EXECUTE FUNCTION prevent_state_branch_attachment_release();`,
		`CREATE FUNCTION enforce_state_volume_head_transition() RETURNS trigger AS $$
		DECLARE next_parent UUID; next_workspace_id INT; next_volume_id TEXT; next_status TEXT; compaction_plans BIGINT;
		BEGIN
			IF NEW.current_generation_id IS NOT DISTINCT FROM OLD.current_generation_id THEN
				RETURN NEW;
			END IF;
			IF NEW.current_generation_id IS NULL THEN
				RAISE EXCEPTION 'state-volume head cannot move backward to null';
			END IF;
			SELECT parent_generation_id, workspace_id, volume_id, status
			INTO next_parent, next_workspace_id, next_volume_id, next_status
			FROM volume_generation WHERE external_id = NEW.current_generation_id;
			IF next_workspace_id IS DISTINCT FROM NEW.workspace_id OR
			   next_volume_id IS DISTINCT FROM NEW.external_id::text OR next_status <> 'available' THEN
				RAISE EXCEPTION 'state-volume head generation does not match volume';
			END IF;
			IF next_parent IS DISTINCT FROM OLD.current_generation_id THEN
				SELECT count(*) INTO compaction_plans FROM state_snapshot_member_plan p
				JOIN state_snapshot s ON s.id = p.state_snapshot_id
				WHERE p.compaction = TRUE AND p.volume_id = NEW.external_id
				  AND p.generation_id = NEW.current_generation_id
				  AND p.compaction_source_generation_id = OLD.current_generation_id
				  AND s.workspace_id = NEW.workspace_id AND s.status = 'pending';
				IF next_parent IS NOT NULL OR compaction_plans <> 1 THEN
					RAISE EXCEPTION 'state-volume head must advance or compact its exact current generation';
				END IF;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_volume_head_transition BEFORE UPDATE ON state_volume
			FOR EACH ROW EXECUTE FUNCTION enforce_state_volume_head_transition();`,
		`CREATE FUNCTION enforce_state_branch_head_transition() RETURNS trigger AS $$
		DECLARE next_parent UUID; next_clone_parent UUID;
			next_workspace_id INT; next_volume_id TEXT; next_status TEXT; compaction_plans BIGINT;
		BEGIN
			IF NEW.current_generation_id IS NOT DISTINCT FROM OLD.current_generation_id THEN
				RETURN NEW;
			END IF;
			IF NEW.current_generation_id IS NULL THEN
				RAISE EXCEPTION 'root-state head cannot move backward to null';
			END IF;
			SELECT parent_generation_id, clone_parent_generation_id,
				workspace_id, volume_id, status
			INTO next_parent, next_clone_parent,
				next_workspace_id, next_volume_id, next_status
			FROM volume_generation WHERE external_id = NEW.current_generation_id;
			IF next_workspace_id IS DISTINCT FROM NEW.workspace_id OR
			   next_volume_id IS DISTINCT FROM NEW.volume_id::text OR next_status <> 'available' THEN
				RAISE EXCEPTION 'root-state head generation does not match lineage';
			END IF;
			IF OLD.current_generation_id IS NULL THEN
				IF next_parent IS NOT NULL THEN
					RAISE EXCEPTION 'first root-state head must be initial or clone-backed';
				END IF;
			ELSIF next_clone_parent IS NOT NULL OR next_parent IS DISTINCT FROM OLD.current_generation_id THEN
				SELECT count(*) INTO compaction_plans FROM state_snapshot_member_plan p
				JOIN state_snapshot s ON s.id = p.state_snapshot_id
				WHERE p.compaction = TRUE AND p.volume_id = NEW.volume_id
				  AND p.generation_id = NEW.current_generation_id
				  AND p.compaction_source_generation_id = OLD.current_generation_id
				  AND s.workspace_id = NEW.workspace_id AND s.status = 'pending';
				IF next_parent IS NOT NULL OR next_clone_parent IS NOT NULL OR compaction_plans <> 1 THEN
					RAISE EXCEPTION 'root-state head must advance or compact its exact current generation';
				END IF;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_branch_head_transition BEFORE UPDATE ON state_branch_lineage
			FOR EACH ROW EXECUTE FUNCTION enforce_state_branch_head_transition();`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downStateVolumeAttachments(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_snapshot_recovery_claim;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_volume_attachment_pending_snapshot ON state_volume_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS prevent_state_volume_attachment_release();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_branch_attachment_pending_snapshot ON state_branch_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS prevent_state_branch_attachment_release();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_snapshot_member_plan_immutable ON state_snapshot_member_plan;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS enforce_state_snapshot_member_plan();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_snapshot_member_plan;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_read_only_attachment_scope ON state_read_only_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS enforce_state_read_only_attachment_scope();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_read_only_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_branch_head_transition ON state_branch_lineage;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS enforce_state_branch_head_transition();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_volume_head_transition ON state_volume;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS enforce_state_volume_head_transition();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_volume_attachment_scope ON state_volume_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS enforce_state_volume_attachment_scope();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TRIGGER IF EXISTS state_branch_attachment_scope ON state_branch_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP FUNCTION IF EXISTS enforce_state_branch_attachment_scope();`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_branch_attachment;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_branch_lineage;`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_volume_attachment;`); err != nil {
		return err
	}
	// Both writable attachment tables use RESTRICT foreign keys to the plan;
	// dropping the dependent tables first keeps rollback valid even on an empty
	// clean-cut database.
	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS state_volume_attachment_plan;`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE state_volume
		DROP COLUMN IF EXISTS current_generation_id,
		DROP COLUMN IF EXISTS next_fencing_token;`)
	return err
}
