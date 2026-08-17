package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upStateCacheRetirement, downStateCacheRetirement)
}

func stateSnapshotTransitionFunctionForCacheRetirement(allowRetirementMetadata bool) string {
	terminalDifference := "NEW IS DISTINCT FROM OLD"
	if allowRetirementMetadata {
		terminalDifference = `(to_jsonb(NEW) - 'updated_at' - 'cache_retire_after') IS DISTINCT FROM
				(to_jsonb(OLD) - 'updated_at' - 'cache_retire_after')`
	}
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION enforce_state_snapshot_transition() RETURNS trigger AS $$
	DECLARE members BIGINT; unavailable BIGINT; roots BIGINT;
	BEGIN
		IF TG_OP = 'UPDATE' AND OLD.status <> 'pending' AND %s THEN
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
	$$ LANGUAGE plpgsql;`, terminalDifference)
}

func upStateCacheRetirement(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE state_volume_release_claim (
			id BIGSERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL CHECK (container_id <> ''),
			source_worker_id TEXT NOT NULL CHECK (source_worker_id <> ''),
			source_worker_instance_id TEXT NOT NULL CHECK (source_worker_instance_id <> ''),
			storage_node_id TEXT NOT NULL CHECK (storage_node_id <> ''),
			recovery_worker_id TEXT NOT NULL CHECK (recovery_worker_id <> ''),
			recovery_worker_instance_id TEXT NOT NULL CHECK (recovery_worker_instance_id <> ''),
			journal_digest TEXT NOT NULL CHECK (journal_digest ~ '^sha256:[0-9a-f]{64}$'),
			claim_generation BIGINT NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
			phase TEXT NOT NULL DEFAULT 'source' CHECK (phase IN ('source', 'claimed', 'completed')),
			completed_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, container_id),
			CHECK ((phase = 'source' AND claim_generation = 0 AND completed_at IS NULL AND
			        recovery_worker_id = source_worker_id AND recovery_worker_instance_id = source_worker_instance_id) OR
			       (phase = 'claimed' AND claim_generation > 0 AND completed_at IS NULL) OR
			       (phase = 'completed' AND completed_at IS NOT NULL))
		);`,
		`CREATE TABLE state_volume_release_claim_member (
			claim_id BIGINT NOT NULL REFERENCES state_volume_release_claim(id) ON DELETE CASCADE,
			volume_id UUID NOT NULL,
			attachment_kind TEXT NOT NULL CHECK (attachment_kind IN ('named', 'branch')),
			attachment_plan_id UUID NOT NULL,
			attachment_token UUID NOT NULL,
			fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
			lease_expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
			PRIMARY KEY(claim_id, volume_id),
			UNIQUE(claim_id, attachment_token)
		);`,
		`CREATE FUNCTION enforce_state_volume_release_claim_transition() RETURNS trigger AS $$
		BEGIN
			IF OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR
			   OLD.container_id IS DISTINCT FROM NEW.container_id OR
			   OLD.source_worker_id IS DISTINCT FROM NEW.source_worker_id OR
			   OLD.source_worker_instance_id IS DISTINCT FROM NEW.source_worker_instance_id OR
			   OLD.storage_node_id IS DISTINCT FROM NEW.storage_node_id OR
			   OLD.journal_digest IS DISTINCT FROM NEW.journal_digest THEN
				RAISE EXCEPTION 'state-volume release source obligation is immutable';
			END IF;
			IF OLD.phase = 'completed' THEN
				IF ROW(OLD.phase, OLD.claim_generation, OLD.recovery_worker_id,
				       OLD.recovery_worker_instance_id, OLD.completed_at) IS DISTINCT FROM
				   ROW(NEW.phase, NEW.claim_generation, NEW.recovery_worker_id,
				       NEW.recovery_worker_instance_id, NEW.completed_at) THEN
					RAISE EXCEPTION 'completed state-volume release claim is immutable';
				END IF;
			ELSIF NEW.phase = 'claimed' THEN
				IF NEW.claim_generation <> OLD.claim_generation + 1 OR NEW.completed_at IS NOT NULL OR
				   (OLD.recovery_worker_id = NEW.recovery_worker_id AND
				    OLD.recovery_worker_instance_id = NEW.recovery_worker_instance_id) THEN
					RAISE EXCEPTION 'state-volume release claim handoff must advance one process epoch';
				END IF;
			ELSIF NEW.phase = 'completed' THEN
				IF NEW.claim_generation <> OLD.claim_generation OR
				   NEW.recovery_worker_id IS DISTINCT FROM OLD.recovery_worker_id OR
				   NEW.recovery_worker_instance_id IS DISTINCT FROM OLD.recovery_worker_instance_id OR
				   NEW.completed_at IS NULL THEN
					RAISE EXCEPTION 'state-volume release completion must preserve its exact claimant';
				END IF;
			ELSE
				RAISE EXCEPTION 'invalid state-volume release claim transition';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_volume_release_claim_transition BEFORE UPDATE ON state_volume_release_claim
			FOR EACH ROW EXECUTE FUNCTION enforce_state_volume_release_claim_transition();`,
		`ALTER TABLE state_snapshot
			ADD COLUMN cache_retire_after TIMESTAMP WITH TIME ZONE,
			ADD COLUMN recovery_proof_token UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE;`,
		stateSnapshotTransitionFunctionForCacheRetirement(true),
		`CREATE FUNCTION enforce_state_snapshot_recovery_proof() RETURNS trigger AS $$
		BEGIN
			IF OLD.recovery_proof_token IS DISTINCT FROM NEW.recovery_proof_token THEN
				RAISE EXCEPTION 'state snapshot recovery proof is immutable';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_snapshot_recovery_proof_immutable BEFORE UPDATE ON state_snapshot
			FOR EACH ROW EXECUTE FUNCTION enforce_state_snapshot_recovery_proof();`,
		`CREATE TABLE state_snapshot_reference (
			id BIGSERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			state_snapshot_id INT REFERENCES state_snapshot(id) ON DELETE SET NULL,
			state_snapshot_external_id UUID NOT NULL,
			kind TEXT NOT NULL CHECK (kind IN ('machine', 'snapshot', 'template', 'internal')),
			reference_id TEXT NOT NULL CHECK (reference_id <> ''),
			released_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, kind, reference_id)
		);`,
		`CREATE INDEX idx_state_snapshot_reference_active
			ON state_snapshot_reference(state_snapshot_id) WHERE released_at IS NULL;`,
		`CREATE FUNCTION enforce_state_snapshot_reference_scope() RETURNS trigger AS $$
		DECLARE snapshot_workspace_id INT;
		BEGIN
			IF TG_OP = 'UPDATE' AND
			   (OLD.workspace_id IS DISTINCT FROM NEW.workspace_id OR
			    OLD.state_snapshot_external_id IS DISTINCT FROM NEW.state_snapshot_external_id OR
			    OLD.kind IS DISTINCT FROM NEW.kind OR OLD.reference_id IS DISTINCT FROM NEW.reference_id OR
			    (OLD.released_at IS NOT NULL AND NEW.released_at IS DISTINCT FROM OLD.released_at)) THEN
				RAISE EXCEPTION 'state snapshot reference identity and terminal release are immutable';
			END IF;
			IF TG_OP = 'UPDATE' AND OLD.state_snapshot_id IS DISTINCT FROM NEW.state_snapshot_id AND
			   NOT (OLD.state_snapshot_id IS NOT NULL AND NEW.state_snapshot_id IS NULL AND
			        OLD.released_at IS NOT NULL) THEN
				RAISE EXCEPTION 'active state snapshot reference target is immutable';
			END IF;
			IF NEW.state_snapshot_id IS NULL THEN
				IF NEW.released_at IS NULL THEN
					RAISE EXCEPTION 'active state snapshot reference requires its snapshot';
				END IF;
				RETURN NEW;
			END IF;
			SELECT workspace_id INTO snapshot_workspace_id FROM state_snapshot WHERE id = NEW.state_snapshot_id;
			IF snapshot_workspace_id IS DISTINCT FROM NEW.workspace_id THEN
				RAISE EXCEPTION 'state snapshot reference workspace does not match snapshot';
			END IF;
			IF NOT EXISTS (SELECT 1 FROM state_snapshot WHERE id=NEW.state_snapshot_id
			               AND external_id=NEW.state_snapshot_external_id) THEN
				RAISE EXCEPTION 'state snapshot reference external identity does not match snapshot';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_snapshot_reference_scope BEFORE INSERT OR UPDATE ON state_snapshot_reference
			FOR EACH ROW EXECUTE FUNCTION enforce_state_snapshot_reference_scope();`,
		`CREATE FUNCTION prevent_referenced_state_snapshot_delete() RETURNS trigger AS $$
		BEGIN
			IF EXISTS (SELECT 1 FROM state_snapshot_reference
			           WHERE state_snapshot_id = OLD.id AND released_at IS NULL) THEN
				RAISE EXCEPTION 'state snapshot has active authoritative references';
			END IF;
			RETURN OLD;
		END;
		$$ LANGUAGE plpgsql;`,
		`CREATE TRIGGER state_snapshot_reference_delete_fence BEFORE DELETE ON state_snapshot
			FOR EACH ROW EXECUTE FUNCTION prevent_referenced_state_snapshot_delete();`,
		`CREATE TABLE state_cache_scope_subscription (
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			stub_external_id TEXT NOT NULL CHECK (stub_external_id <> ''),
			volume_id UUID NOT NULL,
			max_generation BIGINT NOT NULL CHECK (max_generation > 0),
			retirement_revision_generation BIGINT CHECK (retirement_revision_generation > 0),
			retirement_revision_id UUID,
			retirement_authorized_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(workspace_id, stub_external_id, volume_id),
			CHECK ((retirement_revision_generation IS NULL AND retirement_revision_id IS NULL AND
			        retirement_authorized_at IS NULL) OR
			       (retirement_revision_generation IS NOT NULL AND retirement_revision_id IS NOT NULL AND
			        retirement_authorized_at IS NOT NULL))
		);`,
		`CREATE TABLE state_cache_retirement_outbox (
			id BIGSERIAL PRIMARY KEY,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			state_snapshot_id INT NOT NULL REFERENCES state_snapshot(id) ON DELETE RESTRICT,
			stub_external_id TEXT NOT NULL CHECK (stub_external_id <> ''),
			volume_id UUID NOT NULL,
			revision_generation BIGINT NOT NULL CHECK (revision_generation > 0),
			revision_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivering', 'delivered', 'cancelled')),
			attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			delivered_at TIMESTAMP WITH TIME ZONE,
			UNIQUE(workspace_id, stub_external_id, volume_id, revision_generation)
		);`,
		`CREATE INDEX idx_state_cache_retirement_snapshot
			ON state_cache_retirement_outbox(state_snapshot_id, status);`,
		`CREATE INDEX idx_state_cache_retirement_pending
			ON state_cache_retirement_outbox(next_attempt_at, id) WHERE status = 'pending';`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downStateCacheRetirement(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`DROP TABLE IF EXISTS state_cache_retirement_outbox;`,
		`DROP TABLE IF EXISTS state_cache_scope_subscription;`,
		`DROP TRIGGER IF EXISTS state_snapshot_reference_delete_fence ON state_snapshot;`,
		`DROP FUNCTION IF EXISTS prevent_referenced_state_snapshot_delete();`,
		`DROP TRIGGER IF EXISTS state_snapshot_reference_scope ON state_snapshot_reference;`,
		`DROP FUNCTION IF EXISTS enforce_state_snapshot_reference_scope();`,
		`DROP TABLE IF EXISTS state_snapshot_reference;`,
		`DROP TRIGGER IF EXISTS state_snapshot_recovery_proof_immutable ON state_snapshot;`,
		`DROP FUNCTION IF EXISTS enforce_state_snapshot_recovery_proof();`,
		stateSnapshotTransitionFunctionForCacheRetirement(false),
		`ALTER TABLE state_snapshot DROP COLUMN IF EXISTS recovery_proof_token,
			DROP COLUMN IF EXISTS cache_retire_after;`,
		`DROP TABLE IF EXISTS state_volume_release_claim_member;`,
		`DROP TRIGGER IF EXISTS state_volume_release_claim_transition ON state_volume_release_claim;`,
		`DROP FUNCTION IF EXISTS enforce_state_volume_release_claim_transition();`,
		`DROP TABLE IF EXISTS state_volume_release_claim;`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
