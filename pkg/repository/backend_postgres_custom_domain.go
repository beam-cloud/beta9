package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const customDomainSelectColumns = `
	cd.id, cd.external_id, cd.workspace_id, cd.domain, cd.deployment_name, cd.stub_type,
	cd.version, cd.port, cd.status, cd.source, cd.verification_token, cd.verified_at, cd.created_at, cd.updated_at,
	COALESCE(d.subdomain, '') AS subdomain
`

const customDomainReturningColumns = `
	id, external_id, workspace_id, domain, deployment_name, stub_type,
	version, port, status, source, verification_token, verified_at, created_at, updated_at,
	'' AS subdomain
`

const customDomainSubdomainLateralJoin = `
	LEFT JOIN LATERAL (
		SELECT subdomain
		FROM deployment
		WHERE workspace_id = cd.workspace_id
		  AND name = cd.deployment_name
		  AND deleted_at IS NULL
		  AND (cd.stub_type = '' OR stub_type::text = cd.stub_type)
		  AND (cd.version = 0 OR version = cd.version)
		ORDER BY active DESC, version DESC
		LIMIT 1
	) d ON TRUE
`

func GenerateCustomDomainVerificationToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "beta9-verify-" + hex.EncodeToString(buf), nil
}

type preparedCustomDomain struct {
	domain string
	status types.CustomDomainStatus
	source types.CustomDomainSource
	token  string
}

func prepareCustomDomainForWrite(customDomain *types.CustomDomain) (*preparedCustomDomain, error) {
	if customDomain == nil {
		return nil, types.ErrInvalidCustomDomain
	}
	domain := types.NormalizeDomain(customDomain.Domain)
	if domain == "" || customDomain.Port > 65535 || customDomain.Version > math.MaxInt32 {
		return nil, types.ErrInvalidCustomDomain
	}

	token := customDomain.VerificationToken
	if token == "" {
		var err error
		token, err = GenerateCustomDomainVerificationToken()
		if err != nil {
			return nil, err
		}
	}

	status := customDomain.Status
	if status == "" {
		status = types.CustomDomainStatusPending
	}
	source := customDomain.Source
	if source == "" {
		source = types.CustomDomainSourceAPI
	}
	return &preparedCustomDomain{domain: domain, status: status, source: source, token: token}, nil
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

// CreateCustomDomain inserts a new claim for the workspace. Claims held by
// other workspaces never block creation: only an active claim is exclusive,
// and activation requires DNS proof (see ActivateCustomDomain).
func (r *PostgresBackendRepository) CreateCustomDomain(ctx context.Context, customDomain *types.CustomDomain) (*types.CustomDomain, error) {
	p, err := prepareCustomDomainForWrite(customDomain)
	if err != nil {
		return nil, err
	}

	query := `
		WITH inserted AS (
			INSERT INTO custom_domain (
				workspace_id, domain, deployment_name, stub_type, version, port, status, source, verification_token, verified_at
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7::varchar, $8, $9,
				CASE WHEN $7::varchar = 'active' THEN CURRENT_TIMESTAMP ELSE NULL END
			)
			ON CONFLICT (workspace_id, domain) DO NOTHING
			RETURNING *
		)
		SELECT ` + customDomainSelectColumns + `
		FROM inserted cd
		` + customDomainSubdomainLateralJoin + `;
	`

	var created types.CustomDomain
	err = r.client.GetContext(
		ctx, &created, query,
		customDomain.WorkspaceId, p.domain, customDomain.DeploymentName, customDomain.StubType,
		customDomain.Version, customDomain.Port, string(p.status), string(p.source), p.token,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || isUniqueViolation(err) {
			return nil, types.ErrCustomDomainAlreadyExists
		}
		return nil, err
	}

	return &created, nil
}

// UpsertCustomDomains binds domains declared in deployment code, all-or-nothing.
// New domains are created as pending claims. An existing claim in the same
// workspace is kept as-is (status, token, pinned version/port) when it already
// targets the same deployment; if it targets a different deployment the whole
// batch fails with ErrCustomDomainBoundToOtherDeployment instead of silently
// re-routing live traffic.
func (r *PostgresBackendRepository) UpsertCustomDomains(ctx context.Context, customDomains []*types.CustomDomain) ([]types.CustomDomain, error) {
	if len(customDomains) == 0 {
		return nil, nil
	}

	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	results := make([]types.CustomDomain, 0, len(customDomains))
	for _, cd := range customDomains {
		upserted, err := upsertCustomDomainWithRunner(ctx, tx, cd)
		if err != nil {
			return nil, err
		}
		results = append(results, *upserted)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

func upsertCustomDomainWithRunner(ctx context.Context, runner sqlx.ExtContext, customDomain *types.CustomDomain) (*types.CustomDomain, error) {
	p, err := prepareCustomDomainForWrite(customDomain)
	if err != nil {
		return nil, err
	}

	query := `
		WITH upserted AS (
			INSERT INTO custom_domain (
				workspace_id, domain, deployment_name, stub_type, version, port, status, source, verification_token, verified_at
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7::varchar, $8, $9,
				CASE WHEN $7::varchar = 'active' THEN CURRENT_TIMESTAMP ELSE NULL END
			)
			ON CONFLICT (workspace_id, domain) DO UPDATE SET
				updated_at = CURRENT_TIMESTAMP
			WHERE custom_domain.deployment_name = EXCLUDED.deployment_name
			  AND (custom_domain.stub_type = '' OR custom_domain.stub_type = EXCLUDED.stub_type)
			RETURNING *
		)
		SELECT ` + customDomainSelectColumns + `
		FROM upserted cd
		` + customDomainSubdomainLateralJoin + `;
	`

	var result types.CustomDomain
	err = sqlx.GetContext(
		ctx, runner, &result, query,
		customDomain.WorkspaceId, p.domain, customDomain.DeploymentName, customDomain.StubType,
		customDomain.Version, customDomain.Port, string(p.status), string(p.source), p.token,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, types.ErrCustomDomainBoundToOtherDeployment
		}
		if isUniqueViolation(err) {
			return nil, types.ErrCustomDomainAlreadyExists
		}
		return nil, err
	}

	return &result, nil
}

// PruneDeployCustomDomains removes domains that were bound by a previous
// deploy of this deployment but are no longer declared in its code. Domains
// added through the API/CLI (source = 'api') are never touched.
func (r *PostgresBackendRepository) PruneDeployCustomDomains(ctx context.Context, workspaceId uint, deploymentName, stubType string, keep []string) ([]types.CustomDomain, error) {
	return pruneDeployCustomDomainsWithRunner(ctx, r.client, workspaceId, deploymentName, stubType, keep)
}

func pruneDeployCustomDomainsWithRunner(ctx context.Context, runner sqlx.ExtContext, workspaceId uint, deploymentName, stubType string, keep []string) ([]types.CustomDomain, error) {
	normalizedKeep := make([]string, 0, len(keep))
	for _, d := range keep {
		if n := types.NormalizeDomain(d); n != "" {
			normalizedKeep = append(normalizedKeep, n)
		}
	}

	query := `
		DELETE FROM custom_domain
		WHERE workspace_id = $1
		  AND deployment_name = $2
		  AND ($3 = '' OR stub_type = '' OR stub_type = $3)
		  AND source = 'deploy'
		  AND NOT (domain = ANY($4))
		RETURNING ` + customDomainReturningColumns + `;
	`

	pruned := []types.CustomDomain{}
	if err := sqlx.SelectContext(ctx, runner, &pruned, query, workspaceId, deploymentName, stubType, pq.StringArray(normalizedKeep)); err != nil {
		return nil, err
	}
	return pruned, nil
}

// SyncDeployCustomDomains makes the deploy-bound domains of a deployment match
// the list declared by one deploy: it binds the declared domains and prunes
// the ones no longer declared, in a single transaction. Syncs for the same
// workspace, deployment name and stub type are serialized with an advisory
// lock, so overlapping deploys cannot delete each other's bindings. A deploy
// is stale, and changes nothing, only when a newer deploy has already saved its
// domains and is still live, so an older deploy can never prune a newer
// deploy's bindings, while a newer deploy that fails before saving anything
// cannot make an older one skip its own domains.
func (r *PostgresBackendRepository) SyncDeployCustomDomains(ctx context.Context, workspaceId uint, deploymentName, stubType string, deployVersion uint, domains []*types.CustomDomain) (*types.DeployCustomDomainSync, error) {
	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	lockKey := fmt.Sprintf("custom_domain_sync:%d:%s:%s", workspaceId, deploymentName, stubType)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0));`, lockKey); err != nil {
		return nil, err
	}

	// A deploy is stale only if a newer deploy has already saved its domains and
	// that deploy is still live. A newer deployment row alone is not enough: it
	// may still fail and be rolled back before it saves anything, and skipping
	// this deploy then would leave its domains unbound.
	var synced uint
	if err := tx.GetContext(ctx, &synced, `
		SELECT COALESCE(MAX(synced_version), 0) FROM custom_domain_sync
		WHERE workspace_id = $1 AND deployment_name = $2 AND stub_type = $3;
	`, workspaceId, deploymentName, stubType); err != nil {
		return nil, err
	}
	if synced > deployVersion {
		var stillLive bool
		if err := tx.GetContext(ctx, &stillLive, `
			SELECT EXISTS (
				SELECT 1 FROM deployment
				WHERE workspace_id = $1 AND name = $2 AND stub_type::text = $3
				  AND version >= $4 AND deleted_at IS NULL
			);
		`, workspaceId, deploymentName, stubType, synced); err != nil {
			return nil, err
		}
		if stillLive {
			return &types.DeployCustomDomainSync{Stale: true}, nil
		}
	}

	result := &types.DeployCustomDomainSync{}
	keep := make([]string, 0, len(domains))
	for _, cd := range domains {
		upserted, err := upsertCustomDomainWithRunner(ctx, tx, cd)
		if err != nil {
			return nil, err
		}
		result.Bound = append(result.Bound, *upserted)
		keep = append(keep, upserted.Domain)
	}

	pruned, err := pruneDeployCustomDomainsWithRunner(ctx, tx, workspaceId, deploymentName, stubType, keep)
	if err != nil {
		return nil, err
	}
	result.Pruned = pruned

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO custom_domain_sync (workspace_id, deployment_name, stub_type, synced_version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workspace_id, deployment_name, stub_type) DO UPDATE SET
			synced_version = EXCLUDED.synced_version,
			updated_at = CURRENT_TIMESTAMP;
	`, workspaceId, deploymentName, stubType, deployVersion); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *PostgresBackendRepository) GetCustomDomainByIdentifier(ctx context.Context, workspaceId uint, idOrDomain string) (*types.CustomDomain, error) {
	query := `
		SELECT ` + customDomainSelectColumns + `
		FROM custom_domain cd
		` + customDomainSubdomainLateralJoin + `
		WHERE cd.workspace_id = $1
		  AND (cd.external_id::text = $2 OR cd.domain = $3)
		LIMIT 1;
	`

	var cd types.CustomDomain
	err := r.client.GetContext(ctx, &cd, query, workspaceId, strings.TrimSpace(idOrDomain), types.NormalizeDomain(idOrDomain))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &cd, nil
}

// GetActiveCustomDomainByDomain returns the single verified claim for domain, if any.
func (r *PostgresBackendRepository) GetActiveCustomDomainByDomain(ctx context.Context, domain string) (*types.CustomDomain, error) {
	query := `
		SELECT ` + customDomainSelectColumns + `
		FROM custom_domain cd
		` + customDomainSubdomainLateralJoin + `
		WHERE cd.domain = $1
		  AND cd.status = 'active'
		LIMIT 1;
	`

	var cd types.CustomDomain
	err := r.client.GetContext(ctx, &cd, query, types.NormalizeDomain(domain))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &cd, nil
}

func (r *PostgresBackendRepository) ListCustomDomains(ctx context.Context, workspaceId uint, deploymentName string) ([]types.CustomDomain, error) {
	query := `
		SELECT ` + customDomainSelectColumns + `
		FROM custom_domain cd
		` + customDomainSubdomainLateralJoin + `
		WHERE cd.workspace_id = $1
		  AND ($2 = '' OR cd.deployment_name = $2)
		ORDER BY cd.created_at DESC;
	`

	domains := []types.CustomDomain{}
	if err := r.client.SelectContext(ctx, &domains, query, workspaceId, strings.TrimSpace(deploymentName)); err != nil {
		return nil, err
	}
	return domains, nil
}

// ActivateCustomDomain marks a claim as verified. It must only be called after
// DNS ownership has been proven. Any claim on the same domain that is active in
// another workspace is demoted to 'failed' in the same transaction, so the
// current DNS owner can always reclaim a domain from a stale/previous owner.
func (r *PostgresBackendRepository) ActivateCustomDomain(ctx context.Context, workspaceId uint, externalId string) (*types.CustomDomain, error) {
	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var target struct {
		Id     uint   `db:"id"`
		Domain string `db:"domain"`
	}
	err = tx.GetContext(ctx, &target, `
		SELECT id, domain FROM custom_domain
		WHERE workspace_id = $1 AND external_id::text = $2;
	`, workspaceId, strings.TrimSpace(externalId))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, types.ErrCustomDomainNotFound
		}
		return nil, err
	}

	// Lock every claim for this domain so concurrent activations serialize.
	if _, err := tx.ExecContext(ctx, `SELECT id FROM custom_domain WHERE domain = $1 ORDER BY id FOR UPDATE;`, target.Domain); err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE custom_domain
		SET status = 'failed', updated_at = CURRENT_TIMESTAMP
		WHERE domain = $1 AND status = 'active' AND id <> $2;
	`, target.Domain, target.Id); err != nil {
		return nil, err
	}

	var cd types.CustomDomain
	err = tx.GetContext(ctx, &cd, `
		WITH updated AS (
			UPDATE custom_domain
			SET status = 'active',
				verified_at = CURRENT_TIMESTAMP,
				updated_at = CURRENT_TIMESTAMP
			WHERE id = $1
			RETURNING *
		)
		SELECT `+customDomainSelectColumns+`
		FROM updated cd
		`+customDomainSubdomainLateralJoin+`;
	`, target.Id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, types.ErrCustomDomainNotFound
		}
		if isUniqueViolation(err) {
			return nil, types.ErrCustomDomainAlreadyExists
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &cd, nil
}

func (r *PostgresBackendRepository) DeleteCustomDomain(ctx context.Context, workspaceId uint, idOrDomain string) (*types.CustomDomain, error) {
	query := `
		DELETE FROM custom_domain
		WHERE workspace_id = $1
		  AND (external_id::text = $2 OR domain = $3)
		RETURNING ` + customDomainReturningColumns + `;
	`

	var cd types.CustomDomain
	err := r.client.GetContext(ctx, &cd, query, workspaceId, strings.TrimSpace(idOrDomain), types.NormalizeDomain(idOrDomain))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, types.ErrCustomDomainNotFound
		}
		return nil, err
	}

	return &cd, nil
}

func (r *PostgresBackendRepository) GetDeploymentByCustomDomain(ctx context.Context, domain string) (*types.DeploymentWithRelated, *types.CustomDomain, error) {
	cd, err := r.GetActiveCustomDomainByDomain(ctx, domain)
	if err != nil {
		return nil, nil, err
	}
	if cd == nil {
		return nil, nil, types.ErrCustomDomainNotFound
	}

	depQuery := `
		SELECT d.id, d.external_id, d.name, d.active, d.subdomain, d.workspace_id, d.stub_id, d.stub_type,
			   d.version, d.created_at, d.updated_at, d.deleted_at, COALESCE(d.app_id, 0) AS app_id,
			   w.external_id AS "workspace.external_id", w.name AS "workspace.name",
			   s.external_id AS "stub.external_id", s.name AS "stub.name", s.type AS "stub.type", s.config AS "stub.config"
		FROM deployment d
		JOIN workspace w ON d.workspace_id = w.id
		JOIN stub s ON d.stub_id = s.id
		WHERE d.workspace_id = $1
		  AND d.name = $2
		  AND d.deleted_at IS NULL
		  AND ($3 = '' OR d.stub_type::text = $3)
		  AND ($4 = 0 OR d.version = $4)
		  AND d.active = true
		ORDER BY d.version DESC
		LIMIT 1;
	`

	var deployment types.DeploymentWithRelated
	if err := r.client.GetContext(ctx, &deployment, depQuery, cd.WorkspaceId, cd.DeploymentName, cd.StubType, cd.Version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, cd, types.ErrCustomDomainDeploymentInactive
		}
		return nil, cd, err
	}

	return &deployment, cd, nil
}
