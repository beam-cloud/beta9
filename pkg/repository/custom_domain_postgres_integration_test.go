package repository

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func openIntegrationPostgresRepo(t *testing.T) *PostgresBackendRepository {
	t.Helper()

	dsn := os.Getenv("BETA9_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("BETA9_TEST_POSTGRES_DSN not set")
	}

	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	// Reset public schema so MigrateContext runs every migration (001..071) from scratch.
	_, err = db.Exec(`DROP SCHEMA public CASCADE; CREATE SCHEMA public;`)
	require.NoError(t, err)

	repo := &PostgresBackendRepository{
		client: db,
		config: types.PostgresConfig{
			EncryptionKey: "sk_pKz38fK8v7lz01AneJI8MJnR70akmP2CtDNf1IufKcY=",
		},
	}

	ctx := context.Background()
	require.NoError(t, repo.MigrateContext(ctx))

	// Also verify migration 071 down + up roundtrip cleanly against live Postgres.
	require.NoError(t, goose.DownContext(ctx, db.DB, "./"))
	require.NoError(t, goose.UpContext(ctx, db.DB, "./"))

	return repo
}

func seedWorkspaceAndDeployment(t *testing.T, db *sqlx.DB, wsName, depName, stubType, subdomain string, version int, active bool) (uint, string, uint, string) {
	t.Helper()

	var wsID uint
	var wsExtID string
	err := db.QueryRow(
		`INSERT INTO workspace (name) VALUES ($1) RETURNING id, external_id::text`,
		wsName,
	).Scan(&wsID, &wsExtID)
	require.NoError(t, err)

	var stubID uint
	var stubExtID string
	err = db.QueryRow(
		`INSERT INTO stub (name, type, config, workspace_id) VALUES ($1, $2::stub_type, '{"authorized":true}', $3) RETURNING id, external_id::text`,
		depName, stubType, wsID,
	).Scan(&stubID, &stubExtID)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO deployment (external_id, name, active, version, stub_id, stub_type, workspace_id, subdomain)
		 VALUES ($1, $2, $3, $4, $5, $6::stub_type, $7, $8)`,
		uuid.NewString(), depName, active, version, stubID, stubType, wsID, subdomain,
	)
	require.NoError(t, err)

	return wsID, wsExtID, stubID, stubExtID
}

func TestCustomDomainPostgresIntegration(t *testing.T) {
	repo := openIntegrationPostgresRepo(t)
	ctx := context.Background()

	ws1ID, ws1ExtID, _, stub1ExtID := seedWorkspaceAndDeployment(
		t, repo.client, "ws-one", "my-api", types.StubTypeEndpointDeployment, "my-api-ws1-v2", 2, true,
	)
	// Also insert an inactive v1 deployment in ws1 to verify LATERAL ORDER BY active DESC, version DESC.
	var stub1ID uint
	require.NoError(t, repo.client.Get(&stub1ID, `SELECT id FROM stub WHERE external_id::text = $1`, stub1ExtID))
	_, err := repo.client.Exec(
		`INSERT INTO deployment (external_id, name, active, version, stub_id, stub_type, workspace_id, subdomain)
		 VALUES ($1, $2, false, 1, $3, $4::stub_type, $5, $6)`,
		uuid.NewString(), "my-api", stub1ID, types.StubTypeEndpointDeployment, ws1ID, "my-api-ws1-v1",
	)
	require.NoError(t, err)

	// Secondary deployment in ws1 for conflict testing.
	_, err = repo.client.Exec(
		`INSERT INTO deployment (external_id, name, active, version, stub_id, stub_type, workspace_id, subdomain)
		 VALUES ($1, 'other-api', true, 1, $2, $3::stub_type, $4, 'other-api-ws1-v1')`,
		uuid.NewString(), stub1ID, types.StubTypeEndpointDeployment, ws1ID,
	)
	require.NoError(t, err)

	ws2ID, _, _, _ := seedWorkspaceAndDeployment(
		t, repo.client, "ws-two", "takeover-api", types.StubTypeEndpointDeployment, "takeover-ws2-v1", 1, true,
	)

	// 1. CreateCustomDomain normalizes domain, populates subdomain via LATERAL join, and detects same-workspace duplicates.
	cd1, err := repo.CreateCustomDomain(ctx, &types.CustomDomain{
		WorkspaceId:    ws1ID,
		Domain:         "API.Example.COM.",
		DeploymentName: "my-api",
		StubType:       types.StubTypeEndpointDeployment,
	})
	require.NoError(t, err)
	require.Equal(t, "api.example.com", cd1.Domain)
	require.Equal(t, types.CustomDomainStatusPending, cd1.Status)
	require.Equal(t, "my-api-ws1-v2", cd1.Subdomain)
	require.NotEmpty(t, cd1.ExternalId)
	require.NotEmpty(t, cd1.VerificationToken)

	_, err = repo.CreateCustomDomain(ctx, &types.CustomDomain{
		WorkspaceId:    ws1ID,
		Domain:         "api.example.com",
		DeploymentName: "my-api",
		StubType:       types.StubTypeEndpointDeployment,
	})
	require.ErrorIs(t, err, types.ErrCustomDomainAlreadyExists)

	// Another workspace can hold a pending claim for the same domain without squatting.
	cd2, err := repo.CreateCustomDomain(ctx, &types.CustomDomain{
		WorkspaceId:    ws2ID,
		Domain:         "api.example.com",
		DeploymentName: "takeover-api",
		StubType:       types.StubTypeEndpointDeployment,
	})
	require.NoError(t, err)
	require.Equal(t, types.CustomDomainStatusPending, cd2.Status)
	require.Equal(t, "takeover-ws2-v1", cd2.Subdomain)

	// Before activation, GetDeploymentByCustomDomain returns ErrCustomDomainNotFound.
	_, _, err = repo.GetDeploymentByCustomDomain(ctx, "api.example.com")
	require.ErrorIs(t, err, types.ErrCustomDomainNotFound)

	// 2. ActivateCustomDomain activates ws1's claim, then ws2 reclaims it and demotes ws1 to 'failed'.
	activated1, err := repo.ActivateCustomDomain(ctx, ws1ID, cd1.ExternalId)
	require.NoError(t, err)
	require.Equal(t, types.CustomDomainStatusActive, activated1.Status)
	require.NotNil(t, activated1.VerifiedAt)
	require.Equal(t, "my-api-ws1-v2", activated1.Subdomain)

	dep, activeCD, err := repo.GetDeploymentByCustomDomain(ctx, "API.Example.COM.")
	require.NoError(t, err)
	require.Equal(t, activated1.ExternalId, activeCD.ExternalId)
	require.Equal(t, ws1ExtID, dep.Workspace.ExternalId)
	require.Equal(t, stub1ExtID, dep.Stub.ExternalId)
	require.Equal(t, "my-api-ws1-v2", dep.Subdomain)

	activated2, err := repo.ActivateCustomDomain(ctx, ws2ID, cd2.ExternalId)
	require.NoError(t, err)
	require.Equal(t, types.CustomDomainStatusActive, activated2.Status)

	demoted1, err := repo.GetCustomDomainByIdentifier(ctx, ws1ID, cd1.ExternalId)
	require.NoError(t, err)
	require.NotNil(t, demoted1)
	require.Equal(t, types.CustomDomainStatusFailed, demoted1.Status)

	// Re-activate ws1 to test inactive deployment detection.
	_, err = repo.ActivateCustomDomain(ctx, ws1ID, cd1.ExternalId)
	require.NoError(t, err)

	// Deactivate ws1's deployment -> ErrCustomDomainDeploymentInactive.
	_, err = repo.client.Exec(`UPDATE deployment SET active = false WHERE workspace_id = $1 AND name = 'my-api'`, ws1ID)
	require.NoError(t, err)
	_, inactiveCD, err := repo.GetDeploymentByCustomDomain(ctx, "api.example.com")
	require.ErrorIs(t, err, types.ErrCustomDomainDeploymentInactive)
	require.NotNil(t, inactiveCD)

	_, err = repo.client.Exec(`UPDATE deployment SET active = true WHERE workspace_id = $1 AND name = 'my-api' AND version = 2`, ws1ID)
	require.NoError(t, err)

	// 3. UpsertCustomDomains: batch insert, idempotent update, and transactional rollback on conflict.
	upserted, err := repo.UpsertCustomDomains(ctx, []*types.CustomDomain{
		{WorkspaceId: ws1ID, Domain: "a.example.com", DeploymentName: "my-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy},
		{WorkspaceId: ws1ID, Domain: "b.example.com", DeploymentName: "my-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy},
	})
	require.NoError(t, err)
	require.Len(t, upserted, 2)
	require.Equal(t, "my-api-ws1-v2", upserted[0].Subdomain)

	// Re-upsert keeps existing verification token and status.
	reupserted, err := repo.UpsertCustomDomains(ctx, []*types.CustomDomain{
		{WorkspaceId: ws1ID, Domain: "a.example.com", DeploymentName: "my-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy, VerificationToken: "should-not-overwrite"},
	})
	require.NoError(t, err)
	require.Len(t, reupserted, 1)
	require.Equal(t, upserted[0].VerificationToken, reupserted[0].VerificationToken)

	// Conflict in batch must roll back new domain ("c.example.com") too.
	_, err = repo.UpsertCustomDomains(ctx, []*types.CustomDomain{
		{WorkspaceId: ws1ID, Domain: "c.example.com", DeploymentName: "other-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy},
		{WorkspaceId: ws1ID, Domain: "a.example.com", DeploymentName: "other-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy},
	})
	require.ErrorIs(t, err, types.ErrCustomDomainBoundToOtherDeployment)

	rolledBack, err := repo.GetCustomDomainByIdentifier(ctx, ws1ID, "c.example.com")
	require.NoError(t, err)
	require.Nil(t, rolledBack)

	// 4. PruneDeployCustomDomains deletes unkept deploy domains while preserving API-sourced ones.
	pruned, err := repo.PruneDeployCustomDomains(ctx, ws1ID, "my-api", types.StubTypeEndpointDeployment, []string{"a.example.com"})
	require.NoError(t, err)
	require.Len(t, pruned, 1)
	require.Equal(t, "b.example.com", pruned[0].Domain)

	// Empty keep list prunes remaining deploy-sourced domain, keeps api-sourced api.example.com.
	prunedEmpty, err := repo.PruneDeployCustomDomains(ctx, ws1ID, "my-api", types.StubTypeEndpointDeployment, nil)
	require.NoError(t, err)
	require.Len(t, prunedEmpty, 1)
	require.Equal(t, "a.example.com", prunedEmpty[0].Domain)

	listed, err := repo.ListCustomDomains(ctx, ws1ID, "my-api")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, "api.example.com", listed[0].Domain)

	// 5. Concurrent ActivateCustomDomain across two workspaces must serialize via FOR UPDATE
	// with exactly one active winner and no unique-index 23505 error.
	ws1Claim, err := repo.CreateCustomDomain(ctx, &types.CustomDomain{
		WorkspaceId:    ws1ID,
		Domain:         "race.example.com",
		DeploymentName: "my-api",
		StubType:       types.StubTypeEndpointDeployment,
		Version:        2,
	})
	require.NoError(t, err)
	ws2Claim, err := repo.CreateCustomDomain(ctx, &types.CustomDomain{
		WorkspaceId:    ws2ID,
		Domain:         "race.example.com",
		DeploymentName: "takeover-api",
		StubType:       "",
	})
	require.NoError(t, err)

	errCh := make(chan error, 2)
	go func() {
		_, err := repo.ActivateCustomDomain(ctx, ws1ID, ws1Claim.ExternalId)
		errCh <- err
	}()
	go func() {
		_, err := repo.ActivateCustomDomain(ctx, ws2ID, ws2Claim.ExternalId)
		errCh <- err
	}()
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)

	winner, err := repo.GetActiveCustomDomainByDomain(ctx, "race.example.com")
	require.NoError(t, err)
	require.NotNil(t, winner)
	raceDep, _, err := repo.GetDeploymentByCustomDomain(ctx, "race.example.com")
	require.NoError(t, err)
	require.NotNil(t, raceDep)

	// 6. DeleteCustomDomain removes the claim and returns ErrCustomDomainNotFound on second delete.
	deleted, err := repo.DeleteCustomDomain(ctx, ws1ID, "api.example.com")
	require.NoError(t, err)
	require.Equal(t, cd1.ExternalId, deleted.ExternalId)

	_, err = repo.DeleteCustomDomain(ctx, ws1ID, "api.example.com")
	require.ErrorIs(t, err, types.ErrCustomDomainNotFound)
}

func TestSyncDeployCustomDomainsPostgres(t *testing.T) {
	repo := openIntegrationPostgresRepo(t)
	ctx := context.Background()

	wsID, _, stubID, _ := seedWorkspaceAndDeployment(
		t, repo.client, "ws-sync", "sync-api", types.StubTypeEndpointDeployment, "sync-api-v1", 1, true,
	)
	// Newer version of the same deployment.
	_, err := repo.client.Exec(
		`INSERT INTO deployment (external_id, name, active, version, stub_id, stub_type, workspace_id, subdomain)
		 VALUES ($1, 'sync-api', true, 2, $2, $3::stub_type, $4, 'sync-api-v2')`,
		uuid.NewString(), stubID, types.StubTypeEndpointDeployment, wsID,
	)
	require.NoError(t, err)

	domain := func(name string) *types.CustomDomain {
		return &types.CustomDomain{
			WorkspaceId: wsID, Domain: name, DeploymentName: "sync-api",
			StubType: types.StubTypeEndpointDeployment, Status: types.CustomDomainStatusPending,
			Source: types.CustomDomainSourceDeploy,
		}
	}
	names := func(cds []types.CustomDomain) []string {
		out := make([]string, 0, len(cds))
		for _, cd := range cds {
			out = append(out, cd.Domain)
		}
		return out
	}
	listed := func() []string {
		cds, err := repo.ListCustomDomains(ctx, wsID, "sync-api")
		require.NoError(t, err)
		return names(cds)
	}

	// 1. The newest version binds its declared domains.
	res, err := repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2,
		[]*types.CustomDomain{domain("a.customer.io"), domain("b.customer.io")})
	require.NoError(t, err)
	require.False(t, res.Stale)
	require.ElementsMatch(t, []string{"a.customer.io", "b.customer.io"}, names(res.Bound))
	require.Empty(t, res.Pruned)

	// 2. A deploy older than one that already saved its domains is stale: it
	// neither binds nor prunes, so it cannot delete the newer deploy's bindings.
	res, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 1,
		[]*types.CustomDomain{domain("old.customer.io")})
	require.NoError(t, err)
	require.True(t, res.Stale)
	require.ElementsMatch(t, []string{"a.customer.io", "b.customer.io"}, listed())

	// 3. Binding and pruning are one transaction: a later deploy that drops b and
	// adds c ends with exactly a and c, and reports b as pruned.
	res, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2,
		[]*types.CustomDomain{domain("a.customer.io"), domain("c.customer.io")})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"b.customer.io"}, names(res.Pruned))
	require.ElementsMatch(t, []string{"a.customer.io", "c.customer.io"}, listed())

	// 4. A failure rolls back both halves: a domain bound to another deployment
	// makes the sync fail and the previous bindings stay untouched.
	_, err = repo.client.Exec(
		`INSERT INTO deployment (external_id, name, active, version, stub_id, stub_type, workspace_id, subdomain)
		 VALUES ($1, 'other-api', true, 1, $2, $3::stub_type, $4, 'other-api-v1')`,
		uuid.NewString(), stubID, types.StubTypeEndpointDeployment, wsID,
	)
	require.NoError(t, err)
	_, err = repo.CreateCustomDomain(ctx, &types.CustomDomain{
		WorkspaceId: wsID, Domain: "taken.customer.io", DeploymentName: "other-api",
		StubType: types.StubTypeEndpointDeployment, Status: types.CustomDomainStatusPending,
		Source: types.CustomDomainSourceAPI, VerificationToken: "tok",
	})
	require.NoError(t, err)
	_, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2,
		[]*types.CustomDomain{domain("taken.customer.io")})
	require.Error(t, err)
	require.ElementsMatch(t, []string{"a.customer.io", "c.customer.io"}, listed())

	// 4b. A newer deployment row that has not saved its domains yet does not make
	// an older deploy stale. Deploy v3 exists (created, still syncing), so v2
	// must still bind and prune its own domains, even if v3 later fails and is
	// rolled back before saving anything.
	_, err = repo.client.Exec(
		`INSERT INTO deployment (external_id, name, active, version, stub_id, stub_type, workspace_id, subdomain)
		 VALUES ($1, 'sync-api', true, 3, $2, $3::stub_type, $4, 'sync-api-v3')`,
		uuid.NewString(), stubID, types.StubTypeEndpointDeployment, wsID,
	)
	require.NoError(t, err)
	res, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2,
		[]*types.CustomDomain{domain("a.customer.io"), domain("d.customer.io")})
	require.NoError(t, err)
	require.False(t, res.Stale, "an unsynced newer deployment must not make an older deploy skip its domains")
	require.ElementsMatch(t, []string{"a.customer.io", "d.customer.io"}, listed())

	// 4c. Once v3 saves its domains it owns the bindings: a late v2 is stale again.
	res, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 3,
		[]*types.CustomDomain{domain("e.customer.io")})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"e.customer.io"}, listed())
	res, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2,
		[]*types.CustomDomain{domain("a.customer.io")})
	require.NoError(t, err)
	require.True(t, res.Stale)
	require.ElementsMatch(t, []string{"e.customer.io"}, listed())

	// 4d. If v3 is rolled back after saving its domains, it no longer owns them:
	// v2 is not stale and restores its own set.
	_, err = repo.client.Exec(`UPDATE deployment SET deleted_at = CURRENT_TIMESTAMP, active = false WHERE workspace_id = $1 AND name = 'sync-api' AND version = 3`, wsID)
	require.NoError(t, err)
	res, err = repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2,
		[]*types.CustomDomain{domain("a.customer.io"), domain("c.customer.io")})
	require.NoError(t, err)
	require.False(t, res.Stale)
	require.ElementsMatch(t, []string{"a.customer.io", "c.customer.io"}, listed())

	// 5. Concurrent syncs are serialized; whichever finishes last wins and the
	// result is always one of the two declared sets, never a mix.
	var wg sync.WaitGroup
	for _, set := range [][]string{{"x1.customer.io", "x2.customer.io"}, {"y1.customer.io"}} {
		set := set
		wg.Add(1)
		go func() {
			defer wg.Done()
			doms := make([]*types.CustomDomain, 0, len(set))
			for _, n := range set {
				doms = append(doms, domain(n))
			}
			_, err := repo.SyncDeployCustomDomains(ctx, wsID, "sync-api", types.StubTypeEndpointDeployment, 2, doms)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	final := listed()
	require.True(t,
		(len(final) == 2 && final[0] != "" && assertSet(final, "x1.customer.io", "x2.customer.io")) ||
			(len(final) == 1 && final[0] == "y1.customer.io"),
		"unexpected final set: %v", final)
}

func assertSet(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]bool{}
	for _, g := range got {
		seen[g] = true
	}
	for _, w := range want {
		if !seen[w] {
			return false
		}
	}
	return true
}
