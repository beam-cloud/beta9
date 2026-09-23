package gatewayservices

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func authFor(ws *types.Workspace) *auth.AuthInfo {
	return &auth.AuthInfo{Workspace: ws, Token: &types.Token{Id: 1}}
}

// referenceBackendRepo holds secrets and one Postgres deployment named "app-db".
type referenceBackendRepo struct {
	repository.BackendRepository
	secrets         map[string]string
	deploymentLists int
}

func newReferenceBackendRepo() *referenceBackendRepo {
	return &referenceBackendRepo{secrets: map[string]string{"HF_TOKEN": "hf_x"}}
}

func (r *referenceBackendRepo) GetSecretByName(_ context.Context, _ *types.Workspace, name string) (*types.Secret, error) {
	value, ok := r.secrets[name]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return &types.Secret{Name: name, Value: value}, nil
}

func (r *referenceBackendRepo) CreateSecret(_ context.Context, _ *types.Workspace, _ uint, name, value string, _ bool) (*types.Secret, error) {
	r.secrets[name] = value
	return &types.Secret{Name: name, Value: value}, nil
}

func (r *referenceBackendRepo) ListDeploymentsWithRelated(_ context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	r.deploymentLists++
	if filters.Name != "app-db" {
		return nil, nil
	}
	config, _ := json.Marshal(types.StubConfigV1{Serving: &types.ServingConfig{Database: &types.DatabaseServingConfig{Kind: "postgres"}}})
	return []types.DeploymentWithRelated{{
		Deployment: types.Deployment{Name: "app-db", Active: true, Version: 1, ExternalId: "dep-1"},
		Stub:       types.Stub{ExternalId: "stub-1", Config: string(config)},
	}}, nil
}

func TestExpandReferences(t *testing.T) {
	repo := newReferenceBackendRepo()
	gws := &GatewayService{backendRepo: repo}
	ws := &types.Workspace{ExternalId: "ws-1"}

	tests := []struct {
		name         string
		env          []string
		wantEnv      []string
		wantBindings []secretBinding
		wantErr      string
	}{
		{
			name:    "plain values pass through",
			env:     []string{"PORT=8080", "GREETING=hi ${not a ref}"},
			wantEnv: []string{"PORT=8080", "GREETING=hi ${not a ref}"},
		},
		{
			name:         "secret binds under the env name",
			env:          []string{"TOKEN=${{secret.HF_TOKEN}}"},
			wantEnv:      []string{},
			wantBindings: []secretBinding{{Name: "HF_TOKEN", EnvName: "TOKEN"}},
		},
		{
			name:    "missing secret is an error",
			env:     []string{"TOKEN=${{secret.NOPE}}"},
			wantErr: `secret "NOPE" does not exist`,
		},
		{
			name:         "database credentials bind to the service's secrets",
			env:          []string{"DATABASE_URL=${{db.app-db.DATABASE_URL}}", "PGUSER=${{ db.app-db.USERNAME }}"},
			wantEnv:      []string{},
			wantBindings: []secretBinding{{Name: "BETA9_POSTGRES_APP_DB_URL", EnvName: "DATABASE_URL"}, {Name: "BETA9_POSTGRES_APP_DB_USERNAME", EnvName: "PGUSER"}},
		},
		{
			name:    "unknown database is an error",
			env:     []string{"DATABASE_URL=${{db.other.DATABASE_URL}}"},
			wantErr: `database "other" does not exist`,
		},
		{
			name:    "credentials cannot be embedded in a string",
			env:     []string{"DSN=pg://${{db.app-db.PASSWORD}}@host"},
			wantErr: "must be the entire value",
		},
		{
			name:    "unknown reference is an error",
			env:     []string{"X=${{shared.FOO}}"},
			wantErr: `unknown reference "shared.FOO"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, bindings, err := gws.expandReferences(context.Background(), authFor(ws), "my-app", tt.env)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantEnv, env)
			require.ElementsMatch(t, tt.wantBindings, bindings)
		})
	}
}

func TestExpandReferencesLastBindingPerVariableWins(t *testing.T) {
	repo := newReferenceBackendRepo()
	repo.secrets["OLD_URL"] = "x"
	gws := &GatewayService{backendRepo: repo}
	ws := &types.Workspace{Id: 1}

	_, bindings, err := gws.expandReferences(context.Background(), authFor(ws), "app", []string{
		"DATABASE_URL=${{secret.OLD_URL}}",
		"DATABASE_URL=${{db.app-db.DATABASE_URL}}",
	})
	require.NoError(t, err)
	require.Equal(t, []secretBinding{{Name: "BETA9_POSTGRES_APP_DB_URL", EnvName: "DATABASE_URL"}}, bindings)
}

func TestExpandReferencesResolvesEachDatabaseOnce(t *testing.T) {
	repo := newReferenceBackendRepo()
	gws := &GatewayService{backendRepo: repo}
	env := []string{
		"DATABASE_URL=${{db.app-db.DATABASE_URL}}",
		"PGUSER=${{db.app-db.USERNAME}}",
		"PGPASSWORD=${{db.app-db.PASSWORD}}",
		"PGDATABASE=${{db.app-db.DATABASE}}",
	}
	_, bindings, err := gws.expandReferences(context.Background(), authFor(&types.Workspace{}), "app", env)
	require.NoError(t, err)
	require.Len(t, bindings, 4)
	require.Equal(t, 1, repo.deploymentLists)
}

func TestExpandReferencesGeneratesSecretsOnce(t *testing.T) {
	repo := newReferenceBackendRepo()
	gws := &GatewayService{backendRepo: repo}
	ws := &types.Workspace{ExternalId: "ws-1"}

	_, bindings, err := gws.expandReferences(context.Background(), authFor(ws), "my app", []string{"JWT=${{secret(12)}}"})
	require.NoError(t, err)
	require.Equal(t, []secretBinding{{Name: "MY_APP_JWT", EnvName: "JWT"}}, bindings)
	first := repo.secrets["MY_APP_JWT"]
	require.Len(t, first, 12)

	// A redeploy must not rotate the value.
	_, _, err = gws.expandReferences(context.Background(), authFor(ws), "my app", []string{"JWT=${{secret(12)}}"})
	require.NoError(t, err)
	require.Equal(t, first, repo.secrets["MY_APP_JWT"])
}

func TestExpandReferencesInlinesRandomInt(t *testing.T) {
	gws := &GatewayService{backendRepo: newReferenceBackendRepo()}
	env, bindings, err := gws.expandReferences(context.Background(), authFor(&types.Workspace{}), "app", []string{"SEED=n-${{randomInt(5, 6)}}"})
	require.NoError(t, err)
	require.Empty(t, bindings)
	require.Equal(t, []string{"SEED=n-5"}, env)
}

// Grammar shared with the SDK (references.py) and the dashboard.
func TestReferenceGrammar(t *testing.T) {
	length, alphabet, err := parseSecretFunc(`secret(8, "abc")`)
	require.NoError(t, err)
	require.Equal(t, 8, length)
	require.Equal(t, "abc", alphabet)

	_, _, err = parseSecretFunc("secret(0)")
	require.Error(t, err)

	for i := 0; i < 20; i++ {
		n, err := parseRandomInt("randomInt(5, 7)")
		require.NoError(t, err)
		require.True(t, n >= 5 && n < 7, "randomInt(5, 7) = %d", n)
	}
	_, err = parseRandomInt("randomInt(7, 5)")
	require.Error(t, err)

	require.Equal(t, "BETA9_POSTGRES_APP_DB", databaseSecretPrefix("postgres", "app-db"))
	require.Equal(t, "MY_APP_JWT_SECRET", generatedSecretName("my app", "jwt_secret"))
	require.True(t, strings.HasPrefix(databaseSecretPrefix("redis", "!!"), "BETA9_REDIS_SERVICE"))
}

func TestExpandStubReferencesResolvesOwnURLBeforeFirstDeploy(t *testing.T) {
	gws := &GatewayService{backendRepo: newReferenceBackendRepo()}
	gws.appConfig.GatewayService.HTTP.ExternalHost = "app.example.com"
	gws.appConfig.GatewayService.HTTP.ExternalPort = 443
	gws.appConfig.GatewayService.HTTP.TLS = true
	gws.appConfig.GatewayService.InvokeURLType = common.InvokeUrlTypePath
	ws := &types.Workspace{Id: 1}

	env, _, err := gws.expandStubReferences(context.Background(), authFor(ws), &pb.GetOrCreateStubRequest{
		AppName:  "router",
		StubType: types.StubTypePodDeployment,
		Ports:    []uint32{20128},
		Env:      []string{"BASE_URL=${{app.router.URL}}", "PEER=${{app.other.URL}}"},
	})
	require.ErrorContains(t, err, `no active deployment named "other"`)

	env, _, err = gws.expandStubReferences(context.Background(), authFor(ws), &pb.GetOrCreateStubRequest{
		AppName:  "router",
		StubType: types.StubTypePodDeployment,
		Ports:    []uint32{20128},
		Env:      []string{"BASE_URL=${{app.router.URL}}"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"BASE_URL=https://app.example.com/pod/router/latest/20128"}, env)
}
