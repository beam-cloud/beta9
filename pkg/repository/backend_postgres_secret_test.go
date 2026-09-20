package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCreateSecretLoadsMissingWorkspaceSigningKey(t *testing.T) {
	repository, mock := NewBackendPostgresRepositoryForTest()
	repo := repository.(*PostgresBackendRepository)
	workspace := &types.Workspace{Id: 7, ExternalId: "workspace-external-id"}
	signingKey := "sk_pKz38fK8v7lz01AneJI8MJnR70akmP2CtDNf1IufKcY="

	loadSigningKeyQuery := `SELECT id, name, created_at, concurrency_limit_id, signing_key, volume_cache_enabled, multi_gpu_enabled FROM workspace WHERE external_id = $1;`
	mock.ExpectQuery(regexp.QuoteMeta(loadSigningKeyQuery)).
		WithArgs(workspace.ExternalId).
		WillReturnRows(sqlmock.NewRows([]string{"id", "signing_key"}).AddRow(workspace.Id, signingKey))
	mock.ExpectQuery("INSERT INTO workspace_secret").
		WithArgs("API_KEY", sqlmock.AnyArg(), workspace.Id, uint(11)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "name", "workspace_id", "last_updated_by", "created_at", "updated_at",
		}).AddRow(uint(1), "secret-external-id", "API_KEY", workspace.Id, uint(11), time.Now(), time.Now()))

	secret, err := repo.CreateSecret(context.Background(), workspace, 11, "API_KEY", "secret-value", true)

	require.NoError(t, err)
	require.Equal(t, "API_KEY", secret.Name)
	require.NotNil(t, workspace.SigningKey)
	require.Equal(t, signingKey, *workspace.SigningKey)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSecretOperationsRejectMissingWorkspace(t *testing.T) {
	repository, mock := NewBackendPostgresRepositoryForTest()
	repo := repository.(*PostgresBackendRepository)
	ctx := context.Background()

	tests := map[string]func() error{
		"create": func() error {
			_, err := repo.CreateSecret(ctx, nil, 1, "API_KEY", "value", true)
			return err
		},
		"get": func() error {
			_, err := repo.GetSecretByNameDecrypted(ctx, nil, "API_KEY")
			return err
		},
		"get multiple": func() error {
			_, err := repo.GetSecretsByNameDecrypted(ctx, nil, []string{"API_KEY"})
			return err
		},
		"update": func() error {
			_, err := repo.UpdateSecret(ctx, nil, 1, "API_KEY", "value")
			return err
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.EqualError(t, test(), "workspace is required to access secrets")
		})
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSecretOperationsRejectWorkspaceWithoutSigningKey(t *testing.T) {
	repository, mock := NewBackendPostgresRepositoryForTest()
	repo := repository.(*PostgresBackendRepository)
	workspace := &types.Workspace{Id: 7, ExternalId: "workspace-external-id"}

	loadSigningKeyQuery := `SELECT id, name, created_at, concurrency_limit_id, signing_key, volume_cache_enabled, multi_gpu_enabled FROM workspace WHERE external_id = $1;`
	mock.ExpectQuery(regexp.QuoteMeta(loadSigningKeyQuery)).
		WithArgs(workspace.ExternalId).
		WillReturnRows(sqlmock.NewRows([]string{"id", "signing_key"}).AddRow(workspace.Id, nil))

	_, err := repo.CreateSecret(context.Background(), workspace, 1, "API_KEY", "value", true)

	require.EqualError(t, err, "workspace signing key is unavailable")
	require.NoError(t, mock.ExpectationsWereMet())
}
