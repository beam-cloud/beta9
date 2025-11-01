package image

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpsertSecret_CreateNewSecret tests creating a new credential secret
func TestUpsertSecret_CreateNewSecret(t *testing.T) {
	backendRepo, mock := repository.NewBackendPostgresRepositoryForTest()
	
	service := &RuncImageService{
		backendRepo: backendRepo,
	}
	
	ctx := context.Background()
	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Id:         1,
			ExternalId: "test-workspace-id",
			Name:       "test-workspace",
			SigningKey: stringPtr("sk_pKz38fK8v7lz01AneJI8MJnR70akmP2CtDNf1IufKcY="),
		},
		Token: &types.Token{
			Id: 1,
		},
	}
	
	secretName := "oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com"
	secretValue := `{"registry":"187248174200.dkr.ecr.us-east-1.amazonaws.com","type":"aws","credentials":{"AWS_ACCESS_KEY_ID":"AKIAIOSFODNN7EXAMPLE","AWS_SECRET_ACCESS_KEY":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY","AWS_REGION":"us-east-1"}}`
	registry := "187248174200.dkr.ecr.us-east-1.amazonaws.com"
	
	now := time.Now()
	
	// Mock GetSecretByName - secret doesn't exist yet
	mock.ExpectQuery("SELECT (.+) FROM workspace_secret WHERE name = \\$1 AND workspace_id = \\$2").
		WithArgs(secretName, authInfo.Workspace.Id).
		WillReturnError(sql.ErrNoRows)
	
	// Mock CreateSecret - creating new secret (parameters: name, encryptedValue, workspace.Id, tokenId)
	mock.ExpectQuery("INSERT INTO workspace_secret").
		WithArgs(secretName, sqlmock.AnyArg(), authInfo.Workspace.Id, authInfo.Token.Id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "name", "workspace_id", "last_updated_by", "created_at", "updated_at"}).
			AddRow(1, "secret-external-id-1", secretName, authInfo.Workspace.Id, authInfo.Token.Id, now, now))
	
	secret, err := service.upsertSecret(ctx, authInfo, secretName, secretValue, registry)
	
	require.NoError(t, err)
	assert.NotNil(t, secret)
	assert.Equal(t, secretName, secret.Name)
	assert.Equal(t, "secret-external-id-1", secret.ExternalId)
	
	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestUpsertSecret_UpdateExistingSecret tests updating an existing credential secret
func TestUpsertSecret_UpdateExistingSecret(t *testing.T) {
	backendRepo, mock := repository.NewBackendPostgresRepositoryForTest()
	
	service := &RuncImageService{
		backendRepo: backendRepo,
	}
	
	ctx := context.Background()
	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Id:         1,
			ExternalId: "test-workspace-id",
			Name:       "test-workspace",
			SigningKey: stringPtr("sk_pKz38fK8v7lz01AneJI8MJnR70akmP2CtDNf1IufKcY="),
		},
		Token: &types.Token{
			Id: 1,
		},
	}
	
	secretName := "oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com"
	secretValue := `{"registry":"187248174200.dkr.ecr.us-east-1.amazonaws.com","type":"aws","credentials":{"AWS_ACCESS_KEY_ID":"AKIAIOSFODNN7EXAMPLE","AWS_SECRET_ACCESS_KEY":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY","AWS_REGION":"us-east-1"}}`
	registry := "187248174200.dkr.ecr.us-east-1.amazonaws.com"
	
	now := time.Now()
	existingSecret := &types.Secret{
		Id:         1,
		ExternalId: "d7076927-69f2-4a02-a0df-222e02b58034",
		Name:       secretName,
	}
	
	// Mock GetSecretByName - secret exists
	mock.ExpectQuery("SELECT (.+) FROM workspace_secret WHERE name = \\$1 AND workspace_id = \\$2").
		WithArgs(secretName, authInfo.Workspace.Id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "name", "value", "workspace_id", "last_updated_by", "created_at", "updated_at"}).
			AddRow(existingSecret.Id, existingSecret.ExternalId, existingSecret.Name, "old-encrypted-value", authInfo.Workspace.Id, authInfo.Token.Id, now, now))
	
	// Mock UpdateSecret - THIS IS THE FIX: it should use secretName, not secret.ExternalId
	// Parameters: secretId (actually the name!), workspace.Id, encryptedValue, tokenId
	mock.ExpectQuery("UPDATE workspace_secret SET value = \\$3, last_updated_by = \\$4").
		WithArgs(secretName, authInfo.Workspace.Id, sqlmock.AnyArg(), authInfo.Token.Id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "name", "workspace_id", "last_updated_by", "created_at", "updated_at"}).
			AddRow(existingSecret.Id, existingSecret.ExternalId, existingSecret.Name, authInfo.Workspace.Id, authInfo.Token.Id, now, now.Add(time.Minute)))
	
	secret, err := service.upsertSecret(ctx, authInfo, secretName, secretValue, registry)
	
	require.NoError(t, err)
	assert.NotNil(t, secret)
	assert.Equal(t, secretName, secret.Name)
	assert.Equal(t, existingSecret.ExternalId, secret.ExternalId)
	
	// Ensure all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

// TestCredentialSecretUpsert_WithAndWithoutTag tests the complete flow of creating
// and updating secrets for images with and without tags
func TestCredentialSecretUpsert_WithAndWithoutTag(t *testing.T) {
	backendRepo, mock := repository.NewBackendPostgresRepositoryForTest()
	
	service := &RuncImageService{
		backendRepo: backendRepo,
	}
	
	ctx := context.Background()
	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Id:         1,
			ExternalId: "test-workspace-id",
			Name:       "test-workspace",
			SigningKey: stringPtr("sk_pKz38fK8v7lz01AneJI8MJnR70akmP2CtDNf1IufKcY="),
		},
		Token: &types.Token{
			Id: 1,
		},
	}
	
	// Both images should use the same secret name (without tag)
	secretName := "oci-registry-187248174200-dkr-ecr-us-east-1-amazonaws-com"
	secretValue := `{"registry":"187248174200.dkr.ecr.us-east-1.amazonaws.com","type":"aws","credentials":{"AWS_ACCESS_KEY_ID":"AKIAIOSFODNN7EXAMPLE","AWS_SECRET_ACCESS_KEY":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY","AWS_REGION":"us-east-1"}}`
	registry := "187248174200.dkr.ecr.us-east-1.amazonaws.com"
	
	now := time.Now()
	
	// Step 1: Build first image without tag (187248174200.dkr.ecr.us-east-1.amazonaws.com/crsync)
	// This should create a new secret
	t.Run("FirstImage_WithoutTag_CreatesSecret", func(t *testing.T) {
		mock.ExpectQuery("SELECT (.+) FROM workspace_secret WHERE name = \\$1 AND workspace_id = \\$2").
			WithArgs(secretName, authInfo.Workspace.Id).
			WillReturnError(sql.ErrNoRows)
		
		mock.ExpectQuery("INSERT INTO workspace_secret").
			WithArgs(secretName, sqlmock.AnyArg(), authInfo.Workspace.Id, authInfo.Token.Id).
			WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "name", "workspace_id", "last_updated_by", "created_at", "updated_at"}).
				AddRow(1, "secret-external-id-1", secretName, authInfo.Workspace.Id, authInfo.Token.Id, now, now))
		
		secret, err := service.upsertSecret(ctx, authInfo, secretName, secretValue, registry)
		require.NoError(t, err)
		assert.NotNil(t, secret)
		assert.Equal(t, "secret-external-id-1", secret.ExternalId)
		
		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
	
	// Step 2: Build second image with tag (187248174200.dkr.ecr.us-east-1.amazonaws.com/crsync:latest)
	// This should update the existing secret (same registry, so same secret name)
	t.Run("SecondImage_WithTag_UpdatesSecret", func(t *testing.T) {
		existingSecret := &types.Secret{
			Id:         1,
			ExternalId: "secret-external-id-1",
			Name:       secretName,
		}
		
		// Secret exists from previous build
		mock.ExpectQuery("SELECT (.+) FROM workspace_secret WHERE name = \\$1 AND workspace_id = \\$2").
			WithArgs(secretName, authInfo.Workspace.Id).
			WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "name", "value", "workspace_id", "last_updated_by", "created_at", "updated_at"}).
				AddRow(existingSecret.Id, existingSecret.ExternalId, existingSecret.Name, "old-encrypted-value", authInfo.Workspace.Id, authInfo.Token.Id, now, now))
		
		// Update should use secretName (not ExternalId) - THIS IS THE BUG FIX
		mock.ExpectQuery("UPDATE workspace_secret SET value = \\$3, last_updated_by = \\$4").
			WithArgs(secretName, authInfo.Workspace.Id, sqlmock.AnyArg(), authInfo.Token.Id).
			WillReturnRows(sqlmock.NewRows([]string{"id", "external_id", "name", "workspace_id", "last_updated_by", "created_at", "updated_at"}).
				AddRow(existingSecret.Id, existingSecret.ExternalId, existingSecret.Name, authInfo.Workspace.Id, authInfo.Token.Id, now, now.Add(time.Minute)))
		
		secret, err := service.upsertSecret(ctx, authInfo, secretName, secretValue, registry)
		require.NoError(t, err)
		assert.NotNil(t, secret)
		assert.Equal(t, existingSecret.ExternalId, secret.ExternalId)
		
		err = mock.ExpectationsWereMet()
		assert.NoError(t, err)
	})
}
