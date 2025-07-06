package abstractions

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestParseAndValidateDeploymentStubId(t *testing.T) {
	backendRepoForTest, sqlmock := repository.NewBackendPostgresRepositoryForTest()

	stubId := uuid.Must(uuid.NewV4()).String()
	workspaceId := uuid.Must(uuid.NewV4()).String()

	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Id:         1,
			ExternalId: workspaceId,
		},
		Token: &types.Token{
			Workspace: &types.Workspace{
				ExternalId: workspaceId,
			},
			Key: "key",
		},
	}

	tests := []struct {
		name           string
		stubId         string
		deploymentName string
		version        string
		stubType       string
		preloadFn      func()
		expectedStubId string
		expectedErr    error
	}{
		{
			name:           "Latest deployment should succeed",
			version:        "",
			deploymentName: "deploymentName",
			expectedStubId: stubId,
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "stub.external_id", "active", "version"}).AddRow(1, stubId, true, 20))
				mockStubQuery(sqlmock, stubId, workspaceId)
			},
		},
		{
			name:           "Latest deployment not active",
			version:        "",
			deploymentName: "deploymentName",
			expectedStubId: "",
			expectedErr:    apiv1.HTTPBadRequest("Deployment is not active"),
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "stub.external_id", "active", "version"}).AddRow(1, stubId, false, 20))
			},
		},
		{
			name:           "Specific version deployment",
			version:        "20",
			deploymentName: "deploymentName",
			expectedStubId: stubId,
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "stub.external_id", "active", "version"}).AddRow(1, stubId, true, 20))
				mockStubQuery(sqlmock, stubId, workspaceId)
			},
		},
		{
			name:           "No deployment name",
			version:        "",
			deploymentName: "",
			expectedStubId: "",
		},
		{
			name:        "Invalid version",
			version:     "invalid",
			expectedErr: apiv1.HTTPBadRequest("Invalid deployment version"),
		},
		{
			name:           "Valid version - Failed to get deployment",
			version:        "1",
			deploymentName: "deploymentName",
			expectedStubId: "",
			expectedErr:    apiv1.HTTPBadRequest("Invalid deployment"),
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnError(errors.New("failed to get deployment"))
			},
		},
		{
			name:           "Valid version - Deployment not found",
			version:        "1",
			deploymentName: "deploymentName",
			expectedStubId: "",
			expectedErr:    apiv1.HTTPBadRequest("Invalid deployment"),
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnError(nil)
			},
		},
		{
			name:           "Valid version - Stub not found",
			version:        "1",
			deploymentName: "",
			stubId:         "stub",
			expectedStubId: "",
			expectedErr:    apiv1.HTTPBadRequest("Invalid stub"),
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name:           "Valid version - Deployment not active",
			version:        "1",
			deploymentName: "deploymentName",
			expectedStubId: "",
			expectedErr:    apiv1.HTTPBadRequest("Deployment is not active"),
			preloadFn: func() {
				sqlmock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "stub.external_id", "active", "version"}).AddRow(1, stubId, false, 20))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preloadFn != nil {
				tt.preloadFn()
			}

			stubId, err := ParseAndValidateDeploymentStubId(
				context.Background(),
				NewDeploymentStubCache(),
				authInfo,
				tt.stubId,
				tt.deploymentName,
				tt.version,
				tt.stubType,
				backendRepoForTest,
			)

			if err != nil {
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
			}

			assert.Equal(t, tt.expectedStubId, stubId)
		})
	}
}

func mockStubQuery(sqlmock sqlmock.Sqlmock, stubId, workspaceId string) {
	sqlmock.ExpectQuery("(?i)stub").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "name", "type", "config", "config_version", "object_id", "workspace_id", "created_at", "updated_at", "public", "app_id",
			"workspace.id", "workspace.external_id", "workspace.name", "workspace.created_at", "workspace.updated_at", "workspace.signing_key", "workspace.volume_cache_enabled", "workspace.multi_gpu_enabled",
			"object.id", "object.external_id", "object.hash", "object.size", "object.workspace_id", "object.created_at",
			"app.id", "app.external_id", "app.name",
			"workspace.storage.id", "workspace.storage.external_id", "workspace.storage.bucket_name", "workspace.storage.access_key", "workspace.storage.secret_key", "workspace.storage.endpoint_url", "workspace.storage.region", "workspace.storage.created_at", "workspace.storage.updated_at",
		}).AddRow(
			1, stubId, "stub-name", "stub-type", "{}", 1, 1, 1, time.Now(), time.Now(), false, 1,
			1, workspaceId, "workspace-name", time.Now(), time.Now(), nil, false, false,
			1, "object-external-id", "object-hash", 0, 1, time.Now(),
			1, "app-external-id", "app-name",
			1, "storage-external-id", "bucket", nil, nil, nil, nil, time.Now(), time.Now(),
		))
}
