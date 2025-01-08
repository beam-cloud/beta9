package abstractions

import (
	"context"
	"errors"
	"testing"

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

	authInfo := &auth.AuthInfo{
		Workspace: &types.Workspace{
			Id: 1,
		},
		Token: &types.Token{
			Workspace: &types.Workspace{
				ExternalId: "workspaceId",
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
