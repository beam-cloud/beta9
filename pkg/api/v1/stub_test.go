package apiv1

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bytes"
	"io"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"k8s.io/utils/ptr"
)

func NewStubGroupForTest() *StubGroup {
	backendRepo, _ := repository.NewBackendPostgresRepositoryForTest()

	config := types.AppConfig{
		GatewayService: types.GatewayServiceConfig{
			StubLimits: types.StubLimits{
				Cpu:         128000,
				Memory:      40000,
				MaxGpuCount: 2,
			},
		},
		Monitoring: types.MonitoringConfig{
			FluentBit: types.FluentBitConfig{
				Events: types.FluentBitEventConfig{},
			},
		},
	}

	e := echo.New()

	return NewStubGroup(
		e.Group("/stubs"),
		backendRepo,
		repository.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events),
		config,
	)
}

// NewStubGroupWithMockForTest creates a StubGroup with a mock repository for testing
func NewStubGroupWithMockForTest() (*StubGroup, sqlmock.Sqlmock, *echo.Echo) {
	backendRepo, mock := repository.NewBackendPostgresRepositoryForTest()

	config := types.AppConfig{
		GatewayService: types.GatewayServiceConfig{
			StubLimits: types.StubLimits{
				Cpu:         128000,
				Memory:      40000,
				MaxGpuCount: 2,
			},
		},
		Monitoring: types.MonitoringConfig{
			FluentBit: types.FluentBitConfig{
				Events: types.FluentBitEventConfig{},
			},
		},
	}

	e := echo.New()

	stubGroup := NewStubGroup(
		e.Group("/stubs"),
		backendRepo,
		repository.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events),
		config,
	)

	return stubGroup, mock, e
}

func generateDefaultStubWithConfig() *types.Stub {
	return &types.Stub{
		Name:   "Test Stub",
		Config: `{"runtime":{"cpu":1000,"gpu":"","gpu_count":0,"memory":1000,"image_id":"","gpus":[]},"handler":"","on_start":"","on_deploy":"","on_deploy_stub_id":"","python_version":"python3","keep_warm_seconds":600,"max_pending_tasks":100,"callback_url":"","task_policy":{"max_retries":3,"timeout":3600,"expires":"0001-01-01T00:00:00Z","ttl":0},"workers":1,"concurrent_requests":1,"authorized":false,"volumes":null,"autoscaler":{"type":"queue_depth","max_containers":1,"tasks_per_container":1,"min_containers":0},"extra":{},"checkpoint_enabled":false,"work_dir":"","entry_point":["sleep 100"],"ports":[]}`,
	}
}

// generateMockStubRows creates mock database rows for stub queries
func generateMockStubRows(id uint, externalID, name, config string, workspaceID uint) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "external_id", "name", "type", "config", "config_version", "object_id", "workspace_id", "created_at", "updated_at", "public", "app_id", "workspace.id", "workspace.external_id", "workspace.name", "workspace.created_at", "workspace.updated_at", "workspace.signing_key", "workspace.volume_cache_enabled", "workspace.multi_gpu_enabled", "object.id", "object.external_id", "object.hash", "object.size", "object.workspace_id", "object.created_at", "app.id", "app.external_id", "app.name", "workspace.storage.id", "workspace.storage.external_id", "workspace.storage.bucket_name", "workspace.storage.access_key", "workspace.storage.secret_key", "workspace.storage.endpoint_url", "workspace.storage.region", "workspace.storage.created_at", "workspace.storage.updated_at"}).
		AddRow(id, externalID, name, "deployment", config, 1, 1, workspaceID, now, now, false, 1, workspaceID, fmt.Sprintf("workspace-%d", workspaceID), "Test Workspace", now, now, nil, false, false, 1, fmt.Sprintf("obj-%d", id), fmt.Sprintf("hash%d", id), 1000, workspaceID, now, 1, fmt.Sprintf("app-%d", id), "Test App", nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestProcessStubOverrides(t *testing.T) {
	stubGroup := NewStubGroupForTest()

	tests := []struct {
		name     string
		cpu      *int64
		memory   *int64
		gpu      *string
		gpuCount *uint32
		error    bool
	}{
		{
			name: "Test with CPU override",
			cpu:  ptr.To(int64(2000)),
		},
		{
			name:   "Test with Memory override",
			memory: ptr.To(int64(4096)),
		},
		{
			name: "Test with GPU override",
			gpu:  ptr.To(string(types.GPU_A10G)),
		},
		{
			name:     "Test with GPU Count override",
			gpuCount: ptr.To(uint32(2)),
		},
		{
			name:     "Test with all overrides",
			cpu:      ptr.To(int64(2000)),
			memory:   ptr.To(int64(4096)),
			gpu:      ptr.To(string(types.GPU_A10G)),
			gpuCount: ptr.To(uint32(2)),
		},
		{
			name: "Test with no overrides",
		},
		{
			name:  "Test with invalid GPU",
			gpu:   ptr.To("invalid-gpu"),
			error: true,
		},
		{
			name:     "Test with invalid GPU Count",
			gpuCount: ptr.To(uint32(3)),
			error:    true,
		},
		{
			name:   "Test with invalid Memory",
			memory: ptr.To(int64(50000)),
			error:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &types.StubWithRelated{Stub: *generateDefaultStubWithConfig()}

			err := stubGroup.processStubOverrides(OverrideStubConfig{
				Cpu:      tt.cpu,
				Memory:   tt.memory,
				Gpu:      tt.gpu,
				GpuCount: tt.gpuCount,
			}, stub)
			if err != nil {
				if !tt.error {
					t.Errorf("Unexpected error: %v", err)
				}
				return
			} else {
				if tt.error {
					t.Errorf("Expected error but got none")
					return
				}
			}

			var stubConfig types.StubConfigV1
			err = json.Unmarshal([]byte(stub.Config), &stubConfig)
			if err != nil {
				t.Errorf("Failed to unmarshal stub config: %v", err)
			}
			if tt.cpu != nil && *tt.cpu != stubConfig.Runtime.Cpu {
				t.Errorf("Expected CPU %d, got %d", *tt.cpu, stubConfig.Runtime.Cpu)
			}
			if tt.memory != nil && *tt.memory != stubConfig.Runtime.Memory {
				t.Errorf("Expected Memory %d, got %d", *tt.memory, stubConfig.Runtime.Memory)
			}
			if tt.gpu != nil && (len(stubConfig.Runtime.Gpus) == 0 || *tt.gpu != string(stubConfig.Runtime.Gpus[0])) {
				t.Errorf("Expected GPU %s, got %s", *tt.gpu, stubConfig.Runtime.Gpu)
			}
			if tt.gpuCount != nil && *tt.gpuCount != stubConfig.Runtime.GpuCount {
				t.Errorf("Expected GPU Count %d, got %d", *tt.gpuCount, stubConfig.Runtime.GpuCount)
			}
		})
	}

}

func TestUpdateConfig(t *testing.T) {
	tests := []struct {
		name           string
		stubID         string
		workspaceID    uint
		requestBody    UpdateConfigRequest
		setupMock      func(*sqlmock.Sqlmock)
		expectedStatus int
		expectedError  bool
		expectedFields []string
	}{
		{
			name:        "Test successful config update with multiple fields",
			stubID:      "test-stub-123",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu":    2000,
					"runtime.memory": 4096,
					"python_version": "python3.9",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(1, "test-stub-123", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000},"python_version":"python3"}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-123").WillReturnRows(rows)

				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"runtime.cpu", "runtime.memory", "python_version"},
		},
		{
			name:        "Test successful config update with single field",
			stubID:      "test-stub-456",
			workspaceID: 2,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(2, "test-stub-456", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 2)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-456").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 2).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"runtime.cpu"},
		},
		{
			name:        "Test stub not found",
			stubID:      "non-existent-stub",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				(*mock).ExpectQuery("SELECT").WithArgs("non-existent-stub").WillReturnError(sql.ErrNoRows)
			},
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
		},
		{
			name:        "Test workspace mismatch",
			stubID:      "test-stub-789",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(3, "test-stub-789", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 999) // Different workspace
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-789").WillReturnRows(rows)
			},
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
		},
		{
			name:        "Test empty fields",
			stubID:      "test-stub-empty",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(4, "test-stub-empty", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-empty").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test invalid field path",
			stubID:      "test-stub-invalid",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"invalid.field": "value",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(5, "test-stub-invalid", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-invalid").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test invalid config JSON",
			stubID:      "test-stub-bad-config",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(6, "test-stub-bad-config", "Test Stub", `invalid json`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-bad-config").WillReturnRows(rows)
			},
			expectedStatus: http.StatusInternalServerError,
			expectedError:  true,
		},
		{
			name:        "Test CPU and memory validation failure",
			stubID:      "test-stub-limits",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu":    200000, // Exceeds limit
					"runtime.memory": 50000,  // Exceeds limit
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(7, "test-stub-limits", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-limits").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test database update failure",
			stubID:      "test-stub-db-error",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"runtime.cpu": 1500,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(8, "test-stub-db-error", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-db-error").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 8).WillReturnError(sql.ErrConnDone)
			},
			expectedStatus: http.StatusInternalServerError,
			expectedError:  true,
		},
		{
			name:        "Test empty field path",
			stubID:      "test-stub-empty-path",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"": "value",
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(9, "test-stub-empty-path", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-empty-path").WillReturnRows(rows)
			},
			expectedStatus: http.StatusBadRequest,
			expectedError:  true,
		},
		{
			name:        "Test nested field update",
			stubID:      "test-stub-nested",
			workspaceID: 1,
			requestBody: UpdateConfigRequest{
				Fields: map[string]interface{}{
					"task_policy.max_retries": 5,
					"task_policy.timeout":     7200,
				},
			},
			setupMock: func(mock *sqlmock.Sqlmock) {
				rows := generateMockStubRows(10, "test-stub-nested", "Test Stub", `{"runtime":{"cpu":1000,"memory":1000},"task_policy":{"max_retries":3,"timeout":3600}}`, 1)
				(*mock).ExpectQuery("SELECT").WithArgs("test-stub-nested").WillReturnRows(rows)
				(*mock).ExpectExec("UPDATE stub").WithArgs(sqlmock.AnyArg(), 10).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedStatus: http.StatusOK,
			expectedFields: []string{"task_policy.max_retries", "task_policy.timeout"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubGroup, mock, e := NewStubGroupWithMockForTest()

			if tt.setupMock != nil {
				tt.setupMock(&mock)
			}

			jsonBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPatch, "/", nil)
			req.Body = io.NopCloser(bytes.NewReader(jsonBody))
			req.ContentLength = int64(len(jsonBody))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			c.SetParamNames("stubId")
			c.SetParamValues(tt.stubID)

			authCtx := &auth.HttpAuthContext{
				Context: c,
				AuthInfo: &auth.AuthInfo{
					Workspace: &types.Workspace{Id: tt.workspaceID},
				},
			}

			err := stubGroup.UpdateConfig(authCtx)

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Mock expectations were not met: %v", err)
			}

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				if httpErr, ok := err.(*echo.HTTPError); ok {
					if httpErr.Code != tt.expectedStatus {
						t.Errorf("Expected status %d, got %d", tt.expectedStatus, httpErr.Code)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if rec.Code != tt.expectedStatus {
					t.Errorf("Expected status %d, got %d", tt.expectedStatus, rec.Code)
				}

				if tt.expectedStatus == http.StatusOK {
					var response map[string]interface{}
					if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
						t.Errorf("Failed to unmarshal response: %v", err)
					}

					if message, ok := response["message"].(string); !ok {
						t.Error("Response missing message field")
					} else if !strings.Contains(message, "Stub config updated successfully") {
						t.Errorf("Unexpected message: %s", message)
					}

					if updatedFields, ok := response["updated_fields"].([]interface{}); ok {
						if len(updatedFields) != len(tt.expectedFields) {
							t.Errorf("Expected %d updated fields, got %d", len(tt.expectedFields), len(updatedFields))
						}

						for _, expectedField := range tt.expectedFields {
							found := false
							for _, field := range updatedFields {
								if field == expectedField {
									found = true
									break
								}
							}
							if !found {
								t.Errorf("Expected field %s not found in response", expectedField)
							}
						}
					} else {
						t.Error("Response missing updated_fields")
					}
				}
			}
		})
	}
}

func TestUpdateConfigField(t *testing.T) {
	stubGroup := NewStubGroupForTest()

	tests := []struct {
		name        string
		config      *types.StubConfigV1
		fieldPath   string
		value       interface{}
		expectedErr bool
		checkResult func(*types.StubConfigV1) bool
	}{
		{
			name: "Test updating runtime.cpu",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000, Memory: 1000},
			},
			fieldPath: "runtime.cpu",
			value:     2000,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Cpu == 2000
			},
		},
		{
			name: "Test updating runtime.memory",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000, Memory: 1000},
			},
			fieldPath: "runtime.memory",
			value:     4096,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Memory == 4096
			},
		},
		{
			name: "Test updating python_version",
			config: &types.StubConfigV1{
				PythonVersion: "python3",
			},
			fieldPath: "python_version",
			value:     "python3.9",
			checkResult: func(config *types.StubConfigV1) bool {
				return config.PythonVersion == "python3.9"
			},
		},
		{
			name: "Test updating nested field",
			config: &types.StubConfigV1{
				TaskPolicy: types.TaskPolicy{MaxRetries: 3, Timeout: 3600},
			},
			fieldPath: "task_policy.max_retries",
			value:     5,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.TaskPolicy.MaxRetries == 5
			},
		},
		{
			name: "Test updating boolean field",
			config: &types.StubConfigV1{
				Authorized: false,
			},
			fieldPath: "authorized",
			value:     true,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Authorized == true
			},
		},
		{
			name: "Test updating with string value for int field",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath: "runtime.cpu",
			value:     "1500",
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Cpu == 1500
			},
		},
		{
			name: "Test updating with float value for int field",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath: "runtime.cpu",
			value:     2500.0,
			checkResult: func(config *types.StubConfigV1) bool {
				return config.Runtime.Cpu == 2500
			},
		},
		{
			name: "Test invalid field path",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath:   "invalid.field",
			value:       "value",
			expectedErr: true,
		},
		{
			name: "Test empty field path",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath:   "",
			value:       "value",
			expectedErr: true,
		},
		{
			name: "Test empty field name in path",
			config: &types.StubConfigV1{
				Runtime: types.Runtime{Cpu: 1000},
			},
			fieldPath:   "runtime..cpu",
			value:       2000,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configCopy := *tt.config

			err := stubGroup.updateConfigField(&configCopy, tt.fieldPath, tt.value)

			if tt.expectedErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if tt.checkResult != nil && !tt.checkResult(&configCopy) {
					t.Errorf("Field update did not produce expected result")
				}
			}
		})
	}
}
