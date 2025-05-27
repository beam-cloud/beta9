package apiv1

import (
	"encoding/json"
	"testing"

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

func generateDefaultStubWithConfig() *types.Stub {
	return &types.Stub{
		Name:   "Test Stub",
		Config: `{"runtime":{"cpu":1000,"gpu":"","gpu_count":0,"memory":1000,"image_id":"","gpus":[]},"handler":"","on_start":"","on_deploy":"","on_deploy_stub_id":"","python_version":"python3","keep_warm_seconds":600,"max_pending_tasks":100,"callback_url":"","task_policy":{"max_retries":3,"timeout":3600,"expires":"0001-01-01T00:00:00Z","ttl":0},"workers":1,"concurrent_requests":1,"authorized":false,"volumes":null,"autoscaler":{"type":"queue_depth","max_containers":1,"tasks_per_container":1,"min_containers":0},"extra":{},"checkpoint_enabled":false,"work_dir":"","entry_point":["sleep 100"],"ports":[]}`,
	}
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
