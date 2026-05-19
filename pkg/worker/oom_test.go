package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestSetupOOMWatcherSkipsIncompleteGvisorSpecs(t *testing.T) {
	worker := &Worker{
		runtime: &mockRuntime{name: types.ContainerRuntimeGvisor.String()},
		config: types.AppConfig{
			Worker: types.WorkerConfig{
				ContainerResourceLimits: types.ContainerResourceLimitsConfig{
					MemoryEnforced: true,
				},
			},
		},
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}

	tests := []struct {
		name string
		spec *specs.Spec
	}{
		{name: "nil spec"},
		{name: "nil linux", spec: &specs.Spec{}},
		{name: "nil resources", spec: &specs.Spec{Linux: &specs.Linux{}}},
		{name: "nil memory", spec: &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{}}}},
		{name: "nil memory limit", spec: &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{}}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker.setupOOMWatcher(
				context.Background(),
				"container-id",
				123,
				tt.spec,
				&types.ContainerRequest{Memory: 128},
				slog.Default(),
				&atomic.Bool{},
			)
		})
	}
}
