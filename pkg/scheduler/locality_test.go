package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWorkerLocalityRemainsASoftPreference(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*types.Worker)
		want   string
	}{
		{name: "local disk beats best fit", want: "local"},
		{name: "full worker falls back", change: func(w *types.Worker) { w.FreeMemory = 0 }, want: "other"},
		{name: "unavailable worker falls back", change: func(w *types.Worker) { w.Status = types.WorkerStatusDisabled }, want: "other"},
		{name: "incompatible runtime falls back", change: func(w *types.Worker) { w.Runtime = types.ContainerRuntimeMicroVM.String() }, want: "other"},
		{name: "pool priority is preserved", change: func(w *types.Worker) { w.Priority = 0 }, want: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scheduler{workerPoolManager: NewWorkerPoolManager()}
			local := &types.Worker{Id: "local", Status: types.WorkerStatusAvailable, FreeCpu: 16000, FreeMemory: 32000, Priority: 1}
			other := &types.Worker{Id: "other", Status: types.WorkerStatusAvailable, FreeCpu: 4000, FreeMemory: 8000, Priority: 1}
			if tc.change != nil {
				tc.change(local)
			}
			worker, err := s.selectWorkerFromWorkers([]*types.Worker{other, local}, &types.ContainerRequest{Cpu: 1000, Memory: 1000, PreferredWorkerId: "local"})
			require.NoError(t, err)
			require.Equal(t, tc.want, worker.Id)
		})
	}
}
