package worker

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPersistentVMShutdownAllowsSystemdServices(t *testing.T) {
	w := &Worker{config: types.AppConfig{Worker: types.WorkerConfig{TerminationGracePeriod: 30}}}
	req := &types.ContainerRequest{UseVM: true, Env: []string{"BEAM_VM_SYSTEMD=1"}}
	require.Equal(t, 120*time.Second, w.containerTerminationGrace(req))
	require.Equal(t, 30*time.Second, w.containerTerminationGrace(&types.ContainerRequest{UseVM: true}))
	req.UseVM = false
	require.Equal(t, 30*time.Second, w.containerTerminationGrace(req))
	req.UseVM = true
	w.config.Worker.TerminationGracePeriod = 300
	require.Equal(t, 300*time.Second, w.containerTerminationGrace(req))
}
