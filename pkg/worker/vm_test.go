package worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
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

func TestSandboxProcessPreservesImageUserExceptForPersistentVM(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			w := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
			req := &types.ContainerRequest{
				ContainerId: "test",
				UseVM:       true,
				Stub:        types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}},
			}
			if persistent {
				req.Stub.Type = types.StubType(types.StubTypeVM)
				req.Env = []string{"BEAM_VM_SYSTEMD=1"}
			}
			w.containerInstances.Set(req.ContainerId, &ContainerInstance{})
			spec := &specs.Spec{Process: &specs.Process{
				Args: []string{"image-entrypoint"},
				User: specs.User{UID: 1000, GID: 1000},
				Env:  []string{"HOME=/home/app", "USER_SETTING=preserved"},
			}}
			require.NoError(t, w.prepareSandboxProcess(req, spec))
			require.Equal(t, []string{types.WorkerSandboxProcessManagerContainerPath}, spec.Process.Args)
			require.Contains(t, spec.Process.Env, "USER_SETTING=preserved")
			if persistent {
				require.Zero(t, spec.Process.User.UID)
				require.Contains(t, spec.Process.Env, "HOME=/root")
				require.NotContains(t, spec.Process.Env, "HOME=/home/app")
			} else {
				require.Equal(t, uint32(1000), spec.Process.User.UID)
				require.Contains(t, spec.Process.Env, "HOME=/home/app")
			}
			require.Len(t, spec.Mounts, 1)
			require.Equal(t, types.WorkerSandboxProcessManagerContainerPath, spec.Mounts[0].Destination)
		})
	}
}
