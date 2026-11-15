package types

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapacityMemoryReservesNativeVMOverhead(t *testing.T) {
	for _, tc := range []struct {
		memory, expected int64
		useVM            bool
	}{
		{0, 0, true}, {-1, -1, true}, {1, 2, false}, {1024, 1280, false},
		{128, 896, true}, {512, 1280, true}, {1025, 1794, true}, {4096, 5120, true},
	} {
		request := &ContainerRequest{Memory: tc.memory, UseVM: tc.useVM}
		require.Equal(t, tc.expected, request.CapacityMemory())
		require.Equal(t, tc.memory, request.Memory, "billing and the application budget remain unchanged")
	}
}

func TestSandboxPrivateWorkerRuntimeTokenOptIn(t *testing.T) {
	for _, kind := range []StubType{StubType(StubTypeSandbox), StubType(StubTypeVM)} {
		request := &ContainerRequest{Stub: StubWithRelated{Stub: Stub{Type: kind, Config: `{}`}}, Env: []string{"SAFE=value"}}
		require.False(t, request.PrivateWorkerRequest().RuntimeTokenRequired)
		request.Env = append(request.Env, "BETA9_TOKEN=explicit-secret")
		request.Stub.Config = `{"secrets":[{"name":"BETA9_TOKEN"}]}`
		private := request.PrivateWorkerRequest()
		require.False(t, private.RuntimeTokenRequired)
		require.Equal(t, []string{"BETA9_TOKEN"}, private.RuntimeSecretNames)
		require.Equal(t, []string{"SAFE=value"}, private.Env)
	}
}

func TestKeylessCheckpointMayEvict(t *testing.T) {
	request := &ContainerRequest{Gpu: "RTX4090",
		Checkpoint: &Checkpoint{Status: string(CheckpointStatusAvailable), Runtime: "gvisor"}}
	require.False(t, request.MayEvict())
	request.Checkpoint.CompatibilityKey = "v1:host"
	require.True(t, request.MayEvict())
	request.Checkpoint.CompatibilityKey = ""
	request.Checkpoint.Runtime = CheckpointRuntimeFilesystem
	require.True(t, request.MayEvict())
	request.Checkpoint.Runtime = "gvisor"
	request.Gpu = string(NO_GPU)
	require.True(t, request.MayEvict())
	request.Gpu = "RTX4090"
	request.Stub.Type = StubType(StubTypeASGIDeployment)
	require.True(t, request.MayEvict())
}

func TestWorkerStartConcurrencyUsesPoolAndRuntime(t *testing.T) {
	pool := WorkerPoolConfig{
		ContainerRuntime:          ContainerRuntimeRunc.String(),
		ContainerStartConcurrency: 32,
	}

	require.Equal(t, 32, WorkerStartConcurrencyForPool(pool, "", ContainerRuntimeRunc.String(), 16000))
}

func TestWorkerStartConcurrencyCapsByWorkerCPU(t *testing.T) {
	pool := WorkerPoolConfig{
		ContainerRuntime:          ContainerRuntimeRunc.String(),
		ContainerStartConcurrency: 32,
	}

	require.Equal(t, 2, WorkerStartConcurrencyForPool(pool, "", ContainerRuntimeRunc.String(), 1000))
}

func TestWorkerProtoCarriesRuntimeAndPoolSelector(t *testing.T) {
	worker := &Worker{
		Id:           "worker-1",
		Runtime:      ContainerRuntimeRunc.String(),
		PoolSelector: "private-cpu",
	}

	proto := worker.ToProto()
	require.Equal(t, ContainerRuntimeRunc.String(), proto.Runtime)
	require.Equal(t, "private-cpu", proto.PoolSelector)
}

func TestContainerRequestProtoRoundTripPreservesTaskID(t *testing.T) {
	request := &ContainerRequest{TaskId: "task-1"}

	roundTrip := NewContainerRequestFromProto(request.ToProto())
	require.Equal(t, request.TaskId, roundTrip.TaskId)
}

func TestCheckpointProtoRoundTripPreservesRuntime(t *testing.T) {
	checkpoint := &Checkpoint{CheckpointId: "checkpoint-1", Runtime: ContainerRuntimeGvisor.String()}

	require.Equal(t, checkpoint.Runtime, NewCheckpointFromProto(checkpoint.ToProto()).Runtime)
	require.False(t, checkpoint.IsFilesystemOnly())
	checkpoint.Runtime = " FILESYSTEM "
	require.True(t, checkpoint.IsFilesystemOnly())
}

func TestPrivateWorkerRequestRemovesControlPlaneCredentials(t *testing.T) {
	signingKey := "workspace-signing-key"
	storageID := uint(1)
	storageSecret := "storage-secret"
	mountConfig := &MountPointConfig{
		BucketName: "external-bucket",
		AccessKey:  "external-access",
		SecretKey:  "external-secret",
	}
	stubConfig, err := json.Marshal(StubConfigV1{
		Secrets: []Secret{{Name: "SECRET"}},
	})
	require.NoError(t, err)

	request := &ContainerRequest{
		ContainerId: "container-1",
		EntryPoint:  []string{"python"},
		Env:         []string{"BETA9_TOKEN=user-token", "SECRET=value", "SAFE=value"},
		ImageId:     "image-1",
		Stub: StubWithRelated{
			Stub: Stub{Config: string(stubConfig)},
		},
		Workspace: Workspace{
			Name:       "workspace",
			SigningKey: &signingKey,
			Storage: &WorkspaceStorage{
				Id:        &storageID,
				SecretKey: &storageSecret,
			},
		},
		Mounts: []Mount{{
			MountPath:        "/mnt/data",
			MountPointConfig: mountConfig,
		}},
		ImageCredentials:         "image-creds",
		BuildRegistryCredentials: "build-creds",
		BuildOptions: BuildOptions{
			SourceImageCreds: "source-image-creds",
			BuildSecrets:     []string{"SECRET=value"},
		},
	}

	privateRequest := request.PrivateWorkerRequest()

	require.Nil(t, privateRequest.Workspace.SigningKey)
	require.Empty(t, privateRequest.ImageCredentials)
	require.Empty(t, privateRequest.BuildRegistryCredentials)
	require.Empty(t, privateRequest.BuildOptions.SourceImageCreds)
	require.Empty(t, privateRequest.BuildOptions.BuildSecrets)
	require.Equal(t, []string{"SAFE=value"}, privateRequest.Env)
	require.NotNil(t, privateRequest.Workspace.Storage)
	require.Nil(t, privateRequest.Workspace.Storage.SecretKey)
	require.NotSame(t, request.Mounts[0].MountPointConfig, privateRequest.Mounts[0].MountPointConfig)
	require.Empty(t, privateRequest.Mounts[0].MountPointConfig.AccessKey)
	require.Empty(t, privateRequest.Mounts[0].MountPointConfig.SecretKey)

	privateRequest.Mounts[0].MountPointConfig.SecretKey = "changed"
	require.Equal(t, "external-secret", request.Mounts[0].MountPointConfig.SecretKey)

	proto := privateRequest.ToProto()
	require.Empty(t, proto.Workspace.SigningKey)
	require.Empty(t, proto.Workspace.Storage.AccessKey)
	require.Empty(t, proto.Workspace.Storage.SecretKey)
}

func TestContainerRequestProtoRoundTripPreservesGitSource(t *testing.T) {
	dockerfile := ""
	request := &ContainerRequest{
		BuildOptions: BuildOptions{
			Dockerfile: &dockerfile,
			GitSource:  &GitSource{RepoURL: "https://github.com/acme/app", Ref: "main", Commit: "abc", Token: "tok", WorkingDir: "api"},
		},
	}
	require.True(t, request.IsBuildRequest())

	restored := NewContainerRequestFromProto(request.ToProto())
	require.True(t, restored.IsBuildRequest(), "an empty Dockerfile string travels as nil; the git source alone marks a build")
	require.Equal(t, request.BuildOptions.GitSource, restored.BuildOptions.GitSource)

	private := request.PrivateWorkerRequest()
	require.Empty(t, private.BuildOptions.GitSource.Token)
	require.Equal(t, "tok", request.BuildOptions.GitSource.Token, "sanitizing the private copy leaves the original alone")
}

func TestBrokeredSecretWorkerBoundary(t *testing.T) {
	request := &ContainerRequest{Env: []string{"ALIAS=beam_secret_a"}, SecretBindings: []SecretBinding{{EnvName: "ALIAS", SecretName: "KEY", Placeholder: "beam_secret_a", Value: "real-value", Hosts: []string{"api.example.com"}}}, Stub: StubWithRelated{Stub: Stub{Config: `{"secrets":[{"name":"KEY","env_name":"ALIAS"}]}`}}, BlockNetwork: true}
	private := request.PrivateWorkerRequest()
	require.Equal(t, request.Env, private.Env)
	require.Empty(t, private.SecretBindings[0].Value)
	require.Equal(t, "real-value", request.SecretBindings[0].Value)
	roundTrip := NewContainerRequestFromProto(private.ToProto())
	require.Equal(t, private.SecretBindings, roundTrip.SecretBindings)
	require.Equal(t, []string{"api.example.com"}, roundTrip.EgressAllowList())
	roundTrip.AllowList = []string{"other.example.com"}
	require.Equal(t, []string{"other.example.com"}, roundTrip.EgressAllowList())
}

func TestCPUShareMultiplier(t *testing.T) {
	for _, valid := range []CPUShareMultiplier{0, 1, 1.1, 1.25, 16} {
		require.NoError(t, valid.Validate(), "%v", valid)
	}
	for _, invalid := range []CPUShareMultiplier{-1, 0.5, 1.005, 16.01, CPUShareMultiplier(math.NaN())} {
		require.Error(t, invalid.Validate(), "%v", invalid)
	}
	require.EqualValues(t, 16000, CPUShareMultiplier(0).Schedulable(16000))
	require.EqualValues(t, 24000, CPUShareMultiplier(1.5).Schedulable(16000))
	require.EqualValues(t, 22050, CPUShareMultiplier(1.4).Schedulable(15750))
}

// MachineId means two things: the machine a caller chose, and the machine a
// request was placed on. Only the second is undone when the request goes
// back to the backlog.
func TestContainerRequestMachineAssignment(t *testing.T) {
	t.Run("placed on a machine, then returned to the backlog", func(t *testing.T) {
		request := &ContainerRequest{}
		request.AssignMachine("machine-1")
		if request.MachineId != "machine-1" || !request.MachineAssigned {
			t.Fatalf("placed request = %q assigned %v", request.MachineId, request.MachineAssigned)
		}
		request.ReleaseMachine()
		if request.MachineId != "" || request.MachineAssigned {
			t.Fatalf("released request = %q assigned %v, want it free to run anywhere", request.MachineId, request.MachineAssigned)
		}
		// Placed again, elsewhere.
		request.AssignMachine("machine-2")
		request.ReleaseMachine()
		if request.MachineId != "" {
			t.Fatalf("request is bound to %q after its second placement", request.MachineId)
		}
	})

	t.Run("pinned by its caller", func(t *testing.T) {
		request := &ContainerRequest{MachineId: "machine-1"}
		request.AssignMachine("machine-1")
		if request.MachineId != "machine-1" || request.MachineAssigned {
			t.Fatalf("pinned request = %q assigned %v", request.MachineId, request.MachineAssigned)
		}
		request.ReleaseMachine()
		if request.MachineId != "machine-1" {
			t.Fatalf("pinned request lost its machine: %q", request.MachineId)
		}
	})

	t.Run("placed on a worker with no machine", func(t *testing.T) {
		request := &ContainerRequest{}
		request.AssignMachine("")
		if request.MachineId != "" || request.MachineAssigned {
			t.Fatalf("request = %q assigned %v", request.MachineId, request.MachineAssigned)
		}
	})

	t.Run("the flag survives the queue", func(t *testing.T) {
		request := &ContainerRequest{ContainerId: "c"}
		request.AssignMachine("machine-1")
		data, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var queued ContainerRequest
		if err := json.Unmarshal(data, &queued); err != nil {
			t.Fatal(err)
		}
		queued.ReleaseMachine()
		if queued.MachineId != "" {
			t.Fatalf("a queued request stayed bound to %q", queued.MachineId)
		}
	})
}
