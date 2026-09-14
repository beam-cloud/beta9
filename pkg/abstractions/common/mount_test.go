package abstractions

import (
	"encoding/base64"
	"path"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestConfigureContainerRequestMountsOmitsSandboxOutputs(t *testing.T) {
	signingKey := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	workspace := &types.Workspace{Name: "workspace", SigningKey: &signingKey}
	stub := &types.StubWithRelated{
		Stub:   types.Stub{ExternalId: "stub-123", Type: types.StubType(types.StubTypeSandbox)},
		Object: types.Object{ExternalId: "object-123"},
	}

	mounts, err := ConfigureContainerRequestMounts(
		"sandbox-123",
		stub,
		workspace,
		types.StubConfigV1{},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(mounts) != 1 || mounts[0].MountPath != types.WorkerUserCodeVolume {
		t.Fatalf("sandbox mounts = %#v, want only the code mount", mounts)
	}

	stub.Type = types.StubType(types.StubTypePodRun)
	podMounts, err := ConfigureContainerRequestMounts(
		"pod-123",
		stub,
		workspace,
		types.StubConfigV1{},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantPersistentPath := path.Join(types.DefaultOutputsPath, workspace.Name, "stub-123")
	if podMounts[1].LocalPath != wantPersistentPath || podMounts[1].MountType != "" {
		t.Fatalf("pod output mount = %#v, want persistent path %q", podMounts[1], wantPersistentPath)
	}
}

func TestConfigureContainerRequestMountsRejectsUnsafePersistedVolumes(t *testing.T) {
	signingKey := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	workspace := &types.Workspace{Name: "workspace", SigningKey: &signingKey}
	stub := &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}
	for _, volume := range []*pb.Volume{
		nil,
		{Id: "../../..", MountPath: "/frogmnt"},
		{Id: "/etc", MountPath: "/frogmnt"},
		{Id: "..", MountPath: "/frogmnt", Config: &pb.MountPointConfig{}},
		{Id: "", MountPath: "data"},
		{Id: "valid-id", MountPath: "../../worker"},
	} {
		_, err := ConfigureContainerRequestMounts("sandbox-1", stub, workspace, types.StubConfigV1{Volumes: []*pb.Volume{volume}})
		require.Error(t, err, "volume: %#v", volume)
	}
}

func TestValidateVolumeMountPreservesSDKMounts(t *testing.T) {
	for _, volume := range []*pb.Volume{
		{Id: "valid-id", MountPath: "./weights"},
		{Id: "valid-id", MountPath: "/weights"},
		{MountPath: "/bucket", Config: &pb.MountPointConfig{BucketName: "bucket"}},
	} {
		require.NoError(t, ValidateVolumeMount(volume))
	}
}
