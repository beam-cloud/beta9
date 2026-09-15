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

func TestValidateVolume(t *testing.T) {
	for _, v := range []*pb.Volume{
		{Id: "vol-1", MountPath: "./weights"},
		{Id: "vol-1", MountPath: "/weights"},
		{MountPath: "/bucket", Config: &pb.MountPointConfig{BucketName: "bucket"}},
	} {
		require.NoError(t, ValidateVolume(v), "%v", v)
	}
	for _, v := range []*pb.Volume{
		nil,
		{Id: "", MountPath: "data"},
		{Id: "../../..", MountPath: "/frogmnt"},
		{Id: "..", MountPath: "/frogmnt", Config: &pb.MountPointConfig{}},
		{Id: "/etc", MountPath: "/frogmnt"},
		{Id: "vol-1", MountPath: "../../worker"},
	} {
		require.Error(t, ValidateVolume(v), "%v", v)
	}
}

// Volumes persisted before validation existed are checked again when mounted.
func TestConfigureContainerRequestMountsRejectsInvalidVolumes(t *testing.T) {
	signingKey := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	workspace := &types.Workspace{Name: "workspace", SigningKey: &signingKey}
	stub := &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}
	volumes := []*pb.Volume{{Id: "../../..", MountPath: "/frogmnt"}}
	_, err := ConfigureContainerRequestMounts("sandbox-1", stub, workspace, types.StubConfigV1{Volumes: volumes})
	require.Error(t, err)
}
