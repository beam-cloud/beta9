package abstractions

import (
	"encoding/base64"
	"path"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestConfigureContainerRequestMountsUsesEphemeralSandboxOutputs(t *testing.T) {
	signingKey := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	workspace := &types.Workspace{Name: "workspace", SigningKey: &signingKey}

	mounts, err := ConfigureContainerRequestMounts(
		"sandbox-123",
		"object-123",
		workspace,
		types.StubConfigV1{},
		"stub-123",
		types.StubType(types.StubTypeSandbox),
	)
	if err != nil {
		t.Fatal(err)
	}

	if mounts[1].LocalPath != types.TempContainerOutputs("sandbox-123") {
		t.Fatalf("sandbox output path = %q, want %q", mounts[1].LocalPath, types.TempContainerOutputs("sandbox-123"))
	}
	if mounts[1].MountType != types.StorageModeLocal {
		t.Fatalf("sandbox output mount type = %q, want %q", mounts[1].MountType, types.StorageModeLocal)
	}

	podMounts, err := ConfigureContainerRequestMounts(
		"pod-123",
		"object-123",
		workspace,
		types.StubConfigV1{},
		"stub-123",
		types.StubType(types.StubTypePodRun),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantPersistentPath := path.Join(types.DefaultOutputsPath, workspace.Name, "stub-123")
	if podMounts[1].LocalPath != wantPersistentPath || podMounts[1].MountType != "" {
		t.Fatalf("pod output mount = %#v, want persistent path %q", podMounts[1], wantPersistentPath)
	}
}
