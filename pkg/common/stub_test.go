package common

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestVMContainerUsesSandboxTransportWithDistinctIdentity(t *testing.T) {
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	cid := "vm-" + stubID + "-1717f4fc"
	fields, err := ParseSubdomainFields(cid + "-8080")
	require.NoError(t, err)
	require.Equal(t, stubID, fields.StubId)
	require.Equal(t, cid, fields.ContainerId)
	stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: stubID, Type: types.StubType(types.StubTypeVM), Config: `{"authorized":true}`}}
	expected := "/sandbox/container/" + stubID + "/" + cid + "/8080"
	require.Equal(t, expected, BuildHandlerPath(&stub.Stub, fields))
	require.Equal(t, "https://app.example.com"+expected, BuildSandboxURL("https://app.example.com", InvokeUrlTypePath, stub, cid, 8080))
	legacy, err := ExtractStubIdFromContainerId("sandbox-" + stubID + "-1717f4fc")
	require.NoError(t, err)
	require.Equal(t, stubID, legacy)
}

func TestExtractStubIdFromStubScopedContainerId(t *testing.T) {
	stubID := "5e3e31ff-aef4-40b6-a98d-439268a9832e"
	tests := []struct {
		name        string
		containerID string
		wantStubID  string
		wantOK      bool
	}{
		{
			name:        "sandbox",
			containerID: "sandbox-" + stubID + "-1717f4fc",
			wantStubID:  stubID,
			wantOK:      true,
		},
		{
			name:        "vm",
			containerID: "vm-" + stubID + "-1717f4fc",
			wantStubID:  stubID,
			wantOK:      true,
		},
		{
			name:        "pod",
			containerID: "pod-" + stubID + "-1717f4fc",
			wantStubID:  stubID,
			wantOK:      true,
		},
		{
			name:        "endpoint",
			containerID: "endpoint-" + stubID + "-1717f4fc",
			wantStubID:  stubID,
			wantOK:      true,
		},
		{
			name:        "taskqueue",
			containerID: "taskqueue-" + stubID + "-1717f4fc",
			wantStubID:  stubID,
			wantOK:      true,
		},
		{
			name:        "managed",
			containerID: "managed-" + stubID + "-1717f4fc",
			wantStubID:  stubID,
			wantOK:      true,
		},
		{
			name:        "function task id is not a stub id",
			containerID: "function-" + stubID + "-1717f4fc",
			wantOK:      false,
		},
		{
			name:        "invalid",
			containerID: "sandbox-not-enough-parts",
			wantOK:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStubID, gotOK := ExtractStubIdFromStubScopedContainerId(tt.containerID)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %t, want %t", gotOK, tt.wantOK)
			}
			if gotStubID != tt.wantStubID {
				t.Fatalf("stub ID = %q, want %q", gotStubID, tt.wantStubID)
			}
		})
	}
}
