package vm

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGeneratedIdentityIsCompactFriendlyAndRetryable(t *testing.T) {
	s, _, info, runtime, _ := fixture()
	e := managementAPI(s, info)
	request := `{"request_id":"` + uuid.NewString() + `","spec":{"image_id":"base"}}`
	path := "/" + info.Workspace.ExternalId
	response := vmRequest(e, "POST", path, request)
	require.Equal(t, 201, response.Code, response.Body.String())
	var v types.VM
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &v))
	require.Regexp(t, `^[0-9a-f]{16}$`, v.ID)
	require.Regexp(t, `^[a-z]+-[a-z]+-[0-9a-f]{6}$`, v.Name)
	require.True(t, validName.MatchString(v.Name))
	require.Len(t, runtime.requests, 1)
	response = vmRequest(e, "POST", path, request)
	require.Equal(t, 201, response.Code, response.Body.String())
	var retried types.VM
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &retried))
	require.Equal(t, v.ID, retried.ID)
	require.Equal(t, v.Name, retried.Name)
	require.Equal(t, v.Handle, retried.Handle)
	require.Len(t, runtime.requests, 1, "retry must not allocate another VM")
	stored, err := s.repo.GetVM(context.Background(), info.Workspace.Id, v.ID)
	require.NoError(t, err)
	require.Equal(t, v.Name, stored.Name)
}

func TestVMIdentityIsUniqueAcrossWorkspacesAndRandomCreations(t *testing.T) {
	request := uuid.NewString()
	require.NotEqual(t, vmID(1, request), vmID(2, request))
	require.NotEqual(t, vmID(1, ""), vmID(1, ""))
	s, base, info, _, _ := fixture()
	v, err := s.createVM(auth.ContextWithAuthInfo(context.Background(), info), info, "my-dev", base.Spec)
	require.NoError(t, err)
	require.Equal(t, "my-dev", v.Name)
	require.Len(t, v.ID, 16)
}
