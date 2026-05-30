package abstractions

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConfigureContainerRequestNetworkCopiesNetworkControls(t *testing.T) {
	request := &types.ContainerRequest{}
	stubConfig := types.StubConfigV1{
		BlockNetwork:  true,
		DockerEnabled: true,
	}

	err := ConfigureContainerRequestNetwork(request, stubConfig)
	require.NoError(t, err)
	require.True(t, request.BlockNetwork)
	require.True(t, request.DockerEnabled)
	require.Empty(t, request.AllowList)
}

func TestConfigureContainerRequestNetworkCopiesAllowList(t *testing.T) {
	allowList := []string{"10.0.0.0/8"}
	request := &types.ContainerRequest{}

	err := ConfigureContainerRequestNetwork(request, types.StubConfigV1{AllowList: allowList})
	require.NoError(t, err)
	require.Equal(t, allowList, request.AllowList)

	allowList[0] = "192.168.0.0/16"
	require.Equal(t, []string{"10.0.0.0/8"}, request.AllowList)
}

func TestConfigureContainerRequestNetworkRejectsConflictingPolicy(t *testing.T) {
	err := ConfigureContainerRequestNetwork(&types.ContainerRequest{}, types.StubConfigV1{
		BlockNetwork: true,
		AllowList:    []string{"10.0.0.0/8"},
	})
	require.Error(t, err)
}

func TestConfigureContainerRequestNetworkValidatesAllowList(t *testing.T) {
	err := ConfigureContainerRequestNetwork(&types.ContainerRequest{}, types.StubConfigV1{
		AllowList: []string{"not-a-cidr"},
	})
	require.Error(t, err)
}
