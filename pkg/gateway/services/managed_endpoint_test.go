package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestManagedEndpointRejectsRemovedFields(t *testing.T) {
	gws := &GatewayService{appConfig: types.AppConfig{ManagedEndpoints: types.ManagedEndpointsConfig{Enabled: true}}}
	admin := &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeClusterAdmin}, Workspace: &types.Workspace{Id: 1}}
	for _, fields := range []string{
		`"harness":true`,
		`"gpu":{"H100":{"harness":{}}}`,
		`"catalog":{"public":true}`,
		`"catalog":{"free":true}`,
		`"catalog":{"hf_id":"old/model"}`,
	} {
		t.Run(fields, func(t *testing.T) {
			_, err := gws.managedEndpointStubConfig(context.Background(), admin, &pb.GetOrCreateStubRequest{
				StubType:        types.StubTypeManagedEndpointDeployment,
				ManagedEndpoint: `{"endpoint":{"id":"acme/model","kind":"llm",` + fields + `}}`,
			})
			require.ErrorContains(t, err, "unknown field")
		})
	}
	valid := `{"endpoint":{"id":"acme/model","kind":"llm","catalog":{"name":"Model","description":"Description","context_length":32768},"public":true,"allowed_workspaces":["workspace-id"],"gpu":{"H100":{"config":{"max_num_seqs":32}}}}}`
	req := &pb.GetOrCreateStubRequest{StubType: types.StubTypeManagedEndpointDeployment, ManagedEndpoint: valid, Entrypoint: []string{"engine"}}
	config, err := gws.managedEndpointStubConfig(context.Background(), admin, req)
	require.NoError(t, err)
	require.True(t, config.Endpoint.Public)
	require.Equal(t, []string{"workspace-id"}, config.Endpoint.AllowedWorkspaces)
	require.Equal(t, float64(32), config.Endpoint.Gpu["H100"].Config["max_num_seqs"])
	req.ManagedEndpoint += `{}`
	_, err = gws.managedEndpointStubConfig(context.Background(), admin, req)
	require.ErrorContains(t, err, "exactly one JSON object")
}
