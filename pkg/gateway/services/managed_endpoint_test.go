package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestManagedEndpointRejectsPublicationFields(t *testing.T) {
	gws := &GatewayService{appConfig: types.AppConfig{ManagedEndpoints: types.ManagedEndpointsConfig{Enabled: true}}}
	admin := &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeClusterAdmin}, Workspace: &types.Workspace{Id: 1}}
	// Publication (catalog, access, pricing) lives in config.yaml; app.py only
	// says how the model runs.
	for _, fields := range []string{
		`"harness":true`,
		`"gpu":{"H100":{"harness":{}}}`,
		`"catalog":{"name":"Model"}`,
		`"public":true`,
		`"allowed_workspaces":["ws"]`,
		`"pricing":{"request":"0"}`,
	} {
		t.Run(fields, func(t *testing.T) {
			_, err := gws.managedEndpointStubConfig(context.Background(), admin, &pb.GetOrCreateStubRequest{
				StubType:        types.StubTypeManagedEndpointDeployment,
				ManagedEndpoint: `{"endpoint":{"id":"acme/model","kind":"llm",` + fields + `}}`,
			})
			require.ErrorContains(t, err, "unknown field")
		})
	}
	valid := `{"endpoint":{"id":"acme/model","kind":"llm","gpu":{"H100":{"config":{"max_num_seqs":32}}}}}`
	req := &pb.GetOrCreateStubRequest{StubType: types.StubTypeManagedEndpointDeployment, ManagedEndpoint: valid, Entrypoint: []string{"engine"}}
	config, err := gws.managedEndpointStubConfig(context.Background(), admin, req)
	require.NoError(t, err)
	require.Equal(t, "acme/model", config.Endpoint.ID)
	require.Equal(t, float64(32), config.Endpoint.Gpu["H100"].Config["max_num_seqs"])
	require.Equal(t, []types.GpuType{"H100"}, managedGpuTypes(config))
	req.ManagedEndpoint += `{}`
	_, err = gws.managedEndpointStubConfig(context.Background(), admin, req)
	require.ErrorContains(t, err, "exactly one JSON object")
}

func TestManagedEndpointReplicaMemoryLimit(t *testing.T) {
	gws := &GatewayService{appConfig: types.AppConfig{
		GatewayService:   types.GatewayServiceConfig{StubLimits: types.StubLimits{Cpu: 32_000, Memory: 32 * 1024}},
		ManagedEndpoints: types.ManagedEndpointsConfig{Enabled: true, MaxReplicaMemoryMiB: 512 * 1024},
	}}
	admin := &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeClusterAdmin}, Workspace: &types.Workspace{Id: 1}}
	req := &pb.GetOrCreateStubRequest{
		StubType:        types.StubTypeManagedEndpointDeployment,
		ManagedEndpoint: `{"endpoint":{"id":"deepseek/model","kind":"llm","gpu":{"B300":{"count":2}}}}`,
		Entrypoint:      []string{"engine"},
		Cpu:             16_000,
		Memory:          512 * 1024,
	}
	policy := stubResourcePolicy{}
	config, err := gws.managedEndpointStubConfig(context.Background(), admin, req)
	require.NoError(t, err)
	require.Empty(t, policy.validateManagedLimits(gws, req, admin.Workspace, config))
	_, err = gws.managedEndpointStubConfig(context.Background(), nil, req)
	require.ErrorContains(t, err, "cluster admin")
	require.Equal(t, "Memory must be 32GiB or less.", policy.validateManagedLimits(gws, req, admin.Workspace, nil))

	req.Memory++
	require.Equal(t, "Memory must be 512GiB or less.", policy.validateManagedLimits(gws, req, admin.Workspace, config))
	gws.appConfig.ManagedEndpoints.MaxReplicaMemoryMiB = 0
	require.Equal(t, "Memory must be 32GiB or less.", policy.validateManagedLimits(gws, req, admin.Workspace, config))
}
