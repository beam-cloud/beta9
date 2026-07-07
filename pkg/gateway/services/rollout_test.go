package gatewayservices

import (
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestDeploymentRolloutAppliesToAnyAlwaysOnDeployment(t *testing.T) {
	latestConfig := types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MinContainers: 1},
	}
	configBytes, err := json.Marshal(latestConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, stubType := range []types.StubType{
		types.StubType(types.StubTypeEndpointDeployment),
		types.StubType(types.StubTypeFunctionDeployment),
		types.StubType(types.StubTypePodDeployment),
	} {
		if !deploymentRolloutApplies(deploymentRolloutInput{
			stub:   &types.StubWithRelated{Stub: types.Stub{Type: stubType}},
			config: &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 1}},
			latest: &types.DeploymentWithRelated{
				Deployment: types.Deployment{Active: true},
				Stub:       types.Stub{Type: stubType, Config: string(configBytes)},
			},
		}) {
			t.Fatalf("expected rollout to apply to %s", stubType)
		}
	}
}
