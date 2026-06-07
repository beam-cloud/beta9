package abstractions

import (
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func ConfigureContainerRequestNetwork(request *types.ContainerRequest, stubConfig types.StubConfigV1) error {
	if request == nil {
		return fmt.Errorf("container request is nil")
	}
	policy := (&types.ContainerRequest{
		BlockNetwork: stubConfig.BlockNetwork,
		AllowList:    stubConfig.AllowList,
	}).NetworkPolicy()
	if stubConfig.BlockNetwork && policy == types.ContainerNetworkPolicyAllowList {
		return fmt.Errorf("block_network and allow_list cannot both be set")
	}
	if policy == types.ContainerNetworkPolicyAllowList {
		if err := common.ValidateAllowList(stubConfig.AllowList); err != nil {
			return err
		}
	}

	request.BlockNetwork = stubConfig.BlockNetwork
	request.AllowList = append([]string(nil), stubConfig.AllowList...)
	request.DockerEnabled = stubConfig.DockerEnabled
	return nil
}
