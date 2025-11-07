package runtime

import (
	_ "embed"

	types "github.com/beam-cloud/beta9/pkg/types"
)

//go:embed base_runc_config.json
var BaseRuncConfigRaw string

//go:embed base_runsc_config.json
var BaseRunscConfigRaw string

// GetBaseConfig returns the appropriate base config for the runtime
func GetBaseConfig(runtimeName string) string {
	switch runtimeName {
	case types.ContainerRuntimeGvisor.String(), "runsc":
		return BaseRunscConfigRaw
	case types.ContainerRuntimeRunc.String():
		fallthrough
	default:
		return BaseRuncConfigRaw
	}
}
