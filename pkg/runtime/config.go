package runtime

import (
	_ "embed"
)

//go:embed base_runc_config.json
var BaseRuncConfigRaw string

//go:embed base_runsc_config.json
var BaseRunscConfigRaw string

// GetBaseConfig returns the appropriate base config for the runtime
func GetBaseConfig(runtimeName string) string {
	switch runtimeName {
	case "gvisor", "runsc":
		return BaseRunscConfigRaw
	case "runc":
		fallthrough
	default:
		return BaseRuncConfigRaw
	}
}
