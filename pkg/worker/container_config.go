package worker

import (
	_ "embed"
)

//go:embed base_runc_config.json
var baseRuncConfigRaw string

// These parameters are set when runcResourcesEnforced is enabled in the worker config
var cgroupV2Parameters map[string]string = map[string]string{
	// When an OOM occurs within a runc container, the kernel will kill the container and all procceses within it.
	"memory.oom.group": "1",
}
