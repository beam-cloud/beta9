package types

// TODO: Once we revisit the runner development server code, it would probably be a good idea to consolidate this
// Do we actually need a types file for the golang part of the runner?
// It also feels a bit weird to put constants in the types package
var (
	ExitCodeOOMError  = 137
	ExitCodeInterrupt = 130
)

const DevelopmentServerPort int = 2326
const NotebookServerPort int = 8888
const MaxDeploymentSize int64 = 500 * 1024 * 1024 // 500 MiB
