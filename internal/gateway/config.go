package gateway

import "github.com/beam-cloud/beam/internal/types"

type config struct {
	GrpcServerAddress     string
	DefaultFilesystemName string
	DefaultFilesystemPath string
	DefaultObjectPath     string
}

var GatewayConfig config = config{
	GrpcServerAddress:     "0.0.0.0:1993",
	DefaultFilesystemName: "beam-fs",
	DefaultFilesystemPath: types.DefaultFilesystemPath,
	DefaultObjectPath:     types.DefaultObjectPath,
}
