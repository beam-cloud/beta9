package gateway

type config struct {
	GrpcServerAddress     string
	LogVerbosity          string
	DefaultFilesystemName string
	DefaultFilesystemPath string
}

var GatewayConfig config = config{
	GrpcServerAddress:     "0.0.0.0:1993",
	LogVerbosity:          "debug",
	DefaultFilesystemName: "beam-fs",
	DefaultFilesystemPath: "/data",
}
