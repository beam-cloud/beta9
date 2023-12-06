package gateway

type config struct {
	GrpcServerAddress string
	LogVerbosity      string
}

var GatewayConfig config = config{
	GrpcServerAddress: "0.0.0.0:1993",
	LogVerbosity:      "debug",
}
