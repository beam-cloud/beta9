package gateway

type config struct {
	GrpcServerAddress string
}

var GatewayConfig config = config{
	GrpcServerAddress: "0.0.0.0:1993",
}
