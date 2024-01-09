package gateway

type config struct {
	GrpcServerAddress string
	HttpServerAddress string
}

var GatewayConfig config = config{
	GrpcServerAddress: "0.0.0.0:1993",
	HttpServerAddress: "0.0.0.0:1994",
}
