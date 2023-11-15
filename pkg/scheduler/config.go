package scheduler

type config struct {
	GrpcServerAddress string
	HttpServerAddress string
	Version           string
}

var SchedulerConfig config = config{
	GrpcServerAddress: "0.0.0.0:1993",
	Version:           "0.1.0",
}
