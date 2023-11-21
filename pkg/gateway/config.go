package gateway

type config struct {
	ExternalPort        string
	InternalPort        string
	LogVerbosity        string
	ScaleDownDelaySync  uint
	ScaleDownDelayAsync uint

	MaxPendingTasks uint
}

var ActivatorConfig config = config{
	ExternalPort:        ":2002",
	InternalPort:        ":2030",
	LogVerbosity:        "debug",
	ScaleDownDelaySync:  90,
	ScaleDownDelayAsync: 10,
	MaxPendingTasks:     1000,
}
