package types

const (
	WorkerEnvID                   = "WORKER_ID"
	WorkerEnvToken                = "WORKER_TOKEN"
	WorkerEnvPoolName             = "WORKER_POOL_NAME"
	WorkerEnvMachineID            = "WORKER_MACHINE_ID"
	WorkerEnvPersistent           = "WORKER_PERSISTENT"
	WorkerEnvRouteTransport       = "WORKER_ROUTE_TRANSPORT"
	WorkerEnvRouteLocalTargetHost = "WORKER_ROUTE_LOCAL_TARGET_HOST"

	ContainerEnvGatewayGRPCHost = "BETA9_GATEWAY_HOST"
	ContainerEnvGatewayGRPCPort = "BETA9_GATEWAY_PORT"
	ContainerEnvGatewayHTTPHost = "BETA9_GATEWAY_HOST_HTTP"
	ContainerEnvGatewayHTTPPort = "BETA9_GATEWAY_PORT_HTTP"
	ContainerEnvHostname        = "CONTAINER_HOSTNAME"
)
