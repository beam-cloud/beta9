package types

import "io"

type AgentJoinOptions struct {
	GatewayURL                string
	JoinToken                 string
	JoinTokenFile             string
	DevMode                   bool
	ExecutorOverride          string
	TransportOverride         string
	WorkerImage               string
	MaxCPU                    string
	MaxMemory                 string
	MaxGPUs                   uint
	GPUIDs                    string
	NetworkSlots              uint
	ContainerStartConcurrency uint
	Stdout                    io.Writer
	Stderr                    io.Writer
}
