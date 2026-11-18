package types

const (
	MachineStatusRegistered MachineStatus = "registered"
	MachineStatusPending    MachineStatus = "pending"
	MachineStatusAvailable  MachineStatus = "available"
	MachineStatusDisabled   MachineStatus = "disabled"
	// MachineStatusDisconnected is a machine whose agent has stopped
	// reporting in.
	MachineStatusDisconnected MachineStatus = "disconnected"
)

type MachineStatus string
