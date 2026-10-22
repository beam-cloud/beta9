package types

const (
	MachineStatusRegistered MachineStatus = "registered"
	MachineStatusPending    MachineStatus = "pending"
	MachineStatusAvailable  MachineStatus = "available"
	MachineStatusDisabled   MachineStatus = "disabled"
)

type MachineStatus string
