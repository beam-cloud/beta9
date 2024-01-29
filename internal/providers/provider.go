package providers

type Provider interface {
	ListMachines() error
	TerminateMachine(id string) error
	ProvisionMachine() error
}
