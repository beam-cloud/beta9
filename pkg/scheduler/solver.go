package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/types"
)

func NewContainerRequestSolver() *ContainerRequestSolver {
	return &ContainerRequestSolver{
		lastSolution: nil,
	}
}

type ContainerRequestSolver struct {
	lastSolution *ContainerRequestSolverSolution
}

type ContainerRequestSolverVariables struct {
	Requests []types.ContainerRequest
	Machines []types.ProviderMachine // TODO: need to figure out a way to represent internal machines as "Machines" in our system with instance ID and price
	// to do that we need to have a daemonset that runs on each machine which basically connects to our cluster and creates a new machine with the correct state (as well as TTLs it)
	// additionally, we should be able to resize workers dynamically so they occupy the entirety of the server that they're currently on
	// also -- should karpenter remain a thing in this world? we really need the output of the solver to drive whether or not we should spin up a machine
}

type ContainerRequestSolverSolution struct {
}

func (s *ContainerRequestSolver) seed(solution *ContainerRequestSolverSolution) error {
	/* TODO:
	- format previous solution into a way that internal solver can understand?


	*/

	return nil
}

func (s *ContainerRequestSolver) Solve(variables *ContainerRequestSolverVariables) error {
	if s.lastSolution != nil {
		if err := s.seed(s.lastSolution); err != nil {
			return err
		}
	}

	/*s
	TODO:
	 - format variables in a way that internal solver can understand
	 - setup constaints
	 - take output and format it in a way system can understand (i.e. which machine IDs to turn off, etc)
	 -
	*/

	return nil
}
