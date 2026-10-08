package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// RunVM reuses sandbox scheduling while preserving the VM service's reserved
// runtime identity. Never allow a VM to fall back to a container runtime.
func (s *GenericPodService) RunVM(ctx context.Context, info *auth.AuthInfo, stubID, containerID string, ports []uint32) error {
	stub, err := s.backendRepo.GetStubByExternalId(ctx, stubID)
	if err != nil {
		return err
	}
	if stub == nil {
		return fmt.Errorf("VM sandbox stub not found")
	}
	var spec types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &spec); err != nil {
		return err
	}
	if !spec.UseVM || spec.RequiresGPU() || stub.Type != types.StubType(types.StubTypeSandbox) {
		return fmt.Errorf("persistent VM requires a CPU microvm sandbox")
	}
	spec.Ports = ports
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	stub.Config = string(data)
	_, err = s.run(ctx, info, stub, runOptions{containerId: containerID})
	return err
}

func (s *GenericPodService) ForwardVM(ctx echo.Context, stubID, containerID string) error {
	return s.forwardContainerRequest(ctx, stubID, containerID)
}
