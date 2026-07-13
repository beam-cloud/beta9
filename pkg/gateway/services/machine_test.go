package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestDeleteMachineWithoutAuthIsRejected(t *testing.T) {
	response, err := (&GatewayService{}).DeleteMachine(context.Background(), &pb.DeleteMachineRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Ok || response.ErrMsg != "Unauthorized Access" {
		t.Fatalf("DeleteMachine() response = %+v", response)
	}
}

func TestProviderMachineStatusMapsReadyToAvailable(t *testing.T) {
	if got := providerMachineStatus(types.MachineStatusReady); got != types.MachineStatusAvailable {
		t.Fatalf("providerMachineStatus(ready) = %q, want %q", got, types.MachineStatusAvailable)
	}
	if got := providerMachineStatus(types.MachineStatusPending); got != types.MachineStatusPending {
		t.Fatalf("providerMachineStatus(pending) = %q, want %q", got, types.MachineStatusPending)
	}
}
