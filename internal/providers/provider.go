package providers

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
)

const (
	instanceComputeBufferPercent float64 = 10.0
	k3sVersion                   string  = "v1.28.5+k3s1"
)

type Provider interface {
	ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error)
	TerminateMachine(ctx context.Context, poolName, machineId string) error
	Reconcile(ctx context.Context, poolName string)
}

func machineId() string {
	return uuid.New().String()[:8]
}

type userDataConfig struct {
	AuthKey           string
	ControlURL        string
	GatewayHost       string
	Beta9Token        string
	K3sVersion        string
	DisableComponents []string
	MachineId         string
	PoolName          string
}

func populateUserData(config userDataConfig, userDataTemplate string) (string, error) {
	t, err := template.New("userdata").Parse(userDataTemplate)
	if err != nil {
		return "", fmt.Errorf("error parsing user data template: %w", err)
	}

	var populatedTemplate bytes.Buffer
	if err := t.Execute(&populatedTemplate, config); err != nil {
		return "", fmt.Errorf("error executing user data template: %w", err)
	}

	return populatedTemplate.String(), nil
}
