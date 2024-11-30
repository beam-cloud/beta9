package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type LambdaLabsProvider struct {
	*ExternalProvider
}

const (
	lambdaApiBaseUrl string = "https://cloud.lambdalabs.com/api/v1"
)

func NewLambdaLabsProvider(ctx context.Context, appConfig types.AppConfig, providerRepo repository.ProviderRepository, workerRepo repository.WorkerRepository, tailscale *network.Tailscale) (*LambdaLabsProvider, error) {
	lambdaLabsProvider := &LambdaLabsProvider{}

	baseProvider := NewExternalProvider(ctx, &ExternalProviderConfig{
		Name:                 string(types.ProviderLambdaLabs),
		ClusterName:          appConfig.ClusterName,
		AppConfig:            appConfig,
		TailScale:            tailscale,
		ProviderRepo:         providerRepo,
		WorkerRepo:           workerRepo,
		ListMachinesFunc:     lambdaLabsProvider.listMachines,
		TerminateMachineFunc: lambdaLabsProvider.TerminateMachine,
	})
	lambdaLabsProvider.ExternalProvider = baseProvider

	return lambdaLabsProvider, nil
}

func (p *LambdaLabsProvider) ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error) {
	return "", types.NewProviderNotImplemented()
}

func (p *LambdaLabsProvider) listMachines(ctx context.Context, poolName string) (map[string]string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/instances", lambdaApiBaseUrl), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.AppConfig.Providers.LambdaLabs.ApiKey))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lambda api request failed: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data map[string][]map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	// Extract the instance information
	prefix := poolName + "-"
	machines := make(map[string]string)
	for _, item := range data["data"] {
		instanceId, ok := item["id"].(string)
		if !ok {
			continue
		}

		name, ok := item["name"].(string)
		if !ok {
			continue
		}

		// If the instance starts with the pool name, include it
		if strings.HasPrefix(name, prefix) {
			machineId, _ := strings.CutPrefix(name, prefix)
			machines[machineId] = instanceId
		}
	}

	return machines, nil
}

func (p *LambdaLabsProvider) TerminateMachine(ctx context.Context, poolName, instanceId, machineId string) error {
	if instanceId == "" {
		return errors.New("invalid instance ID")
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/instance-operations/terminate", lambdaApiBaseUrl), nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.AppConfig.Providers.LambdaLabs.ApiKey))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("lambda api request failed: %d", resp.StatusCode)
	}

	err = p.ProviderRepo.RemoveMachine(p.Name, poolName, machineId)
	if err != nil {
		log.Error().Str("provider", p.Name).Str("machine_id", machineId).Err(err).Msg("unable to remove machine state")
		return err
	}

	log.Info().Str("provider", p.Name).Str("machine_id", machineId).Msg("terminated machine")
	return nil
}
