package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type failingLegacyProviderRepo struct {
	repository.ProviderRepository
}

func (failingLegacyProviderRepo) ListAllMachines(string, string, bool) ([]*types.ProviderMachine, error) {
	return nil, errors.New("list machines failed")
}

type namedLegacyProvider struct {
	providers.Provider
}

func (namedLegacyProvider) GetName() string {
	return "test-provider"
}

func TestLegacyMachineGPUCompatible(t *testing.T) {
	tests := []struct {
		name           string
		machineGPU     string
		requestedGPU   string
		requestedCount uint32
		want           bool
	}{
		{name: "zero GPU request", machineGPU: "A10G", requestedGPU: "H100", want: true},
		{name: "empty type", machineGPU: "A10G", requestedCount: 1, want: true},
		{name: "any type", machineGPU: "A10G", requestedGPU: "any", requestedCount: 1, want: true},
		{name: "matching type", machineGPU: "A10G", requestedGPU: "a10g", requestedCount: 1, want: true},
		{name: "different type", machineGPU: "A10G", requestedGPU: "H100", requestedCount: 1, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := legacyMachineGPUCompatible(test.machineGPU, test.requestedGPU, test.requestedCount); got != test.want {
				t.Fatalf("legacyMachineGPUCompatible() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestLegacyCleanupReleasesLockWhenMachineListingFails(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(redisServer.Close)

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{redisServer.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		t.Fatal(err)
	}

	poolRepo := repository.NewWorkerPoolRedisRepository(redisClient)
	controller := &LegacyExternalWorkerPoolController{
		ctx:            context.Background(),
		name:           "legacy-pool",
		provider:       namedLegacyProvider{},
		providerRepo:   failingLegacyProviderRepo{},
		workerPoolRepo: poolRepo,
	}
	controller.cleanupWorkers(&WorkerResourceCleaner{})

	if err := poolRepo.SetWorkerCleanerLock(controller.name); err != nil {
		t.Fatalf("cleaner lock remained held after list failure: %v", err)
	}
	_ = poolRepo.RemoveWorkerCleanerLock(controller.name)
}
