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
	"k8s.io/apimachinery/pkg/api/resource"
)

type failingProviderRepo struct {
	repository.ProviderRepository
}

func (failingProviderRepo) ListAllMachines(string, string, bool) ([]*types.ProviderMachine, error) {
	return nil, errors.New("list machines failed")
}

type namedProvider struct {
	providers.Provider
}

func (namedProvider) GetName() string {
	return "test-provider"
}

func TestProviderMachineGPUCompatible(t *testing.T) {
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
			if got := providerMachineGPUCompatible(test.machineGPU, test.requestedGPU, test.requestedCount); got != test.want {
				t.Fatalf("providerMachineGPUCompatible() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestProviderWorkerCapsAdvertisedCPUWithoutReservingItAll(t *testing.T) {
	resources := providerWorkerResources(2000, 16000, "", 0)
	if got := resources.Limits.Cpu(); got == nil || got.Cmp(resource.MustParse("16")) != 0 {
		t.Fatalf("CPU limit = %v, want 16", got)
	}
	if got := resources.Requests.Cpu(); got == nil || got.Cmp(resource.MustParse("2")) != 0 {
		t.Fatalf("CPU request = %v, want 2", got)
	}
}

func TestProviderWorkerCPURequestNeverExceedsItsLimit(t *testing.T) {
	resources := providerWorkerResources(2000, 500, "", 0)
	if got := resources.Limits.Cpu(); got == nil || got.Cmp(resource.MustParse("500m")) != 0 {
		t.Fatalf("CPU limit = %v, want 500m", got)
	}
	if got := resources.Requests.Cpu(); got == nil || got.Cmp(resource.MustParse("500m")) != 0 {
		t.Fatalf("CPU request = %v, want 500m", got)
	}
}

func TestProviderWorkerCPUCapPreservesGPUResources(t *testing.T) {
	resources := providerWorkerResources(2000, 16000, "RTX4090", 2)
	wantGPU := resource.MustParse("2")
	if got := resources.Requests["nvidia.com/gpu"]; got.Cmp(wantGPU) != 0 {
		t.Fatalf("GPU request = %v, want 2", got)
	}
	if got := resources.Limits["nvidia.com/gpu"]; got.Cmp(wantGPU) != 0 {
		t.Fatalf("GPU limit = %v, want 2", got)
	}
}

func TestProviderCleanupReleasesLockWhenMachineListingFails(t *testing.T) {
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
	controller := &ProviderWorkerPoolController{
		ctx:            context.Background(),
		name:           "provider-pool",
		provider:       namedProvider{},
		providerRepo:   failingProviderRepo{},
		workerPoolRepo: poolRepo,
	}
	controller.cleanupWorkers(&WorkerResourceCleaner{})

	if err := poolRepo.SetWorkerCleanerLock(controller.name); err != nil {
		t.Fatalf("cleaner lock remained held after list failure: %v", err)
	}
	_ = poolRepo.RemoveWorkerCleanerLock(controller.name)
}
