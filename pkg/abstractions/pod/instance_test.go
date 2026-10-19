package pod

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type blockedInstanceRepository struct {
	repository.BackendRepository
	started chan struct{}
	release chan struct{}
}

func (r *blockedInstanceRepository) GetStubByExternalId(_ context.Context, id string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	if id == "slow" {
		close(r.started)
		<-r.release
	}
	return nil, errors.New("stub unavailable")
}

func TestSlowInstanceLookupDoesNotBlockOtherVMsOrCleanup(t *testing.T) {
	repo := &blockedInstanceRepository{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(repo.release) })
	service := &GenericPodService{ctx: context.Background(), backendRepo: repo, podInstances: common.NewSafeMap[*podInstance]()}
	slow := make(chan error, 1)
	go func() { _, err := service.getOrCreatePodInstance("slow"); slow <- err }()
	<-repo.started

	fast := make(chan error, 1)
	go func() { _, err := service.getOrCreatePodInstance("other"); fast <- err }()
	select {
	case err := <-fast:
		require.ErrorContains(t, err, "invalid stub id")
	case <-time.After(time.Second):
		t.Fatal("a different VM waited behind the slow database lookup")
	}
	instance := &podInstance{}
	service.podInstances.Set("finished", instance)
	cleaned := make(chan struct{})
	go func() { service.deletePodInstance("finished", instance); close(cleaned) }()
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("instance cleanup waited behind the slow database lookup")
	}
}
