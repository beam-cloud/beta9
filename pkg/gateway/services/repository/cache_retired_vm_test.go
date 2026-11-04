package repository_services

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/stretchr/testify/require"
)

type retiredStubBackend struct {
	repository.BackendRepository
	keys  []string
	err   error
	calls int
}

func (b *retiredStubBackend) ListRetiredVMCacheStubKeys(_ context.Context, keys []string) ([]string, error) {
	b.calls++
	return b.keys, b.err
}

func TestFilterRetiredVMCacheStubsKeepsOrderAndFailsConservatively(t *testing.T) {
	stubs := []cache.RecentStub{{WorkspaceID: "ws", StubID: "live"}, {WorkspaceID: "ws", StubID: "retired"}, {WorkspaceID: "other", StubID: "retired"}}
	backend := &retiredStubBackend{keys: []string{"ws|retired"}}
	s := &WorkerRepositoryService{backendRepo: backend}
	require.Equal(t, stubs, s.filterRetiredVMCacheStubs(context.Background(), stubs))
	require.Zero(t, backend.calls, "classification is opt-in during staging validation")
	s.appConfig.Cache.Reconciliation.FilterRetiredVMStubs = true
	require.Equal(t, []cache.RecentStub{stubs[0], stubs[2]}, s.filterRetiredVMCacheStubs(context.Background(), stubs))
	require.Equal(t, 1, backend.calls, "all stubs are classified in one database call")
	backend.err = errors.New("database unavailable")
	require.Equal(t, stubs, s.filterRetiredVMCacheStubs(context.Background(), stubs), "a failed lookup must not shrink the protection set")
}
