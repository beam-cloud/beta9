package image

import (
	"context"
	"errors"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type platformBuildBackend struct {
	repository.BackendRepository
	t    *testing.T
	fail string
}

func (r *platformBuildBackend) GetObjectByHash(_ context.Context, hash string, workspace uint) (*types.Object, error) {
	require.Equal(r.t, abstractions.EmptyStubObjectHash(), hash)
	require.Equal(r.t, uint(42), workspace)
	if r.fail == "object" {
		return nil, errors.New("object unavailable")
	}
	return &types.Object{Id: 3}, nil
}
func (r *platformBuildBackend) GetOrCreateApp(_ context.Context, workspace uint, name string) (*types.App, error) {
	require.Equal(r.t, uint(42), workspace)
	require.Equal(r.t, "managed-endpoints-build", name)
	if r.fail == "app" {
		return nil, errors.New("app unavailable")
	}
	return &types.App{Id: 4}, nil
}
func (r *platformBuildBackend) GetOrCreateStub(_ context.Context, name, kind string, config types.StubConfigV1, object, workspace uint, force bool, app uint) (types.Stub, error) {
	require.Equal(r.t, types.StubTypePlatformDeployer, kind)
	require.Equal(r.t, uint(42), workspace)
	require.Equal(r.t, uint(3), object)
	require.Equal(r.t, uint(4), app)
	require.False(r.t, force)
	if r.fail == "stub" {
		return types.Stub{}, errors.New("stub unavailable")
	}
	return types.Stub{ExternalId: "platform-build-stub", Type: types.StubType(kind)}, nil
}
func TestPlatformBuildPersistsEnforcementIdentityWithoutChangingBuildContext(t *testing.T) {
	b := &Builder{backendRepo: &platformBuildBackend{t: t}}
	request := &types.ContainerRequest{Workspace: types.Workspace{Id: 42}, Stub: types.StubWithRelated{
		Stub: types.Stub{Type: types.StubType(types.StubTypePlatformDeployer)}, Object: types.Object{ExternalId: "user-build-context"},
	}}
	require.NoError(t, b.attachPlatformBuildStub(context.Background(), request))
	require.Equal(t, "platform-build-stub", request.StubId)
	require.Equal(t, request.StubId, request.Stub.ExternalId)
	require.True(t, request.Stub.Type.IsPlatformWorkload())
	require.Equal(t, "user-build-context", request.Stub.Object.ExternalId)
}
func TestPlatformBuildIdentityFailureStopsAdmission(t *testing.T) {
	for _, failure := range []string{"object", "app", "stub"} {
		t.Run(failure, func(t *testing.T) {
			b := &Builder{backendRepo: &platformBuildBackend{t: t, fail: failure}}
			request := &types.ContainerRequest{Workspace: types.Workspace{Id: 42}, Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePlatformDeployer)}}}
			require.Error(t, b.attachPlatformBuildStub(context.Background(), request))
			require.Empty(t, request.StubId)
		})
	}
}
func TestOrdinaryBuildDoesNotResolveBillingIdentity(t *testing.T) {
	b := &Builder{}
	request := &types.ContainerRequest{}
	require.NoError(t, b.attachPlatformBuildStub(context.Background(), request))
	require.Empty(t, request.StubId)
}
