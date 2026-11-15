package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// fakeMirrors holds the mirrors of workspace 7 by name, each a list of
// generations with the latest last.
type fakeMirrors struct {
	mirrors map[string][]types.VMMirror
	down    bool
}

func (m *fakeMirrors) commit(name string) types.VMMirror {
	generations := m.mirrors[name]
	id := uuid.NewString()
	if len(generations) > 0 {
		id = generations[0].ID
	}
	pin := types.VMMirror{Name: name, ID: id, Generation: int64(len(generations) + 1), ManifestDigest: fmt.Sprintf("%064x", len(generations)+1), ManifestSize: 100}
	m.mirrors[name] = append(generations, pin)
	return pin
}

func (m *fakeMirrors) Resolve(_ context.Context, workspaceID uint, requested *types.VMMirror) (*types.VMMirror, error) {
	generations := m.mirrors[requested.Name]
	if workspaceID != 7 || len(generations) == 0 {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "mirror has no generation yet; run `beam setup mirror`")
	}
	pin := generations[len(generations)-1]
	if requested.Generation != 0 {
		if requested.Generation > int64(len(generations)) {
			return nil, echo.NewHTTPError(http.StatusBadRequest, "no such generation")
		}
		pin = generations[requested.Generation-1]
	}
	if requested.ID != "" && requested.ID != pin.ID || requested.ManifestDigest != "" && requested.ManifestDigest != pin.ManifestDigest {
		return nil, echo.NewHTTPError(http.StatusConflict, "mirror was purged")
	}
	return &pin, nil
}

func (m *fakeMirrors) Active(_ context.Context, workspaceID uint, pin *types.VMMirror) (bool, error) {
	if m.down {
		return false, fmt.Errorf("database unavailable")
	}
	generations := m.mirrors[pin.Name]
	return workspaceID == 7 && len(generations) > 0 && generations[0].ID == pin.ID, nil
}

func createVMRequest(t *testing.T, s *Service, info *auth.AuthInfo, body string) (int, types.VM, string) {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	c := &auth.HttpAuthContext{Context: echo.New().NewContext(req, rec), AuthInfo: info}
	if err := s.create(c); err != nil {
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok, err)
		return httpErr.Code, types.VM{}, fmt.Sprint(httpErr.Message)
	}
	var v types.VM
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v))
	return rec.Code, v, ""
}

func TestNewVMPinsLatestMirrorGeneration(t *testing.T) {
	s, _, info, runtime, _ := fixture()
	mirrors := &fakeMirrors{mirrors: map[string][]types.VMMirror{}}
	s.mirrors = mirrors

	code, _, message := createVMRequest(t, s, info, `{"name":"early","spec":{"image_id":"base","mirror":{"name":"laptop"}}}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, message, "beam setup mirror")

	mirrors.commit("laptop")
	second := mirrors.commit("laptop")
	requestID := uuid.NewString()
	body := fmt.Sprintf(`{"name":"dev2","request_id":%q,"spec":{"image_id":"base","mirror":{"name":"laptop"}}}`, requestID)
	code, created, _ := createVMRequest(t, s, info, body)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, &second, created.Spec.Mirror)
	require.Equal(t, &second, runtime.launched[len(runtime.launched)-1].Mirror, "the worker is told which generation to mount")
	stored, err := s.repo.GetVM(context.Background(), 7, created.ID)
	require.NoError(t, err)
	require.Equal(t, &second, stored.Spec.Mirror)

	// The pin holds: a retry of the same request returns the same VM on
	// the same generation even though the mirror has moved on.
	third := mirrors.commit("laptop")
	code, retried, _ := createVMRequest(t, s, info, body)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, created.ID, retried.ID)
	require.Equal(t, &second, retried.Spec.Mirror)

	// A new VM gets the new generation, or an older one it asks for.
	code, newer, _ := createVMRequest(t, s, info, `{"name":"dev3","spec":{"image_id":"base","mirror":{"name":"laptop"}}}`)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, &third, newer.Spec.Mirror)
	code, older, _ := createVMRequest(t, s, info, `{"name":"dev4","spec":{"image_id":"base","mirror":{"name":"laptop","generation":1}}}`)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, int64(1), older.Spec.Mirror.Generation)

	// Nothing but the name and generation is taken from the caller.
	forged := fmt.Sprintf(`{"name":"dev5","spec":{"image_id":"base","mirror":{"name":"laptop","id":%q,"manifest_digest":"%064x","manifest_size":5}}}`, uuid.NewString(), 99)
	code, sanitized, _ := createVMRequest(t, s, info, forged)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, &third, sanitized.Spec.Mirror)

	for _, invalid := range []string{`{"name":""}`, `{"name":"../x"}`, `{"name":"laptop","generation":-1}`, `{"name":"laptop","generation":9}`, `{"name":"absent"}`, `{"name":"laptop","unknown":1}`} {
		code, _, _ = createVMRequest(t, s, info, `{"spec":{"image_id":"base","mirror":`+invalid+`}}`)
		require.Equal(t, http.StatusBadRequest, code, invalid)
	}
	other := &auth.AuthInfo{Workspace: &types.Workspace{Id: 8, ExternalId: uuid.NewString()}, Token: info.Token}
	code, _, _ = createVMRequest(t, s, other, `{"spec":{"image_id":"base","mirror":{"name":"laptop"}}}`)
	require.Equal(t, http.StatusBadRequest, code, "another workspace's mirror")

	s.mirrors = nil
	code, _, message = createVMRequest(t, s, info, `{"spec":{"image_id":"base","mirror":{"name":"laptop"}}}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, message, "not available")
}

func TestSeededVMsKeepTheirMirrorGeneration(t *testing.T) {
	s, v, info, _, _ := fixture()
	mirrors := &fakeMirrors{mirrors: map[string][]types.VMMirror{}}
	s.mirrors = mirrors
	first := mirrors.commit("laptop")
	mirrors.commit("laptop")
	mirrors.commit("other")
	store := s.repo.(*vmStore)
	base := types.VMSpec{ImageID: "image", CPU: 2000, Memory: 2048, DiskSize: "50GiB"}
	pinned := base
	pinned.Mirror = &first
	store.artifacts = []types.VMArtifact{
		{ID: uuid.NewString(), Name: "pinned", Kind: "template", RootSnapshotID: "root-a", Spec: pinned},
		{ID: uuid.NewString(), Name: "plain", Kind: "template", RootSnapshotID: "root-b", Spec: base},
		{ID: uuid.NewString(), Name: "saved", Kind: "snapshot", RootSnapshotID: "root-c", Spec: pinned},
	}

	// The root disk was written over generation 1, so that is what the new
	// VM reads under it, whatever the latest is.
	for _, body := range []string{
		`{"template":"pinned"}`,
		`{"snapshot":"saved"}`,
		`{"template":"pinned","spec":{"mirror":{"name":"laptop"}}}`,
		`{"template":"pinned","spec":{"mirror":{"name":"laptop","generation":1}}}`,
		`{"template":"pinned","spec":{"mirror":null}}`,
	} {
		code, child, message := createVMRequest(t, s, info, body)
		require.Equal(t, http.StatusCreated, code, body+": "+message)
		require.Equal(t, &first, child.Spec.Mirror, body)
	}
	for _, body := range []string{
		`{"template":"pinned","spec":{"mirror":{"name":"other"}}}`,
		`{"template":"pinned","spec":{"mirror":{"name":"laptop","generation":2}}}`,
		`{"snapshot":"saved","spec":{"mirror":{"name":"other"}}}`,
	} {
		code, _, message := createVMRequest(t, s, info, body)
		require.Equal(t, http.StatusBadRequest, code, body)
		require.Contains(t, message, "keeps that")
	}

	// A seed without a mirror can take one.
	code, child, _ := createVMRequest(t, s, info, `{"template":"plain","spec":{"mirror":{"name":"laptop"}}}`)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, int64(2), child.Spec.Mirror.Generation)
	code, child, _ = createVMRequest(t, s, info, `{"template":"plain"}`)
	require.Equal(t, http.StatusCreated, code)
	require.Nil(t, child.Spec.Mirror)

	// Snapshots, templates and forks of a pinned VM carry its pin; a
	// published template does not.
	v.Spec.Mirror = &first
	v.RootSnapshotID = "root-live"
	require.Equal(t, &first, artifact(v, uuid.NewString(), "snapshot", "s", "").Spec.Mirror)
	require.Nil(t, artifact(v, uuid.NewString(), "template", "t", "").Spec.PublicTemplate().Mirror)
	fork, err := s.createVM(context.Background(), info, types.VMCreateOptions{Name: "fork", Spec: v.Spec, PreserveDesktopTransport: true})
	require.NoError(t, err)
	require.Equal(t, &first, fork.Spec.Mirror)

	// A pin cannot be changed afterwards.
	req := httptest.NewRequest("PATCH", "/", strings.NewReader(`{"mirror":{"name":"other"}}`))
	c := &auth.HttpAuthContext{Context: echo.New().NewContext(req, httptest.NewRecorder()), AuthInfo: info}
	c.SetParamNames("name")
	c.SetParamValues(v.Name)
	err = s.update(c)
	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, err.(*echo.HTTPError).Code)
}

// A published template shares the whole root disk, which holds every
// mirrored file the VM changed. Private artifacts keep the pin.
func TestMirroredVMsCannotBePublished(t *testing.T) {
	s, v, info, _, _ := fixture()
	mirrors := &fakeMirrors{mirrors: map[string][]types.VMMirror{}}
	s.mirrors = mirrors
	pin := mirrors.commit("laptop")
	pin.Home = "/Users/luke"
	v.Spec.Mirror = &pin
	ctx := context.Background()

	_, err := s.capture(ctx, v, types.VMArtifactTemplate, "shared", "", true)
	require.ErrorContains(t, err, "publish from a VM that never had a mirror")
	response := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId+"/"+v.Name+"/template", `{"name":"shared","public":true}`)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Empty(t, s.repo.(*vmStore).artifacts)
	for _, kind := range []string{types.VMArtifactTemplate, types.VMArtifactSnapshot} {
		a, err := s.capture(ctx, v, kind, "private-"+kind, "", false)
		require.NoError(t, err)
		require.Equal(t, &pin, a.Spec.Mirror)
	}

	v.Spec.Mirror = nil
	a, err := s.capture(ctx, v, types.VMArtifactTemplate, "clean", "", true)
	require.NoError(t, err)
	require.True(t, a.Public)

	// Where the tree goes is the gateway's to say, not the caller's.
	require.Equal(t, &types.VMMirror{Name: "laptop", Generation: 1}, requestedMirror(&types.VMMirror{Name: "laptop", Generation: 1, ID: pin.ID, Home: "/Users/someone"}))
}

func TestPurgedMirrorLeavesVMsRunningWithoutTheLayer(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	mirrors := &fakeMirrors{mirrors: map[string][]types.VMMirror{}}
	s.mirrors = mirrors
	pin := mirrors.commit("laptop")
	v.Spec.Mirror = &pin
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	restart := func() types.VMSpec {
		delete(runtime.containers.states, v.ContainerID)
		v.ContainerID = ""
		require.NoError(t, s.start(ctx, info, v))
		return runtime.launched[len(runtime.launched)-1]
	}
	require.Equal(t, &pin, restart().Mirror)

	// An outage is not a purge: the VM does not start without its files.
	mirrors.down = true
	delete(runtime.containers.states, v.ContainerID)
	v.ContainerID = ""
	launches := len(runtime.launched)
	require.ErrorContains(t, s.start(ctx, info, v), "check mirror laptop")
	require.Len(t, runtime.launched, launches)
	require.Empty(t, v.ContainerID)
	mirrors.down = false

	// Purged, and the name reused by a new mirror: the old pin is dead.
	delete(mirrors.mirrors, "laptop")
	mirrors.commit("laptop")
	require.Nil(t, restart().Mirror)
	require.Equal(t, &pin, v.Spec.Mirror, "the VM still records what it was created with")
	fork, err := s.createVM(ctx, info, types.VMCreateOptions{Name: "fork", Spec: v.Spec, PreserveDesktopTransport: true})
	require.NoError(t, err)
	require.Equal(t, &pin, fork.Spec.Mirror)
	require.Nil(t, runtime.launched[len(runtime.launched)-1].Mirror)

	store := s.repo.(*vmStore)
	store.artifacts = []types.VMArtifact{{ID: uuid.NewString(), Name: "old", Kind: "template", RootSnapshotID: "root", Spec: v.Spec}}
	code, child, _ := createVMRequest(t, s, info, `{"template":"old"}`)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, &pin, child.Spec.Mirror)
	require.Nil(t, runtime.launched[len(runtime.launched)-1].Mirror)
}
