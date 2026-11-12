package apiv1

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

const listWorkspaceID = "workspace-1"

type listContainerRepo struct {
	repository.ContainerRepository
	mu     sync.Mutex
	states []types.ContainerState
	err    error
	reads  int
	// reading, when set, receives each read as it starts, and the read then
	// waits for release.
	reading chan struct{}
	release chan struct{}
}

func (r *listContainerRepo) GetActiveContainersByWorkspaceId(_ string) ([]types.ContainerState, error) {
	r.mu.Lock()
	r.reads++
	states, err, reading, release := r.states, r.err, r.reading, r.release
	r.mu.Unlock()
	if reading != nil {
		reading <- struct{}{}
		<-release
	}
	return states, err
}

func (r *listContainerRepo) set(change func(*listContainerRepo)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	change(r)
}

func (r *listContainerRepo) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

type listBackendRepo struct {
	repository.BackendRepository
}

func (listBackendRepo) ListAppIDsByStubExternalIDs(_ context.Context, _ string, stubIDs []string) (map[string]string, error) {
	apps := make(map[string]string, len(stubIDs))
	for _, stubID := range stubIDs {
		apps[stubID] = "app-of-" + stubID
	}
	return apps, nil
}

// runningContainers is a workspace's containers as they are while one app
// runs many of them.
func runningContainers(count int) []types.ContainerState {
	states := make([]types.ContainerState, count)
	for i := range states {
		states[i] = types.ContainerState{
			ContainerId: fmt.Sprintf("sandbox-0d0c6a5e-6f0b-4f65-9a0c-0d6f6b1f7c11-%08x", i*2654435761),
			StubId:      "0d0c6a5e-6f0b-4f65-9a0c-0d6f6b1f7c11",
			WorkspaceId: listWorkspaceID,
			Status:      types.ContainerStatusRunning,
			ScheduledAt: 1767323045 + int64(i/500),
			StartedAt:   1767323046 + int64(i/500),
			Cpu:         1000,
			Memory:      1024,
			WorkerId:    fmt.Sprintf("%08x", (i/2000)*40503),
			MachineId:   fmt.Sprintf("%08x", (i/2000)*2246822519),
		}
	}
	return states
}

func newListGroup(states []types.ContainerState) (*ContainerGroup, *listContainerRepo) {
	containers := &listContainerRepo{states: states}
	return &ContainerGroup{containerRepo: containers, backendRepo: listBackendRepo{}}, containers
}

func listContainers(group *ContainerGroup, request *http.Request) (*httptest.ResponseRecorder, error) {
	recorder := httptest.NewRecorder()
	ctx := echo.New().NewContext(request, recorder)
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues(listWorkspaceID)
	return recorder, group.ListContainersByWorkspaceId(ctx)
}

func listRequest() *http.Request {
	return httptest.NewRequest(http.MethodGet, "/"+listWorkspaceID, nil)
}

// expireSharedList makes the workspace's shared list as old as one that has
// been served for its whole life.
func expireSharedList(t *testing.T, group *ContainerGroup) {
	t.Helper()
	group.lists.mu.Lock()
	defer group.lists.mu.Unlock()
	entry := group.lists.large[listWorkspaceID]
	require.NotNil(t, entry, "the workspace shares a list")
	entry.freshUntil = time.Now().Add(-time.Millisecond)
}

func TestListContainersReadsASmallWorkspaceForEveryRequest(t *testing.T) {
	states := runningContainers(3)
	group, containers := newListGroup(states)

	for request := 1; request <= 3; request++ {
		recorder, err := listContainers(group, listRequest())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, request, containers.readCount())
		require.Empty(t, recorder.Header().Get(echo.HeaderContentEncoding))

		var listed []ContainerStateWithAppId
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &listed))
		require.Len(t, listed, len(states))
		for i, container := range listed {
			require.Equal(t, states[i], container.ContainerState)
			require.Equal(t, "app-of-"+states[i].StubId, container.AppId)
		}
	}

	// A container that starts is in the very next list.
	containers.set(func(r *listContainerRepo) { r.states = runningContainers(4) })
	recorder, err := listContainers(group, listRequest())
	require.NoError(t, err)
	var listed []ContainerStateWithAppId
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &listed))
	require.Len(t, listed, 4)
}

func TestListContainersCompressesForClientsThatAcceptIt(t *testing.T) {
	group, _ := newListGroup(runningContainers(10000))

	plain, err := listContainers(group, listRequest())
	require.NoError(t, err)
	require.Empty(t, plain.Header().Get(echo.HeaderContentEncoding))
	require.Contains(t, plain.Header().Get(echo.HeaderContentType), echo.MIMEApplicationJSON)
	require.Equal(t, echo.HeaderAcceptEncoding, plain.Header().Get(echo.HeaderVary))

	request := listRequest()
	request.Header.Set(echo.HeaderAcceptEncoding, "gzip, deflate, br")
	compressed, err := listContainers(group, request)
	require.NoError(t, err)
	require.Equal(t, "gzip", compressed.Header().Get(echo.HeaderContentEncoding))
	require.Contains(t, compressed.Header().Get(echo.HeaderContentType), echo.MIMEApplicationJSON)
	require.Equal(t, echo.HeaderAcceptEncoding, compressed.Header().Get(echo.HeaderVary))

	sent := compressed.Body.Len()
	reader, err := gzip.NewReader(compressed.Body)
	require.NoError(t, err)
	inflated, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, plain.Body.String(), string(inflated))

	t.Logf("10,000 containers: %d bytes, %d compressed", len(inflated), sent)
	require.Positive(t, sent)
	require.Less(t, sent*5, len(inflated))

	// An HTTP client that asks for compression by default reads the same list.
	e := echo.New()
	e.GET("/:workspaceId", group.ListContainersByWorkspaceId)
	server := httptest.NewServer(e)
	defer server.Close()
	response, err := http.Get(server.URL + "/" + listWorkspaceID)
	require.NoError(t, err)
	defer response.Body.Close()
	received, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.True(t, response.Uncompressed, "the client inflated a compressed response")
	require.Equal(t, plain.Body.String(), string(received))
}

func TestListContainersSharesOneReadOfALargeWorkspace(t *testing.T) {
	group, containers := newListGroup(runningContainers(sharedContainerListMinContainers))

	first, err := listContainers(group, listRequest())
	require.NoError(t, err)
	require.Equal(t, 1, containers.readCount())

	// While the list is fresh, nothing is read again.
	for range 20 {
		recorder, err := listContainers(group, listRequest())
		require.NoError(t, err)
		require.Equal(t, first.Body.String(), recorder.Body.String())
	}
	require.Equal(t, 1, containers.readCount())

	// Once it is not, requests that arrive together wait on one read.
	expireSharedList(t, group)
	reading, release := make(chan struct{}, 64), make(chan struct{})
	containers.set(func(r *listContainerRepo) {
		r.states = runningContainers(sharedContainerListMinContainers + 500)
		r.reading, r.release = reading, release
	})
	const together = 20
	bodies := make(chan string, together)
	for range together {
		go func() {
			recorder, err := listContainers(group, listRequest())
			if err != nil {
				bodies <- err.Error()
				return
			}
			bodies <- recorder.Body.String()
		}()
	}
	<-reading
	close(release)
	var shared string
	for i := range together {
		body := <-bodies
		if i == 0 {
			shared = body
		}
		require.Equal(t, shared, body)
	}
	require.Equal(t, 2, containers.readCount())
	var listed []ContainerStateWithAppId
	require.NoError(t, json.Unmarshal([]byte(shared), &listed))
	require.Len(t, listed, sharedContainerListMinContainers+500)
	containers.set(func(r *listContainerRepo) { r.reading, r.release = nil, nil })

	// After a container is stopped, the next request reads again.
	group.lists.forget(listWorkspaceID)
	_, err = listContainers(group, listRequest())
	require.NoError(t, err)
	require.Equal(t, 3, containers.readCount())

	// A workspace that is small again is read for every request again.
	containers.set(func(r *listContainerRepo) { r.states = runningContainers(10) })
	expireSharedList(t, group)
	for request := 1; request <= 3; request++ {
		recorder, err := listContainers(group, listRequest())
		require.NoError(t, err)
		require.Equal(t, 3+request, containers.readCount())
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &listed))
		require.Len(t, listed, 10)
	}
}

func TestListContainersDoesNotKeepAFailedRead(t *testing.T) {
	group, containers := newListGroup(runningContainers(sharedContainerListMinContainers))
	_, err := listContainers(group, listRequest())
	require.NoError(t, err)

	expireSharedList(t, group)
	containers.set(func(r *listContainerRepo) { r.err = errors.New("store went away") })
	_, err = listContainers(group, listRequest())
	var httpError *echo.HTTPError
	require.ErrorAs(t, err, &httpError)
	require.Equal(t, http.StatusInternalServerError, httpError.Code)

	containers.set(func(r *listContainerRepo) { r.err = nil })
	recorder, err := listContainers(group, listRequest())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, 3, containers.readCount())
}

func TestListContainersStopsWaitingWhenTheClientGoesAway(t *testing.T) {
	group, containers := newListGroup(runningContainers(sharedContainerListMinContainers))
	_, err := listContainers(group, listRequest())
	require.NoError(t, err)

	expireSharedList(t, group)
	reading, release := make(chan struct{}, 1), make(chan struct{})
	containers.set(func(r *listContainerRepo) { r.reading, r.release = reading, release })

	requestCtx, cancel := context.WithCancel(context.Background())
	gone := make(chan error, 1)
	go func() {
		_, err := listContainers(group, listRequest().WithContext(requestCtx))
		gone <- err
	}()
	<-reading
	cancel()
	require.ErrorIs(t, <-gone, context.Canceled)

	// The read it started still finishes, for the requests that follow.
	waiting := make(chan error, 1)
	go func() {
		_, err := listContainers(group, listRequest())
		waiting <- err
	}()
	close(release)
	require.NoError(t, <-waiting)
	require.Equal(t, 2, containers.readCount())
}
