package apiv1

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

const (
	// sharedContainerListMinContainers is the size from which a workspace's
	// container list is built once for everyone who asks for it, instead of
	// once for each request. Building it reads every container's state and
	// resolves their apps, and a view that follows the containers as they
	// change asks for it about once a second, so for a large workspace the
	// work would otherwise grow with the number of views that are open.
	// A smaller workspace is read fresh for every request.
	sharedContainerListMinContainers = 1000
	// sharedContainerListMaxAge is how long a shared list is served before it
	// is built again. A list that took longer than half of this to build is
	// served for twice as long as it took, so building it is never most of
	// what the gateway does for a workspace.
	sharedContainerListMaxAge = time.Second
	// sharedContainerListRetention is how long after its last list a
	// workspace is still taken to be large, and that list is kept.
	sharedContainerListRetention = time.Minute
	// sharedContainerListBuildTimeout bounds a build that several requests
	// wait on, since it outlives the request that started it.
	sharedContainerListBuildTimeout = 30 * time.Second
	// containerListCompressMinBytes is the size from which the list is sent
	// compressed to a client that accepts it. Its entries repeat the same
	// field names and identifiers, so a long list is mostly repetition.
	containerListCompressMinBytes = 4096
)

type ContainerGroup struct {
	routerGroup   *echo.Group
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	scheduler     scheduler.Scheduler
	containerRepo repository.ContainerRepository
	lists         containerListCache
}

// containerList is the response to a request for a workspace's containers.
// One list can be written to many requests.
type containerList struct {
	body       []byte
	containers int

	compress   sync.Once
	compressed []byte
}

// gzipped returns the body compressed, or nil if it is too small to be worth
// compressing. It is compressed once, the first time a client asks for it.
func (l *containerList) gzipped() []byte {
	l.compress.Do(func() {
		if len(l.body) < containerListCompressMinBytes {
			return
		}
		var compressed bytes.Buffer
		writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
		if err != nil {
			return
		}
		if _, err := writer.Write(l.body); err != nil {
			return
		}
		if err := writer.Close(); err != nil {
			return
		}
		l.compressed = compressed.Bytes()
	})
	return l.compressed
}

func (l *containerList) write(ctx echo.Context) error {
	header := ctx.Response().Header()
	header.Add(echo.HeaderVary, echo.HeaderAcceptEncoding)
	if acceptsGzip(ctx.Request().Header.Get(echo.HeaderAcceptEncoding)) {
		if compressed := l.gzipped(); compressed != nil {
			header.Set(echo.HeaderContentEncoding, "gzip")
			return ctx.Blob(http.StatusOK, echo.MIMEApplicationJSON, compressed)
		}
	}
	return ctx.JSONBlob(http.StatusOK, l.body)
}

// acceptsGzip reports whether an Accept-Encoding header allows gzip: it names
// gzip, or failing that "*", with a quality that is not zero.
func acceptsGzip(header string) bool {
	wildcard := false
	for _, entry := range strings.Split(header, ",") {
		coding, params, _ := strings.Cut(entry, ";")
		accepted := true
		if name, quality, ok := strings.Cut(params, "="); ok && strings.EqualFold(strings.TrimSpace(name), "q") {
			q, err := strconv.ParseFloat(strings.TrimSpace(quality), 64)
			accepted = err == nil && q > 0
		}
		switch strings.ToLower(strings.TrimSpace(coding)) {
		case "gzip":
			return accepted
		case "*":
			wildcard = accepted
		}
	}
	return wildcard
}

// containerListCache holds the latest list of each workspace that is large
// enough to share one (see sharedContainerListMinContainers).
type containerListCache struct {
	mu    sync.Mutex
	large map[string]*sharedContainerList
	// builds holds each workspace whose list is being built.
	builds  map[string]*containerListBuilds
	flights singleflight.Group
}

// containerListBuilds is kept for a workspace while its list is being built.
type containerListBuilds struct {
	running int
	// forgotten counts the calls to forget since the first of the builds
	// began. A list being built during one may not show what it was for.
	forgotten uint64
}

type sharedContainerList struct {
	list       *containerList
	freshUntil time.Time
	// expire forgets the workspace once nothing has asked for its list for
	// sharedContainerListRetention.
	expire *time.Timer
}

// get returns the workspace's list: built for this request alone unless the
// workspace is known to be large, in which case a list that is still fresh is
// returned, and one that is not is built once for everyone waiting.
func (c *containerListCache) get(
	ctx context.Context,
	workspaceID string,
	build func(context.Context) (*containerList, error),
) (*containerList, error) {
	list, shared := c.latest(workspaceID)
	if list != nil {
		return list, nil
	}
	if !shared {
		return c.build(ctx, workspaceID, build)
	}

	result := c.flights.DoChan(c.flight(workspaceID), func() (any, error) {
		// A request that found the list stale may get here after another has rebuilt it.
		if list, _ := c.latest(workspaceID); list != nil {
			return list, nil
		}
		// The list is for every request waiting on it, so it is not abandoned
		// when the request that happened to start it goes away.
		buildCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedContainerListBuildTimeout)
		defer cancel()
		return c.build(buildCtx, workspaceID, build)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case built := <-result:
		if built.Err != nil {
			return nil, built.Err
		}
		return built.Val.(*containerList), nil
	}
}

// latest returns the workspace's shared list if it is still fresh, and
// whether the workspace shares one at all.
func (c *containerListCache) latest(workspaceID string) (*containerList, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.large[workspaceID]
	if entry == nil {
		return nil, false
	}
	if time.Now().Before(entry.freshUntil) {
		return entry.list, true
	}
	return nil, true
}

// flight names the build a request made now may wait on: one that began
// after everything forgotten so far.
func (c *containerListCache) flight(workspaceID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	forgotten := uint64(0)
	if builds := c.builds[workspaceID]; builds != nil {
		forgotten = builds.forgotten
	}
	return workspaceID + "/" + strconv.FormatUint(forgotten, 10)
}

// began notes that a build of the workspace's list has started, and returns
// how much had been forgotten by then.
func (c *containerListCache) began(workspaceID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	builds := c.builds[workspaceID]
	if builds == nil {
		if c.builds == nil {
			c.builds = map[string]*containerListBuilds{}
		}
		builds = &containerListBuilds{}
		c.builds[workspaceID] = builds
	}
	builds.running++
	return builds.forgotten
}

// ended notes that a build of the workspace's list is over, and returns how
// much has been forgotten by now. It is called with mu held.
func (c *containerListCache) ended(workspaceID string) uint64 {
	builds := c.builds[workspaceID]
	if builds.running--; builds.running == 0 {
		delete(c.builds, workspaceID)
	}
	return builds.forgotten
}

func (c *containerListCache) build(
	ctx context.Context,
	workspaceID string,
	build func(context.Context) (*containerList, error),
) (*containerList, error) {
	started := time.Now()
	forgotten := c.began(workspaceID)
	list, err := build(ctx)
	took := time.Since(started)

	c.mu.Lock()
	defer c.mu.Unlock()
	// A list read while something was forgotten may not show it. It answers
	// the requests that were waiting for it, and is stale from the start.
	stale := c.ended(workspaceID) != forgotten
	if err != nil {
		return nil, err
	}
	if previous := c.large[workspaceID]; previous != nil {
		previous.expire.Stop()
		delete(c.large, workspaceID)
	}
	if list.containers < sharedContainerListMinContainers {
		return list, nil
	}

	entry := &sharedContainerList{list: list}
	if !stale {
		entry.freshUntil = time.Now().Add(max(sharedContainerListMaxAge, 2*took))
	}
	entry.expire = time.AfterFunc(sharedContainerListRetention, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.large[workspaceID] == entry {
			delete(c.large, workspaceID)
		}
	})
	if c.large == nil {
		c.large = map[string]*sharedContainerList{}
	}
	c.large[workspaceID] = entry
	return list, nil
}

// forget makes the next request to this gateway build the workspace's list
// again, so it sees the result of something that has just been done to its
// containers. A list that is being built at the time is not waited on by the
// requests that follow, and is not kept as fresh.
func (c *containerListCache) forget(workspaceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.large[workspaceID]; entry != nil {
		entry.freshUntil = time.Time{}
	}
	if builds := c.builds[workspaceID]; builds != nil {
		builds.forgotten++
	}
}

func NewContainerGroup(
	g *echo.Group,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	scheduler scheduler.Scheduler,
	config types.AppConfig,
) *ContainerGroup {
	group := &ContainerGroup{routerGroup: g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		scheduler:     scheduler,
		config:        config,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListContainersByWorkspaceId))
	g.GET("/admin/:containerId", auth.WithClusterAdminAuth(group.GetContainerAsAdmin))
	g.GET("/:workspaceId/:containerId", auth.WithWorkspaceAuth(group.GetContainer))
	g.POST("/:workspaceId/:containerId/stop", auth.WithWorkspaceAuth(group.StopContainer))
	g.POST("/:workspaceId/stop-all", auth.WithStrictWorkspaceAuth(group.StopAllWorkspaceContainers))

	return group
}

type ContainerStateWithAppId struct {
	types.ContainerState
	AppId string `json:"app_id"`
}

func (c *ContainerGroup) ListContainersByWorkspaceId(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	list, err := c.lists.get(ctx.Request().Context(), workspaceId, func(ctx context.Context) (*containerList, error) {
		return c.buildContainerList(ctx, workspaceId)
	})
	if err != nil {
		return err
	}
	return list.write(ctx)
}

func (c *ContainerGroup) buildContainerList(ctx context.Context, workspaceId string) (*containerList, error) {
	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to get containers")
	}

	containerStatesWithAppId := make([]ContainerStateWithAppId, len(containerStates))
	stubIDs := make([]string, 0, len(containerStates))
	seenStubIDs := map[string]struct{}{}
	for _, containerState := range containerStates {
		if containerState.StubId == "" {
			continue
		}
		if _, ok := seenStubIDs[containerState.StubId]; ok {
			continue
		}
		seenStubIDs[containerState.StubId] = struct{}{}
		stubIDs = append(stubIDs, containerState.StubId)
	}

	stubIdsToAppIds, err := c.backendRepo.ListAppIDsByStubExternalIDs(ctx, workspaceId, stubIDs)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to get app")
	}

	for i, containerState := range containerStates {
		containerStatesWithAppId[i].ContainerState = containerState
		containerStatesWithAppId[i].AppId = stubIdsToAppIds[containerState.StubId]
	}

	body, err := json.Marshal(containerStatesWithAppId)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to get containers")
	}
	return &containerList{body: body, containers: len(containerStatesWithAppId)}, nil
}

func (c *ContainerGroup) GetContainerAsAdmin(ctx echo.Context) error {
	containerId := ctx.Param("containerId")

	containerState, err := c.containerRepo.GetContainerState(containerId)
	if err != nil {
		return HTTPBadRequest("Invalid container id")
	}

	return ctx.JSON(http.StatusOK, containerState)
}

func (c *ContainerGroup) GetContainer(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerId := ctx.Param("containerId")

	containerState, err := c.containerRepo.GetContainerState(containerId)
	if err != nil {
		return HTTPBadRequest("Invalid container id")
	}

	if containerState.WorkspaceId != workspaceId {
		return HTTPBadRequest("Invalid workspace id")
	}

	return ctx.JSON(http.StatusOK, containerState)
}

func (c *ContainerGroup) StopAllWorkspaceContainers(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return HTTPInternalServerError("Failed to stop containers")
	}

	for _, state := range containerStates {
		err := c.scheduler.Stop(&types.StopContainerArgs{ContainerId: state.ContainerId, Reason: types.StopContainerReasonUser})
		if err != nil {
			log.Error().Str("container_id", state.ContainerId).Err(err).Msg("failed to stop container")
		}
	}
	c.lists.forget(workspaceId)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"message": "all containers stopped",
	})
}

func (c *ContainerGroup) StopContainer(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerId := ctx.Param("containerId")
	force := ctx.QueryParam("force") == "true"

	state, err := c.containerRepo.GetContainerState(containerId)
	if err != nil {
		return HTTPBadRequest("Invalid container id")
	}

	if state.WorkspaceId != workspaceId {
		return HTTPBadRequest("Invalid workspace id")
	}

	err = c.scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Force: force, Reason: types.StopContainerReasonUser})
	if err != nil {
		if strings.Contains(err.Error(), "event already exists") {
			return HTTPConflict("Container is already stopping")
		}
		return HTTPInternalServerError("Failed to stop container")
	}
	c.lists.forget(workspaceId)

	return ctx.NoContent(http.StatusOK)
}
