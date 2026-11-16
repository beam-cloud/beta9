package mirror

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// StoredObject is an object as the bucket lists it.
type StoredObject struct {
	Key      string
	Size     int64
	Modified time.Time
}

// ObjectStore is the part of a workspace bucket the service uses.
type ObjectStore interface {
	// Head returns the object's size, or exists false.
	Head(ctx context.Context, key string) (size int64, exists bool, err error)
	// Download returns the object, failing if it is larger than limit.
	Download(ctx context.Context, key string, limit int64) ([]byte, error)
	PresignPut(ctx context.Context, key string, expires time.Duration) (string, error)
	PresignGet(ctx context.Context, key string, expires time.Duration) (string, error)
	List(ctx context.Context, prefix string) ([]StoredObject, error)
	Delete(ctx context.Context, key string) error
}

type Config struct {
	// KeepGenerations is how many of the newest generations are kept even
	// when nothing references them.
	KeepGenerations int
	// SweepGrace protects objects that no kept generation references yet:
	// an upload in progress has objects before it has a manifest.
	SweepGrace    time.Duration
	SweepInterval time.Duration
	URLExpiry     time.Duration
}

func (c Config) withDefaults() Config {
	if c.KeepGenerations <= 0 {
		c.KeepGenerations = 5
	}
	if c.SweepGrace <= 0 {
		c.SweepGrace = 48 * time.Hour
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 6 * time.Hour
	}
	if c.URLExpiry <= 0 {
		c.URLExpiry = time.Hour
	}
	return c
}

const (
	maxObjectBatch   = 1000
	maxManifestBytes = 256 << 20
	headConcurrency  = 32
	maxMissingListed = 1000
	// maxCommitObjects bounds the objects one commit adds, about 100 GiB of
	// new files, and so the request body the gateway holds while it decodes
	// one, about 13 MB.
	maxCommitObjects = 100_000
	maxCommitBody    = maxCommitObjects*128 + 1<<16
)

var validMirrorName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

type ServiceOpts struct {
	Config      Config
	BackendRepo repository.BackendRepository
	// Storage is the default workspace storage configuration, for presigning.
	Storage    types.WorkspaceStorageConfig
	RouteGroup *echo.Group
	// Store overrides the bucket client, for tests.
	Store func(context.Context, *types.Workspace) (ObjectStore, error)
}

type Service struct {
	repo       repository.MirrorRepository
	workspaces repository.BackendRepository
	config     Config
	store      func(context.Context, *types.Workspace) (ObjectStore, error)
	// Bounds the commits checking objects in the bucket at once, each with
	// headConcurrency requests in flight.
	commits chan struct{}
	now     func() time.Time
}

func New(ctx context.Context, opts ServiceOpts) (*Service, error) {
	repo, ok := opts.BackendRepo.(repository.MirrorRepository)
	if !ok {
		return nil, errors.New("backend does not support mirrors")
	}
	s := &Service{
		repo:       repo,
		workspaces: opts.BackendRepo,
		config:     opts.Config.withDefaults(),
		store:      opts.Store,
		commits:    make(chan struct{}, 8),
		now:        time.Now,
	}
	if s.store == nil {
		s.store = func(ctx context.Context, workspace *types.Workspace) (ObjectStore, error) {
			if !workspace.StorageAvailable() {
				return nil, echo.NewHTTPError(http.StatusPreconditionFailed, "workspace storage is unavailable")
			}
			client, err := clients.NewWorkspaceStorageClientWithDefaultPresignEndpoint(ctx, workspace.Name, workspace.Storage, opts.Storage)
			if err != nil {
				return nil, err
			}
			return bucketStore{client}, nil
		}
	}
	if opts.RouteGroup != nil {
		registerRoutes(opts.RouteGroup, s)
	}
	go s.sweepLoop(ctx)
	return s, nil
}

func registerRoutes(api *echo.Group, s *Service) {
	// The same alias and token rules as the VM routes.
	api.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if c.Param("workspaceId") == "self" {
				cc, ok := c.(*auth.HttpAuthContext)
				if !ok || cc.AuthInfo == nil || cc.AuthInfo.Workspace == nil {
					return echo.NewHTTPError(http.StatusUnauthorized)
				}
				values := append([]string(nil), c.ParamValues()...)
				for i, name := range c.ParamNames() {
					if name == "workspaceId" {
						values[i] = cc.AuthInfo.Workspace.ExternalId
					}
				}
				c.SetParamValues(values...)
			}
			return next(c)
		}
	})
	api.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(s.list))
	api.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(s.create))
	api.GET("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.get))
	api.DELETE("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.purge))
	api.POST("/:workspaceId/:name/objects", auth.WithStrictWorkspaceAuth(s.missingObjects))
	api.POST("/:workspaceId/:name/objects/download", auth.WithStrictWorkspaceAuth(s.downloadObjects))
	api.GET("/:workspaceId/:name/generations", auth.WithStrictWorkspaceAuth(s.generations))
	api.POST("/:workspaceId/:name/generations", auth.WithStrictWorkspaceAuth(s.commit))
	api.GET("/:workspaceId/:name/generations/:number", auth.WithStrictWorkspaceAuth(s.generation))
}

func decode(c echo.Context, dst any) error {
	return decodeLimited(c, dst, 1<<20)
}

func decodeLimited(c echo.Context, dst any, limit int64) error {
	d := json.NewDecoder(io.LimitReader(c.Request().Body, limit))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return nil
}

func requestContext(c echo.Context) (context.Context, *auth.AuthInfo) {
	info := c.(*auth.HttpAuthContext).AuthInfo
	return auth.ContextWithAuthInfo(c.Request().Context(), info), info
}

func apiError(err error) error {
	var httpErr *echo.HTTPError
	switch {
	case errors.As(err, &httpErr):
		return httpErr
	case errors.Is(err, sql.ErrNoRows):
		return echo.NewHTTPError(http.StatusNotFound, "mirror not found")
	}
	log.Error().Err(err).Msg("mirror request failed")
	return echo.NewHTTPError(http.StatusInternalServerError, "mirror request failed")
}

type mirrorResponse struct {
	types.Mirror
	References []types.MirrorReference `json:"references"`
}

func (s *Service) list(c echo.Context) error {
	ctx, info := requestContext(c)
	mirrors, err := s.repo.ListMirrors(ctx, info.Workspace.Id)
	if err != nil {
		return apiError(err)
	}
	if mirrors == nil {
		mirrors = []types.Mirror{}
	}
	return c.JSON(http.StatusOK, mirrors)
}

func (s *Service) create(c echo.Context) error {
	ctx, info := requestContext(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}
	if !validMirrorName.MatchString(req.Name) {
		return echo.NewHTTPError(http.StatusBadRequest, "name must be at most 63 lowercase letters, digits or hyphens and not begin with a hyphen")
	}
	mirror, created, err := s.repo.EnsureMirror(ctx, info.Workspace.Id, req.Name)
	if err != nil {
		return apiError(err)
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	return c.JSON(status, mirror)
}

func (s *Service) get(c echo.Context) error {
	ctx, info := requestContext(c)
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	references, err := s.repo.ListMirrorReferences(ctx, info.Workspace.Id, mirror.ID)
	if err != nil {
		return apiError(err)
	}
	return c.JSON(http.StatusOK, mirrorResponse{Mirror: *mirror, References: references})
}

type objectRef struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type objectURL struct {
	Digest string `json:"digest"`
	URL    string `json:"url"`
}

// missingObjects answers which of a batch of objects the store lacks and
// hands back an upload URL for each. An object present with another size
// counts as missing, so a client can replace a bad upload.
func (s *Service) missingObjects(c echo.Context) error {
	ctx, info := requestContext(c)
	var req struct {
		Objects []objectRef `json:"objects"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}
	if len(req.Objects) == 0 || len(req.Objects) > maxObjectBatch {
		return echo.NewHTTPError(http.StatusBadRequest, "send between 1 and "+strconv.Itoa(maxObjectBatch)+" objects")
	}
	for _, object := range req.Objects {
		if _, err := ParseDigest(object.Digest); err != nil || object.Size <= 0 || object.Size > maxManifestBytes {
			return echo.NewHTTPError(http.StatusBadRequest, "each object needs a lowercase SHA-256 digest and its size")
		}
	}
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	bucket, err := s.store(ctx, info.Workspace)
	if err != nil {
		return apiError(err)
	}

	missing := make([]bool, len(req.Objects))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(headConcurrency)
	for i, object := range req.Objects {
		group.Go(func() error {
			size, exists, err := bucket.Head(groupCtx, types.MirrorObjectKey(mirror.ID, object.Digest))
			missing[i] = !exists || size != object.Size
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return apiError(err)
	}

	// Serialize issuing new PUT URLs with purge. The lookups above can
	// outlast a purge, including one followed by reuse of the name.
	unlock, err := s.repo.LockVM(ctx, mirror.ID)
	if err != nil {
		return apiError(err)
	}
	defer unlock()
	current, err := s.repo.GetMirror(ctx, info.Workspace.Id, mirror.Name)
	if err != nil {
		return apiError(err)
	}
	if current.ID != mirror.ID {
		return apiError(sql.ErrNoRows)
	}

	response := struct {
		Missing []objectURL `json:"missing"`
		Expires int64       `json:"expires_in"`
	}{Missing: []objectURL{}, Expires: int64(s.config.URLExpiry.Seconds())}
	for i, object := range req.Objects {
		if !missing[i] {
			continue
		}
		url, err := bucket.PresignPut(ctx, types.MirrorObjectKey(mirror.ID, object.Digest), s.config.URLExpiry)
		if err != nil {
			return apiError(err)
		}
		response.Missing = append(response.Missing, objectURL{Digest: object.Digest, URL: url})
	}
	return c.JSON(http.StatusOK, response)
}

// downloadObjects returns read URLs, for restoring files to a machine.
func (s *Service) downloadObjects(c echo.Context) error {
	ctx, info := requestContext(c)
	var req struct {
		Digests []string `json:"digests"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}
	if len(req.Digests) == 0 || len(req.Digests) > maxObjectBatch {
		return echo.NewHTTPError(http.StatusBadRequest, "send between 1 and "+strconv.Itoa(maxObjectBatch)+" digests")
	}
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	bucket, err := s.store(ctx, info.Workspace)
	if err != nil {
		return apiError(err)
	}
	urls := make([]objectURL, 0, len(req.Digests))
	for _, digest := range req.Digests {
		if _, err := ParseDigest(digest); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		url, err := bucket.PresignGet(ctx, types.MirrorObjectKey(mirror.ID, digest), s.config.URLExpiry)
		if err != nil {
			return apiError(err)
		}
		urls = append(urls, objectURL{Digest: digest, URL: url})
	}
	return c.JSON(http.StatusOK, map[string]any{"urls": urls, "expires_in": int64(s.config.URLExpiry.Seconds())})
}

func (s *Service) generations(c echo.Context) error {
	ctx, info := requestContext(c)
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	all, err := s.repo.ListMirrorGenerations(ctx, mirror.ID)
	if err != nil {
		return apiError(err)
	}
	generations := make([]types.MirrorGeneration, 0, len(all))
	for _, generation := range all {
		if generation.ExpiredAt == nil {
			generations = append(generations, generation)
		}
	}
	return c.JSON(http.StatusOK, generations)
}

func (s *Service) generation(c echo.Context) error {
	ctx, info := requestContext(c)
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	number := int64(0)
	if value := c.Param("number"); value != "latest" {
		if number, err = strconv.ParseInt(value, 10, 64); err != nil || number <= 0 {
			return echo.NewHTTPError(http.StatusBadRequest, "generation must be a positive number or latest")
		}
	}
	generation, err := s.repo.GetMirrorGeneration(ctx, mirror.ID, number)
	if errors.Is(err, sql.ErrNoRows) || err == nil && generation.ExpiredAt != nil {
		return echo.NewHTTPError(http.StatusNotFound, "generation not found")
	}
	if err != nil {
		return apiError(err)
	}
	bucket, err := s.store(ctx, info.Workspace)
	if err != nil {
		return apiError(err)
	}
	url, err := bucket.PresignGet(ctx, types.MirrorObjectKey(mirror.ID, generation.ManifestDigest), s.config.URLExpiry)
	if err != nil {
		return apiError(err)
	}
	return c.JSON(http.StatusOK, struct {
		types.MirrorGeneration
		ManifestURL string `json:"manifest_url"`
	}{*generation, url})
}

// loadManifest downloads a manifest object and checks it against its name.
func loadManifest(ctx context.Context, bucket ObjectStore, mirrorID, digest string) (*Manifest, error) {
	stored, err := bucket.Download(ctx, types.MirrorObjectKey(mirrorID, digest), maxManifestBytes)
	if err != nil {
		return nil, err
	}
	manifest, err := ParseBounded(stored)
	if err != nil {
		return nil, err
	}
	if manifest.Digest().String() != digest {
		return nil, ErrObjectMismatch
	}
	return manifest, nil
}

// commit makes an uploaded manifest the mirror's next generation. The
// gateway does not read manifests: the client declares the manifest's size
// and totals and the objects it adds over its parent, and the manifest and
// every added object must be in the bucket at the stated size. A worker
// parses and validates a manifest before it mounts it. The mirror lock keeps
// a sweep from deleting the added objects between the check and the insert.
func (s *Service) commit(c echo.Context) error {
	ctx, info := requestContext(c)
	var req struct {
		Parent         int64       `json:"parent"`
		ManifestDigest string      `json:"manifest_digest"`
		ManifestSize   int64       `json:"manifest_size"`
		Added          []objectRef `json:"added"`
		Entries        int64       `json:"entries"`
		Objects        int64       `json:"objects"`
		TotalSize      int64       `json:"total_size"`
		Home           string      `json:"home"`
	}
	if err := decodeLimited(c, &req, maxCommitBody); err != nil {
		return err
	}
	if _, err := ParseDigest(req.ManifestDigest); err != nil || req.Parent < 0 || req.ManifestSize <= 0 || req.ManifestSize > maxManifestBytes {
		return echo.NewHTTPError(http.StatusBadRequest, "send the parent generation and the manifest's SHA-256 digest and size")
	}
	if req.Entries <= 0 || req.Entries > MaxEntries || req.Objects < 0 || req.Objects > math.MaxUint32 || req.TotalSize < 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "send the manifest's entry and object counts and its total size")
	}
	if len(req.Added) > maxCommitObjects {
		return echo.NewHTTPError(http.StatusBadRequest, "a commit adds at most "+strconv.Itoa(maxCommitObjects)+" objects")
	}
	added := make(map[string]int64, len(req.Added))
	for _, object := range req.Added {
		if _, err := ParseDigest(object.Digest); err != nil || object.Size <= 0 || object.Size > maxManifestBytes {
			return echo.NewHTTPError(http.StatusBadRequest, "each added object needs a lowercase SHA-256 digest and its size")
		}
		added[object.Digest] = object.Size
	}
	if int64(len(added)) > req.Objects {
		return echo.NewHTTPError(http.StatusBadRequest, "a commit adds no more objects than its manifest names")
	}
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	bucket, err := s.store(ctx, info.Workspace)
	if err != nil {
		return apiError(err)
	}

	select {
	case s.commits <- struct{}{}:
		defer func() { <-s.commits }()
	case <-ctx.Done():
		return ctx.Err()
	}
	unlock, err := s.repo.LockVM(ctx, mirror.ID)
	if err != nil {
		return apiError(err)
	}
	defer unlock()

	conflict := func(latest *types.MirrorGeneration) error {
		return c.JSON(http.StatusConflict, map[string]any{
			"message": "another writer committed first; rebase on the latest generation",
			"latest":  latest,
		})
	}
	latest, err := s.repo.GetMirrorGeneration(ctx, mirror.ID, 0)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return apiError(err)
	}
	if latest == nil && req.Parent != 0 || latest != nil && latest.Number != req.Parent {
		return conflict(latest)
	}

	size, exists, err := bucket.Head(ctx, types.MirrorObjectKey(mirror.ID, req.ManifestDigest))
	if err != nil {
		return apiError(err)
	}
	if !exists || size != req.ManifestSize {
		return echo.NewHTTPError(http.StatusUnprocessableEntity, "the manifest object is not in the store")
	}

	// The parent's objects were checked when it was committed and it has
	// been retained since, so only what this generation adds is checked.
	var mu sync.Mutex
	missing := []string{}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(headConcurrency)
	for digest, want := range added {
		group.Go(func() error {
			size, exists, err := bucket.Head(groupCtx, types.MirrorObjectKey(mirror.ID, digest))
			if err != nil {
				return err
			}
			if !exists || size != want {
				mu.Lock()
				if len(missing) < maxMissingListed {
					missing = append(missing, digest)
				}
				mu.Unlock()
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return apiError(err)
	}
	if len(missing) > 0 {
		return c.JSON(http.StatusUnprocessableEntity, map[string]any{
			"message": "the manifest references objects that are not in the store",
			"missing": missing,
		})
	}

	// A home ValidHome refuses records none; it never fails a commit.
	home := ""
	if ValidHome(req.Home) {
		home = req.Home
	}
	generation := &types.MirrorGeneration{
		MirrorID:       mirror.ID,
		Parent:         req.Parent,
		ManifestDigest: req.ManifestDigest,
		ManifestSize:   req.ManifestSize,
		Entries:        req.Entries,
		Objects:        req.Objects,
		TotalSize:      req.TotalSize,
		Home:           home,
		TokenID:        info.Token.ExternalId,
	}
	if err := s.repo.CommitMirrorGeneration(ctx, generation); err != nil {
		if errors.Is(err, repository.ErrMirrorGenerationConflict) {
			latest, _ := s.repo.GetMirrorGeneration(ctx, mirror.ID, 0)
			return conflict(latest)
		}
		return apiError(err)
	}
	if _, err := s.expireGenerations(ctx, mirror); err != nil {
		log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("could not expire mirror generations")
	}
	return c.JSON(http.StatusCreated, generation)
}

// purge deletes a mirror and everything it stores. VMs, snapshots and
// templates pinned to it would lose every mirrored file they have not
// changed, so it is refused while any exist unless forced.
func (s *Service) purge(c echo.Context) error {
	ctx, info := requestContext(c)
	mirror, err := s.repo.GetMirror(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	bucket, err := s.store(ctx, info.Workspace)
	if err != nil {
		return apiError(err)
	}
	unlock, err := s.repo.LockVM(ctx, mirror.ID)
	if err != nil {
		return apiError(err)
	}
	defer unlock()

	references, err := s.repo.ListMirrorReferences(ctx, info.Workspace.Id, mirror.ID)
	if err != nil {
		return apiError(err)
	}
	if len(references) > 0 && c.QueryParam("force") != "true" {
		return c.JSON(http.StatusConflict, map[string]any{
			"message":    "the mirror is in use; purging it removes its files from everything listed",
			"references": references,
		})
	}
	// Forget it first: from here nothing new can pin or extend it, and the
	// sweep finishes the deletion if this request does not.
	if err := s.repo.PurgeMirror(ctx, mirror.ID); err != nil {
		return apiError(err)
	}
	if err := s.deleteObjects(ctx, bucket, mirror.ID); err != nil {
		log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("mirror purged; object deletion will be retried")
		return c.JSON(http.StatusAccepted, map[string]any{"message": "the mirror is purged; its objects are still being deleted"})
	}
	return c.NoContent(http.StatusNoContent)
}

// Resolve turns the mirror a VM request names into a pinned generation: the
// latest one, or the one the request numbers. A request that already carries
// a pin, as a VM made from a snapshot or template does, gets that pin back
// only if it still names a generation of this workspace's mirror, so nothing
// in a pin is taken from the caller on trust. Such a pin keeps its home,
// where its root disk holds the files it changed.
func (s *Service) Resolve(ctx context.Context, workspaceID uint, requested *types.VMMirror) (*types.VMMirror, error) {
	if requested == nil {
		return nil, nil
	}
	name := requested.Name
	if !validMirrorName.MatchString(name) {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "invalid mirror name")
	}
	if requested.Generation < 0 {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "invalid mirror generation")
	}
	mirror, err := s.repo.GetMirror(ctx, workspaceID, name)
	if errors.Is(err, sql.ErrNoRows) || err == nil && mirror.Latest == nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "mirror "+name+" has no generation yet; wait for its first sync from Beam Mirror")
	}
	if err != nil {
		return nil, err
	}
	if requested.ID != "" && requested.ID != mirror.ID {
		return nil, echo.NewHTTPError(http.StatusConflict, "mirror "+name+" was purged after this snapshot was taken")
	}
	generation := mirror.Latest
	if requested.Generation != 0 && requested.Generation != generation.Number {
		generation, err = s.repo.GetMirrorGeneration(ctx, mirror.ID, requested.Generation)
		// An expired generation can be inherited, which keeps it, but not
		// asked for anew.
		if errors.Is(err, sql.ErrNoRows) || err == nil && generation.ExpiredAt != nil && requested.ID == "" {
			return nil, echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("mirror %s has no generation %d", name, requested.Generation))
		}
		if err != nil {
			return nil, err
		}
	}
	if requested.ManifestDigest != "" && requested.ManifestDigest != generation.ManifestDigest {
		return nil, echo.NewHTTPError(http.StatusConflict, "mirror "+name+" was purged after this snapshot was taken")
	}
	home := generation.Home
	if requested.ID != "" {
		home = requested.Home
	}
	return &types.VMMirror{
		Name:           mirror.Name,
		ID:             mirror.ID,
		Generation:     generation.Number,
		ManifestDigest: generation.ManifestDigest,
		ManifestSize:   generation.ManifestSize,
		Home:           home,
	}, nil
}

// Active reports whether a VM's pin still names a mirror that exists. It
// stops being true when the mirror is purged, and the VM then starts
// without the layer.
func (s *Service) Active(ctx context.Context, workspaceID uint, pin *types.VMMirror) (bool, error) {
	if !pin.Pinned() {
		return false, nil
	}
	mirror, err := s.repo.GetMirror(ctx, workspaceID, pin.Name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return mirror.ID == pin.ID, nil
}

// expireGenerations marks the generations nothing needs: those older than
// the newest KeepGenerations that no VM, snapshot or template is pinned to.
// A generation something is pinned to after all is kept again. It returns
// every generation the mirror still has.
func (s *Service) expireGenerations(ctx context.Context, mirror *types.Mirror) ([]types.MirrorGeneration, error) {
	generations, err := s.repo.ListMirrorGenerations(ctx, mirror.ID)
	if err != nil {
		return nil, err
	}
	references, err := s.repo.ListMirrorReferences(ctx, mirror.WorkspaceID, mirror.ID)
	if err != nil {
		return nil, err
	}
	pinned := map[int64]bool{}
	for _, reference := range references {
		pinned[reference.Generation] = true
	}
	now := s.now()
	var expire, revive []int64
	for i := range generations {
		generation := &generations[i]
		keep := i < s.config.KeepGenerations || pinned[generation.Number]
		switch {
		case keep && generation.ExpiredAt != nil:
			revive = append(revive, generation.Number)
			generation.ExpiredAt = nil
		case !keep && generation.ExpiredAt == nil:
			expire = append(expire, generation.Number)
			generation.ExpiredAt = &now
		}
	}
	if err := s.repo.SetMirrorGenerationsExpired(ctx, mirror.ID, revive, false); err != nil {
		return nil, err
	}
	return generations, s.repo.SetMirrorGenerationsExpired(ctx, mirror.ID, expire, true)
}

// Sweep deletes the objects of a mirror that are older than the grace period
// and that no generation references, counting generations that expired
// within the grace period: a VM being created may have pinned one just
// before it expired. It deletes nothing unless every such manifest could be
// read, because an unreadable manifest would make its objects look
// unreferenced.
func (s *Service) Sweep(ctx context.Context, bucket ObjectStore, mirror *types.Mirror) (int, error) {
	unlock, err := s.repo.LockVM(ctx, mirror.ID)
	if err != nil {
		return 0, err
	}
	defer unlock()

	generations, err := s.expireGenerations(ctx, mirror)
	if err != nil {
		return 0, err
	}
	cutoff := s.now().Add(-s.config.SweepGrace)
	live := map[string]bool{}
	var dead []int64
	for _, generation := range generations {
		if generation.ExpiredAt != nil && generation.ExpiredAt.Before(cutoff) {
			dead = append(dead, generation.Number)
			continue
		}
		manifest, err := loadManifest(ctx, bucket, mirror.ID, generation.ManifestDigest)
		if err != nil {
			return 0, fmt.Errorf("read manifest of generation %d: %w", generation.Number, err)
		}
		live[generation.ManifestDigest] = true
		for i := range manifest.Objects() {
			live[manifest.Object(i).Digest.String()] = true
		}
	}
	// Forget dead generations before their objects go, so that nothing can
	// inherit a pin to a generation that no longer reads.
	if err := s.repo.DeleteMirrorGenerations(ctx, mirror.ID, dead); err != nil {
		return 0, err
	}
	objects, err := bucket.List(ctx, types.MirrorPrefix(mirror.ID))
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, object := range objects {
		if live[path.Base(object.Key)] || object.Modified.After(cutoff) {
			continue
		}
		if err := bucket.Delete(ctx, object.Key); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) deleteObjects(ctx context.Context, bucket ObjectStore, mirrorID string) error {
	// An issued PUT URL can outlast a purge, and a transfer can finish
	// after the URL expires. Only a listing started after both windows
	// have passed can finish the cleanup. Use the same transfer allowance
	// as the sweep uses to protect uncommitted uploads.
	purgedBefore := s.now().Add(-s.config.URLExpiry).Add(-s.config.SweepGrace)
	objects, err := bucket.List(ctx, types.MirrorPrefix(mirrorID))
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := bucket.Delete(ctx, object.Key); err != nil {
			return err
		}
	}
	return s.repo.MarkMirrorObjectsDeleted(ctx, mirrorID, purgedBefore)
}

func (s *Service) bucketFor(ctx context.Context, workspaceID uint) (ObjectStore, error) {
	workspace, err := s.workspaces.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	if workspace.Storage == nil && workspace.StorageId != nil {
		if workspace.Storage, err = s.workspaces.GetWorkspaceStorage(ctx, *workspace.StorageId); err != nil {
			return nil, err
		}
	}
	return s.store(ctx, workspace)
}

// SweepAll finishes interrupted purges and sweeps every mirror once.
func (s *Service) SweepAll(ctx context.Context) {
	purged, err := s.repo.ListPurgedMirrors(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("could not list purged mirrors")
	}
	for _, mirror := range purged {
		bucket, err := s.bucketFor(ctx, mirror.WorkspaceID)
		if err == nil {
			err = s.deleteObjects(ctx, bucket, mirror.ID)
		}
		if err != nil {
			log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("could not delete purged mirror objects")
		}
	}
	mirrors, err := s.repo.ListMirrorIDs(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("could not list mirrors to sweep")
		return
	}
	for _, mirror := range mirrors {
		if ctx.Err() != nil {
			return
		}
		bucket, err := s.bucketFor(ctx, mirror.WorkspaceID)
		if err != nil {
			log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("could not open mirror bucket")
			continue
		}
		deleted, err := s.Sweep(ctx, bucket, &mirror)
		if err != nil {
			log.Warn().Err(err).Str("mirror_id", mirror.ID).Msg("mirror sweep failed")
		} else if deleted > 0 {
			log.Info().Str("mirror_id", mirror.ID).Int("objects", deleted).Msg("swept unreferenced mirror objects")
		}
	}
}

func (s *Service) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.SweepAll(ctx)
		}
	}
}

type bucketStore struct {
	client *clients.WorkspaceStorageClient
}

func (b bucketStore) Head(ctx context.Context, key string) (int64, bool, error) {
	exists, object, err := b.client.Head(ctx, key)
	if err != nil || !exists || object == nil || object.ContentLength == nil {
		return 0, false, err
	}
	return *object.ContentLength, true, nil
}

func (b bucketStore) Download(ctx context.Context, key string, limit int64) ([]byte, error) {
	body, err := b.client.DownloadWithReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, io.LimitReader(body, limit+1)); err != nil {
		return nil, err
	}
	if int64(buffer.Len()) > limit {
		return nil, errors.New("object is larger than the limit")
	}
	return buffer.Bytes(), nil
}

func (b bucketStore) PresignPut(ctx context.Context, key string, expires time.Duration) (string, error) {
	return b.client.GeneratePresignedPutURL(ctx, key, int64(expires.Seconds()))
}

func (b bucketStore) PresignGet(ctx context.Context, key string, expires time.Duration) (string, error) {
	return b.client.GeneratePresignedGetURL(ctx, key, int64(expires.Seconds()))
}

func (b bucketStore) List(ctx context.Context, prefix string) ([]StoredObject, error) {
	listed, err := b.client.ListWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	objects := make([]StoredObject, 0, len(listed))
	for _, object := range listed {
		if object.Key == nil || object.LastModified == nil {
			continue
		}
		stored := StoredObject{Key: *object.Key, Modified: *object.LastModified}
		if object.Size != nil {
			stored.Size = *object.Size
		}
		objects = append(objects, stored)
	}
	return objects, nil
}

func (b bucketStore) Delete(ctx context.Context, key string) error {
	return b.client.Delete(ctx, key)
}
