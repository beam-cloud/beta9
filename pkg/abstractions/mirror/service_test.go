package mirror

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	store "github.com/beam-cloud/beta9/pkg/mirror"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type memoryRepo struct {
	repository.BackendRepository
	mu          sync.Mutex
	mirrors     []*types.Mirror
	purged      map[string]bool
	deleted     map[string]bool
	generations map[string][]types.MirrorGeneration
	references  []types.MirrorReference
	locks       map[string]*sync.Mutex
	now         func() time.Time
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{
		purged: map[string]bool{}, deleted: map[string]bool{},
		generations: map[string][]types.MirrorGeneration{},
		locks:       map[string]*sync.Mutex{},
	}
}

func (r *memoryRepo) latest(id string) *types.MirrorGeneration {
	generations := r.generations[id]
	if len(generations) == 0 {
		return nil
	}
	latest := generations[len(generations)-1]
	return &latest
}

func (r *memoryRepo) EnsureMirror(ctx context.Context, workspaceID uint, name string) (*types.Mirror, bool, error) {
	if mirror, err := r.GetMirror(ctx, workspaceID, name); err == nil {
		return mirror, false, nil
	}
	r.mu.Lock()
	r.mirrors = append(r.mirrors, &types.Mirror{ID: uuid.NewString(), WorkspaceID: workspaceID, Name: name, CreatedAt: time.Now()})
	r.mu.Unlock()
	mirror, err := r.GetMirror(ctx, workspaceID, name)
	return mirror, true, err
}

func (r *memoryRepo) GetMirror(_ context.Context, workspaceID uint, name string) (*types.Mirror, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, mirror := range r.mirrors {
		if mirror.WorkspaceID == workspaceID && mirror.Name == name && !r.purged[mirror.ID] {
			copy := *mirror
			copy.Latest = r.latest(mirror.ID)
			return &copy, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (r *memoryRepo) ListMirrors(_ context.Context, workspaceID uint) ([]types.Mirror, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var mirrors []types.Mirror
	for _, mirror := range r.mirrors {
		if mirror.WorkspaceID == workspaceID && !r.purged[mirror.ID] {
			copy := *mirror
			copy.Latest = r.latest(mirror.ID)
			mirrors = append(mirrors, copy)
		}
	}
	return mirrors, nil
}

func (r *memoryRepo) ListMirrorIDs(context.Context) ([]types.Mirror, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var mirrors []types.Mirror
	for _, mirror := range r.mirrors {
		if !r.purged[mirror.ID] {
			mirrors = append(mirrors, *mirror)
		}
	}
	return mirrors, nil
}

func (r *memoryRepo) CommitMirrorGeneration(_ context.Context, generation *types.MirrorGeneration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	latest := r.latest(generation.MirrorID)
	if r.purged[generation.MirrorID] || latest == nil && generation.Parent != 0 || latest != nil && latest.Number != generation.Parent {
		return repository.ErrMirrorGenerationConflict
	}
	generation.Number = generation.Parent + 1
	generation.CreatedAt = time.Now()
	r.generations[generation.MirrorID] = append(r.generations[generation.MirrorID], *generation)
	return nil
}

func (r *memoryRepo) GetMirrorGeneration(_ context.Context, mirrorID string, number int64) (*types.MirrorGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if number == 0 {
		if latest := r.latest(mirrorID); latest != nil {
			return latest, nil
		}
		return nil, sql.ErrNoRows
	}
	for _, generation := range r.generations[mirrorID] {
		if generation.Number == number {
			return &generation, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (r *memoryRepo) ListMirrorGenerations(_ context.Context, mirrorID string) ([]types.MirrorGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	generations := slices.Clone(r.generations[mirrorID])
	slices.Reverse(generations)
	return generations, nil
}

func (r *memoryRepo) ListMirrorReferences(context.Context, uint, string) ([]types.MirrorReference, error) {
	return r.references, nil
}

func (r *memoryRepo) SetMirrorGenerationsExpired(_ context.Context, mirrorID string, numbers []int64, expired bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.generations[mirrorID] {
		generation := &r.generations[mirrorID][i]
		if !slices.Contains(numbers, generation.Number) {
			continue
		}
		if !expired {
			generation.ExpiredAt = nil
		} else if generation.ExpiredAt == nil {
			now := r.now()
			generation.ExpiredAt = &now
		}
	}
	return nil
}

func (r *memoryRepo) DeleteMirrorGenerations(_ context.Context, mirrorID string, numbers []int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.generations[mirrorID] = slices.DeleteFunc(r.generations[mirrorID], func(generation types.MirrorGeneration) bool {
		return generation.ExpiredAt != nil && slices.Contains(numbers, generation.Number)
	})
	return nil
}

func (r *memoryRepo) PurgeMirror(_ context.Context, mirrorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.purged[mirrorID] = true
	delete(r.generations, mirrorID)
	return nil
}

func (r *memoryRepo) MarkMirrorObjectsDeleted(_ context.Context, mirrorID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted[mirrorID] = true
	return nil
}

func (r *memoryRepo) ListPurgedMirrors(context.Context) ([]types.Mirror, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var mirrors []types.Mirror
	for _, mirror := range r.mirrors {
		if r.purged[mirror.ID] && !r.deleted[mirror.ID] {
			mirrors = append(mirrors, *mirror)
		}
	}
	return mirrors, nil
}

func (r *memoryRepo) LockVM(_ context.Context, id string) (func(), error) {
	r.mu.Lock()
	lock := r.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		r.locks[id] = lock
	}
	r.mu.Unlock()
	lock.Lock()
	return lock.Unlock, nil
}

func (r *memoryRepo) GetWorkspace(_ context.Context, id uint) (*types.Workspace, error) {
	return &types.Workspace{Id: id, ExternalId: "ws"}, nil
}

type memoryBucket struct {
	mu         sync.Mutex
	objects    map[string][]byte
	modified   map[string]time.Time
	failDelete bool
}

func newMemoryBucket() *memoryBucket {
	return &memoryBucket{objects: map[string][]byte{}, modified: map[string]time.Time{}}
}

func (b *memoryBucket) put(key string, data []byte, modified time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key], b.modified[key] = bytes.Clone(data), modified
}

func (b *memoryBucket) Head(_ context.Context, key string) (int64, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	return int64(len(data)), ok, nil
}

func (b *memoryBucket) Download(_ context.Context, key string, limit int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok || int64(len(data)) > limit {
		return nil, errors.New("not found")
	}
	return data, nil
}

func (b *memoryBucket) PresignPut(_ context.Context, key string, _ time.Duration) (string, error) {
	return "put://" + key, nil
}

func (b *memoryBucket) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return "get://" + key, nil
}

func (b *memoryBucket) List(_ context.Context, prefix string) ([]StoredObject, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var objects []StoredObject
	for key, data := range b.objects {
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, StoredObject{Key: key, Size: int64(len(data)), Modified: b.modified[key]})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func (b *memoryBucket) Delete(_ context.Context, key string) error {
	if b.failDelete {
		return errors.New("bucket unavailable")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
	return nil
}

type harness struct {
	t       *testing.T
	repo    *memoryRepo
	bucket  *memoryBucket
	service *Service
	server  *echo.Echo
	token   *types.Token
	now     time.Time
}

func newHarness(t *testing.T, config Config) *harness {
	t.Helper()
	h := &harness{
		t: t, repo: newMemoryRepo(), bucket: newMemoryBucket(), server: echo.New(),
		token: &types.Token{ExternalId: "token", TokenType: types.TokenTypeWorkspace},
		now:   time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	group := h.server.Group("/mirror", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return next(&auth.HttpAuthContext{Context: c, AuthInfo: &auth.AuthInfo{
				Workspace: &types.Workspace{Id: 7, ExternalId: "ws"}, Token: h.token,
			}})
		}
	})
	service, err := New(ctx, ServiceOpts{
		Config: config, BackendRepo: h.repo, RouteGroup: group,
		Store: func(context.Context, *types.Workspace) (ObjectStore, error) { return h.bucket, nil },
	})
	require.NoError(t, err)
	service.now = func() time.Time { return h.now }
	h.repo.now = service.now
	h.service = service
	return h
}

func (h *harness) call(method, path string, body any, out any) int {
	h.t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		require.NoError(h.t, err)
		reader = bytes.NewReader(data)
	}
	request := httptest.NewRequest(method, "/mirror/self"+path, reader)
	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, request)
	if out != nil && recorder.Body.Len() > 0 {
		require.NoError(h.t, json.Unmarshal(recorder.Body.Bytes(), out), recorder.Body.String())
	}
	return recorder.Code
}

// push plays the client: build a generation from files, upload what the
// gateway says is missing, and commit on top of parent.
func (h *harness) push(name string, parent int64, files map[string]string) (int, types.MirrorGeneration, map[string]any) {
	h.t.Helper()
	mirror, err := h.repo.GetMirror(context.Background(), 7, name)
	require.NoError(h.t, err)
	objects := map[string][]byte{}
	writer, err := store.NewWriter(store.DefaultWriterOptions(), func(object store.Object, data []byte) error {
		objects[object.Digest.String()] = bytes.Clone(data)
		return nil
	})
	require.NoError(h.t, err)
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		require.NoError(h.t, writer.AddFile(path, 0o644, 0, strings.NewReader(files[path])))
	}
	stored, manifest, err := writer.Finish(nil)
	require.NoError(h.t, err)
	objects[manifest.Digest().String()] = stored

	var refs []objectRef
	for digest, data := range objects {
		refs = append(refs, objectRef{Digest: digest, Size: int64(len(data))})
	}
	var check struct {
		Missing []objectURL `json:"missing"`
	}
	require.Equal(h.t, http.StatusOK, h.call(http.MethodPost, "/"+name+"/objects", map[string]any{"objects": refs}, &check))
	for _, missing := range check.Missing {
		require.Equal(h.t, "put://"+types.MirrorObjectKey(mirror.ID, missing.Digest), missing.URL)
		h.bucket.put(types.MirrorObjectKey(mirror.ID, missing.Digest), objects[missing.Digest], h.now)
	}
	var raw json.RawMessage
	status := h.call(http.MethodPost, "/"+name+"/generations", map[string]any{"parent": parent, "manifest_digest": manifest.Digest().String()}, &raw)
	var generation types.MirrorGeneration
	var failure map[string]any
	if status == http.StatusCreated {
		require.NoError(h.t, json.Unmarshal(raw, &generation))
	} else {
		_ = json.Unmarshal(raw, &failure)
	}
	return status, generation, failure
}

func TestCreateListAndAuth(t *testing.T) {
	h := newHarness(t, Config{})
	var mirror types.Mirror
	require.Equal(t, http.StatusCreated, h.call(http.MethodPost, "", map[string]string{"name": "laptop"}, &mirror))
	require.Equal(t, "laptop", mirror.Name)
	var again types.Mirror
	require.Equal(t, http.StatusOK, h.call(http.MethodPost, "", map[string]string{"name": "laptop"}, &again), "creating is safe to repeat")
	require.Equal(t, mirror.ID, again.ID)
	for _, name := range []string{"", "-x", "Has Space", "../etc", strings.Repeat("a", 64)} {
		require.Equal(t, http.StatusBadRequest, h.call(http.MethodPost, "", map[string]string{"name": name}, nil), name)
	}
	var mirrors []types.Mirror
	require.Equal(t, http.StatusOK, h.call(http.MethodGet, "", nil, &mirrors))
	require.Len(t, mirrors, 1)
	require.Equal(t, http.StatusNotFound, h.call(http.MethodGet, "/other", nil, nil))

	// Restricted tokens are refused, as on the VM routes.
	h.token = &types.Token{ExternalId: "token", TokenType: types.TokenTypeWorkspaceRestricted}
	require.Equal(t, http.StatusUnauthorized, h.call(http.MethodGet, "", nil, nil))
	require.Equal(t, http.StatusUnauthorized, h.call(http.MethodPost, "/laptop/generations", map[string]any{}, nil))
	h.token = &types.Token{ExternalId: "token", TokenType: types.TokenTypeWorkspace}
	request := httptest.NewRequest(http.MethodGet, "/mirror/another-workspace", nil)
	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
}

func TestPushCommitsOnlyCompleteGenerations(t *testing.T) {
	h := newHarness(t, Config{})
	require.Equal(t, http.StatusCreated, h.call(http.MethodPost, "", map[string]string{"name": "laptop"}, nil))
	mirror, _ := h.repo.GetMirror(context.Background(), 7, "laptop")

	status, first, _ := h.push("laptop", 0, map[string]string{"a.txt": "one", "b.txt": "two"})
	require.Equal(t, http.StatusCreated, status)
	require.Equal(t, int64(1), first.Number)
	require.Equal(t, int64(3), first.Entries)
	require.Equal(t, int64(6), first.TotalSize)

	// A second push uploads only what changed.
	before := len(h.bucket.objects)
	status, second, _ := h.push("laptop", 1, map[string]string{"a.txt": "one", "b.txt": "two"})
	require.Equal(t, http.StatusCreated, status)
	require.Equal(t, int64(2), second.Number)
	require.Equal(t, before, len(h.bucket.objects), "an identical tree adds no objects, not even a manifest")

	// Two writers on the same parent: the second is told to rebase.
	status, _, failure := h.push("laptop", 1, map[string]string{"a.txt": "other writer"})
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, float64(2), failure["latest"].(map[string]any)["number"])
	status, _, _ = h.push("laptop", 5, map[string]string{"a.txt": "from the future"})
	require.Equal(t, http.StatusConflict, status)

	// A manifest whose objects are not all uploaded is not committed, and
	// the reply says which are missing.
	var objects []store.Object
	writer, err := store.NewWriter(store.DefaultWriterOptions(), func(object store.Object, _ []byte) error {
		objects = append(objects, object)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, writer.AddFile("c.txt", 0o644, 0, strings.NewReader("never uploaded")))
	stored, manifest, err := writer.Finish(nil)
	require.NoError(t, err)
	h.bucket.put(types.MirrorObjectKey(mirror.ID, manifest.Digest().String()), stored, h.now)
	var incomplete struct {
		Missing []string `json:"missing"`
	}
	commit := map[string]any{"parent": 2, "manifest_digest": manifest.Digest().String()}
	require.Equal(t, http.StatusUnprocessableEntity, h.call(http.MethodPost, "/laptop/generations", commit, &incomplete))
	require.Equal(t, []string{objects[0].Digest.String()}, incomplete.Missing)
	// Present at the wrong size is as good as absent.
	h.bucket.put(types.MirrorObjectKey(mirror.ID, objects[0].Digest.String()), []byte("short"), h.now)
	require.Equal(t, http.StatusUnprocessableEntity, h.call(http.MethodPost, "/laptop/generations", commit, nil))
	var check struct {
		Missing []objectURL `json:"missing"`
	}
	require.Equal(t, http.StatusOK, h.call(http.MethodPost, "/laptop/objects", map[string]any{"objects": []objectRef{{Digest: objects[0].Digest.String(), Size: int64(objects[0].Size)}}}, &check))
	require.Len(t, check.Missing, 1)

	// Bytes that are not a valid manifest, or not the bytes the digest
	// names, are not committed either.
	junk := []byte("not a manifest")
	junkDigest := store.DigestOf(junk).String()
	h.bucket.put(types.MirrorObjectKey(mirror.ID, junkDigest), junk, h.now)
	require.Equal(t, http.StatusUnprocessableEntity, h.call(http.MethodPost, "/laptop/generations", map[string]any{"parent": 2, "manifest_digest": junkDigest}, nil))
	wrongName := strings.Repeat("ab", 32)
	h.bucket.put(types.MirrorObjectKey(mirror.ID, wrongName), stored, h.now)
	require.Equal(t, http.StatusUnprocessableEntity, h.call(http.MethodPost, "/laptop/generations", map[string]any{"parent": 2, "manifest_digest": wrongName}, nil))
	require.Equal(t, http.StatusUnprocessableEntity, h.call(http.MethodPost, "/laptop/generations", map[string]any{"parent": 2, "manifest_digest": strings.Repeat("cd", 32)}, nil), "manifest never uploaded")
	require.Equal(t, http.StatusBadRequest, h.call(http.MethodPost, "/laptop/generations", map[string]any{"parent": 2, "manifest_digest": "../../etc"}, nil))

	// Nor is a manifest that would cost a worker more memory than a
	// generation may.
	limit := maxManifestBody
	maxManifestBody = 200
	status, _, failure = h.push("laptop", 2, map[string]string{"a.txt": "one", "b.txt": "two", "c.txt": "three"})
	maxManifestBody = limit
	require.Equal(t, http.StatusUnprocessableEntity, status)
	require.Contains(t, failure["message"], "more files than a generation can hold")

	var generations []types.MirrorGeneration
	require.Equal(t, http.StatusOK, h.call(http.MethodGet, "/laptop/generations", nil, &generations))
	require.Len(t, generations, 2)
	require.Equal(t, int64(2), generations[0].Number)
	var latest struct {
		types.MirrorGeneration
		ManifestURL string `json:"manifest_url"`
	}
	require.Equal(t, http.StatusOK, h.call(http.MethodGet, "/laptop/generations/latest", nil, &latest))
	require.Equal(t, int64(2), latest.Number)
	require.Equal(t, "get://"+types.MirrorObjectKey(mirror.ID, latest.ManifestDigest), latest.ManifestURL)
	require.Equal(t, http.StatusNotFound, h.call(http.MethodGet, "/laptop/generations/9", nil, nil))

	pin, err := h.service.Resolve(context.Background(), 7, &types.VMMirror{Name: "laptop"})
	require.NoError(t, err)
	require.True(t, pin.Pinned())
	require.Equal(t, int64(2), pin.Generation)
	require.Equal(t, mirror.ID, pin.ID)
	older, err := h.service.Resolve(context.Background(), 7, &types.VMMirror{Name: "laptop", Generation: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), older.Generation)
	// An inherited pin is checked, not trusted.
	inherited, err := h.service.Resolve(context.Background(), 7, older)
	require.NoError(t, err)
	require.Equal(t, older, inherited)
	for _, forged := range []types.VMMirror{
		{Name: "laptop", ID: uuid.NewString(), Generation: 1, ManifestDigest: older.ManifestDigest},
		{Name: "laptop", ID: mirror.ID, Generation: 1, ManifestDigest: pin.ManifestDigest + "0"},
		{Name: "laptop", Generation: 9},
		{Name: "laptop", Generation: -1},
		{Name: "absent"},
		{Name: "../laptop"},
	} {
		_, err = h.service.Resolve(context.Background(), 7, &forged)
		require.Error(t, err, forged)
	}
	_, err = h.service.Resolve(context.Background(), 7, &types.VMMirror{Name: "absent"})
	require.ErrorContains(t, err, "beam setup mirror")
	_, err = h.service.Resolve(context.Background(), 8, &types.VMMirror{Name: "laptop"})
	require.Error(t, err, "another workspace's mirror")
	active, err := h.service.Active(context.Background(), 7, pin)
	require.NoError(t, err)
	require.True(t, active)
	active, _ = h.service.Active(context.Background(), 8, pin)
	require.False(t, active)
}

func TestRetentionAndSweep(t *testing.T) {
	h := newHarness(t, Config{KeepGenerations: 2, SweepGrace: 24 * time.Hour})
	require.Equal(t, http.StatusCreated, h.call(http.MethodPost, "", map[string]string{"name": "laptop"}, nil))
	mirror, _ := h.repo.GetMirror(context.Background(), 7, "laptop")
	// A VM is pinned to generation 1.
	h.repo.references = []types.MirrorReference{{Kind: "vm", Name: "devbox", Generation: 1}}
	for i := range 5 {
		status, _, _ := h.push("laptop", int64(i), map[string]string{"file.txt": fmt.Sprintf("version %d", i+1)})
		require.Equal(t, http.StatusCreated, status)
	}
	var generations []types.MirrorGeneration
	require.Equal(t, http.StatusOK, h.call(http.MethodGet, "/laptop/generations", nil, &generations))
	numbers := []int64{}
	for _, generation := range generations {
		numbers = append(numbers, generation.Number)
	}
	require.Equal(t, []int64{5, 4, 1}, numbers, "the newest two and the pinned one are kept")

	// An upload in progress: objects with no manifest yet.
	h.bucket.put(types.MirrorObjectKey(mirror.ID, strings.Repeat("11", 32)), []byte("uploading"), h.now)
	total := len(h.bucket.objects)
	require.Equal(t, 11, total, "five packs, five manifests and the upload")

	// Inside the grace period nothing is deleted.
	deleted, err := h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	require.Zero(t, deleted)

	// After it, exactly the expired generations' objects and the abandoned
	// upload go; every kept generation still reads completely.
	h.now = h.now.Add(25 * time.Hour)
	deleted, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	require.Equal(t, 5, deleted)
	for _, generation := range generations {
		manifest, err := loadManifest(context.Background(), h.bucket, mirror.ID, generation.ManifestDigest)
		require.NoError(t, err)
		for i := range manifest.Objects() {
			_, exists, _ := h.bucket.Head(context.Background(), types.MirrorObjectKey(mirror.ID, manifest.Object(i).Digest.String()))
			require.True(t, exists)
		}
	}

	// Once the VM is gone its generation expires on the next sweep, and its
	// objects outlive it by the grace period.
	listed := func() []int64 {
		var generations []types.MirrorGeneration
		require.Equal(t, http.StatusOK, h.call(http.MethodGet, "/laptop/generations", nil, &generations))
		numbers := []int64{}
		for _, generation := range generations {
			numbers = append(numbers, generation.Number)
		}
		return numbers
	}
	h.repo.references = nil
	deleted, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, []int64{5, 4}, listed())
	require.Equal(t, http.StatusNotFound, h.call(http.MethodGet, "/laptop/generations/1", nil, nil))
	_, err = h.service.Resolve(context.Background(), 7, &types.VMMirror{Name: "laptop", Generation: 1})
	require.Error(t, err, "an expired generation cannot be asked for")

	// A VM that pinned it just before it expired turns up: it is kept again.
	first, err := h.repo.GetMirrorGeneration(context.Background(), mirror.ID, 1)
	require.NoError(t, err)
	inherited, err := h.service.Resolve(context.Background(), 7, &types.VMMirror{Name: "laptop", ID: mirror.ID, Generation: 1, ManifestDigest: first.ManifestDigest})
	require.NoError(t, err)
	h.repo.references = []types.MirrorReference{{Kind: "vm", Name: "fork", Generation: inherited.Generation}}
	h.now = h.now.Add(time.Hour)
	deleted, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, []int64{5, 4, 1}, listed())
	h.now = h.now.Add(72 * time.Hour)
	deleted, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	require.Zero(t, deleted)

	// Unpinned for longer than the grace period, it goes for good.
	h.repo.references = nil
	_, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	h.now = h.now.Add(25 * time.Hour)
	deleted, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	_, err = h.repo.GetMirrorGeneration(context.Background(), mirror.ID, 1)
	require.ErrorIs(t, err, sql.ErrNoRows)
	_, err = h.service.Resolve(context.Background(), 7, inherited)
	require.Error(t, err, "a pin to a deleted generation cannot be inherited")

	// A manifest that cannot be read stops the sweep before it deletes.
	h.bucket.put(types.MirrorObjectKey(mirror.ID, strings.Repeat("22", 32)), []byte("stale"), h.now.Add(-72*time.Hour))
	latest, _ := h.repo.GetMirrorGeneration(context.Background(), mirror.ID, 0)
	manifestKey := types.MirrorObjectKey(mirror.ID, latest.ManifestDigest)
	saved := h.bucket.objects[manifestKey]
	delete(h.bucket.objects, manifestKey)
	before := len(h.bucket.objects)
	_, err = h.service.Sweep(context.Background(), h.bucket, mirror)
	require.Error(t, err)
	require.Equal(t, before, len(h.bucket.objects))
	h.bucket.put(manifestKey, saved, h.now)
	h.service.SweepAll(context.Background())
	_, exists, _ := h.bucket.Head(context.Background(), types.MirrorObjectKey(mirror.ID, strings.Repeat("22", 32)))
	require.False(t, exists)
}

func TestPurge(t *testing.T) {
	h := newHarness(t, Config{})
	require.Equal(t, http.StatusCreated, h.call(http.MethodPost, "", map[string]string{"name": "laptop"}, nil))
	require.Equal(t, http.StatusCreated, h.call(http.MethodPost, "", map[string]string{"name": "other"}, nil))
	mirror, _ := h.repo.GetMirror(context.Background(), 7, "laptop")
	status, _, _ := h.push("laptop", 0, map[string]string{"a.txt": "one"})
	require.Equal(t, http.StatusCreated, status)
	status, _, _ = h.push("other", 0, map[string]string{"b.txt": "two"})
	require.Equal(t, http.StatusCreated, status)

	// In use: refused, and the reply names what would lose its files.
	h.repo.references = []types.MirrorReference{{Kind: "vm", Name: "devbox", Generation: 1}, {Kind: "template", Name: "base", Generation: 1}}
	var refusal struct {
		References []types.MirrorReference `json:"references"`
	}
	require.Equal(t, http.StatusConflict, h.call(http.MethodDelete, "/laptop", nil, &refusal))
	require.Len(t, refusal.References, 2)
	require.Len(t, h.bucket.objects, 4)
	var shown mirrorResponse
	require.Equal(t, http.StatusOK, h.call(http.MethodGet, "/laptop", nil, &shown))
	require.Len(t, shown.References, 2)

	// Forced: every object of this mirror is gone and the other is intact.
	require.Equal(t, http.StatusNoContent, h.call(http.MethodDelete, "/laptop?force=true", nil, nil))
	objects, _ := h.bucket.List(context.Background(), types.MirrorPrefix(mirror.ID))
	require.Empty(t, objects)
	require.Len(t, h.bucket.objects, 2)
	require.Equal(t, http.StatusNotFound, h.call(http.MethodGet, "/laptop", nil, nil))
	require.Equal(t, http.StatusNotFound, h.call(http.MethodPost, "/laptop/generations", map[string]any{"parent": 1, "manifest_digest": strings.Repeat("ab", 32)}, nil))
	pin := &types.VMMirror{Name: "laptop", ID: mirror.ID, Generation: 1, ManifestDigest: strings.Repeat("ab", 32)}
	_, err := h.service.Resolve(context.Background(), 7, pin)
	require.Error(t, err)
	active, err := h.service.Active(context.Background(), 7, pin)
	require.NoError(t, err)
	require.False(t, active)

	// The name is free again and starts from nothing.
	var fresh types.Mirror
	require.Equal(t, http.StatusCreated, h.call(http.MethodPost, "", map[string]string{"name": "laptop"}, &fresh))
	require.NotEqual(t, mirror.ID, fresh.ID)
	require.Nil(t, fresh.Latest)
	active, _ = h.service.Active(context.Background(), 7, pin)
	require.False(t, active, "a new mirror of the same name does not revive old pins")

	// If the bucket fails mid-purge the mirror is still gone, and the
	// sweep finishes deleting its objects.
	h.repo.references = nil
	other, _ := h.repo.GetMirror(context.Background(), 7, "other")
	h.bucket.failDelete = true
	require.Equal(t, http.StatusAccepted, h.call(http.MethodDelete, "/other", nil, nil))
	require.Equal(t, http.StatusNotFound, h.call(http.MethodGet, "/other", nil, nil))
	require.Len(t, h.bucket.objects, 2)
	h.bucket.failDelete = false
	h.service.SweepAll(context.Background())
	objects, _ = h.bucket.List(context.Background(), types.MirrorPrefix(other.ID))
	require.Empty(t, objects)
}
