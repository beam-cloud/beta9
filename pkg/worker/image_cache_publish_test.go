package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type testImageLayerSource struct {
	data  []byte
	meta  *cache.FSMetadata
	short bool
	reads int
}

func newTestImageLayerSource(data []byte) (*testImageLayerSource, string) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	return &testImageLayerSource{data: append([]byte(nil), data...), meta: &cache.FSMetadata{Hash: hash, Size: uint64(len(data))}}, hash
}

func requireEmptyLayerSpool(t *testing.T, dir string) {
	t.Helper()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func (s *testImageLayerSource) CacheFSMetadata(context.Context, string) (*cache.FSMetadata, error) {
	return s.meta, nil
}
func (s *testImageLayerSource) ReadContentInto(ctx context.Context, hash string, offset int64, dest []byte, opts cache.ClientOptions) (int64, error) {
	s.reads++
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n := copy(dest, s.data[offset:])
	if s.short {
		n--
	}
	return int64(n), nil
}

type testImageLayerDestination struct {
	complete      bool
	checkErr      error
	size          int64
	data          []byte
	path, routing string
	lock          bool
	storeErr      error
	returnedHash  string
}

func (d *testImageLayerDestination) IsCachedOnSelectedHost(hash, key string, sizes ...int64) (bool, error) {
	d.size = sizes[0]
	return d.complete, d.checkErr
}
func (d *testImageLayerDestination) StoreContentFromLocalFile(source cache.LocalContentSource, opts cache.StoreContentOptions) (string, error) {
	if d.storeErr != nil {
		return "", d.storeErr
	}
	data, err := os.ReadFile(source.Path)
	if err != nil {
		return "", err
	}
	d.data, d.path, d.routing, d.lock = data, source.CachePath, opts.RoutingKey, opts.Lock
	if d.returnedHash != "" {
		return d.returnedHash, nil
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func TestPublishImageLayerCleansSpoolAfterStoreFailure(t *testing.T) {
	data := []byte("node")
	for _, destination := range []*testImageLayerDestination{
		{storeErr: errors.New("cache unavailable")},
		{returnedHash: "wrong-hash"},
	} {
		source, hash := newTestImageLayerSource(data)
		dir := t.TempDir()
		require.Error(t, publishImageLayer(context.Background(), source, destination, hash, dir))
		requireEmptyLayerSpool(t, dir)
	}
}

func TestPublishImageLayerCopiesAndVerifiesBuildSeed(t *testing.T) {
	data := bytes.Repeat([]byte("node"), 1500000)
	source, hash := newTestImageLayerSource(data)
	destination := &testImageLayerDestination{}
	dir := t.TempDir()
	require.NoError(t, publishImageLayer(context.Background(), source, destination, hash, dir))
	require.Equal(t, data, destination.data)
	require.Equal(t, imageLayerContentCachePath(hash), destination.path)
	require.Equal(t, hash, destination.routing)
	require.True(t, destination.lock)
	require.EqualValues(t, len(data), destination.size)
	require.Equal(t, 2, source.reads)
	requireEmptyLayerSpool(t, dir)
}

func TestPublishImageLayerLetsWriterRecoverUnavailablePrecheck(t *testing.T) {
	data := []byte("node")
	source, hash := newTestImageLayerSource(data)
	destination := &testImageLayerDestination{checkErr: cache.ErrSelectedHostUnavailable}
	require.NoError(t, publishImageLayer(context.Background(), source, destination, hash, t.TempDir()))
	require.Equal(t, data, destination.data)
}

func TestPublishImageLayerRejectsInvalidMetadata(t *testing.T) {
	data := []byte("node")
	_, hash := newTestImageLayerSource(data)
	for _, tc := range []struct {
		name string
		meta *cache.FSMetadata
	}{
		{"missing", nil},
		{"wrong-hash", &cache.FSMetadata{Hash: "other", Size: uint64(len(data))}},
		{"empty", &cache.FSMetadata{Hash: hash}},
		{"overflow", &cache.FSMetadata{Hash: hash, Size: uint64(1) << 63}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &testImageLayerSource{data: data, meta: tc.meta}
			destination := &testImageLayerDestination{}
			dir := t.TempDir()
			require.Error(t, publishImageLayer(context.Background(), source, destination, hash, dir))
			require.Zero(t, source.reads)
			require.Empty(t, destination.data)
			requireEmptyLayerSpool(t, dir)
		})
	}
}

func TestPublishImageLayerSkipsCompleteReplicaAndRejectsBadSource(t *testing.T) {
	data := []byte("node")
	for _, mode := range []string{"complete", "short", "corrupt", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			source, hash := newTestImageLayerSource(data)
			destination := &testImageLayerDestination{complete: mode == "complete"}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "short" {
				source.short = true
			}
			if mode == "corrupt" {
				source.data[0]++
			}
			if mode == "canceled" {
				cancel()
			}
			err := publishImageLayer(ctx, source, destination, hash, t.TempDir())
			if mode == "complete" {
				require.NoError(t, err)
				require.Zero(t, source.reads)
			} else {
				require.Error(t, err)
			}
			if mode == "short" {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Empty(t, destination.data)
		})
	}
}

func TestImageRuntimeCachePoolsPreservesIsolation(t *testing.T) {
	provider := types.ProviderGeneric
	config := types.AppConfig{}
	config.Cache.Global.DefaultLocality = "default"
	config.Worker.Pools = map[string]types.WorkerPoolConfig{
		"build":    {Mode: types.PoolModeLocal},
		"default":  {Mode: types.PoolModeLocal, ConfigGroup: "default"},
		"ovh":      {Mode: types.PoolModeExternal, Provider: &provider, ConfigGroup: "ovh-east"},
		"ovh-peer": {Mode: types.PoolModeExternal, Provider: &provider, ConfigGroup: "ovh-east"},
		"private":  {Mode: types.PoolModePrivate, ConfigGroup: "private"},
		"provider": {Mode: types.PoolModeProvider, ConfigGroup: "provider"},
		"agent":    {Mode: types.PoolModeExternal, ConfigGroup: "agent"},
		"selector": {Mode: types.PoolModeLocal, ConfigGroup: "selector", RequiresPoolSelector: true},
	}
	t.Setenv(types.CacheLocalityEnv, "default")
	require.Equal(t, []string{"ovh"}, imageRuntimeCachePools(config, "build"))
	require.Empty(t, imageRuntimeCachePools(config, "private"))
	require.Empty(t, imageRuntimeCachePools(config, "ovh"))
	require.Empty(t, imageRuntimeCachePools(config, "selector"))
	require.Empty(t, imageRuntimeCachePools(config, "unknown"))
}

func TestImageRuntimeCachePoolsResolvesDefaultLocality(t *testing.T) {
	t.Setenv(types.CacheLocalityEnv, "")
	config := types.AppConfig{}
	config.Worker.Pools = map[string]types.WorkerPoolConfig{
		"build": {Mode: types.PoolModeLocal, ConfigGroup: "build-only"},
		"a":     {},
		"b":     {ConfigGroup: cacheDefaultLocality},
	}
	require.Equal(t, []string{"a"}, imageRuntimeCachePools(config, "build"))
	config.Cache.Global.DefaultLocality = "custom-default"
	require.Equal(t, []string{"a", "b"}, imageRuntimeCachePools(config, "build"))
	config.Worker.Pools["b"] = types.WorkerPoolConfig{ConfigGroup: "custom-default"}
	require.Equal(t, []string{"a"}, imageRuntimeCachePools(config, "build"))
}

func TestImageCachePublicationPreservesBuildResponseBudget(t *testing.T) {
	require.Equal(t, 30*time.Second, imageCachePublicationTimeout(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.InDelta(t, float64(5*time.Second), float64(imageCachePublicationTimeout(ctx)), float64(time.Second))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Zero(t, imageCachePublicationTimeout(ctx))
}
