package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"strings"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeAdvertiseHostForJoinHostPort(t *testing.T) {
	host := normalizeAdvertiseHost("[2600:1f18:37a4:c02::c0dd]")
	require.Equal(t, "2600:1f18:37a4:c02::c0dd", host)
	require.Equal(t, "[2600:1f18:37a4:c02::c0dd]:2049", net.JoinHostPort(host, "2049"))
}

func TestNormalizeAdvertiseHostLeavesIPv4HostUnchanged(t *testing.T) {
	host := normalizeAdvertiseHost("10.100.61.10")
	require.Equal(t, "10.100.61.10", host)
	require.Equal(t, "10.100.61.10:2049", net.JoinHostPort(host, "2049"))
}

func TestNormalizeAdvertiseHostLeavesDNSHostUnchanged(t *testing.T) {
	host := normalizeAdvertiseHost("machine-abc123.tailnet.example")
	require.Equal(t, "machine-abc123.tailnet.example", host)
	require.Equal(t, "machine-abc123.tailnet.example:2049", net.JoinHostPort(host, "2049"))
}

func TestServerDrainRejectsNewRPCs(t *testing.T) {
	server := &Server{}
	server.Drain()

	_, err := server.GetState(context.Background(), &proto.CacheGetStateRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))

	_, err = server.GetContent(context.Background(), &proto.CacheGetContentRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestStoreContentFromSourceWithExpectedHashIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, 4)
	content := []byte("already cached content")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	storedHash, _, err := store.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, hash, storedHash)

	server := &Server{cas: store}
	resp, err := server.storeContentFromSource(ctx, &proto.CacheStoreContentFromSourceRequest{Source: &proto.CacheSource{
		Path:         "missing/source",
		BucketName:   "bucket",
		ExpectedHash: hash,
	}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Ok)
	require.Equal(t, hash, resp.Hash)
}

func TestStoreContentFromSourcePropagatesS3StoreError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := &Server{ctx: ctx, cas: newTestStore(t, 4)}
	resp, err := server.storeContentFromSource(ctx, &proto.CacheStoreContentFromSourceRequest{Source: &proto.CacheSource{
		Path:         "missing/source",
		BucketName:   "bucket",
		Region:       "us-east-1",
		EndpointUrl:  "http://127.0.0.1:1",
		AccessKey:    "access-key",
		SecretKey:    "secret-key",
		ExpectedHash: strings.Repeat("a", sha256.Size*2),
	}})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.Ok)
	require.NotEmpty(t, resp.ErrorMsg)
	require.NotEqual(t, "stored content hash is empty", resp.ErrorMsg)
}

func TestOCIRequiredContentOriginPathRoundTrips(t *testing.T) {
	source := RequiredContentSource{
		Type:        RequiredContentSourceOCIRegistry,
		Registry:    "registry.localhost:5000",
		Repository:  "stage/beta9-users",
		Reference:   "sha256:manifest",
		LayerDigest: "sha256:layer",
	}

	path := OCIRequiredContentOriginPath(source)
	got, ok := ParseOCIRequiredContentOriginPath(path)

	require.True(t, ok)
	require.Equal(t, source.Type, got.Type)
	require.Equal(t, source.Registry, got.Registry)
	require.Equal(t, source.Repository, got.Repository)
	require.Equal(t, source.Reference, got.Reference)
	require.Equal(t, source.LayerDigest, got.LayerDigest)
}

func TestStoreSyntheticContentInCacheFSCreatesVolumeFilePath(t *testing.T) {
	ctx := context.Background()
	registry := NewMockCacheMetadataStore()
	server := &Server{metadataStore: registry}

	err := server.StoreSyntheticContentInCacheFS(ctx, "/volumes/workspace/.cache", "sha256-content", 123)
	require.NoError(t, err)

	dir, err := registry.GetFsNode(ctx, GenerateFsID("/volumes"))
	require.NoError(t, err)
	require.Equal(t, "/volumes", dir.Path)
	require.Equal(t, uint32(fuse.S_IFDIR|0755), dir.Mode)

	node, err := registry.GetFsNode(ctx, GenerateFsID("/volumes/workspace/.cache"))
	require.NoError(t, err)
	require.Equal(t, "/volumes/workspace/.cache", node.Path)
	require.Equal(t, "sha256-content", node.Hash)
	require.Equal(t, uint64(123), node.Size)
	require.Equal(t, uint32(fuse.S_IFREG|0644), node.Mode)

	children, err := registry.GetFsNodeChildren(ctx, GenerateFsID("/volumes/workspace"))
	require.NoError(t, err)
	require.Len(t, children, 1)
	require.Equal(t, node.ID, children[0].ID)
}
