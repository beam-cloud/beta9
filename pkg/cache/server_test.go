package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"testing"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdvertiseAddrFallsBackToPublicIP(t *testing.T) {
	// Unspecified bind (e.g. ":2050"), no explicit advertise host.
	listener := &net.TCPAddr{IP: net.IPv4zero, Port: 2050}

	// No private IP: must advertise the public IP, not an empty host.
	csPublic := &Server{cas: &Store{currentHost: &Host{}}, publicIpAddr: "15.204.241.150"}
	require.Equal(t, "15.204.241.150:2050", csPublic.advertiseAddr(listener, ""))

	// Private IP present: preferred over the public IP.
	csPrivate := &Server{cas: &Store{currentHost: &Host{}}, privateIpAddr: "10.0.0.5", publicIpAddr: "15.204.241.150"}
	require.Equal(t, "10.0.0.5:2050", csPrivate.advertiseAddr(listener, ""))

	// Explicit advertise host wins over both.
	require.Equal(t, "1.2.3.4:2050", csPrivate.advertiseAddr(listener, "1.2.3.4"))
}

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
