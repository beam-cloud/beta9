package cache

import (
	"context"
	"net"
	"testing"

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
