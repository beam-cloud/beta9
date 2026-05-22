package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSamePortRawReadTransportAndGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        5,
			ObjectTtlS:           300,
			ReadTransport: ServerReadTransportConfig{
				Enabled:  true,
				Sendfile: false,
			},
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerRegistry(NewMockRegistry()), WithServerHostID("raw-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	content := []byte("hello raw cache transport")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	require.NoError(t, server.cas.Add(context.Background(), hash, content))

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte(rawReadMagic))
	require.NoError(t, err)
	require.NoError(t, writeRawReadRequest(conn, hash, 1, 3))
	status, length, err := readRawReadResponseHeader(conn)
	require.NoError(t, err)
	require.Equal(t, rawReadStatusOK, status)
	require.Equal(t, int64(3), length)
	body := make([]byte, length)
	_, err = io.ReadFull(conn, body)
	require.NoError(t, err)
	require.Equal(t, []byte("ell"), body)

	dialCtx, dialCancel := context.WithTimeout(ctx, time.Second)
	defer dialCancel()
	grpcConn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	defer grpcConn.Close()
	state, err := proto.NewCacheClient(grpcConn).GetState(dialCtx, &proto.CacheGetStateRequest{})
	require.NoError(t, err)
	require.Equal(t, Version, state.GetVersion())
}
