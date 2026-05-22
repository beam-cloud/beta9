package cachegrpc

import (
	"crypto/rand"
	"testing"

	betapb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	gproto "google.golang.org/protobuf/proto"
)

func TestCodecV2MarshalLargeCacheResponseSplitsPayload(t *testing.T) {
	codec := New(1)
	payload := []byte("cache-payload")
	resp := &betapb.CacheGetContentResponse{Ok: true, Content: payload}

	buffers, err := codec.Marshal(resp)
	require.NoError(t, err)
	defer buffers.Free()
	require.Len(t, buffers, 2)
	require.Nil(t, resp.Content)

	var decoded betapb.CacheGetContentResponse
	require.NoError(t, gproto.Unmarshal(buffers.Materialize(), &decoded))
	require.True(t, decoded.GetOk())
	require.Equal(t, payload, decoded.GetContent())
}

func BenchmarkGRPCCodecV2MarshalLargeResponse(b *testing.B) {
	codec := New(64 * 1024)
	payload := make([]byte, 1024*1024)
	_, _ = rand.Read(payload)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		resp := &betapb.CacheGetContentResponse{Ok: true, Content: payload}
		buffers, err := codec.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
		buffers.Free()
	}
}

func BenchmarkGRPCCodecProtoMarshalLargeResponse(b *testing.B) {
	codec := encoding.GetCodecV2(proto.Name)
	payload := make([]byte, 1024*1024)
	_, _ = rand.Read(payload)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		resp := &betapb.CacheGetContentResponse{Ok: true, Content: payload}
		buffers, err := codec.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
		buffers.Free()
	}
}
