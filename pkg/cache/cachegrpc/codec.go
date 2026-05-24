package cachegrpc

import (
	"fmt"

	betapb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/encoding"
	grpcproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

type Codec struct {
	proto    encoding.CodecV2
	minBytes int
}

func New(minBytes int) encoding.CodecV2 {
	if minBytes <= 0 {
		minBytes = 64 * 1024
	}
	return &Codec{
		proto:    encoding.GetCodecV2(grpcproto.Name),
		minBytes: minBytes,
	}
}

func (c *Codec) Name() string {
	return grpcproto.Name
}

func (c *Codec) Marshal(v any) (mem.BufferSlice, error) {
	resp, ok := v.(*betapb.CacheGetContentResponse)
	if !ok || len(resp.Content) < c.minBytes {
		return c.delegateMarshal(v)
	}

	header := make([]byte, 0, 16)
	if resp.Ok {
		header = protowire.AppendTag(header, 1, protowire.VarintType)
		header = protowire.AppendVarint(header, 1)
	}
	header = protowire.AppendTag(header, 2, protowire.BytesType)
	header = protowire.AppendVarint(header, uint64(len(resp.Content)))

	payload := resp.Content
	resp.Content = nil
	return mem.BufferSlice{
		mem.SliceBuffer(header),
		mem.NewBuffer(&payload, mem.DefaultBufferPool()),
	}, nil
}

func (c *Codec) Unmarshal(data mem.BufferSlice, v any) error {
	if c.proto == nil {
		return fmt.Errorf("default grpc proto codec v2 is not registered")
	}
	return c.proto.Unmarshal(data, v)
}

func (c *Codec) delegateMarshal(v any) (mem.BufferSlice, error) {
	if c.proto == nil {
		return nil, fmt.Errorf("default grpc proto codec v2 is not registered")
	}
	return c.proto.Marshal(v)
}
