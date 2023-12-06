// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: map.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// MapServiceClient is the client API for MapService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MapServiceClient interface {
	MapSet(ctx context.Context, in *MapSetRequest, opts ...grpc.CallOption) (*MapSetResponse, error)
	MapGet(ctx context.Context, in *MapGetRequest, opts ...grpc.CallOption) (*MapGetResponse, error)
	MapDelete(ctx context.Context, in *MapDeleteRequest, opts ...grpc.CallOption) (*MapDeleteResponse, error)
	MapCount(ctx context.Context, in *MapCountRequest, opts ...grpc.CallOption) (*MapCountResponse, error)
}

type mapServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMapServiceClient(cc grpc.ClientConnInterface) MapServiceClient {
	return &mapServiceClient{cc}
}

func (c *mapServiceClient) MapSet(ctx context.Context, in *MapSetRequest, opts ...grpc.CallOption) (*MapSetResponse, error) {
	out := new(MapSetResponse)
	err := c.cc.Invoke(ctx, "/map.MapService/MapSet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mapServiceClient) MapGet(ctx context.Context, in *MapGetRequest, opts ...grpc.CallOption) (*MapGetResponse, error) {
	out := new(MapGetResponse)
	err := c.cc.Invoke(ctx, "/map.MapService/MapGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mapServiceClient) MapDelete(ctx context.Context, in *MapDeleteRequest, opts ...grpc.CallOption) (*MapDeleteResponse, error) {
	out := new(MapDeleteResponse)
	err := c.cc.Invoke(ctx, "/map.MapService/MapDelete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mapServiceClient) MapCount(ctx context.Context, in *MapCountRequest, opts ...grpc.CallOption) (*MapCountResponse, error) {
	out := new(MapCountResponse)
	err := c.cc.Invoke(ctx, "/map.MapService/MapCount", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MapServiceServer is the server API for MapService service.
// All implementations must embed UnimplementedMapServiceServer
// for forward compatibility
type MapServiceServer interface {
	MapSet(context.Context, *MapSetRequest) (*MapSetResponse, error)
	MapGet(context.Context, *MapGetRequest) (*MapGetResponse, error)
	MapDelete(context.Context, *MapDeleteRequest) (*MapDeleteResponse, error)
	MapCount(context.Context, *MapCountRequest) (*MapCountResponse, error)
	mustEmbedUnimplementedMapServiceServer()
}

// UnimplementedMapServiceServer must be embedded to have forward compatible implementations.
type UnimplementedMapServiceServer struct {
}

func (UnimplementedMapServiceServer) MapSet(context.Context, *MapSetRequest) (*MapSetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MapSet not implemented")
}
func (UnimplementedMapServiceServer) MapGet(context.Context, *MapGetRequest) (*MapGetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MapGet not implemented")
}
func (UnimplementedMapServiceServer) MapDelete(context.Context, *MapDeleteRequest) (*MapDeleteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MapDelete not implemented")
}
func (UnimplementedMapServiceServer) MapCount(context.Context, *MapCountRequest) (*MapCountResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MapCount not implemented")
}
func (UnimplementedMapServiceServer) mustEmbedUnimplementedMapServiceServer() {}

// UnsafeMapServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MapServiceServer will
// result in compilation errors.
type UnsafeMapServiceServer interface {
	mustEmbedUnimplementedMapServiceServer()
}

func RegisterMapServiceServer(s grpc.ServiceRegistrar, srv MapServiceServer) {
	s.RegisterService(&MapService_ServiceDesc, srv)
}

func _MapService_MapSet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MapSetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MapServiceServer).MapSet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/map.MapService/MapSet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MapServiceServer).MapSet(ctx, req.(*MapSetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MapService_MapGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MapGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MapServiceServer).MapGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/map.MapService/MapGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MapServiceServer).MapGet(ctx, req.(*MapGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MapService_MapDelete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MapDeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MapServiceServer).MapDelete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/map.MapService/MapDelete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MapServiceServer).MapDelete(ctx, req.(*MapDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MapService_MapCount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MapCountRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MapServiceServer).MapCount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/map.MapService/MapCount",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MapServiceServer).MapCount(ctx, req.(*MapCountRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// MapService_ServiceDesc is the grpc.ServiceDesc for MapService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MapService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "map.MapService",
	HandlerType: (*MapServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "MapSet",
			Handler:    _MapService_MapSet_Handler,
		},
		{
			MethodName: "MapGet",
			Handler:    _MapService_MapGet_Handler,
		},
		{
			MethodName: "MapDelete",
			Handler:    _MapService_MapDelete_Handler,
		},
		{
			MethodName: "MapCount",
			Handler:    _MapService_MapCount_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "map.proto",
}
