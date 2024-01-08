// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: volume.proto

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

const (
	VolumeService_GetOrCreateVolume_FullMethodName = "/volume.VolumeService/GetOrCreateVolume"
)

// VolumeServiceClient is the client API for VolumeService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type VolumeServiceClient interface {
	GetOrCreateVolume(ctx context.Context, in *GetOrCreateVolumeRequest, opts ...grpc.CallOption) (*GetOrCreateVolumeResponse, error)
}

type volumeServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewVolumeServiceClient(cc grpc.ClientConnInterface) VolumeServiceClient {
	return &volumeServiceClient{cc}
}

func (c *volumeServiceClient) GetOrCreateVolume(ctx context.Context, in *GetOrCreateVolumeRequest, opts ...grpc.CallOption) (*GetOrCreateVolumeResponse, error) {
	out := new(GetOrCreateVolumeResponse)
	err := c.cc.Invoke(ctx, VolumeService_GetOrCreateVolume_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// VolumeServiceServer is the server API for VolumeService service.
// All implementations must embed UnimplementedVolumeServiceServer
// for forward compatibility
type VolumeServiceServer interface {
	GetOrCreateVolume(context.Context, *GetOrCreateVolumeRequest) (*GetOrCreateVolumeResponse, error)
	mustEmbedUnimplementedVolumeServiceServer()
}

// UnimplementedVolumeServiceServer must be embedded to have forward compatible implementations.
type UnimplementedVolumeServiceServer struct {
}

func (UnimplementedVolumeServiceServer) GetOrCreateVolume(context.Context, *GetOrCreateVolumeRequest) (*GetOrCreateVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOrCreateVolume not implemented")
}
func (UnimplementedVolumeServiceServer) mustEmbedUnimplementedVolumeServiceServer() {}

// UnsafeVolumeServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to VolumeServiceServer will
// result in compilation errors.
type UnsafeVolumeServiceServer interface {
	mustEmbedUnimplementedVolumeServiceServer()
}

func RegisterVolumeServiceServer(s grpc.ServiceRegistrar, srv VolumeServiceServer) {
	s.RegisterService(&VolumeService_ServiceDesc, srv)
}

func _VolumeService_GetOrCreateVolume_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOrCreateVolumeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VolumeServiceServer).GetOrCreateVolume(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: VolumeService_GetOrCreateVolume_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VolumeServiceServer).GetOrCreateVolume(ctx, req.(*GetOrCreateVolumeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// VolumeService_ServiceDesc is the grpc.ServiceDesc for VolumeService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var VolumeService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "volume.VolumeService",
	HandlerType: (*VolumeServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetOrCreateVolume",
			Handler:    _VolumeService_GetOrCreateVolume_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "volume.proto",
}
