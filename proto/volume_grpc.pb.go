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
	VolumeService_DeleteVolume_FullMethodName      = "/volume.VolumeService/DeleteVolume"
	VolumeService_ListVolumes_FullMethodName       = "/volume.VolumeService/ListVolumes"
	VolumeService_ListPath_FullMethodName          = "/volume.VolumeService/ListPath"
	VolumeService_DeletePath_FullMethodName        = "/volume.VolumeService/DeletePath"
	VolumeService_CopyPathStream_FullMethodName    = "/volume.VolumeService/CopyPathStream"
	VolumeService_MovePath_FullMethodName          = "/volume.VolumeService/MovePath"
)

// VolumeServiceClient is the client API for VolumeService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type VolumeServiceClient interface {
	GetOrCreateVolume(ctx context.Context, in *GetOrCreateVolumeRequest, opts ...grpc.CallOption) (*GetOrCreateVolumeResponse, error)
	DeleteVolume(ctx context.Context, in *DeleteVolumeRequest, opts ...grpc.CallOption) (*DeleteVolumeResponse, error)
	ListVolumes(ctx context.Context, in *ListVolumesRequest, opts ...grpc.CallOption) (*ListVolumesResponse, error)
	ListPath(ctx context.Context, in *ListPathRequest, opts ...grpc.CallOption) (*ListPathResponse, error)
	DeletePath(ctx context.Context, in *DeletePathRequest, opts ...grpc.CallOption) (*DeletePathResponse, error)
	CopyPathStream(ctx context.Context, opts ...grpc.CallOption) (VolumeService_CopyPathStreamClient, error)
	MovePath(ctx context.Context, in *MovePathRequest, opts ...grpc.CallOption) (*MovePathResponse, error)
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

func (c *volumeServiceClient) DeleteVolume(ctx context.Context, in *DeleteVolumeRequest, opts ...grpc.CallOption) (*DeleteVolumeResponse, error) {
	out := new(DeleteVolumeResponse)
	err := c.cc.Invoke(ctx, VolumeService_DeleteVolume_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *volumeServiceClient) ListVolumes(ctx context.Context, in *ListVolumesRequest, opts ...grpc.CallOption) (*ListVolumesResponse, error) {
	out := new(ListVolumesResponse)
	err := c.cc.Invoke(ctx, VolumeService_ListVolumes_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *volumeServiceClient) ListPath(ctx context.Context, in *ListPathRequest, opts ...grpc.CallOption) (*ListPathResponse, error) {
	out := new(ListPathResponse)
	err := c.cc.Invoke(ctx, VolumeService_ListPath_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *volumeServiceClient) DeletePath(ctx context.Context, in *DeletePathRequest, opts ...grpc.CallOption) (*DeletePathResponse, error) {
	out := new(DeletePathResponse)
	err := c.cc.Invoke(ctx, VolumeService_DeletePath_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *volumeServiceClient) CopyPathStream(ctx context.Context, opts ...grpc.CallOption) (VolumeService_CopyPathStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &VolumeService_ServiceDesc.Streams[0], VolumeService_CopyPathStream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &volumeServiceCopyPathStreamClient{stream}
	return x, nil
}

type VolumeService_CopyPathStreamClient interface {
	Send(*CopyPathRequest) error
	CloseAndRecv() (*CopyPathResponse, error)
	grpc.ClientStream
}

type volumeServiceCopyPathStreamClient struct {
	grpc.ClientStream
}

func (x *volumeServiceCopyPathStreamClient) Send(m *CopyPathRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *volumeServiceCopyPathStreamClient) CloseAndRecv() (*CopyPathResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(CopyPathResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *volumeServiceClient) MovePath(ctx context.Context, in *MovePathRequest, opts ...grpc.CallOption) (*MovePathResponse, error) {
	out := new(MovePathResponse)
	err := c.cc.Invoke(ctx, VolumeService_MovePath_FullMethodName, in, out, opts...)
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
	DeleteVolume(context.Context, *DeleteVolumeRequest) (*DeleteVolumeResponse, error)
	ListVolumes(context.Context, *ListVolumesRequest) (*ListVolumesResponse, error)
	ListPath(context.Context, *ListPathRequest) (*ListPathResponse, error)
	DeletePath(context.Context, *DeletePathRequest) (*DeletePathResponse, error)
	CopyPathStream(VolumeService_CopyPathStreamServer) error
	MovePath(context.Context, *MovePathRequest) (*MovePathResponse, error)
	mustEmbedUnimplementedVolumeServiceServer()
}

// UnimplementedVolumeServiceServer must be embedded to have forward compatible implementations.
type UnimplementedVolumeServiceServer struct {
}

func (UnimplementedVolumeServiceServer) GetOrCreateVolume(context.Context, *GetOrCreateVolumeRequest) (*GetOrCreateVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOrCreateVolume not implemented")
}
func (UnimplementedVolumeServiceServer) DeleteVolume(context.Context, *DeleteVolumeRequest) (*DeleteVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteVolume not implemented")
}
func (UnimplementedVolumeServiceServer) ListVolumes(context.Context, *ListVolumesRequest) (*ListVolumesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListVolumes not implemented")
}
func (UnimplementedVolumeServiceServer) ListPath(context.Context, *ListPathRequest) (*ListPathResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListPath not implemented")
}
func (UnimplementedVolumeServiceServer) DeletePath(context.Context, *DeletePathRequest) (*DeletePathResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeletePath not implemented")
}
func (UnimplementedVolumeServiceServer) CopyPathStream(VolumeService_CopyPathStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method CopyPathStream not implemented")
}
func (UnimplementedVolumeServiceServer) MovePath(context.Context, *MovePathRequest) (*MovePathResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MovePath not implemented")
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

func _VolumeService_DeleteVolume_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteVolumeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VolumeServiceServer).DeleteVolume(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: VolumeService_DeleteVolume_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VolumeServiceServer).DeleteVolume(ctx, req.(*DeleteVolumeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _VolumeService_ListVolumes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListVolumesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VolumeServiceServer).ListVolumes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: VolumeService_ListVolumes_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VolumeServiceServer).ListVolumes(ctx, req.(*ListVolumesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _VolumeService_ListPath_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListPathRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VolumeServiceServer).ListPath(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: VolumeService_ListPath_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VolumeServiceServer).ListPath(ctx, req.(*ListPathRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _VolumeService_DeletePath_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeletePathRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VolumeServiceServer).DeletePath(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: VolumeService_DeletePath_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VolumeServiceServer).DeletePath(ctx, req.(*DeletePathRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _VolumeService_CopyPathStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(VolumeServiceServer).CopyPathStream(&volumeServiceCopyPathStreamServer{stream})
}

type VolumeService_CopyPathStreamServer interface {
	SendAndClose(*CopyPathResponse) error
	Recv() (*CopyPathRequest, error)
	grpc.ServerStream
}

type volumeServiceCopyPathStreamServer struct {
	grpc.ServerStream
}

func (x *volumeServiceCopyPathStreamServer) SendAndClose(m *CopyPathResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *volumeServiceCopyPathStreamServer) Recv() (*CopyPathRequest, error) {
	m := new(CopyPathRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _VolumeService_MovePath_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MovePathRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VolumeServiceServer).MovePath(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: VolumeService_MovePath_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VolumeServiceServer).MovePath(ctx, req.(*MovePathRequest))
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
		{
			MethodName: "DeleteVolume",
			Handler:    _VolumeService_DeleteVolume_Handler,
		},
		{
			MethodName: "ListVolumes",
			Handler:    _VolumeService_ListVolumes_Handler,
		},
		{
			MethodName: "ListPath",
			Handler:    _VolumeService_ListPath_Handler,
		},
		{
			MethodName: "DeletePath",
			Handler:    _VolumeService_DeletePath_Handler,
		},
		{
			MethodName: "MovePath",
			Handler:    _VolumeService_MovePath_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "CopyPathStream",
			Handler:       _VolumeService_CopyPathStream_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "volume.proto",
}
