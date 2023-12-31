// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: gateway.proto

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
	GatewayService_Authorize_FullMethodName       = "/gateway.GatewayService/Authorize"
	GatewayService_HeadObject_FullMethodName      = "/gateway.GatewayService/HeadObject"
	GatewayService_PutObject_FullMethodName       = "/gateway.GatewayService/PutObject"
	GatewayService_GetTaskStream_FullMethodName   = "/gateway.GatewayService/GetTaskStream"
	GatewayService_GetNextTask_FullMethodName     = "/gateway.GatewayService/GetNextTask"
	GatewayService_StartTask_FullMethodName       = "/gateway.GatewayService/StartTask"
	GatewayService_EndTask_FullMethodName         = "/gateway.GatewayService/EndTask"
	GatewayService_MonitorTask_FullMethodName     = "/gateway.GatewayService/MonitorTask"
	GatewayService_GetOrCreateStub_FullMethodName = "/gateway.GatewayService/GetOrCreateStub"
)

// GatewayServiceClient is the client API for GatewayService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type GatewayServiceClient interface {
	Authorize(ctx context.Context, in *AuthorizeRequest, opts ...grpc.CallOption) (*AuthorizeResponse, error)
	HeadObject(ctx context.Context, in *HeadObjectRequest, opts ...grpc.CallOption) (*HeadObjectResponse, error)
	PutObject(ctx context.Context, in *PutObjectRequest, opts ...grpc.CallOption) (*PutObjectResponse, error)
	GetTaskStream(ctx context.Context, in *GetTaskStreamRequest, opts ...grpc.CallOption) (GatewayService_GetTaskStreamClient, error)
	GetNextTask(ctx context.Context, in *GetNextTaskRequest, opts ...grpc.CallOption) (*GetNextTaskResponse, error)
	StartTask(ctx context.Context, in *StartTaskRequest, opts ...grpc.CallOption) (*StartTaskResponse, error)
	EndTask(ctx context.Context, in *EndTaskRequest, opts ...grpc.CallOption) (*EndTaskResponse, error)
	MonitorTask(ctx context.Context, in *MonitorTaskRequest, opts ...grpc.CallOption) (GatewayService_MonitorTaskClient, error)
	GetOrCreateStub(ctx context.Context, in *GetOrCreateStubRequest, opts ...grpc.CallOption) (*GetOrCreateStubResponse, error)
}

type gatewayServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewGatewayServiceClient(cc grpc.ClientConnInterface) GatewayServiceClient {
	return &gatewayServiceClient{cc}
}

func (c *gatewayServiceClient) Authorize(ctx context.Context, in *AuthorizeRequest, opts ...grpc.CallOption) (*AuthorizeResponse, error) {
	out := new(AuthorizeResponse)
	err := c.cc.Invoke(ctx, GatewayService_Authorize_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) HeadObject(ctx context.Context, in *HeadObjectRequest, opts ...grpc.CallOption) (*HeadObjectResponse, error) {
	out := new(HeadObjectResponse)
	err := c.cc.Invoke(ctx, GatewayService_HeadObject_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) PutObject(ctx context.Context, in *PutObjectRequest, opts ...grpc.CallOption) (*PutObjectResponse, error) {
	out := new(PutObjectResponse)
	err := c.cc.Invoke(ctx, GatewayService_PutObject_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) GetTaskStream(ctx context.Context, in *GetTaskStreamRequest, opts ...grpc.CallOption) (GatewayService_GetTaskStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &GatewayService_ServiceDesc.Streams[0], GatewayService_GetTaskStream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &gatewayServiceGetTaskStreamClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type GatewayService_GetTaskStreamClient interface {
	Recv() (*TaskStreamResponse, error)
	grpc.ClientStream
}

type gatewayServiceGetTaskStreamClient struct {
	grpc.ClientStream
}

func (x *gatewayServiceGetTaskStreamClient) Recv() (*TaskStreamResponse, error) {
	m := new(TaskStreamResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *gatewayServiceClient) GetNextTask(ctx context.Context, in *GetNextTaskRequest, opts ...grpc.CallOption) (*GetNextTaskResponse, error) {
	out := new(GetNextTaskResponse)
	err := c.cc.Invoke(ctx, GatewayService_GetNextTask_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) StartTask(ctx context.Context, in *StartTaskRequest, opts ...grpc.CallOption) (*StartTaskResponse, error) {
	out := new(StartTaskResponse)
	err := c.cc.Invoke(ctx, GatewayService_StartTask_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) EndTask(ctx context.Context, in *EndTaskRequest, opts ...grpc.CallOption) (*EndTaskResponse, error) {
	out := new(EndTaskResponse)
	err := c.cc.Invoke(ctx, GatewayService_EndTask_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) MonitorTask(ctx context.Context, in *MonitorTaskRequest, opts ...grpc.CallOption) (GatewayService_MonitorTaskClient, error) {
	stream, err := c.cc.NewStream(ctx, &GatewayService_ServiceDesc.Streams[1], GatewayService_MonitorTask_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &gatewayServiceMonitorTaskClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type GatewayService_MonitorTaskClient interface {
	Recv() (*MonitorTaskResponse, error)
	grpc.ClientStream
}

type gatewayServiceMonitorTaskClient struct {
	grpc.ClientStream
}

func (x *gatewayServiceMonitorTaskClient) Recv() (*MonitorTaskResponse, error) {
	m := new(MonitorTaskResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *gatewayServiceClient) GetOrCreateStub(ctx context.Context, in *GetOrCreateStubRequest, opts ...grpc.CallOption) (*GetOrCreateStubResponse, error) {
	out := new(GetOrCreateStubResponse)
	err := c.cc.Invoke(ctx, GatewayService_GetOrCreateStub_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GatewayServiceServer is the server API for GatewayService service.
// All implementations must embed UnimplementedGatewayServiceServer
// for forward compatibility
type GatewayServiceServer interface {
	Authorize(context.Context, *AuthorizeRequest) (*AuthorizeResponse, error)
	HeadObject(context.Context, *HeadObjectRequest) (*HeadObjectResponse, error)
	PutObject(context.Context, *PutObjectRequest) (*PutObjectResponse, error)
	GetTaskStream(*GetTaskStreamRequest, GatewayService_GetTaskStreamServer) error
	GetNextTask(context.Context, *GetNextTaskRequest) (*GetNextTaskResponse, error)
	StartTask(context.Context, *StartTaskRequest) (*StartTaskResponse, error)
	EndTask(context.Context, *EndTaskRequest) (*EndTaskResponse, error)
	MonitorTask(*MonitorTaskRequest, GatewayService_MonitorTaskServer) error
	GetOrCreateStub(context.Context, *GetOrCreateStubRequest) (*GetOrCreateStubResponse, error)
	mustEmbedUnimplementedGatewayServiceServer()
}

// UnimplementedGatewayServiceServer must be embedded to have forward compatible implementations.
type UnimplementedGatewayServiceServer struct {
}

func (UnimplementedGatewayServiceServer) Authorize(context.Context, *AuthorizeRequest) (*AuthorizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Authorize not implemented")
}
func (UnimplementedGatewayServiceServer) HeadObject(context.Context, *HeadObjectRequest) (*HeadObjectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HeadObject not implemented")
}
func (UnimplementedGatewayServiceServer) PutObject(context.Context, *PutObjectRequest) (*PutObjectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PutObject not implemented")
}
func (UnimplementedGatewayServiceServer) GetTaskStream(*GetTaskStreamRequest, GatewayService_GetTaskStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method GetTaskStream not implemented")
}
func (UnimplementedGatewayServiceServer) GetNextTask(context.Context, *GetNextTaskRequest) (*GetNextTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNextTask not implemented")
}
func (UnimplementedGatewayServiceServer) StartTask(context.Context, *StartTaskRequest) (*StartTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartTask not implemented")
}
func (UnimplementedGatewayServiceServer) EndTask(context.Context, *EndTaskRequest) (*EndTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EndTask not implemented")
}
func (UnimplementedGatewayServiceServer) MonitorTask(*MonitorTaskRequest, GatewayService_MonitorTaskServer) error {
	return status.Errorf(codes.Unimplemented, "method MonitorTask not implemented")
}
func (UnimplementedGatewayServiceServer) GetOrCreateStub(context.Context, *GetOrCreateStubRequest) (*GetOrCreateStubResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOrCreateStub not implemented")
}
func (UnimplementedGatewayServiceServer) mustEmbedUnimplementedGatewayServiceServer() {}

// UnsafeGatewayServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to GatewayServiceServer will
// result in compilation errors.
type UnsafeGatewayServiceServer interface {
	mustEmbedUnimplementedGatewayServiceServer()
}

func RegisterGatewayServiceServer(s grpc.ServiceRegistrar, srv GatewayServiceServer) {
	s.RegisterService(&GatewayService_ServiceDesc, srv)
}

func _GatewayService_Authorize_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthorizeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).Authorize(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_Authorize_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).Authorize(ctx, req.(*AuthorizeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_HeadObject_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeadObjectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).HeadObject(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_HeadObject_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).HeadObject(ctx, req.(*HeadObjectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_PutObject_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutObjectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).PutObject(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_PutObject_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).PutObject(ctx, req.(*PutObjectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_GetTaskStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(GetTaskStreamRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(GatewayServiceServer).GetTaskStream(m, &gatewayServiceGetTaskStreamServer{stream})
}

type GatewayService_GetTaskStreamServer interface {
	Send(*TaskStreamResponse) error
	grpc.ServerStream
}

type gatewayServiceGetTaskStreamServer struct {
	grpc.ServerStream
}

func (x *gatewayServiceGetTaskStreamServer) Send(m *TaskStreamResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _GatewayService_GetNextTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetNextTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).GetNextTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_GetNextTask_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).GetNextTask(ctx, req.(*GetNextTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_StartTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).StartTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_StartTask_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).StartTask(ctx, req.(*StartTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_EndTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EndTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).EndTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_EndTask_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).EndTask(ctx, req.(*EndTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_MonitorTask_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(MonitorTaskRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(GatewayServiceServer).MonitorTask(m, &gatewayServiceMonitorTaskServer{stream})
}

type GatewayService_MonitorTaskServer interface {
	Send(*MonitorTaskResponse) error
	grpc.ServerStream
}

type gatewayServiceMonitorTaskServer struct {
	grpc.ServerStream
}

func (x *gatewayServiceMonitorTaskServer) Send(m *MonitorTaskResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _GatewayService_GetOrCreateStub_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOrCreateStubRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).GetOrCreateStub(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_GetOrCreateStub_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).GetOrCreateStub(ctx, req.(*GetOrCreateStubRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// GatewayService_ServiceDesc is the grpc.ServiceDesc for GatewayService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var GatewayService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gateway.GatewayService",
	HandlerType: (*GatewayServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Authorize",
			Handler:    _GatewayService_Authorize_Handler,
		},
		{
			MethodName: "HeadObject",
			Handler:    _GatewayService_HeadObject_Handler,
		},
		{
			MethodName: "PutObject",
			Handler:    _GatewayService_PutObject_Handler,
		},
		{
			MethodName: "GetNextTask",
			Handler:    _GatewayService_GetNextTask_Handler,
		},
		{
			MethodName: "StartTask",
			Handler:    _GatewayService_StartTask_Handler,
		},
		{
			MethodName: "EndTask",
			Handler:    _GatewayService_EndTask_Handler,
		},
		{
			MethodName: "GetOrCreateStub",
			Handler:    _GatewayService_GetOrCreateStub_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "GetTaskStream",
			Handler:       _GatewayService_GetTaskStream_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "MonitorTask",
			Handler:       _GatewayService_MonitorTask_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "gateway.proto",
}
