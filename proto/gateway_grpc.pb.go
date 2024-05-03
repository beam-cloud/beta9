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
	GatewayService_Authorize_FullMethodName            = "/gateway.GatewayService/Authorize"
	GatewayService_SignPayload_FullMethodName          = "/gateway.GatewayService/SignPayload"
	GatewayService_HeadObject_FullMethodName           = "/gateway.GatewayService/HeadObject"
	GatewayService_PutObject_FullMethodName            = "/gateway.GatewayService/PutObject"
	GatewayService_PutObjectStream_FullMethodName      = "/gateway.GatewayService/PutObjectStream"
	GatewayService_ReplaceObjectContent_FullMethodName = "/gateway.GatewayService/ReplaceObjectContent"
	GatewayService_StartTask_FullMethodName            = "/gateway.GatewayService/StartTask"
	GatewayService_EndTask_FullMethodName              = "/gateway.GatewayService/EndTask"
	GatewayService_StopTasks_FullMethodName            = "/gateway.GatewayService/StopTasks"
	GatewayService_ListTasks_FullMethodName            = "/gateway.GatewayService/ListTasks"
	GatewayService_GetOrCreateStub_FullMethodName      = "/gateway.GatewayService/GetOrCreateStub"
	GatewayService_DeployStub_FullMethodName           = "/gateway.GatewayService/DeployStub"
)

// GatewayServiceClient is the client API for GatewayService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type GatewayServiceClient interface {
	Authorize(ctx context.Context, in *AuthorizeRequest, opts ...grpc.CallOption) (*AuthorizeResponse, error)
	SignPayload(ctx context.Context, in *SignPayloadRequest, opts ...grpc.CallOption) (*SignPayloadResponse, error)
	HeadObject(ctx context.Context, in *HeadObjectRequest, opts ...grpc.CallOption) (*HeadObjectResponse, error)
	PutObject(ctx context.Context, in *PutObjectRequest, opts ...grpc.CallOption) (*PutObjectResponse, error)
	PutObjectStream(ctx context.Context, opts ...grpc.CallOption) (GatewayService_PutObjectStreamClient, error)
	ReplaceObjectContent(ctx context.Context, opts ...grpc.CallOption) (GatewayService_ReplaceObjectContentClient, error)
	StartTask(ctx context.Context, in *StartTaskRequest, opts ...grpc.CallOption) (*StartTaskResponse, error)
	EndTask(ctx context.Context, in *EndTaskRequest, opts ...grpc.CallOption) (*EndTaskResponse, error)
	StopTasks(ctx context.Context, in *StopTasksRequest, opts ...grpc.CallOption) (*StopTasksResponse, error)
	ListTasks(ctx context.Context, in *ListTasksRequest, opts ...grpc.CallOption) (*ListTasksResponse, error)
	GetOrCreateStub(ctx context.Context, in *GetOrCreateStubRequest, opts ...grpc.CallOption) (*GetOrCreateStubResponse, error)
	DeployStub(ctx context.Context, in *DeployStubRequest, opts ...grpc.CallOption) (*DeployStubResponse, error)
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

func (c *gatewayServiceClient) SignPayload(ctx context.Context, in *SignPayloadRequest, opts ...grpc.CallOption) (*SignPayloadResponse, error) {
	out := new(SignPayloadResponse)
	err := c.cc.Invoke(ctx, GatewayService_SignPayload_FullMethodName, in, out, opts...)
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

func (c *gatewayServiceClient) PutObjectStream(ctx context.Context, opts ...grpc.CallOption) (GatewayService_PutObjectStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &GatewayService_ServiceDesc.Streams[0], GatewayService_PutObjectStream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &gatewayServicePutObjectStreamClient{stream}
	return x, nil
}

type GatewayService_PutObjectStreamClient interface {
	Send(*PutObjectRequest) error
	CloseAndRecv() (*PutObjectResponse, error)
	grpc.ClientStream
}

type gatewayServicePutObjectStreamClient struct {
	grpc.ClientStream
}

func (x *gatewayServicePutObjectStreamClient) Send(m *PutObjectRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *gatewayServicePutObjectStreamClient) CloseAndRecv() (*PutObjectResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(PutObjectResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *gatewayServiceClient) ReplaceObjectContent(ctx context.Context, opts ...grpc.CallOption) (GatewayService_ReplaceObjectContentClient, error) {
	stream, err := c.cc.NewStream(ctx, &GatewayService_ServiceDesc.Streams[1], GatewayService_ReplaceObjectContent_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &gatewayServiceReplaceObjectContentClient{stream}
	return x, nil
}

type GatewayService_ReplaceObjectContentClient interface {
	Send(*ReplaceObjectContentRequest) error
	CloseAndRecv() (*ReplaceObjectContentResponse, error)
	grpc.ClientStream
}

type gatewayServiceReplaceObjectContentClient struct {
	grpc.ClientStream
}

func (x *gatewayServiceReplaceObjectContentClient) Send(m *ReplaceObjectContentRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *gatewayServiceReplaceObjectContentClient) CloseAndRecv() (*ReplaceObjectContentResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(ReplaceObjectContentResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
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

func (c *gatewayServiceClient) StopTasks(ctx context.Context, in *StopTasksRequest, opts ...grpc.CallOption) (*StopTasksResponse, error) {
	out := new(StopTasksResponse)
	err := c.cc.Invoke(ctx, GatewayService_StopTasks_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) ListTasks(ctx context.Context, in *ListTasksRequest, opts ...grpc.CallOption) (*ListTasksResponse, error) {
	out := new(ListTasksResponse)
	err := c.cc.Invoke(ctx, GatewayService_ListTasks_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) GetOrCreateStub(ctx context.Context, in *GetOrCreateStubRequest, opts ...grpc.CallOption) (*GetOrCreateStubResponse, error) {
	out := new(GetOrCreateStubResponse)
	err := c.cc.Invoke(ctx, GatewayService_GetOrCreateStub_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayServiceClient) DeployStub(ctx context.Context, in *DeployStubRequest, opts ...grpc.CallOption) (*DeployStubResponse, error) {
	out := new(DeployStubResponse)
	err := c.cc.Invoke(ctx, GatewayService_DeployStub_FullMethodName, in, out, opts...)
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
	SignPayload(context.Context, *SignPayloadRequest) (*SignPayloadResponse, error)
	HeadObject(context.Context, *HeadObjectRequest) (*HeadObjectResponse, error)
	PutObject(context.Context, *PutObjectRequest) (*PutObjectResponse, error)
	PutObjectStream(GatewayService_PutObjectStreamServer) error
	ReplaceObjectContent(GatewayService_ReplaceObjectContentServer) error
	StartTask(context.Context, *StartTaskRequest) (*StartTaskResponse, error)
	EndTask(context.Context, *EndTaskRequest) (*EndTaskResponse, error)
	StopTasks(context.Context, *StopTasksRequest) (*StopTasksResponse, error)
	ListTasks(context.Context, *ListTasksRequest) (*ListTasksResponse, error)
	GetOrCreateStub(context.Context, *GetOrCreateStubRequest) (*GetOrCreateStubResponse, error)
	DeployStub(context.Context, *DeployStubRequest) (*DeployStubResponse, error)
	mustEmbedUnimplementedGatewayServiceServer()
}

// UnimplementedGatewayServiceServer must be embedded to have forward compatible implementations.
type UnimplementedGatewayServiceServer struct {
}

func (UnimplementedGatewayServiceServer) Authorize(context.Context, *AuthorizeRequest) (*AuthorizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Authorize not implemented")
}
func (UnimplementedGatewayServiceServer) SignPayload(context.Context, *SignPayloadRequest) (*SignPayloadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SignPayload not implemented")
}
func (UnimplementedGatewayServiceServer) HeadObject(context.Context, *HeadObjectRequest) (*HeadObjectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HeadObject not implemented")
}
func (UnimplementedGatewayServiceServer) PutObject(context.Context, *PutObjectRequest) (*PutObjectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PutObject not implemented")
}
func (UnimplementedGatewayServiceServer) PutObjectStream(GatewayService_PutObjectStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method PutObjectStream not implemented")
}
func (UnimplementedGatewayServiceServer) ReplaceObjectContent(GatewayService_ReplaceObjectContentServer) error {
	return status.Errorf(codes.Unimplemented, "method ReplaceObjectContent not implemented")
}
func (UnimplementedGatewayServiceServer) StartTask(context.Context, *StartTaskRequest) (*StartTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartTask not implemented")
}
func (UnimplementedGatewayServiceServer) EndTask(context.Context, *EndTaskRequest) (*EndTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method EndTask not implemented")
}
func (UnimplementedGatewayServiceServer) StopTasks(context.Context, *StopTasksRequest) (*StopTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StopTasks not implemented")
}
func (UnimplementedGatewayServiceServer) ListTasks(context.Context, *ListTasksRequest) (*ListTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListTasks not implemented")
}
func (UnimplementedGatewayServiceServer) GetOrCreateStub(context.Context, *GetOrCreateStubRequest) (*GetOrCreateStubResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOrCreateStub not implemented")
}
func (UnimplementedGatewayServiceServer) DeployStub(context.Context, *DeployStubRequest) (*DeployStubResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeployStub not implemented")
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

func _GatewayService_SignPayload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SignPayloadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).SignPayload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_SignPayload_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).SignPayload(ctx, req.(*SignPayloadRequest))
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

func _GatewayService_PutObjectStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(GatewayServiceServer).PutObjectStream(&gatewayServicePutObjectStreamServer{stream})
}

type GatewayService_PutObjectStreamServer interface {
	SendAndClose(*PutObjectResponse) error
	Recv() (*PutObjectRequest, error)
	grpc.ServerStream
}

type gatewayServicePutObjectStreamServer struct {
	grpc.ServerStream
}

func (x *gatewayServicePutObjectStreamServer) SendAndClose(m *PutObjectResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *gatewayServicePutObjectStreamServer) Recv() (*PutObjectRequest, error) {
	m := new(PutObjectRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _GatewayService_ReplaceObjectContent_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(GatewayServiceServer).ReplaceObjectContent(&gatewayServiceReplaceObjectContentServer{stream})
}

type GatewayService_ReplaceObjectContentServer interface {
	SendAndClose(*ReplaceObjectContentResponse) error
	Recv() (*ReplaceObjectContentRequest, error)
	grpc.ServerStream
}

type gatewayServiceReplaceObjectContentServer struct {
	grpc.ServerStream
}

func (x *gatewayServiceReplaceObjectContentServer) SendAndClose(m *ReplaceObjectContentResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *gatewayServiceReplaceObjectContentServer) Recv() (*ReplaceObjectContentRequest, error) {
	m := new(ReplaceObjectContentRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
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

func _GatewayService_StopTasks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StopTasksRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).StopTasks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_StopTasks_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).StopTasks(ctx, req.(*StopTasksRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GatewayService_ListTasks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListTasksRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).ListTasks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_ListTasks_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).ListTasks(ctx, req.(*ListTasksRequest))
	}
	return interceptor(ctx, in, info, handler)
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

func _GatewayService_DeployStub_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeployStubRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GatewayServiceServer).DeployStub(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: GatewayService_DeployStub_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GatewayServiceServer).DeployStub(ctx, req.(*DeployStubRequest))
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
			MethodName: "SignPayload",
			Handler:    _GatewayService_SignPayload_Handler,
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
			MethodName: "StartTask",
			Handler:    _GatewayService_StartTask_Handler,
		},
		{
			MethodName: "EndTask",
			Handler:    _GatewayService_EndTask_Handler,
		},
		{
			MethodName: "StopTasks",
			Handler:    _GatewayService_StopTasks_Handler,
		},
		{
			MethodName: "ListTasks",
			Handler:    _GatewayService_ListTasks_Handler,
		},
		{
			MethodName: "GetOrCreateStub",
			Handler:    _GatewayService_GetOrCreateStub_Handler,
		},
		{
			MethodName: "DeployStub",
			Handler:    _GatewayService_DeployStub_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "PutObjectStream",
			Handler:       _GatewayService_PutObjectStream_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "ReplaceObjectContent",
			Handler:       _GatewayService_ReplaceObjectContent_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "gateway.proto",
}
