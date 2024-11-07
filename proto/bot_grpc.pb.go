// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: bot.proto

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
	BotService_StartBotServe_FullMethodName     = "/bot.BotService/StartBotServe"
	BotService_BotServeKeepAlive_FullMethodName = "/bot.BotService/BotServeKeepAlive"
	BotService_PopBotTask_FullMethodName        = "/bot.BotService/PopBotTask"
	BotService_PushBotMarker_FullMethodName     = "/bot.BotService/PushBotMarker"
	BotService_PushBotEvent_FullMethodName      = "/bot.BotService/PushBotEvent"
)

// BotServiceClient is the client API for BotService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BotServiceClient interface {
	StartBotServe(ctx context.Context, in *StartBotServeRequest, opts ...grpc.CallOption) (*StartBotServeResponse, error)
	BotServeKeepAlive(ctx context.Context, in *BotServeKeepAliveRequest, opts ...grpc.CallOption) (*BotServeKeepAliveResponse, error)
	PopBotTask(ctx context.Context, in *PopBotTaskRequest, opts ...grpc.CallOption) (*PopBotTaskResponse, error)
	PushBotMarker(ctx context.Context, in *PushBotMarkerRequest, opts ...grpc.CallOption) (*PushBotMarkerResponse, error)
	PushBotEvent(ctx context.Context, in *PushBotEventRequest, opts ...grpc.CallOption) (*PushBotEventResponse, error)
}

type botServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBotServiceClient(cc grpc.ClientConnInterface) BotServiceClient {
	return &botServiceClient{cc}
}

func (c *botServiceClient) StartBotServe(ctx context.Context, in *StartBotServeRequest, opts ...grpc.CallOption) (*StartBotServeResponse, error) {
	out := new(StartBotServeResponse)
	err := c.cc.Invoke(ctx, BotService_StartBotServe_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *botServiceClient) BotServeKeepAlive(ctx context.Context, in *BotServeKeepAliveRequest, opts ...grpc.CallOption) (*BotServeKeepAliveResponse, error) {
	out := new(BotServeKeepAliveResponse)
	err := c.cc.Invoke(ctx, BotService_BotServeKeepAlive_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *botServiceClient) PopBotTask(ctx context.Context, in *PopBotTaskRequest, opts ...grpc.CallOption) (*PopBotTaskResponse, error) {
	out := new(PopBotTaskResponse)
	err := c.cc.Invoke(ctx, BotService_PopBotTask_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *botServiceClient) PushBotMarker(ctx context.Context, in *PushBotMarkerRequest, opts ...grpc.CallOption) (*PushBotMarkerResponse, error) {
	out := new(PushBotMarkerResponse)
	err := c.cc.Invoke(ctx, BotService_PushBotMarker_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *botServiceClient) PushBotEvent(ctx context.Context, in *PushBotEventRequest, opts ...grpc.CallOption) (*PushBotEventResponse, error) {
	out := new(PushBotEventResponse)
	err := c.cc.Invoke(ctx, BotService_PushBotEvent_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BotServiceServer is the server API for BotService service.
// All implementations must embed UnimplementedBotServiceServer
// for forward compatibility
type BotServiceServer interface {
	StartBotServe(context.Context, *StartBotServeRequest) (*StartBotServeResponse, error)
	BotServeKeepAlive(context.Context, *BotServeKeepAliveRequest) (*BotServeKeepAliveResponse, error)
	PopBotTask(context.Context, *PopBotTaskRequest) (*PopBotTaskResponse, error)
	PushBotMarker(context.Context, *PushBotMarkerRequest) (*PushBotMarkerResponse, error)
	PushBotEvent(context.Context, *PushBotEventRequest) (*PushBotEventResponse, error)
	mustEmbedUnimplementedBotServiceServer()
}

// UnimplementedBotServiceServer must be embedded to have forward compatible implementations.
type UnimplementedBotServiceServer struct {
}

func (UnimplementedBotServiceServer) StartBotServe(context.Context, *StartBotServeRequest) (*StartBotServeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartBotServe not implemented")
}
func (UnimplementedBotServiceServer) BotServeKeepAlive(context.Context, *BotServeKeepAliveRequest) (*BotServeKeepAliveResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BotServeKeepAlive not implemented")
}
func (UnimplementedBotServiceServer) PopBotTask(context.Context, *PopBotTaskRequest) (*PopBotTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PopBotTask not implemented")
}
func (UnimplementedBotServiceServer) PushBotMarker(context.Context, *PushBotMarkerRequest) (*PushBotMarkerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PushBotMarker not implemented")
}
func (UnimplementedBotServiceServer) PushBotEvent(context.Context, *PushBotEventRequest) (*PushBotEventResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PushBotEvent not implemented")
}
func (UnimplementedBotServiceServer) mustEmbedUnimplementedBotServiceServer() {}

// UnsafeBotServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BotServiceServer will
// result in compilation errors.
type UnsafeBotServiceServer interface {
	mustEmbedUnimplementedBotServiceServer()
}

func RegisterBotServiceServer(s grpc.ServiceRegistrar, srv BotServiceServer) {
	s.RegisterService(&BotService_ServiceDesc, srv)
}

func _BotService_StartBotServe_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartBotServeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BotServiceServer).StartBotServe(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BotService_StartBotServe_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BotServiceServer).StartBotServe(ctx, req.(*StartBotServeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BotService_BotServeKeepAlive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BotServeKeepAliveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BotServiceServer).BotServeKeepAlive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BotService_BotServeKeepAlive_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BotServiceServer).BotServeKeepAlive(ctx, req.(*BotServeKeepAliveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BotService_PopBotTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PopBotTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BotServiceServer).PopBotTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BotService_PopBotTask_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BotServiceServer).PopBotTask(ctx, req.(*PopBotTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BotService_PushBotMarker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PushBotMarkerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BotServiceServer).PushBotMarker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BotService_PushBotMarker_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BotServiceServer).PushBotMarker(ctx, req.(*PushBotMarkerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BotService_PushBotEvent_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PushBotEventRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BotServiceServer).PushBotEvent(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BotService_PushBotEvent_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BotServiceServer).PushBotEvent(ctx, req.(*PushBotEventRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// BotService_ServiceDesc is the grpc.ServiceDesc for BotService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BotService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "bot.BotService",
	HandlerType: (*BotServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "StartBotServe",
			Handler:    _BotService_StartBotServe_Handler,
		},
		{
			MethodName: "BotServeKeepAlive",
			Handler:    _BotService_BotServeKeepAlive_Handler,
		},
		{
			MethodName: "PopBotTask",
			Handler:    _BotService_PopBotTask_Handler,
		},
		{
			MethodName: "PushBotMarker",
			Handler:    _BotService_PushBotMarker_Handler,
		},
		{
			MethodName: "PushBotEvent",
			Handler:    _BotService_PushBotEvent_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "bot.proto",
}
