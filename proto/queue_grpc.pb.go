// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: queue.proto

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
	SimpleQueueService_Enqueue_FullMethodName = "/simplequeue.SimpleQueueService/Enqueue"
	SimpleQueueService_Dequeue_FullMethodName = "/simplequeue.SimpleQueueService/Dequeue"
	SimpleQueueService_Peek_FullMethodName    = "/simplequeue.SimpleQueueService/Peek"
	SimpleQueueService_Empty_FullMethodName   = "/simplequeue.SimpleQueueService/Empty"
	SimpleQueueService_Size_FullMethodName    = "/simplequeue.SimpleQueueService/Size"
)

// SimpleQueueServiceClient is the client API for SimpleQueueService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SimpleQueueServiceClient interface {
	Enqueue(ctx context.Context, in *SimpleQueueEnqueueRequest, opts ...grpc.CallOption) (*SimpleQueueEnqueueResponse, error)
	Dequeue(ctx context.Context, in *SimpleQueueDequeueRequest, opts ...grpc.CallOption) (*SimpleQueueDequeueResponse, error)
	Peek(ctx context.Context, in *SimpleQueueRequest, opts ...grpc.CallOption) (*SimpleQueuePeekResponse, error)
	Empty(ctx context.Context, in *SimpleQueueRequest, opts ...grpc.CallOption) (*SimpleQueueEmptyResponse, error)
	Size(ctx context.Context, in *SimpleQueueRequest, opts ...grpc.CallOption) (*SimpleQueueSizeResponse, error)
}

type simpleQueueServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSimpleQueueServiceClient(cc grpc.ClientConnInterface) SimpleQueueServiceClient {
	return &simpleQueueServiceClient{cc}
}

func (c *simpleQueueServiceClient) Enqueue(ctx context.Context, in *SimpleQueueEnqueueRequest, opts ...grpc.CallOption) (*SimpleQueueEnqueueResponse, error) {
	out := new(SimpleQueueEnqueueResponse)
	err := c.cc.Invoke(ctx, SimpleQueueService_Enqueue_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simpleQueueServiceClient) Dequeue(ctx context.Context, in *SimpleQueueDequeueRequest, opts ...grpc.CallOption) (*SimpleQueueDequeueResponse, error) {
	out := new(SimpleQueueDequeueResponse)
	err := c.cc.Invoke(ctx, SimpleQueueService_Dequeue_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simpleQueueServiceClient) Peek(ctx context.Context, in *SimpleQueueRequest, opts ...grpc.CallOption) (*SimpleQueuePeekResponse, error) {
	out := new(SimpleQueuePeekResponse)
	err := c.cc.Invoke(ctx, SimpleQueueService_Peek_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simpleQueueServiceClient) Empty(ctx context.Context, in *SimpleQueueRequest, opts ...grpc.CallOption) (*SimpleQueueEmptyResponse, error) {
	out := new(SimpleQueueEmptyResponse)
	err := c.cc.Invoke(ctx, SimpleQueueService_Empty_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simpleQueueServiceClient) Size(ctx context.Context, in *SimpleQueueRequest, opts ...grpc.CallOption) (*SimpleQueueSizeResponse, error) {
	out := new(SimpleQueueSizeResponse)
	err := c.cc.Invoke(ctx, SimpleQueueService_Size_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SimpleQueueServiceServer is the server API for SimpleQueueService service.
// All implementations must embed UnimplementedSimpleQueueServiceServer
// for forward compatibility
type SimpleQueueServiceServer interface {
	Enqueue(context.Context, *SimpleQueueEnqueueRequest) (*SimpleQueueEnqueueResponse, error)
	Dequeue(context.Context, *SimpleQueueDequeueRequest) (*SimpleQueueDequeueResponse, error)
	Peek(context.Context, *SimpleQueueRequest) (*SimpleQueuePeekResponse, error)
	Empty(context.Context, *SimpleQueueRequest) (*SimpleQueueEmptyResponse, error)
	Size(context.Context, *SimpleQueueRequest) (*SimpleQueueSizeResponse, error)
	mustEmbedUnimplementedSimpleQueueServiceServer()
}

// UnimplementedSimpleQueueServiceServer must be embedded to have forward compatible implementations.
type UnimplementedSimpleQueueServiceServer struct {
}

func (UnimplementedSimpleQueueServiceServer) Enqueue(context.Context, *SimpleQueueEnqueueRequest) (*SimpleQueueEnqueueResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Enqueue not implemented")
}
func (UnimplementedSimpleQueueServiceServer) Dequeue(context.Context, *SimpleQueueDequeueRequest) (*SimpleQueueDequeueResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Dequeue not implemented")
}
func (UnimplementedSimpleQueueServiceServer) Peek(context.Context, *SimpleQueueRequest) (*SimpleQueuePeekResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Peek not implemented")
}
func (UnimplementedSimpleQueueServiceServer) Empty(context.Context, *SimpleQueueRequest) (*SimpleQueueEmptyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Empty not implemented")
}
func (UnimplementedSimpleQueueServiceServer) Size(context.Context, *SimpleQueueRequest) (*SimpleQueueSizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Size not implemented")
}
func (UnimplementedSimpleQueueServiceServer) mustEmbedUnimplementedSimpleQueueServiceServer() {}

// UnsafeSimpleQueueServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SimpleQueueServiceServer will
// result in compilation errors.
type UnsafeSimpleQueueServiceServer interface {
	mustEmbedUnimplementedSimpleQueueServiceServer()
}

func RegisterSimpleQueueServiceServer(s grpc.ServiceRegistrar, srv SimpleQueueServiceServer) {
	s.RegisterService(&SimpleQueueService_ServiceDesc, srv)
}

func _SimpleQueueService_Enqueue_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimpleQueueEnqueueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleQueueServiceServer).Enqueue(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SimpleQueueService_Enqueue_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleQueueServiceServer).Enqueue(ctx, req.(*SimpleQueueEnqueueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimpleQueueService_Dequeue_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimpleQueueDequeueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleQueueServiceServer).Dequeue(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SimpleQueueService_Dequeue_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleQueueServiceServer).Dequeue(ctx, req.(*SimpleQueueDequeueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimpleQueueService_Peek_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimpleQueueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleQueueServiceServer).Peek(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SimpleQueueService_Peek_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleQueueServiceServer).Peek(ctx, req.(*SimpleQueueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimpleQueueService_Empty_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimpleQueueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleQueueServiceServer).Empty(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SimpleQueueService_Empty_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleQueueServiceServer).Empty(ctx, req.(*SimpleQueueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimpleQueueService_Size_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimpleQueueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleQueueServiceServer).Size(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: SimpleQueueService_Size_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleQueueServiceServer).Size(ctx, req.(*SimpleQueueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// SimpleQueueService_ServiceDesc is the grpc.ServiceDesc for SimpleQueueService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SimpleQueueService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "simplequeue.SimpleQueueService",
	HandlerType: (*SimpleQueueServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Enqueue",
			Handler:    _SimpleQueueService_Enqueue_Handler,
		},
		{
			MethodName: "Dequeue",
			Handler:    _SimpleQueueService_Dequeue_Handler,
		},
		{
			MethodName: "Peek",
			Handler:    _SimpleQueueService_Peek_Handler,
		},
		{
			MethodName: "Empty",
			Handler:    _SimpleQueueService_Empty_Handler,
		},
		{
			MethodName: "Size",
			Handler:    _SimpleQueueService_Size_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "queue.proto",
}
