// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: taskqueue.proto

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
	TaskQueueService_TaskQueuePut_FullMethodName    = "/taskqueue.TaskQueueService/TaskQueuePut"
	TaskQueueService_TaskQueuePop_FullMethodName    = "/taskqueue.TaskQueueService/TaskQueuePop"
	TaskQueueService_TaskQueueLength_FullMethodName = "/taskqueue.TaskQueueService/TaskQueueLength"
)

// TaskQueueServiceClient is the client API for TaskQueueService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TaskQueueServiceClient interface {
	TaskQueuePut(ctx context.Context, in *TaskQueuePutRequest, opts ...grpc.CallOption) (*TaskQueuePutResponse, error)
	TaskQueuePop(ctx context.Context, in *TaskQueuePopRequest, opts ...grpc.CallOption) (*TaskQueuePopResponse, error)
	TaskQueueLength(ctx context.Context, in *TaskQueueLengthRequest, opts ...grpc.CallOption) (*TaskQueueLengthResponse, error)
}

type taskQueueServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewTaskQueueServiceClient(cc grpc.ClientConnInterface) TaskQueueServiceClient {
	return &taskQueueServiceClient{cc}
}

func (c *taskQueueServiceClient) TaskQueuePut(ctx context.Context, in *TaskQueuePutRequest, opts ...grpc.CallOption) (*TaskQueuePutResponse, error) {
	out := new(TaskQueuePutResponse)
	err := c.cc.Invoke(ctx, TaskQueueService_TaskQueuePut_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *taskQueueServiceClient) TaskQueuePop(ctx context.Context, in *TaskQueuePopRequest, opts ...grpc.CallOption) (*TaskQueuePopResponse, error) {
	out := new(TaskQueuePopResponse)
	err := c.cc.Invoke(ctx, TaskQueueService_TaskQueuePop_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *taskQueueServiceClient) TaskQueueLength(ctx context.Context, in *TaskQueueLengthRequest, opts ...grpc.CallOption) (*TaskQueueLengthResponse, error) {
	out := new(TaskQueueLengthResponse)
	err := c.cc.Invoke(ctx, TaskQueueService_TaskQueueLength_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TaskQueueServiceServer is the server API for TaskQueueService service.
// All implementations must embed UnimplementedTaskQueueServiceServer
// for forward compatibility
type TaskQueueServiceServer interface {
	TaskQueuePut(context.Context, *TaskQueuePutRequest) (*TaskQueuePutResponse, error)
	TaskQueuePop(context.Context, *TaskQueuePopRequest) (*TaskQueuePopResponse, error)
	TaskQueueLength(context.Context, *TaskQueueLengthRequest) (*TaskQueueLengthResponse, error)
	mustEmbedUnimplementedTaskQueueServiceServer()
}

// UnimplementedTaskQueueServiceServer must be embedded to have forward compatible implementations.
type UnimplementedTaskQueueServiceServer struct {
}

func (UnimplementedTaskQueueServiceServer) TaskQueuePut(context.Context, *TaskQueuePutRequest) (*TaskQueuePutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TaskQueuePut not implemented")
}
func (UnimplementedTaskQueueServiceServer) TaskQueuePop(context.Context, *TaskQueuePopRequest) (*TaskQueuePopResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TaskQueuePop not implemented")
}
func (UnimplementedTaskQueueServiceServer) TaskQueueLength(context.Context, *TaskQueueLengthRequest) (*TaskQueueLengthResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TaskQueueLength not implemented")
}
func (UnimplementedTaskQueueServiceServer) mustEmbedUnimplementedTaskQueueServiceServer() {}

// UnsafeTaskQueueServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TaskQueueServiceServer will
// result in compilation errors.
type UnsafeTaskQueueServiceServer interface {
	mustEmbedUnimplementedTaskQueueServiceServer()
}

func RegisterTaskQueueServiceServer(s grpc.ServiceRegistrar, srv TaskQueueServiceServer) {
	s.RegisterService(&TaskQueueService_ServiceDesc, srv)
}

func _TaskQueueService_TaskQueuePut_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TaskQueuePutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TaskQueueServiceServer).TaskQueuePut(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TaskQueueService_TaskQueuePut_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TaskQueueServiceServer).TaskQueuePut(ctx, req.(*TaskQueuePutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TaskQueueService_TaskQueuePop_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TaskQueuePopRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TaskQueueServiceServer).TaskQueuePop(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TaskQueueService_TaskQueuePop_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TaskQueueServiceServer).TaskQueuePop(ctx, req.(*TaskQueuePopRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TaskQueueService_TaskQueueLength_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TaskQueueLengthRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TaskQueueServiceServer).TaskQueueLength(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TaskQueueService_TaskQueueLength_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TaskQueueServiceServer).TaskQueueLength(ctx, req.(*TaskQueueLengthRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// TaskQueueService_ServiceDesc is the grpc.ServiceDesc for TaskQueueService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TaskQueueService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "taskqueue.TaskQueueService",
	HandlerType: (*TaskQueueServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "TaskQueuePut",
			Handler:    _TaskQueueService_TaskQueuePut_Handler,
		},
		{
			MethodName: "TaskQueuePop",
			Handler:    _TaskQueueService_TaskQueuePop_Handler,
		},
		{
			MethodName: "TaskQueueLength",
			Handler:    _TaskQueueService_TaskQueueLength_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "taskqueue.proto",
}
