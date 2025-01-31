// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: worker_repo.proto

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
	WorkerRepositoryService_GetNextContainerRequest_FullMethodName   = "/WorkerRepositoryService/GetNextContainerRequest"
	WorkerRepositoryService_SetImagePullLock_FullMethodName          = "/WorkerRepositoryService/SetImagePullLock"
	WorkerRepositoryService_RemoveImagePullLock_FullMethodName       = "/WorkerRepositoryService/RemoveImagePullLock"
	WorkerRepositoryService_AddContainerToWorker_FullMethodName      = "/WorkerRepositoryService/AddContainerToWorker"
	WorkerRepositoryService_RemoveContainerFromWorker_FullMethodName = "/WorkerRepositoryService/RemoveContainerFromWorker"
	WorkerRepositoryService_GetWorkerById_FullMethodName             = "/WorkerRepositoryService/GetWorkerById"
	WorkerRepositoryService_ToggleWorkerAvailable_FullMethodName     = "/WorkerRepositoryService/ToggleWorkerAvailable"
	WorkerRepositoryService_RemoveWorker_FullMethodName              = "/WorkerRepositoryService/RemoveWorker"
	WorkerRepositoryService_UpdateWorkerCapacity_FullMethodName      = "/WorkerRepositoryService/UpdateWorkerCapacity"
	WorkerRepositoryService_SetWorkerKeepAlive_FullMethodName        = "/WorkerRepositoryService/SetWorkerKeepAlive"
	WorkerRepositoryService_SetNetworkLock_FullMethodName            = "/WorkerRepositoryService/SetNetworkLock"
	WorkerRepositoryService_RemoveNetworkLock_FullMethodName         = "/WorkerRepositoryService/RemoveNetworkLock"
	WorkerRepositoryService_SetContainerIp_FullMethodName            = "/WorkerRepositoryService/SetContainerIp"
	WorkerRepositoryService_GetContainerIp_FullMethodName            = "/WorkerRepositoryService/GetContainerIp"
	WorkerRepositoryService_GetContainerIps_FullMethodName           = "/WorkerRepositoryService/GetContainerIps"
	WorkerRepositoryService_RemoveContainerIp_FullMethodName         = "/WorkerRepositoryService/RemoveContainerIp"
)

// WorkerRepositoryServiceClient is the client API for WorkerRepositoryService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WorkerRepositoryServiceClient interface {
	GetNextContainerRequest(ctx context.Context, in *GetNextContainerRequestRequest, opts ...grpc.CallOption) (*GetNextContainerRequestResponse, error)
	SetImagePullLock(ctx context.Context, in *SetImagePullLockRequest, opts ...grpc.CallOption) (*SetImagePullLockResponse, error)
	RemoveImagePullLock(ctx context.Context, in *RemoveImagePullLockRequest, opts ...grpc.CallOption) (*RemoveImagePullLockResponse, error)
	AddContainerToWorker(ctx context.Context, in *AddContainerToWorkerRequest, opts ...grpc.CallOption) (*AddContainerToWorkerResponse, error)
	RemoveContainerFromWorker(ctx context.Context, in *RemoveContainerFromWorkerRequest, opts ...grpc.CallOption) (*RemoveContainerFromWorkerResponse, error)
	GetWorkerById(ctx context.Context, in *GetWorkerByIdRequest, opts ...grpc.CallOption) (*GetWorkerByIdResponse, error)
	ToggleWorkerAvailable(ctx context.Context, in *ToggleWorkerAvailableRequest, opts ...grpc.CallOption) (*ToggleWorkerAvailableResponse, error)
	RemoveWorker(ctx context.Context, in *RemoveWorkerRequest, opts ...grpc.CallOption) (*RemoveWorkerResponse, error)
	UpdateWorkerCapacity(ctx context.Context, in *UpdateWorkerCapacityRequest, opts ...grpc.CallOption) (*UpdateWorkerCapacityResponse, error)
	SetWorkerKeepAlive(ctx context.Context, in *SetWorkerKeepAliveRequest, opts ...grpc.CallOption) (*SetWorkerKeepAliveResponse, error)
	SetNetworkLock(ctx context.Context, in *SetNetworkLockRequest, opts ...grpc.CallOption) (*SetNetworkLockResponse, error)
	RemoveNetworkLock(ctx context.Context, in *RemoveNetworkLockRequest, opts ...grpc.CallOption) (*RemoveNetworkLockResponse, error)
	SetContainerIp(ctx context.Context, in *SetContainerIpRequest, opts ...grpc.CallOption) (*SetContainerIpResponse, error)
	GetContainerIp(ctx context.Context, in *GetContainerIpRequest, opts ...grpc.CallOption) (*GetContainerIpResponse, error)
	GetContainerIps(ctx context.Context, in *GetContainerIpsRequest, opts ...grpc.CallOption) (*GetContainerIpsResponse, error)
	RemoveContainerIp(ctx context.Context, in *RemoveContainerIpRequest, opts ...grpc.CallOption) (*RemoveContainerIpResponse, error)
}

type workerRepositoryServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWorkerRepositoryServiceClient(cc grpc.ClientConnInterface) WorkerRepositoryServiceClient {
	return &workerRepositoryServiceClient{cc}
}

func (c *workerRepositoryServiceClient) GetNextContainerRequest(ctx context.Context, in *GetNextContainerRequestRequest, opts ...grpc.CallOption) (*GetNextContainerRequestResponse, error) {
	out := new(GetNextContainerRequestResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_GetNextContainerRequest_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) SetImagePullLock(ctx context.Context, in *SetImagePullLockRequest, opts ...grpc.CallOption) (*SetImagePullLockResponse, error) {
	out := new(SetImagePullLockResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_SetImagePullLock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) RemoveImagePullLock(ctx context.Context, in *RemoveImagePullLockRequest, opts ...grpc.CallOption) (*RemoveImagePullLockResponse, error) {
	out := new(RemoveImagePullLockResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_RemoveImagePullLock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) AddContainerToWorker(ctx context.Context, in *AddContainerToWorkerRequest, opts ...grpc.CallOption) (*AddContainerToWorkerResponse, error) {
	out := new(AddContainerToWorkerResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_AddContainerToWorker_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) RemoveContainerFromWorker(ctx context.Context, in *RemoveContainerFromWorkerRequest, opts ...grpc.CallOption) (*RemoveContainerFromWorkerResponse, error) {
	out := new(RemoveContainerFromWorkerResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_RemoveContainerFromWorker_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) GetWorkerById(ctx context.Context, in *GetWorkerByIdRequest, opts ...grpc.CallOption) (*GetWorkerByIdResponse, error) {
	out := new(GetWorkerByIdResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_GetWorkerById_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) ToggleWorkerAvailable(ctx context.Context, in *ToggleWorkerAvailableRequest, opts ...grpc.CallOption) (*ToggleWorkerAvailableResponse, error) {
	out := new(ToggleWorkerAvailableResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_ToggleWorkerAvailable_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) RemoveWorker(ctx context.Context, in *RemoveWorkerRequest, opts ...grpc.CallOption) (*RemoveWorkerResponse, error) {
	out := new(RemoveWorkerResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_RemoveWorker_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) UpdateWorkerCapacity(ctx context.Context, in *UpdateWorkerCapacityRequest, opts ...grpc.CallOption) (*UpdateWorkerCapacityResponse, error) {
	out := new(UpdateWorkerCapacityResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_UpdateWorkerCapacity_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) SetWorkerKeepAlive(ctx context.Context, in *SetWorkerKeepAliveRequest, opts ...grpc.CallOption) (*SetWorkerKeepAliveResponse, error) {
	out := new(SetWorkerKeepAliveResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_SetWorkerKeepAlive_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) SetNetworkLock(ctx context.Context, in *SetNetworkLockRequest, opts ...grpc.CallOption) (*SetNetworkLockResponse, error) {
	out := new(SetNetworkLockResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_SetNetworkLock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) RemoveNetworkLock(ctx context.Context, in *RemoveNetworkLockRequest, opts ...grpc.CallOption) (*RemoveNetworkLockResponse, error) {
	out := new(RemoveNetworkLockResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_RemoveNetworkLock_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) SetContainerIp(ctx context.Context, in *SetContainerIpRequest, opts ...grpc.CallOption) (*SetContainerIpResponse, error) {
	out := new(SetContainerIpResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_SetContainerIp_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) GetContainerIp(ctx context.Context, in *GetContainerIpRequest, opts ...grpc.CallOption) (*GetContainerIpResponse, error) {
	out := new(GetContainerIpResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_GetContainerIp_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) GetContainerIps(ctx context.Context, in *GetContainerIpsRequest, opts ...grpc.CallOption) (*GetContainerIpsResponse, error) {
	out := new(GetContainerIpsResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_GetContainerIps_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerRepositoryServiceClient) RemoveContainerIp(ctx context.Context, in *RemoveContainerIpRequest, opts ...grpc.CallOption) (*RemoveContainerIpResponse, error) {
	out := new(RemoveContainerIpResponse)
	err := c.cc.Invoke(ctx, WorkerRepositoryService_RemoveContainerIp_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WorkerRepositoryServiceServer is the server API for WorkerRepositoryService service.
// All implementations must embed UnimplementedWorkerRepositoryServiceServer
// for forward compatibility
type WorkerRepositoryServiceServer interface {
	GetNextContainerRequest(context.Context, *GetNextContainerRequestRequest) (*GetNextContainerRequestResponse, error)
	SetImagePullLock(context.Context, *SetImagePullLockRequest) (*SetImagePullLockResponse, error)
	RemoveImagePullLock(context.Context, *RemoveImagePullLockRequest) (*RemoveImagePullLockResponse, error)
	AddContainerToWorker(context.Context, *AddContainerToWorkerRequest) (*AddContainerToWorkerResponse, error)
	RemoveContainerFromWorker(context.Context, *RemoveContainerFromWorkerRequest) (*RemoveContainerFromWorkerResponse, error)
	GetWorkerById(context.Context, *GetWorkerByIdRequest) (*GetWorkerByIdResponse, error)
	ToggleWorkerAvailable(context.Context, *ToggleWorkerAvailableRequest) (*ToggleWorkerAvailableResponse, error)
	RemoveWorker(context.Context, *RemoveWorkerRequest) (*RemoveWorkerResponse, error)
	UpdateWorkerCapacity(context.Context, *UpdateWorkerCapacityRequest) (*UpdateWorkerCapacityResponse, error)
	SetWorkerKeepAlive(context.Context, *SetWorkerKeepAliveRequest) (*SetWorkerKeepAliveResponse, error)
	SetNetworkLock(context.Context, *SetNetworkLockRequest) (*SetNetworkLockResponse, error)
	RemoveNetworkLock(context.Context, *RemoveNetworkLockRequest) (*RemoveNetworkLockResponse, error)
	SetContainerIp(context.Context, *SetContainerIpRequest) (*SetContainerIpResponse, error)
	GetContainerIp(context.Context, *GetContainerIpRequest) (*GetContainerIpResponse, error)
	GetContainerIps(context.Context, *GetContainerIpsRequest) (*GetContainerIpsResponse, error)
	RemoveContainerIp(context.Context, *RemoveContainerIpRequest) (*RemoveContainerIpResponse, error)
	mustEmbedUnimplementedWorkerRepositoryServiceServer()
}

// UnimplementedWorkerRepositoryServiceServer must be embedded to have forward compatible implementations.
type UnimplementedWorkerRepositoryServiceServer struct {
}

func (UnimplementedWorkerRepositoryServiceServer) GetNextContainerRequest(context.Context, *GetNextContainerRequestRequest) (*GetNextContainerRequestResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNextContainerRequest not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) SetImagePullLock(context.Context, *SetImagePullLockRequest) (*SetImagePullLockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetImagePullLock not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) RemoveImagePullLock(context.Context, *RemoveImagePullLockRequest) (*RemoveImagePullLockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveImagePullLock not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) AddContainerToWorker(context.Context, *AddContainerToWorkerRequest) (*AddContainerToWorkerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddContainerToWorker not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) RemoveContainerFromWorker(context.Context, *RemoveContainerFromWorkerRequest) (*RemoveContainerFromWorkerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveContainerFromWorker not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) GetWorkerById(context.Context, *GetWorkerByIdRequest) (*GetWorkerByIdResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkerById not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) ToggleWorkerAvailable(context.Context, *ToggleWorkerAvailableRequest) (*ToggleWorkerAvailableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ToggleWorkerAvailable not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) RemoveWorker(context.Context, *RemoveWorkerRequest) (*RemoveWorkerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveWorker not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) UpdateWorkerCapacity(context.Context, *UpdateWorkerCapacityRequest) (*UpdateWorkerCapacityResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateWorkerCapacity not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) SetWorkerKeepAlive(context.Context, *SetWorkerKeepAliveRequest) (*SetWorkerKeepAliveResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetWorkerKeepAlive not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) SetNetworkLock(context.Context, *SetNetworkLockRequest) (*SetNetworkLockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetNetworkLock not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) RemoveNetworkLock(context.Context, *RemoveNetworkLockRequest) (*RemoveNetworkLockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveNetworkLock not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) SetContainerIp(context.Context, *SetContainerIpRequest) (*SetContainerIpResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetContainerIp not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) GetContainerIp(context.Context, *GetContainerIpRequest) (*GetContainerIpResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetContainerIp not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) GetContainerIps(context.Context, *GetContainerIpsRequest) (*GetContainerIpsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetContainerIps not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) RemoveContainerIp(context.Context, *RemoveContainerIpRequest) (*RemoveContainerIpResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveContainerIp not implemented")
}
func (UnimplementedWorkerRepositoryServiceServer) mustEmbedUnimplementedWorkerRepositoryServiceServer() {
}

// UnsafeWorkerRepositoryServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WorkerRepositoryServiceServer will
// result in compilation errors.
type UnsafeWorkerRepositoryServiceServer interface {
	mustEmbedUnimplementedWorkerRepositoryServiceServer()
}

func RegisterWorkerRepositoryServiceServer(s grpc.ServiceRegistrar, srv WorkerRepositoryServiceServer) {
	s.RegisterService(&WorkerRepositoryService_ServiceDesc, srv)
}

func _WorkerRepositoryService_GetNextContainerRequest_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetNextContainerRequestRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).GetNextContainerRequest(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_GetNextContainerRequest_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).GetNextContainerRequest(ctx, req.(*GetNextContainerRequestRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_SetImagePullLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetImagePullLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).SetImagePullLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_SetImagePullLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).SetImagePullLock(ctx, req.(*SetImagePullLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_RemoveImagePullLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveImagePullLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).RemoveImagePullLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_RemoveImagePullLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).RemoveImagePullLock(ctx, req.(*RemoveImagePullLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_AddContainerToWorker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddContainerToWorkerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).AddContainerToWorker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_AddContainerToWorker_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).AddContainerToWorker(ctx, req.(*AddContainerToWorkerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_RemoveContainerFromWorker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveContainerFromWorkerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).RemoveContainerFromWorker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_RemoveContainerFromWorker_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).RemoveContainerFromWorker(ctx, req.(*RemoveContainerFromWorkerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_GetWorkerById_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWorkerByIdRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).GetWorkerById(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_GetWorkerById_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).GetWorkerById(ctx, req.(*GetWorkerByIdRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_ToggleWorkerAvailable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ToggleWorkerAvailableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).ToggleWorkerAvailable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_ToggleWorkerAvailable_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).ToggleWorkerAvailable(ctx, req.(*ToggleWorkerAvailableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_RemoveWorker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveWorkerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).RemoveWorker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_RemoveWorker_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).RemoveWorker(ctx, req.(*RemoveWorkerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_UpdateWorkerCapacity_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateWorkerCapacityRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).UpdateWorkerCapacity(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_UpdateWorkerCapacity_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).UpdateWorkerCapacity(ctx, req.(*UpdateWorkerCapacityRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_SetWorkerKeepAlive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetWorkerKeepAliveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).SetWorkerKeepAlive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_SetWorkerKeepAlive_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).SetWorkerKeepAlive(ctx, req.(*SetWorkerKeepAliveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_SetNetworkLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetNetworkLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).SetNetworkLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_SetNetworkLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).SetNetworkLock(ctx, req.(*SetNetworkLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_RemoveNetworkLock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveNetworkLockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).RemoveNetworkLock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_RemoveNetworkLock_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).RemoveNetworkLock(ctx, req.(*RemoveNetworkLockRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_SetContainerIp_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetContainerIpRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).SetContainerIp(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_SetContainerIp_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).SetContainerIp(ctx, req.(*SetContainerIpRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_GetContainerIp_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetContainerIpRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).GetContainerIp(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_GetContainerIp_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).GetContainerIp(ctx, req.(*GetContainerIpRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_GetContainerIps_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetContainerIpsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).GetContainerIps(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_GetContainerIps_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).GetContainerIps(ctx, req.(*GetContainerIpsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerRepositoryService_RemoveContainerIp_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveContainerIpRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerRepositoryServiceServer).RemoveContainerIp(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WorkerRepositoryService_RemoveContainerIp_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerRepositoryServiceServer).RemoveContainerIp(ctx, req.(*RemoveContainerIpRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// WorkerRepositoryService_ServiceDesc is the grpc.ServiceDesc for WorkerRepositoryService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var WorkerRepositoryService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "WorkerRepositoryService",
	HandlerType: (*WorkerRepositoryServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetNextContainerRequest",
			Handler:    _WorkerRepositoryService_GetNextContainerRequest_Handler,
		},
		{
			MethodName: "SetImagePullLock",
			Handler:    _WorkerRepositoryService_SetImagePullLock_Handler,
		},
		{
			MethodName: "RemoveImagePullLock",
			Handler:    _WorkerRepositoryService_RemoveImagePullLock_Handler,
		},
		{
			MethodName: "AddContainerToWorker",
			Handler:    _WorkerRepositoryService_AddContainerToWorker_Handler,
		},
		{
			MethodName: "RemoveContainerFromWorker",
			Handler:    _WorkerRepositoryService_RemoveContainerFromWorker_Handler,
		},
		{
			MethodName: "GetWorkerById",
			Handler:    _WorkerRepositoryService_GetWorkerById_Handler,
		},
		{
			MethodName: "ToggleWorkerAvailable",
			Handler:    _WorkerRepositoryService_ToggleWorkerAvailable_Handler,
		},
		{
			MethodName: "RemoveWorker",
			Handler:    _WorkerRepositoryService_RemoveWorker_Handler,
		},
		{
			MethodName: "UpdateWorkerCapacity",
			Handler:    _WorkerRepositoryService_UpdateWorkerCapacity_Handler,
		},
		{
			MethodName: "SetWorkerKeepAlive",
			Handler:    _WorkerRepositoryService_SetWorkerKeepAlive_Handler,
		},
		{
			MethodName: "SetNetworkLock",
			Handler:    _WorkerRepositoryService_SetNetworkLock_Handler,
		},
		{
			MethodName: "RemoveNetworkLock",
			Handler:    _WorkerRepositoryService_RemoveNetworkLock_Handler,
		},
		{
			MethodName: "SetContainerIp",
			Handler:    _WorkerRepositoryService_SetContainerIp_Handler,
		},
		{
			MethodName: "GetContainerIp",
			Handler:    _WorkerRepositoryService_GetContainerIp_Handler,
		},
		{
			MethodName: "GetContainerIps",
			Handler:    _WorkerRepositoryService_GetContainerIps_Handler,
		},
		{
			MethodName: "RemoveContainerIp",
			Handler:    _WorkerRepositoryService_RemoveContainerIp_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "worker_repo.proto",
}
