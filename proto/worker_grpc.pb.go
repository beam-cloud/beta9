// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: worker.proto

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
	RunCService_RunCKill_FullMethodName          = "/runc.RunCService/RunCKill"
	RunCService_RunCExec_FullMethodName          = "/runc.RunCService/RunCExec"
	RunCService_RunCStatus_FullMethodName        = "/runc.RunCService/RunCStatus"
	RunCService_RunCStreamLogs_FullMethodName    = "/runc.RunCService/RunCStreamLogs"
	RunCService_RunCArchive_FullMethodName       = "/runc.RunCService/RunCArchive"
	RunCService_RunCSyncWorkspace_FullMethodName = "/runc.RunCService/RunCSyncWorkspace"
)

// RunCServiceClient is the client API for RunCService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RunCServiceClient interface {
	RunCKill(ctx context.Context, in *RunCKillRequest, opts ...grpc.CallOption) (*RunCKillResponse, error)
	RunCExec(ctx context.Context, in *RunCExecRequest, opts ...grpc.CallOption) (*RunCExecResponse, error)
	RunCStatus(ctx context.Context, in *RunCStatusRequest, opts ...grpc.CallOption) (*RunCStatusResponse, error)
	RunCStreamLogs(ctx context.Context, in *RunCStreamLogsRequest, opts ...grpc.CallOption) (RunCService_RunCStreamLogsClient, error)
	RunCArchive(ctx context.Context, in *RunCArchiveRequest, opts ...grpc.CallOption) (RunCService_RunCArchiveClient, error)
	RunCSyncWorkspace(ctx context.Context, in *SyncContainerWorkspaceRequest, opts ...grpc.CallOption) (*SyncContainerWorkspaceResponse, error)
}

type runCServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRunCServiceClient(cc grpc.ClientConnInterface) RunCServiceClient {
	return &runCServiceClient{cc}
}

func (c *runCServiceClient) RunCKill(ctx context.Context, in *RunCKillRequest, opts ...grpc.CallOption) (*RunCKillResponse, error) {
	out := new(RunCKillResponse)
	err := c.cc.Invoke(ctx, RunCService_RunCKill_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *runCServiceClient) RunCExec(ctx context.Context, in *RunCExecRequest, opts ...grpc.CallOption) (*RunCExecResponse, error) {
	out := new(RunCExecResponse)
	err := c.cc.Invoke(ctx, RunCService_RunCExec_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *runCServiceClient) RunCStatus(ctx context.Context, in *RunCStatusRequest, opts ...grpc.CallOption) (*RunCStatusResponse, error) {
	out := new(RunCStatusResponse)
	err := c.cc.Invoke(ctx, RunCService_RunCStatus_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *runCServiceClient) RunCStreamLogs(ctx context.Context, in *RunCStreamLogsRequest, opts ...grpc.CallOption) (RunCService_RunCStreamLogsClient, error) {
	stream, err := c.cc.NewStream(ctx, &RunCService_ServiceDesc.Streams[0], RunCService_RunCStreamLogs_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &runCServiceRunCStreamLogsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RunCService_RunCStreamLogsClient interface {
	Recv() (*RunCLogEntry, error)
	grpc.ClientStream
}

type runCServiceRunCStreamLogsClient struct {
	grpc.ClientStream
}

func (x *runCServiceRunCStreamLogsClient) Recv() (*RunCLogEntry, error) {
	m := new(RunCLogEntry)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *runCServiceClient) RunCArchive(ctx context.Context, in *RunCArchiveRequest, opts ...grpc.CallOption) (RunCService_RunCArchiveClient, error) {
	stream, err := c.cc.NewStream(ctx, &RunCService_ServiceDesc.Streams[1], RunCService_RunCArchive_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &runCServiceRunCArchiveClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type RunCService_RunCArchiveClient interface {
	Recv() (*RunCArchiveResponse, error)
	grpc.ClientStream
}

type runCServiceRunCArchiveClient struct {
	grpc.ClientStream
}

func (x *runCServiceRunCArchiveClient) Recv() (*RunCArchiveResponse, error) {
	m := new(RunCArchiveResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *runCServiceClient) RunCSyncWorkspace(ctx context.Context, in *SyncContainerWorkspaceRequest, opts ...grpc.CallOption) (*SyncContainerWorkspaceResponse, error) {
	out := new(SyncContainerWorkspaceResponse)
	err := c.cc.Invoke(ctx, RunCService_RunCSyncWorkspace_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RunCServiceServer is the server API for RunCService service.
// All implementations must embed UnimplementedRunCServiceServer
// for forward compatibility
type RunCServiceServer interface {
	RunCKill(context.Context, *RunCKillRequest) (*RunCKillResponse, error)
	RunCExec(context.Context, *RunCExecRequest) (*RunCExecResponse, error)
	RunCStatus(context.Context, *RunCStatusRequest) (*RunCStatusResponse, error)
	RunCStreamLogs(*RunCStreamLogsRequest, RunCService_RunCStreamLogsServer) error
	RunCArchive(*RunCArchiveRequest, RunCService_RunCArchiveServer) error
	RunCSyncWorkspace(context.Context, *SyncContainerWorkspaceRequest) (*SyncContainerWorkspaceResponse, error)
	mustEmbedUnimplementedRunCServiceServer()
}

// UnimplementedRunCServiceServer must be embedded to have forward compatible implementations.
type UnimplementedRunCServiceServer struct {
}

func (UnimplementedRunCServiceServer) RunCKill(context.Context, *RunCKillRequest) (*RunCKillResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunCKill not implemented")
}
func (UnimplementedRunCServiceServer) RunCExec(context.Context, *RunCExecRequest) (*RunCExecResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunCExec not implemented")
}
func (UnimplementedRunCServiceServer) RunCStatus(context.Context, *RunCStatusRequest) (*RunCStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunCStatus not implemented")
}
func (UnimplementedRunCServiceServer) RunCStreamLogs(*RunCStreamLogsRequest, RunCService_RunCStreamLogsServer) error {
	return status.Errorf(codes.Unimplemented, "method RunCStreamLogs not implemented")
}
func (UnimplementedRunCServiceServer) RunCArchive(*RunCArchiveRequest, RunCService_RunCArchiveServer) error {
	return status.Errorf(codes.Unimplemented, "method RunCArchive not implemented")
}
func (UnimplementedRunCServiceServer) RunCSyncWorkspace(context.Context, *SyncContainerWorkspaceRequest) (*SyncContainerWorkspaceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunCSyncWorkspace not implemented")
}
func (UnimplementedRunCServiceServer) mustEmbedUnimplementedRunCServiceServer() {}

// UnsafeRunCServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RunCServiceServer will
// result in compilation errors.
type UnsafeRunCServiceServer interface {
	mustEmbedUnimplementedRunCServiceServer()
}

func RegisterRunCServiceServer(s grpc.ServiceRegistrar, srv RunCServiceServer) {
	s.RegisterService(&RunCService_ServiceDesc, srv)
}

func _RunCService_RunCKill_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RunCKillRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RunCServiceServer).RunCKill(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RunCService_RunCKill_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RunCServiceServer).RunCKill(ctx, req.(*RunCKillRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RunCService_RunCExec_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RunCExecRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RunCServiceServer).RunCExec(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RunCService_RunCExec_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RunCServiceServer).RunCExec(ctx, req.(*RunCExecRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RunCService_RunCStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RunCStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RunCServiceServer).RunCStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RunCService_RunCStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RunCServiceServer).RunCStatus(ctx, req.(*RunCStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RunCService_RunCStreamLogs_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RunCStreamLogsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RunCServiceServer).RunCStreamLogs(m, &runCServiceRunCStreamLogsServer{stream})
}

type RunCService_RunCStreamLogsServer interface {
	Send(*RunCLogEntry) error
	grpc.ServerStream
}

type runCServiceRunCStreamLogsServer struct {
	grpc.ServerStream
}

func (x *runCServiceRunCStreamLogsServer) Send(m *RunCLogEntry) error {
	return x.ServerStream.SendMsg(m)
}

func _RunCService_RunCArchive_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RunCArchiveRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(RunCServiceServer).RunCArchive(m, &runCServiceRunCArchiveServer{stream})
}

type RunCService_RunCArchiveServer interface {
	Send(*RunCArchiveResponse) error
	grpc.ServerStream
}

type runCServiceRunCArchiveServer struct {
	grpc.ServerStream
}

func (x *runCServiceRunCArchiveServer) Send(m *RunCArchiveResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _RunCService_RunCSyncWorkspace_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SyncContainerWorkspaceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RunCServiceServer).RunCSyncWorkspace(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RunCService_RunCSyncWorkspace_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RunCServiceServer).RunCSyncWorkspace(ctx, req.(*SyncContainerWorkspaceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RunCService_ServiceDesc is the grpc.ServiceDesc for RunCService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RunCService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "runc.RunCService",
	HandlerType: (*RunCServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RunCKill",
			Handler:    _RunCService_RunCKill_Handler,
		},
		{
			MethodName: "RunCExec",
			Handler:    _RunCService_RunCExec_Handler,
		},
		{
			MethodName: "RunCStatus",
			Handler:    _RunCService_RunCStatus_Handler,
		},
		{
			MethodName: "RunCSyncWorkspace",
			Handler:    _RunCService_RunCSyncWorkspace_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "RunCStreamLogs",
			Handler:       _RunCService_RunCStreamLogs_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RunCArchive",
			Handler:       _RunCService_RunCArchive_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "worker.proto",
}
