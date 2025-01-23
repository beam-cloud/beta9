// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.25.1
// source: repository.proto

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
	RepositoryService_Execute_FullMethodName = "/repository.RepositoryService/Execute"
)

// RepositoryServiceClient is the client API for RepositoryService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RepositoryServiceClient interface {
	Execute(ctx context.Context, in *RepositoryRequest, opts ...grpc.CallOption) (*RepositoryResponse, error)
}

type repositoryServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRepositoryServiceClient(cc grpc.ClientConnInterface) RepositoryServiceClient {
	return &repositoryServiceClient{cc}
}

func (c *repositoryServiceClient) Execute(ctx context.Context, in *RepositoryRequest, opts ...grpc.CallOption) (*RepositoryResponse, error) {
	out := new(RepositoryResponse)
	err := c.cc.Invoke(ctx, RepositoryService_Execute_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RepositoryServiceServer is the server API for RepositoryService service.
// All implementations must embed UnimplementedRepositoryServiceServer
// for forward compatibility
type RepositoryServiceServer interface {
	Execute(context.Context, *RepositoryRequest) (*RepositoryResponse, error)
	mustEmbedUnimplementedRepositoryServiceServer()
}

// UnimplementedRepositoryServiceServer must be embedded to have forward compatible implementations.
type UnimplementedRepositoryServiceServer struct {
}

func (UnimplementedRepositoryServiceServer) Execute(context.Context, *RepositoryRequest) (*RepositoryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Execute not implemented")
}
func (UnimplementedRepositoryServiceServer) mustEmbedUnimplementedRepositoryServiceServer() {}

// UnsafeRepositoryServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RepositoryServiceServer will
// result in compilation errors.
type UnsafeRepositoryServiceServer interface {
	mustEmbedUnimplementedRepositoryServiceServer()
}

func RegisterRepositoryServiceServer(s grpc.ServiceRegistrar, srv RepositoryServiceServer) {
	s.RegisterService(&RepositoryService_ServiceDesc, srv)
}

func _RepositoryService_Execute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RepositoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RepositoryServiceServer).Execute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RepositoryService_Execute_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RepositoryServiceServer).Execute(ctx, req.(*RepositoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RepositoryService_ServiceDesc is the grpc.ServiceDesc for RepositoryService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RepositoryService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "repository.RepositoryService",
	HandlerType: (*RepositoryServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Execute",
			Handler:    _RepositoryService_Execute_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "repository.proto",
}
