// Code generated by goa v3.19.1, DO NOT EDIT.
//
// ContainerRepository gRPC server types
//
// Command:
// $ goa gen github.com/beam-cloud/beta9/pkg/repository/dsl -o proto/goa

package server

import (
	containerrepository "github.com/beam-cloud/beta9/proto/goa/gen/container_repository"
	container_repositorypb "github.com/beam-cloud/beta9/proto/goa/gen/grpc/container_repository/pb"
)

// NewGetContainerStatePayload builds the payload of the "GetContainerState"
// endpoint of the "ContainerRepository" service from the gRPC request type.
func NewGetContainerStatePayload(message *container_repositorypb.GetContainerStateRequest) *containerrepository.GetContainerStatePayload {
	v := &containerrepository.GetContainerStatePayload{
		ContainerID: message.ContainerId,
	}
	return v
}

// NewProtoGetContainerStateResponse builds the gRPC response type from the
// result of the "GetContainerState" endpoint of the "ContainerRepository"
// service.
func NewProtoGetContainerStateResponse() *container_repositorypb.GetContainerStateResponse {
	message := &container_repositorypb.GetContainerStateResponse{}
	return message
}

// NewSetContainerStatePayload builds the payload of the "SetContainerState"
// endpoint of the "ContainerRepository" service from the gRPC request type.
func NewSetContainerStatePayload(message *container_repositorypb.SetContainerStateRequest) *containerrepository.SetContainerStatePayload {
	v := &containerrepository.SetContainerStatePayload{
		ContainerID: message.ContainerId,
	}
	if message.State != nil {
		v.State = protobufContainerRepositorypbContainerStateToContainerrepositoryContainerState(message.State)
	}
	return v
}

// NewProtoSetContainerStateResponse builds the gRPC response type from the
// result of the "SetContainerState" endpoint of the "ContainerRepository"
// service.
func NewProtoSetContainerStateResponse() *container_repositorypb.SetContainerStateResponse {
	message := &container_repositorypb.SetContainerStateResponse{}
	return message
}

// protobufContainerRepositorypbContainerStateToContainerrepositoryContainerState
// builds a value of type *containerrepository.ContainerState from a value of
// type *container_repositorypb.ContainerState.
func protobufContainerRepositorypbContainerStateToContainerrepositoryContainerState(v *container_repositorypb.ContainerState) *containerrepository.ContainerState {
	if v == nil {
		return nil
	}
	res := &containerrepository.ContainerState{}

	return res
}

// svcContainerrepositoryContainerStateToContainerRepositorypbContainerState
// builds a value of type *container_repositorypb.ContainerState from a value
// of type *containerrepository.ContainerState.
func svcContainerrepositoryContainerStateToContainerRepositorypbContainerState(v *containerrepository.ContainerState) *container_repositorypb.ContainerState {
	if v == nil {
		return nil
	}
	res := &container_repositorypb.ContainerState{}

	return res
}
