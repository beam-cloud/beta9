package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/proto"
)

const (
	defaultRPCTimeout = 10 * time.Second
)

type RepoName string

const (
	RepoNameContainer RepoName = "container"
)

type RemoteRepositoryServiceServer struct {
	proto.UnimplementedRepositoryServiceServer
	containerRepo ContainerRepository
}

func NewRepositoryServiceServer(containerRepo ContainerRepository) *RemoteRepositoryServiceServer {
	return &RemoteRepositoryServiceServer{
		containerRepo: containerRepo,
	}
}

func (s *RemoteRepositoryServiceServer) Execute(ctx context.Context, req *proto.RepositoryRequest) (*proto.RepositoryResponse, error) {
	handlers := map[string]func(context.Context, map[string]interface{}) (interface{}, error){
		"GetContainerState":     s.getContainerState,
		"SetContainerAddress":   s.setContainerAddress,
		"SetWorkerAddress":      s.setWorkerAddress,
		"UpdateContainerStatus": s.updateContainerStatus,
		"SetContainerExitCode":  s.setContainerExitCode,
	}

	if handler, exists := handlers[req.MethodName]; exists {
		request, err := deserializeRequest(req.Payload)
		if err != nil {
			return &proto.RepositoryResponse{
				ErrMsg: err.Error(),
			}, nil
		}

		result, err := handler(ctx, request)
		if err != nil {
			return &proto.RepositoryResponse{
				ErrMsg: err.Error(),
			}, nil
		}

		responsePayload, err := serializeResponse(result)
		if err != nil {
			return &proto.RepositoryResponse{
				ErrMsg: err.Error(),
			}, nil
		}

		return &proto.RepositoryResponse{
			Payload: responsePayload,
		}, nil
	}

	return &proto.RepositoryResponse{
		ErrMsg: fmt.Sprintf("Unknown method: %s", req.MethodName),
	}, nil
}

func deserializeRequest(payload []byte) (map[string]interface{}, error) {
	var request map[string]interface{}
	err := json.Unmarshal(payload, &request)
	return request, err
}

func serializeResponse(response interface{}) ([]byte, error) {
	return json.Marshal(response)
}

// executeRequest is a helper to execute a request against the repo svc
func executeRequest(ctx context.Context, client proto.RepositoryServiceClient, repoName RepoName, methodName string, requestData interface{}) ([]byte, error) {
	payload, err := json.Marshal(requestData)
	if err != nil {
		return nil, &RepositorySerializationError{Err: err}
	}

	req := &proto.RepositoryRequest{
		RepoName:   string(repoName),
		MethodName: methodName,
		Payload:    payload,
	}

	ctx, cancel := withTimeout(ctx)
	defer cancel()

	resp, err := client.Execute(ctx, req)
	if err != nil {
		return nil, &RepositoryTimeoutError{Err: err}
	}

	if resp.ErrMsg != "" {
		return nil, &RepositoryServerError{Message: resp.ErrMsg}
	}

	return resp.Payload, nil
}

// Error types
type RepositorySerializationError struct {
	Err error
}

func (e *RepositorySerializationError) Error() string {
	return fmt.Sprintf("repository serialization error: %v", e.Err)
}

type RepositoryTimeoutError struct {
	Err error
}

func (e *RepositoryTimeoutError) Error() string {
	return fmt.Sprintf("repository timeout error: %v", e.Err)
}

type RepositoryServerError struct {
	Message string
}

func (e *RepositoryServerError) Error() string {
	return fmt.Sprintf("repository server error: %s", e.Message)
}

type RepositoryNotImplementedError struct {
	MethodName string
}

func (e *RepositoryNotImplementedError) Error() string {
	return fmt.Sprintf("repository method not implemented: %s", e.MethodName)
}

func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, defaultRPCTimeout)
}
