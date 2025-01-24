package repository

import (
	"context"
	"encoding/json"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/proto"
)

type ContainerRemoteRepository struct {
	client proto.RepositoryServiceClient
}

// Proxy methods to call the internal repository methods
func (s *RemoteRepositoryServiceServer) getContainerState(ctx context.Context, request map[string]interface{}) (interface{}, error) {
	return s.containerRepo.GetContainerState(request["containerId"].(string))
}

func (s *RemoteRepositoryServiceServer) setContainerAddress(ctx context.Context, request map[string]interface{}) (interface{}, error) {
	return nil, s.containerRepo.SetContainerAddress(request["containerId"].(string), request["addr"].(string))
}

func (s *RemoteRepositoryServiceServer) updateContainerStatus(ctx context.Context, request map[string]interface{}) (interface{}, error) {
	return s.containerRepo.UpdateContainerStatus(request["containerId"].(string), types.ContainerStatus(request["status"].(string)), request["expirySeconds"].(float64)), nil
}

func (s *RemoteRepositoryServiceServer) setWorkerAddress(ctx context.Context, request map[string]interface{}) (interface{}, error) {
	return nil, s.containerRepo.SetWorkerAddress(request["containerId"].(string), request["addr"].(string))
}

func (s *RemoteRepositoryServiceServer) setContainerExitCode(ctx context.Context, request map[string]interface{}) (interface{}, error) {
	return nil, s.containerRepo.SetContainerExitCode(request["containerId"].(string), int(request["exitCode"].(float64)))
}

func NewContainerRemoteRepository(client proto.RepositoryServiceClient) ContainerRepository {
	return &ContainerRemoteRepository{
		client: client,
	}
}

func (cr *ContainerRemoteRepository) GetContainerState(containerId string) (*types.ContainerState, error) {
	request := map[string]string{
		"containerId": containerId,
	}

	resp, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "GetContainerState", request)
	if err != nil {
		return nil, err
	}

	var containerState types.ContainerState
	err = json.Unmarshal(resp, &containerState)
	if err != nil {
		return nil, &RepositorySerializationError{Err: err}
	}

	return &containerState, nil
}

func (cr *ContainerRemoteRepository) SetContainerState(containerId string, info *types.ContainerState) error {
	request := map[string]interface{}{
		"containerId": containerId,
		"info":        info,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "SetContainerState", request)
	return err
}

func (cr *ContainerRemoteRepository) SetContainerExitCode(containerId string, exitCode int) error {
	request := map[string]interface{}{
		"containerId": containerId,
		"exitCode":    exitCode,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "SetContainerExitCode", request)
	return err
}

func (cr *ContainerRemoteRepository) GetContainerExitCode(containerId string) (int, error) {
	request := map[string]string{
		"containerId": containerId,
	}

	resp, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "GetContainerExitCode", request)
	if err != nil {
		return -1, err
	}

	var exitCode int
	err = json.Unmarshal(resp, &exitCode)
	if err != nil {
		return -1, &RepositorySerializationError{Err: err}
	}

	return exitCode, nil
}

func (cr *ContainerRemoteRepository) UpdateContainerStatus(containerId string, status types.ContainerStatus, expirySeconds float64) error {
	request := map[string]interface{}{
		"containerId":   containerId,
		"status":        status,
		"expirySeconds": expirySeconds,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "UpdateContainerStatus", request)
	return err
}

func (cr *ContainerRemoteRepository) UpdateAssignedContainerGPU(containerId string, gpuType string) error {
	return &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) DeleteContainerState(containerId string) error {
	request := map[string]string{
		"containerId": containerId,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "DeleteContainerState", request)
	return err
}

func (cr *ContainerRemoteRepository) SetContainerAddress(containerId string, addr string) error {
	request := map[string]string{
		"containerId": containerId,
		"addr":        addr,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "SetContainerAddress", request)
	return err
}

func (cr *ContainerRemoteRepository) GetContainerAddress(containerId string) (string, error) {
	request := map[string]string{
		"containerId": containerId,
	}

	resp, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "GetContainerAddress", request)
	if err != nil {
		return "", err
	}

	var addr string
	err = json.Unmarshal(resp, &addr)
	if err != nil {
		return "", &RepositorySerializationError{Err: err}
	}

	return addr, nil
}

func (cr *ContainerRemoteRepository) SetWorkerAddress(containerId string, addr string) error {
	request := map[string]string{
		"containerId": containerId,
		"addr":        addr,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "SetWorkerAddress", request)
	return err
}

func (cr *ContainerRemoteRepository) GetWorkerAddress(ctx context.Context, containerId string) (string, error) {
	request := map[string]string{
		"containerId": containerId,
	}

	resp, err := executeRequest(ctx, cr.client, RepoNameContainer, "GetWorkerAddress", request)
	if err != nil {
		return "", err
	}

	var addr string
	err = json.Unmarshal(resp, &addr)
	if err != nil {
		return "", &RepositorySerializationError{Err: err}
	}

	return addr, nil
}

func (cr *ContainerRemoteRepository) SetContainerStateWithConcurrencyLimit(quota *types.ConcurrencyLimit, containerRequest *types.ContainerRequest) error {
	return &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	return nil, &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) GetActiveContainersByWorkspaceId(workspaceId string) ([]types.ContainerState, error) {
	return nil, &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) GetActiveContainersByWorkerId(workerId string) ([]types.ContainerState, error) {
	return nil, &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) GetFailedContainersByStubId(stubId string) ([]string, error) {
	return nil, &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) UpdateCheckpointState(workspaceName, checkpointId string, checkpointState *types.CheckpointState) error {
	request := map[string]interface{}{
		"workspaceName":   workspaceName,
		"checkpointId":    checkpointId,
		"checkpointState": checkpointState,
	}

	_, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "UpdateCheckpointState", request)
	return err
}

func (cr *ContainerRemoteRepository) GetCheckpointState(workspaceName, checkpointId string) (*types.CheckpointState, error) {
	request := map[string]string{
		"workspaceName": workspaceName,
		"checkpointId":  checkpointId,
	}

	resp, err := executeRequest(context.Background(), cr.client, RepoNameContainer, "GetCheckpointState", request)
	if err != nil {
		return nil, err
	}

	var checkpointState types.CheckpointState
	err = json.Unmarshal(resp, &checkpointState)
	if err != nil {
		return nil, &RepositorySerializationError{Err: err}
	}

	return &checkpointState, nil
}

func (cr *ContainerRemoteRepository) GetStubState(stubId string) (string, error) {
	return "", &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) SetStubState(stubId, state string) error {
	return &RepositoryNotImplementedError{}
}

func (cr *ContainerRemoteRepository) DeleteStubState(stubId string) error {
	return &RepositoryNotImplementedError{}
}
