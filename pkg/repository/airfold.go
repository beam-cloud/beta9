package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"net/http"
	"strings"
	"time"
)

func NewAirfoldRepo(afConfig types.AirfoldConfig) WarehouseRepository {
	airfoldEnabled := false
	if afConfig.ApiKey != "" {
		airfoldEnabled = true
	}

	return &AirfoldRepo{
		afConfig:       afConfig,
		airfoldEnabled: airfoldEnabled,
	}
}

type AirfoldRepo struct {
	afConfig       types.AirfoldConfig
	airfoldEnabled bool
}

func (t *AirfoldRepo) IsEnabled() bool {
	return t.airfoldEnabled
}

func (t *AirfoldRepo) sendToWarehouse(data interface{}) {
	if !t.airfoldEnabled {
		return
	}

	eventBytes, err := json.Marshal(data)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal event object")
		return
	}

	client := &http.Client{}

	req, err := http.NewRequest("POST", t.afConfig.Endpoint+"/events/"+t.afConfig.TableName, bytes.NewBuffer(eventBytes))
	if err != nil {
		log.Error().Err(err).Msg("failed to create http request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.afConfig.ApiKey)

	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("failed to send payload to Airfold")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted {
		return
	}

	log.Error().Int("status_code", resp.StatusCode).Msg("unexpected status code from Airfold server")
}

func (t *AirfoldRepo) PushTaskUpdatedEvent(task *types.TaskWithRelated) {
	if !t.airfoldEnabled {
		return
	}

	whEvent := types.WarehouseEventTaskSchema{
		ID:                  task.ExternalId,
		CreatedAt:           task.CreatedAt,
		UpdatedAt:           time.Now(),
		EventType:           types.EventTaskUpdated,
		Status:              task.Status,
		ContainerID:         task.ContainerId,
		StubID:              task.Stub.ExternalId,
		Stub:                task.Stub,
		WorkspaceID:         task.WorkspaceId,
		WorkspaceExternalID: task.Workspace.ExternalId,
		Workspace:           task.Workspace,
	}

	if task.StartedAt.Valid {
		whEvent.StartedAt = &task.StartedAt.Time
	}

	if task.EndedAt.Valid {
		whEvent.EndedAt = &task.EndedAt.Time
	}

	t.sendToWarehouse(whEvent)
}

func (t *AirfoldRepo) PushTaskCreatedEvent(task *types.TaskWithRelated) {
	if !t.airfoldEnabled {
		return
	}

	whEvent := types.WarehouseEventTaskSchema{
		ID:                  task.ExternalId,
		CreatedAt:           task.CreatedAt,
		UpdatedAt:           task.CreatedAt,
		EventType:           types.EventTaskCreated,
		Status:              task.Status,
		ContainerID:         task.ContainerId,
		StubID:              task.Stub.ExternalId,
		Stub:                task.Stub,
		WorkspaceID:         task.WorkspaceId,
		WorkspaceExternalID: task.Workspace.ExternalId,
		Workspace:           task.Workspace,
	}

	if task.StartedAt.Valid {
		whEvent.StartedAt = &task.StartedAt.Time
	}

	if task.EndedAt.Valid {
		whEvent.EndedAt = &task.EndedAt.Time
	}

	t.sendToWarehouse(whEvent)
}

func (t *AirfoldRepo) AggregateTasksByTimeWindow(ctx context.Context, filters types.TaskFilter) ([]types.TaskCountByTime, error) {
	if !t.airfoldEnabled {
		return nil, nil
	}

	client := &http.Client{}

	req, err := http.NewRequestWithContext(ctx, "GET", t.afConfig.Endpoint+"/pipes/tasks_agg_by_time_window.json", nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to create http request")
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.afConfig.ApiKey)

	query := req.URL.Query()

	if filters.WorkspaceID > 0 {
		query.Add("workspace_id", fmt.Sprintf("%d", filters.WorkspaceID))
	}

	if len(filters.StubIds) > 0 {
		query.Add("stub_ids", strings.Join(filters.StubIds, ","))
	}

	if filters.CreatedAtStart != "" {
		query.Add("created_at_start", filters.CreatedAtStart)
	}

	if filters.CreatedAtEnd != "" {
		query.Add("created_at_end", filters.CreatedAtEnd)
	}

	req.URL.RawQuery = query.Encode()

	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("failed to get tasks_agg_by_time_window data")
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error: %s from tasks_agg_by_time_window endpoint", resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)

	var wrapper struct {
		Data []types.TaskCountByTime `json:"data"`
	}

	if err := decoder.Decode(&wrapper); err != nil {
		log.Error().Err(err).Msg("failed to decode response")
		return nil, err
	}

	return wrapper.Data, nil
}
