package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	thunderAPIURLEnv               = "THUNDER_API_URL"
	thunderAPITokenEnv             = "THUNDER_API_TOKEN"
	thunderZoneIDEnv               = "THUNDER_ZONE_ID"
	thunderEnrollmentTokenPath     = "/api/v1/enrollment-tokens"
	thunderEnrollmentTokenNodePath = "/api/v1/enrollment-tokens/%s/node"
	thunderEnrollmentRoleClient    = "client"
	thunderEnrollmentExpiresSecond = 604800
)

type ContainerThunderManager struct {
	apiURL      string
	apiToken    string
	client      *http.Client
	allocations *common.SafeMap[thunderAllocation]
}

type thunderAllocation struct {
	EnrollmentTokenID string
	EnrollmentToken   string
	APIURL            string
	APIToken          string
	Response          thunderEnrollmentTokenResponse
}

type thunderEnrollmentTokenRequest struct {
	OrgID            string `json:"orgId"`
	ZoneID           string `json:"zoneId"`
	Role             string `json:"role"`
	GPUType          string `json:"gpuType"`
	GPUCount         int    `json:"gpuCount"`
	ExpiresInSeconds int    `json:"expiresInSeconds"`
}

type thunderEnrollmentTokenResponse struct {
	EnrollmentTokenID string    `json:"enrollmentTokenId"`
	EnrollmentToken   string    `json:"enrollmentToken"`
	OrgID             string    `json:"orgId"`
	ZoneID            string    `json:"zoneId"`
	Role              string    `json:"role"`
	GPUType           string    `json:"gpuType"`
	GPUCount          int       `json:"gpuCount"`
	ExpiresAt         time.Time `json:"expiresAt"`
}

type thunderDeleteEnrollmentTokenNodeResponse struct {
	EnrollmentTokenID string    `json:"enrollmentTokenId"`
	Role              string    `json:"role"`
	ClientID          string    `json:"clientId"`
	HostID            string    `json:"hostId"`
	NodeDeleted       bool      `json:"nodeDeleted"`
	DeletedAt         time.Time `json:"deletedAt"`
}

func NewContainerThunderManagerFromEnv() GPUManager {
	return NewContainerThunderManager(os.Getenv(thunderAPIURLEnv), os.Getenv(thunderAPITokenEnv), nil)
}

func NewContainerThunderManager(apiURL, apiToken string, client *http.Client) *ContainerThunderManager {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &ContainerThunderManager{
		apiURL:      strings.TrimRight(apiURL, "/"),
		apiToken:    apiToken,
		client:      client,
		allocations: common.NewSafeMap[thunderAllocation](),
	}
}

func (c *ContainerThunderManager) AssignGPUDevices(request *types.ContainerRequest) ([]int, error) {
	if request == nil {
		return nil, fmt.Errorf("missing container request")
	}

	apiURL, apiToken := c.thunderCredentials(request)
	if apiURL == "" {
		return nil, fmt.Errorf("%s is required for virtualized GPU requests", thunderAPIURLEnv)
	}
	if apiToken == "" {
		return nil, fmt.Errorf("%s is required for virtualized GPU requests", thunderAPITokenEnv)
	}

	zoneID := thunderZoneID(request)
	if zoneID == "" {
		return nil, fmt.Errorf("%s is required for virtualized GPU requests", thunderZoneIDEnv)
	}

	payload := thunderEnrollmentTokenRequest{
		OrgID:            "",
		ZoneID:           zoneID,
		Role:             thunderEnrollmentRoleClient,
		GPUType:          strings.ToLower(thunderGPUType(request)),
		GPUCount:         int(thunderGPUCount(request)),
		ExpiresInSeconds: thunderEnrollmentExpiresSecond,
	}

	log.Info().
		Str("container_id", request.ContainerId).
		Str("zone_id", payload.ZoneID).
		Str("gpu_type", payload.GPUType).
		Int("gpu_count", payload.GPUCount).
		Msg("requesting Thunder enrollment token")

	var response thunderEnrollmentTokenResponse
	if err := c.doThunderRequest(http.MethodPost, apiURL, apiToken, thunderEnrollmentTokenPath, payload, &response); err != nil {
		log.Error().
			Str("container_id", request.ContainerId).
			Str("zone_id", payload.ZoneID).
			Str("gpu_type", payload.GPUType).
			Int("gpu_count", payload.GPUCount).
			Err(err).
			Msg("failed to assign Thunder virtual GPU")
		return nil, err
	}

	c.allocations.Set(request.ContainerId, thunderAllocation{
		EnrollmentTokenID: response.EnrollmentTokenID,
		EnrollmentToken:   response.EnrollmentToken,
		APIURL:            apiURL,
		APIToken:          apiToken,
		Response:          response,
	})

	log.Info().
		Str("container_id", request.ContainerId).
		Str("enrollment_token_id", response.EnrollmentTokenID).
		Str("zone_id", response.ZoneID).
		Str("gpu_type", response.GPUType).
		Int("gpu_count", response.GPUCount).
		Msg("assigned Thunder virtual GPU")
	return []int{}, nil
}

func (c *ContainerThunderManager) GetContainerGPUDevices(containerId string) []int {
	return []int{}
}

func (c *ContainerThunderManager) UnassignGPUDevices(containerId string) {
	allocation, ok := c.allocations.Get(containerId)
	if !ok {
		log.Debug().Str("container_id", containerId).Msg("skipping Thunder virtual GPU unassign because no allocation was recorded")
		return
	}
	if strings.TrimSpace(allocation.EnrollmentTokenID) == "" {
		log.Error().Str("container_id", containerId).Msg("missing Thunder enrollment token id for virtual GPU unenrollment")
		c.allocations.Delete(containerId)
		return
	}

	path := fmt.Sprintf(thunderEnrollmentTokenNodePath, allocation.EnrollmentTokenID)
	log.Info().
		Str("container_id", containerId).
		Str("enrollment_token_id", allocation.EnrollmentTokenID).
		Msg("unassigning Thunder virtual GPU")
	if err := c.doThunderRequest(http.MethodDelete, allocation.APIURL, allocation.APIToken, path, nil, nil); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to unregister Thunder virtual GPU client")
	} else {
		log.Info().
			Str("container_id", containerId).
			Str("enrollment_token_id", allocation.EnrollmentTokenID).
			Msg("unassigned Thunder virtual GPU")
	}
	c.allocations.Delete(containerId)
}

func (c *ContainerThunderManager) CDIDevices(assignedDevices []int) []string {
	return []string{}
}

func (c *ContainerThunderManager) InjectEnvVars(env []string) []string {
	return injectCudaEnvVars(env)
}

func (c *ContainerThunderManager) InjectAssignedEnvVars(env []string, assignedDevices []int) []string {
	return env
}

func (c *ContainerThunderManager) InjectMounts(mounts []specs.Mount) []specs.Mount {
	return injectCudaMounts(mounts)
}

func (c *ContainerThunderManager) doThunderRequest(method, apiURL, apiToken, path string, payload any, response any) error {
	endpoint, err := thunderEndpoint(apiURL, path)
	if err != nil {
		return err
	}

	var bodyReader *bytes.Reader
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, endpoint, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		log.Error().Str("method", method).Str("path", path).Err(err).Msg("Thunder API request failed")
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("Thunder API %s %s returned status %d", method, path, resp.StatusCode)
		log.Error().Str("method", method).Str("path", path).Int("status_code", resp.StatusCode).Msg("Thunder API request failed")
		return err
	}
	if response == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		log.Error().Str("method", method).Str("path", path).Err(err).Msg("Thunder API response decode failed")
		return err
	}
	return nil
}

func thunderEndpoint(apiURL, path string) (string, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(apiURL, "/") + path, nil
}

func thunderGPUType(request *types.ContainerRequest) string {
	if request == nil {
		return ""
	}
	if request.Gpu != "" && request.Gpu != string(types.NO_GPU) {
		return request.Gpu
	}
	for _, gpu := range request.GpuRequest {
		if gpu != "" && gpu != string(types.NO_GPU) {
			return gpu
		}
	}
	return request.Gpu
}

func thunderGPUCount(request *types.ContainerRequest) uint32 {
	if request == nil {
		return 0
	}
	if request.GpuCount > 0 {
		return request.GpuCount
	}
	if request.RequiresGPU() {
		return 1
	}
	return 0
}

func (c *ContainerThunderManager) thunderCredentials(request *types.ContainerRequest) (string, string) {
	apiURL := strings.TrimSpace(c.apiURL)
	apiToken := strings.TrimSpace(c.apiToken)
	if request == nil {
		return apiURL, apiToken
	}

	if value, ok := thunderRequestEnv(request.Env, thunderAPIURLEnv); ok {
		apiURL = value
	}
	if value, ok := thunderRequestEnv(request.Env, thunderAPITokenEnv); ok {
		apiToken = value
	}
	return apiURL, apiToken
}

func thunderZoneID(request *types.ContainerRequest) string {
	zoneID := strings.TrimSpace(os.Getenv(thunderZoneIDEnv))
	if request == nil {
		return zoneID
	}
	if value, ok := thunderRequestEnv(request.Env, thunderZoneIDEnv); ok {
		zoneID = value
	}
	return zoneID
}

func thunderRequestEnv(env []string, key string) (string, bool) {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(item, prefix)), true
		}
	}
	return "", false
}

func (s *Worker) installThunderClient(ctx context.Context, request *types.ContainerRequest) error {
	if s == nil || request == nil || !request.GpuVirtualized {
		return nil
	}
	manager, ok := s.containerThunderManager.(*ContainerThunderManager)
	if !ok || manager == nil {
		return fmt.Errorf("thunder manager unavailable")
	}
	allocation, ok := manager.allocations.Get(request.ContainerId)
	if !ok || strings.TrimSpace(allocation.EnrollmentToken) == "" {
		return fmt.Errorf("missing Thunder enrollment token for container %s", request.ContainerId)
	}
	instance, ok := s.containerInstances.Get(request.ContainerId)
	if !ok || instance == nil || instance.Runtime == nil {
		return fmt.Errorf("container runtime unavailable for Thunder install")
	}

	env := append([]string(nil), instance.Spec.Process.Env...)
	if !containsEnvKey(env, "PATH") {
		env = append(env, "PATH="+strings.Join(defaultContainerPath, ":"))
	}
	cwd := "/"
	if instance.Spec != nil && instance.Spec.Process != nil {
		if instance.Spec.Process.Cwd != "" {
			cwd = instance.Spec.Process.Cwd
		}
	}
	cmd := "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_INSTALL_MODE=client THUNDER_AUTH_TOKEN=" + common.ShellQuote(allocation.EnrollmentToken) + " sh"
	proc := specs.Process{
		Args: []string{"sh", "-c", cmd},
		Cwd:  cwd,
		Env:  env,
	}
	installCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	log.Info().
		Str("container_id", request.ContainerId).
		Str("enrollment_token_id", allocation.EnrollmentTokenID).
		Msg("installing Thunder client in sandbox")
	if err := instance.Runtime.Exec(installCtx, request.ContainerId, proc, nil); err != nil {
		return fmt.Errorf("failed to install Thunder client: %w", err)
	}
	log.Info().
		Str("container_id", request.ContainerId).
		Str("enrollment_token_id", allocation.EnrollmentTokenID).
		Msg("installed Thunder client in sandbox")
	return nil
}

func containsEnvKey(env []string, key string) bool {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return true
		}
	}
	return false
}
