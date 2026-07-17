package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	thunderAPIURLEnv          = "THUNDER_API_URL"
	thunderAPITokenEnv        = "THUNDER_API_TOKEN"
	thunderRegisterClientPath = "/register-client"
	thunderDeleteClientPath   = "/delete-client"
	thunderLibraryAssetPath   = "/assets/libthunder.so"
	thunderRequestAttempts    = 3
	thunderRetryDelay         = time.Second
)

const (
	thunderConfigPath  = "/etc/thunder/config.json"
	thunderTokenPath   = "/etc/thunder/token"
	thunderLibraryPath = "/etc/thunder/libthunder.so"
)

type ContainerThunderManager struct {
	apiURL      string
	apiToken    string
	client      *http.Client
	allocations *common.SafeMap[thunderAllocation]
}

type thunderAllocation struct {
	Token string
}

type thunderRegisterClientRequest struct {
	DeviceID string `json:"deviceId"`
	GPUType  string `json:"gpuType"`
	GPUCount int    `json:"gpuCount"`
}

type thunderRegisterClientResponse struct {
	Token string `json:"token"`
}

type thunderDeleteClientRequest struct {
	DeviceID string `json:"deviceId"`
	Token    string `json:"token"`
}

type thunderConfigFile struct {
	DeviceID        string `json:"deviceId"`
	GPUCount        int    `json:"gpuCount"`
	GPUType         string `json:"gpuType"`
	EnableGRPCTLS   bool   `json:"enableGrpcTls"`
	CentralApiUrl   string `json:"centralApiUrl"`
	CentralZoneId   string `json:"centralZoneId"`
	CentralApiToken string `json:"centralApiToken"`
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
	if c.apiURL == "" {
		return nil, fmt.Errorf("%s is required for virtualized GPU requests", thunderAPIURLEnv)
	}
	if c.apiToken == "" {
		return nil, fmt.Errorf("%s is required for virtualized GPU requests", thunderAPITokenEnv)
	}

	payload := thunderRegisterClientRequest{
		DeviceID: request.ContainerId,
		GPUType:  thunderGPUType(request),
		GPUCount: int(thunderGPUCount(request)),
	}

	var response thunderRegisterClientResponse
	if err := c.doThunderRequest(http.MethodPost, thunderRegisterClientPath, payload, &response); err != nil {
		return nil, err
	}

	c.allocations.Set(request.ContainerId, thunderAllocation{Token: response.Token})
	return []int{}, nil
}

func (c *ContainerThunderManager) GetContainerGPUDevices(containerId string) []int {
	return []int{}
}

func (c *ContainerThunderManager) UnassignGPUDevices(containerId string) {
	allocation, ok := c.allocations.Get(containerId)
	if !ok {
		return
	}

	payload := thunderDeleteClientRequest{DeviceID: containerId, Token: allocation.Token}
	if err := c.doThunderRequest(http.MethodPost, thunderDeleteClientPath, payload, nil); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to unregister Thunder virtual GPU client")
	}
	c.allocations.Delete(containerId)
}

func (c *ContainerThunderManager) CDIDevices(assignedDevices []int) []string {
	return []string{}
}

func (c *ContainerThunderManager) InjectEnvVars(env []string) []string {
	return withLDPreload(injectCudaEnvVars(env), thunderLibraryPath)
}

func (c *ContainerThunderManager) InjectAssignedEnvVars(env []string, assignedDevices []int) []string {
	return env
}

func (c *ContainerThunderManager) InjectMounts(mounts []specs.Mount) []specs.Mount {
	return injectCudaMounts(mounts)
}

func (c *ContainerThunderManager) doThunderRequest(method, path string, payload any, response any) error {
	endpoint, err := c.thunderEndpoint(path)
	if err != nil {
		return err
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= thunderRequestAttempts; attempt++ {
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(req)
		if err == nil && resp != nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				defer resp.Body.Close()
				if response == nil {
					return nil
				}
				return json.NewDecoder(resp.Body).Decode(response)
			}
			resp.Body.Close()
			lastErr = fmt.Errorf("Thunder API %s %s returned status %d", method, path, resp.StatusCode)
		} else if err != nil {
			lastErr = err
		}

		if attempt < thunderRequestAttempts {
			time.Sleep(thunderRetryDelay)
		}
	}

	return lastErr
}

func (c *ContainerThunderManager) thunderEndpoint(path string) (string, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return c.apiURL + path, nil
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

func (c *ContainerThunderManager) PrepareContainerFilesystem(request *types.ContainerRequest, rootPath string) error {
	return c.InjectThunderFiles(request, rootPath)
}

func (c *ContainerThunderManager) InjectThunderFiles(request *types.ContainerRequest, rootPath string) error {
	if request == nil {
		return fmt.Errorf("missing container request")
	}
	if rootPath == "" {
		return fmt.Errorf("missing container root path")
	}

	allocation, err := c.thunderAllocationForRequest(request)
	if err != nil {
		return err
	}

	if err := c.createThunderLibrary(rootPath); err != nil {
		return err
	}
	if err := createThunderToken(rootPath, allocation.Token); err != nil {
		return err
	}
	if err := c.createThunderConfig(rootPath, request); err != nil {
		return err
	}
	return nil
}

func (c *ContainerThunderManager) thunderAllocationForRequest(request *types.ContainerRequest) (thunderAllocation, error) {
	allocation, ok := c.allocations.Get(request.ContainerId)
	if ok {
		return allocation, nil
	}

	if _, err := c.AssignGPUDevices(request); err != nil {
		return thunderAllocation{}, err
	}
	allocation, ok = c.allocations.Get(request.ContainerId)
	if !ok {
		return thunderAllocation{}, fmt.Errorf("missing Thunder allocation for container %s", request.ContainerId)
	}
	return allocation, nil
}

func (c *ContainerThunderManager) createThunderLibrary(rootPath string) error {
	contents, err := c.downloadThunderAsset(thunderLibraryAssetPath)
	if err != nil {
		return err
	}
	return writeThunderFile(rootPath, thunderLibraryPath, contents, 0644)
}

func createThunderToken(rootPath string, token string) error {
	return writeThunderFile(rootPath, thunderTokenPath, []byte(token), 0644)
}

func (c *ContainerThunderManager) createThunderConfig(rootPath string, request *types.ContainerRequest) error {
	config := thunderConfigFile{
		DeviceID:        request.ContainerId,
		GPUCount:        int(thunderGPUCount(request)),
		GPUType:         thunderGPUType(request),
		EnableGRPCTLS:   false,
		CentralApiUrl:   c.apiURL,
		CentralZoneId:   "thunder-beam",
		CentralApiToken: c.apiToken,
	}

	contents, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return writeThunderFile(rootPath, thunderConfigPath, contents, 0644)
}

func writeThunderFile(rootPath string, containerPath string, contents []byte, perm os.FileMode) error {
	targetPath := thunderHostPath(rootPath, containerPath)
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	return os.WriteFile(targetPath, contents, perm)
}

func thunderHostPath(rootPath string, containerPath string) string {
	cleanPath := filepath.Clean(containerPath)
	cleanPath = strings.TrimPrefix(cleanPath, string(os.PathSeparator))
	return filepath.Join(rootPath, cleanPath)
}

func (c *ContainerThunderManager) downloadThunderAsset(path string) ([]byte, error) {
	if c.apiURL == "" {
		return nil, fmt.Errorf("%s is required for Thunder asset download", thunderAPIURLEnv)
	}

	endpoint, err := c.thunderEndpoint(path)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= thunderRequestAttempts; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.client.Do(req)
		if err == nil && resp != nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				contents, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if readErr != nil {
					return nil, readErr
				}
				return contents, nil
			}
			resp.Body.Close()
			lastErr = fmt.Errorf("Thunder asset GET %s returned status %d", path, resp.StatusCode)
		} else if err != nil {
			lastErr = err
		}

		if attempt < thunderRequestAttempts {
			time.Sleep(thunderRetryDelay)
		}
	}

	return nil, lastErr
}
