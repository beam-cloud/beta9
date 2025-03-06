package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerCostResponse struct {
	CostPerMs float64 `json:"cost_per_ms"`
}

type ContainerCostClient struct {
	client   *http.Client
	endpoint string
	token    string
}

func NewContainerCostClient(config types.ContainerCostHookConfig) *ContainerCostClient {
	return &ContainerCostClient{
		client:   &http.Client{},
		endpoint: config.Endpoint,
		token:    config.Token,
	}
}

func (c *ContainerCostClient) GetContainerCostPerMs(request *types.ContainerRequest) (float64, error) {
	var requestBody bytes.Buffer
	if err := json.NewEncoder(&requestBody).Encode(request); err != nil {
		return 0, err
	}

	req, err := http.NewRequest("POST", c.endpoint, &requestBody)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}

	var response ContainerCostResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, err
	}

	return response.CostPerMs, nil
}
