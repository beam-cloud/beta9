package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	types "github.com/beam-cloud/beam/internal/types"
	"github.com/okteto/okteto/pkg/log"
)

type ApiConfig struct {
	RequestTimeout int
	BackendBaseUrl string
}

var clientConfig ApiConfig = ApiConfig{}

func init() {
	clientConfig.BackendBaseUrl = os.Getenv("BEAMAPI_BASE_URL")
}

type ApiClient struct {
	token string
}

func NewApiClient(token string) *ApiClient {
	return &ApiClient{
		token: token,
	}
}

func (c *ApiClient) GetContainerConfig(request *ContainerConfigRequest) (*ContainerConfigResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	responseBody, err := c.post("v1/app/config/", payload)
	if err != nil {
		return nil, err
	}

	var response ContainerConfigResponse
	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *ApiClient) parseValidationError(responseBody []byte) (string, error) {
	var validationError ValidationError

	if err := json.Unmarshal(responseBody, &validationError); err != nil {
		return "", err
	}

	return validationError.Details[0], nil
}

func (c *ApiClient) post(route string, payload []byte) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", clientConfig.BackendBaseUrl, route)

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("Authorization", fmt.Sprintf("s2s %s", c.token))

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			log.Print("\r")
			log.Warning("Network error: unable to connect to Beam host")
		}

		return nil, err
	}

	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// TODO: check more status codes
	if response.StatusCode == http.StatusUnauthorized {
		return nil, &types.CredentialsErrorInvalidProfile{}
	}

	if response.StatusCode == http.StatusBadRequest {
		details, parseError := c.parseValidationError(responseBody)

		if parseError != nil {
			return nil, parseError
		}

		return nil, errors.New(details)
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("status code %s: request did not succeed ", strconv.Itoa(response.StatusCode))
	}

	return responseBody, nil
}
