package thunder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
)

const (
	thunderAPIURLEnv           = "THUNDER_API_URL"
	thunderAPITokenEnv         = "THUNDER_API_TOKEN"
	thunderEnrollmentTokenPath = "/api/v1/enrollment-tokens"
	thunderZonesPath           = "/api/v1/zones"

	thunderEnrollmentRoleClient = "client"
	thunderEnrollmentRoleServer = "server"
)

type Client struct {
	apiURL     string
	apiToken   string
	httpClient *http.Client
}

type Zone struct {
	ZoneID      string `json:"zoneId"`
	OrgID       string `json:"orgId"`
	DisplayName string `json:"displayName"`
}

type EnrollmentToken struct {
	EnrollmentTokenID string    `json:"enrollmentTokenId"`
	EnrollmentToken   string    `json:"enrollmentToken"`
	OrgID             string    `json:"orgId"`
	ZoneID            string    `json:"zoneId"`
	Role              string    `json:"role"`
	GPUType           string    `json:"gpuType"`
	GPUCount          int       `json:"gpuCount"`
	ExpiresAt         time.Time `json:"expiresAt"`
}

type DeleteEnrollmentTokenNodeResponse struct {
	EnrollmentTokenID string    `json:"enrollmentTokenId"`
	Role              string    `json:"role"`
	ClientID          string    `json:"clientId"`
	HostID            string    `json:"hostId"`
	NodeDeleted       bool      `json:"nodeDeleted"`
	DeletedAt         time.Time `json:"deletedAt"`
}

type ThunderError struct {
	StatusCode int
	ErrorCode  string
	Message    string
	Code       int
}

func (e *ThunderError) Error() string {
	if e == nil {
		return "Thunder API error"
	}
	parts := []string{fmt.Sprintf("Thunder API returned status %d", e.StatusCode)}
	if e.ErrorCode != "" {
		parts = append(parts, e.ErrorCode)
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	return strings.Join(parts, ": ")
}

func NewClientFromEnv(httpClient *http.Client) *Client {
	return NewClient(os.Getenv(thunderAPIURLEnv), os.Getenv(thunderAPITokenEnv), httpClient)
}

func NewClient(apiURL, apiToken string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		apiURL:     strings.TrimRight(strings.TrimSpace(apiURL), "/"),
		apiToken:   strings.TrimSpace(apiToken),
		httpClient: httpClient,
	}
}

func (c *Client) ClientInstallCommand(enrollmentToken string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("Thunder client is required")
	}
	if c.apiURL == "" {
		return "", fmt.Errorf("%s is required", thunderAPIURLEnv)
	}
	enrollmentToken = strings.TrimSpace(enrollmentToken)
	if enrollmentToken == "" {
		return "", fmt.Errorf("Thunder enrollment token is required")
	}
	return "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_NOWARN=1 THUNDER_INSTALL_MODE=client THUNDER_CENTRAL_URL=" + common.ShellQuote(c.apiURL) + " THUNDER_ENROLLMENT_TOKEN=" + common.ShellQuote(enrollmentToken) + " sh", nil
}

func (c *Client) CreateZone(ctx context.Context, displayName string) (*Zone, error) {
	var response Zone
	payload := createZoneRequest{DisplayName: strings.TrimSpace(displayName)}
	if err := c.do(ctx, http.MethodPost, thunderZonesPath, payload, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) DeleteZone(ctx context.Context, zoneID string) error {
	zoneID = strings.TrimSpace(zoneID)
	if zoneID == "" {
		return fmt.Errorf("Thunder zone id is required")
	}
	return c.do(ctx, http.MethodDelete, "/api/v1/zones/"+url.PathEscape(zoneID), nil, nil)
}

func (c *Client) CreateClientEnrollmentToken(ctx context.Context, zoneID, gpuType string, gpuCount int) (*EnrollmentToken, error) {
	zoneID = strings.TrimSpace(zoneID)
	gpuType = strings.TrimSpace(gpuType)
	if zoneID == "" {
		return nil, fmt.Errorf("Thunder zone id is required")
	}
	if gpuType == "" {
		return nil, fmt.Errorf("Thunder GPU type is required for client enrollment")
	}
	if gpuCount <= 0 {
		return nil, fmt.Errorf("Thunder GPU count must be greater than zero for client enrollment")
	}
	return c.createEnrollmentToken(ctx, createEnrollmentTokenRequest{
		ZoneID:   zoneID,
		Role:     thunderEnrollmentRoleClient,
		GPUType:  gpuType,
		GPUCount: gpuCount,
	})
}

func (c *Client) CreateServerEnrollmentToken(ctx context.Context, zoneID string) (*EnrollmentToken, error) {
	zoneID = strings.TrimSpace(zoneID)
	if zoneID == "" {
		return nil, fmt.Errorf("Thunder zone id is required")
	}
	return c.createEnrollmentToken(ctx, createEnrollmentTokenRequest{
		ZoneID: zoneID,
		Role:   thunderEnrollmentRoleServer,
	})
}

func (c *Client) DeleteEnrollmentTokenNode(ctx context.Context, enrollmentTokenID string) (*DeleteEnrollmentTokenNodeResponse, error) {
	enrollmentTokenID = strings.TrimSpace(enrollmentTokenID)
	if enrollmentTokenID == "" {
		return nil, fmt.Errorf("Thunder enrollment token id is required")
	}
	var response DeleteEnrollmentTokenNodeResponse
	path := fmt.Sprintf("/api/v1/enrollment-tokens/%s/node", url.PathEscape(enrollmentTokenID))
	if err := c.do(ctx, http.MethodDelete, path, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) createEnrollmentToken(ctx context.Context, payload createEnrollmentTokenRequest) (*EnrollmentToken, error) {
	var response EnrollmentToken
	if err := c.do(ctx, http.MethodPost, thunderEnrollmentTokenPath, payload, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *Client) do(ctx context.Context, method, path string, payload any, response any) error {
	if c == nil {
		return fmt.Errorf("Thunder client is required")
	}
	if c.apiURL == "" {
		return fmt.Errorf("%s is required", thunderAPIURLEnv)
	}
	if c.apiToken == "" {
		return fmt.Errorf("%s is required", thunderAPITokenEnv)
	}
	endpoint, err := thunderEndpoint(c.apiURL, path)
	if err != nil {
		return err
	}

	var body io.Reader
	if payload != nil {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(payload); err != nil {
			return err
		}
		body = buf
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if response != nil {
		req.Header.Set("Accept", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return decodeThunderError(resp)
	}
	if response == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(response)
}

func decodeThunderError(resp *http.Response) error {
	apiErr := &ThunderError{StatusCode: resp.StatusCode}
	var payload thunderErrorPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err == nil {
		apiErr.ErrorCode = payload.Error
		apiErr.Message = payload.Message
		apiErr.Code = payload.Code
	}
	return apiErr
}

func thunderEndpoint(apiURL, path string) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(apiURL), "/")
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid Thunder API URL %q", apiURL)
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path, nil
}

type createZoneRequest struct {
	DisplayName string `json:"displayName,omitempty"`
}

type createEnrollmentTokenRequest struct {
	ZoneID   string `json:"zoneId"`
	Role     string `json:"role"`
	GPUType  string `json:"gpuType,omitempty"`
	GPUCount int    `json:"gpuCount,omitempty"`
}

type thunderErrorPayload struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}
