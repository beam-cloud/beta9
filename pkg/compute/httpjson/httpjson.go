package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

type Client struct {
	BaseURL    string
	Token      string
	AuthHeader string
	Client     *http.Client
}

func (c Client) Do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		header := c.AuthHeader
		value := c.Token
		if header == "" {
			header = "Authorization"
			value = "Bearer " + c.Token
		}
		req.Header.Set(header, value)
	}

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s failed with status %d: %s", method, path, resp.StatusCode, string(data))
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

func String(m map[string]any, keys ...string) string {
	return lookup(m, keys, stringValue)
}

func Int64(m map[string]any, keys ...string) int64 {
	return lookup(m, keys, int64Value)
}

func Float64(m map[string]any, keys ...string) float64 {
	return lookup(m, keys, float64Value)
}

func Array(data map[string]any, keys ...string) []any {
	return lookup(data, keys, arrayValue)
}

func lookup[T any](data map[string]any, keys []string, convert func(any) (T, bool)) T {
	var zero T
	for _, key := range keys {
		if v, ok := data[key]; ok {
			if out, ok := convert(v); ok {
				return out
			}
		}
	}
	return zero
}

func stringValue(value any) (string, bool) {
	switch t := value.(type) {
	case string:
		return t, true
	case fmt.Stringer:
		return t.String(), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
}

func int64Value(value any) (int64, bool) {
	switch t := value.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	case string:
		i, err := strconv.ParseInt(t, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func float64Value(value any) (float64, bool) {
	switch t := value.(type) {
	case float64:
		return t, true
	case int64:
		return float64(t), true
	case int:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func arrayValue(value any) ([]any, bool) {
	arr, ok := value.([]any)
	return arr, ok
}
