package task

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func setupEchoContext(jsonBody string, queryParams url.Values) echo.Context {
	e := echo.New()
	reqURL := "/"
	if len(queryParams) > 0 {
		reqURL += "?" + queryParams.Encode()
	}
	req := httptest.NewRequest(http.MethodPost, reqURL, strings.NewReader(jsonBody))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

	// Parse the query params into the request
	req.URL.RawQuery = queryParams.Encode()

	rec := httptest.NewRecorder()
	return e.NewContext(req, rec)
}

func TestSerializeHttpPayload(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		queryParams url.Values
		wantPayload *types.TaskPayload
		wantErr     bool
	}{
		{
			name: "complete empty body",
			body: ``,
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{},
			},
			wantErr: false,
		},
		{
			name: "empty json object",
			body: `{}`,
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{},
			},
			wantErr: false,
		},
		{
			name: "kwargs only",
			body: `{"mykwarg": 1, "mykwarg2": 2}`,
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"mykwarg": 1.0, "mykwarg2": 2.0},
			},
			wantErr: false,
		},
		{
			name: "args and kwargs",
			body: `{"args": [1, 2, 3], "mykwarg": "value"}`,
			wantPayload: &types.TaskPayload{
				Args:   []interface{}{1.0, 2.0, 3.0},
				Kwargs: map[string]interface{}{"mykwarg": "value"},
			},
			wantErr: false,
		},
		{
			name: "args only",
			body: `{"args": [1, 2, 3]}`,
			wantPayload: &types.TaskPayload{
				Args:   []interface{}{1.0, 2.0, 3.0},
				Kwargs: make(map[string]interface{}),
			},
			wantErr: false,
		},
		{
			name: "explicit kwargs and args",
			body: `{"kwargs": {"test": 1}, "args": [1, 2, 3]}`,
			wantPayload: &types.TaskPayload{
				Args:   []interface{}{1.0, 2.0, 3.0},
				Kwargs: map[string]interface{}{"test": 1.0},
			},
			wantErr: false,
		},
		{
			name:        "malformed json",
			body:        `{"args": [1, 2, 3}`,
			wantPayload: nil,
			wantErr:     true,
		},
		{
			name: "nested kwargs",
			body: `{"kwargs": {"nestedList": [1, 2, 3], "nestedMap": {"key": "value"}}}`,
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"nestedList": []interface{}{1.0, 2.0, 3.0}, "nestedMap": map[string]interface{}{"key": "value"}},
			},
			wantErr: false,
		},
		{
			name:        "list of strings",
			body:        `{}`,
			queryParams: url.Values{"listOfStrings": []string{"a", "b", "c"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"listOfStrings": []string{"a", "b", "c"}},
			},
			wantErr: false,
		},
		{
			name:        "single number",
			body:        `{}`,
			queryParams: url.Values{"sleep": []string{"100"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"sleep": float64(100)},
			},
			wantErr: false,
		},
		{
			name:        "single number no body",
			queryParams: url.Values{"sleep": []string{"100"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"sleep": float64(100)},
			},
			wantErr: false,
		},
		{
			name:        "list of ints",
			queryParams: url.Values{"sleep": []string{"100", "200", "300"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"sleep": []float64{100, 200, 300}},
			},
			wantErr: false,
		},
		{
			name:        "list of floats",
			queryParams: url.Values{"sleep": []string{"100.1", "200.2", "300.3"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"sleep": []float64{100.1, 200.2, 300.3}},
			},
			wantErr: false,
		},
		{
			name:        "list of mixed ints and floats",
			queryParams: url.Values{"sleep": []string{"100", "200.2", "300"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"sleep": []float64{100.0, 200.2, 300.0}},
			},
			wantErr: false,
		},
		{
			name:        "mix of strings and numbers",
			queryParams: url.Values{"sleep": []string{"Today", "200.2", "300"}},
			wantPayload: &types.TaskPayload{
				Args:   nil,
				Kwargs: map[string]interface{}{"sleep": []string{"Today", "200.2", "300"}},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := setupEchoContext(tt.body, tt.queryParams)
			got, err := SerializeHttpPayload(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerializeHttpPayload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.wantPayload) {
				t.Errorf("SerializeHttpPayload() got = %+v, want %+v", got, tt.wantPayload)
			}
		})
	}
}
