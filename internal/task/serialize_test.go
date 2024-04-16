package task

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

func setupEchoContext(jsonBody string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(jsonBody))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	return e.NewContext(req, rec)
}
func TestSerializeHttpPayload(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantPayload *types.TaskPayload
		wantErr     bool
	}{
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
				Kwargs: nil,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := setupEchoContext(tt.body)
			got, err := SerializeHttpPayload(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerializeHttpPayload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.wantPayload) {
				t.Errorf("SerializeHttpPayload() got = %v, want %v", got, tt.wantPayload)
			}
		})
	}
}
