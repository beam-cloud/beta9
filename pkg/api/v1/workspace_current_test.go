package apiv1

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestCurrentWorkspacePlatformOperatorIsDerivedFromToken(t *testing.T) {
	tests := []struct {
		name  string
		token *types.Token
		want  bool
	}{
		{name: "active cluster admin", token: &types.Token{TokenType: types.TokenTypeClusterAdmin, Active: true}, want: true},
		{name: "inactive cluster admin", token: &types.Token{TokenType: types.TokenTypeClusterAdmin}, want: false},
		{name: "disabled cluster admin", token: &types.Token{TokenType: types.TokenTypeClusterAdmin, Active: true, DisabledByClusterAdmin: true}, want: false},
		{name: "workspace primary", token: &types.Token{TokenType: types.TokenTypeWorkspacePrimary, Active: true}, want: false},
		{name: "workspace restricted", token: &types.Token{TokenType: types.TokenTypeWorkspaceRestricted, Active: true}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			recorder := httptest.NewRecorder()
			ctx := e.NewContext(httptest.NewRequest("GET", "/api/v1/workspace/current?platform_operator=true", nil), recorder)
			authCtx := &auth.HttpAuthContext{
				Context: ctx,
				AuthInfo: &auth.AuthInfo{
					Workspace: &types.Workspace{ExternalId: "workspace-1", Name: "test"},
					Token:     tt.token,
				},
			}

			if err := (&WorkspaceGroup{}).CurrentWorkspace(authCtx); err != nil {
				t.Fatal(err)
			}
			var body map[string]interface{}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			got, ok := body["platform_operator"].(bool)
			if !ok || got != tt.want {
				t.Fatalf("platform_operator = %#v, want %v", body["platform_operator"], tt.want)
			}
		})
	}
}
