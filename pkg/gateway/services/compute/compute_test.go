package compute

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestPrivatePoolCreatedByAuthRequiresCreatorToken(t *testing.T) {
	authInfo := &auth.AuthInfo{
		Token: &types.Token{ExternalId: "token-owner"},
	}

	tests := []struct {
		name  string
		state *model.PoolState
		want  bool
	}{
		{
			name:  "matching creator",
			state: &model.PoolState{CreatedByTokenID: "token-owner"},
			want:  true,
		},
		{
			name:  "different creator",
			state: &model.PoolState{CreatedByTokenID: "other-token"},
			want:  false,
		},
		{
			name:  "missing creator",
			state: &model.PoolState{},
			want:  false,
		},
		{
			name:  "nil state",
			state: nil,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computePoolCreatedByAuth(tt.state, authInfo); got != tt.want {
				t.Fatalf("computePoolCreatedByAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilterPrivatePoolsCreatedByAuth(t *testing.T) {
	authInfo := &auth.AuthInfo{
		Token: &types.Token{ExternalId: "token-owner"},
	}
	states := []*model.PoolState{
		{Name: "owned", CreatedByTokenID: "token-owner"},
		{Name: "other", CreatedByTokenID: "other-token"},
		{Name: "legacy"},
	}

	filtered := filterPrivatePoolsCreatedByAuth(states, authInfo)
	if len(filtered) != 1 {
		t.Fatalf("expected one owned pool, got %d", len(filtered))
	}
	if filtered[0].Name != "owned" {
		t.Fatalf("expected owned pool, got %q", filtered[0].Name)
	}
}

func TestIsLocalGatewayURL(t *testing.T) {
	tests := []struct {
		rawURL string
		want   bool
	}{
		{rawURL: "http://localhost:1994", want: true},
		{rawURL: "http://127.0.0.1:1994", want: true},
		{rawURL: "http://[::1]:1994", want: true},
		{rawURL: "https://gateway.beam.cloud", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.rawURL, func(t *testing.T) {
			if got := isLocalGatewayURL(tt.rawURL); got != tt.want {
				t.Fatalf("isLocalGatewayURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateAgentTransportConfig(t *testing.T) {
	s := &Service{
		appConfig: types.AppConfig{
			Tailscale: types.TailscaleConfig{
				Enabled:      true,
				AuthKey:      "tskey-auth-gateway",
				AgentAuthKey: "tskey-auth-worker",
			},
		},
	}

	if err := s.validateAgentTransportConfig(types.BackendRouteTransportTSNet); err != nil {
		t.Fatalf("expected configured tsnet transport to pass, got %v", err)
	}

	s.appConfig.Tailscale.AgentAuthKey = ""
	if err := s.validateAgentTransportConfig(types.BackendRouteTransportTSNet); err == nil {
		t.Fatal("expected missing agent auth key to fail")
	}
}
