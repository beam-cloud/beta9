package gatewayservices

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestHybridPoolCreatedByAuthRequiresCreatorToken(t *testing.T) {
	authInfo := &auth.AuthInfo{
		Token: &types.Token{ExternalId: "token-owner"},
	}

	tests := []struct {
		name  string
		state *hybrid.PoolState
		want  bool
	}{
		{
			name:  "matching creator",
			state: &hybrid.PoolState{CreatedByTokenID: "token-owner"},
			want:  true,
		},
		{
			name:  "different creator",
			state: &hybrid.PoolState{CreatedByTokenID: "other-token"},
			want:  false,
		},
		{
			name:  "missing creator",
			state: &hybrid.PoolState{},
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
			if got := hybridPoolCreatedByAuth(tt.state, authInfo); got != tt.want {
				t.Fatalf("hybridPoolCreatedByAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilterHybridPoolsCreatedByAuth(t *testing.T) {
	authInfo := &auth.AuthInfo{
		Token: &types.Token{ExternalId: "token-owner"},
	}
	states := []*hybrid.PoolState{
		{Name: "owned", CreatedByTokenID: "token-owner"},
		{Name: "other", CreatedByTokenID: "other-token"},
		{Name: "legacy"},
	}

	filtered := filterHybridPoolsCreatedByAuth(states, authInfo)
	if len(filtered) != 1 {
		t.Fatalf("expected one owned pool, got %d", len(filtered))
	}
	if filtered[0].Name != "owned" {
		t.Fatalf("expected owned pool, got %q", filtered[0].Name)
	}
}
