package compute

import (
	"context"
	"reflect"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

type fakeTelemetryCredentialIssuer struct {
	args []s2.IssueAccessTokenArgs
}

func (f *fakeTelemetryCredentialIssuer) Issue(_ context.Context, args s2.IssueAccessTokenArgs) (*s2.IssueAccessTokenResponse, error) {
	f.args = append(f.args, args)
	return &s2.IssueAccessTokenResponse{AccessToken: "token-" + string(args.ID)}, nil
}

func TestScopedTelemetryConfigUsesWorkspaceWriteOnlyPrefixes(t *testing.T) {
	issuer := &fakeTelemetryCredentialIssuer{}
	service := &Service{
		appConfig: types.AppConfig{
			Database: types.DatabaseConfig{
				S2: types.S2Config{
					ApiKey: "root-token",
					Basin:  "events-basin",
				},
			},
		},
		telemetryCredentials: issuer,
	}

	config, err := service.scopedTelemetryConfig(context.Background(), "workspace-123")
	if err != nil {
		t.Fatalf("scopedTelemetryConfig() error = %v", err)
	}
	if config == nil || !config.Enabled {
		t.Fatalf("expected enabled scoped telemetry config, got %#v", config)
	}
	if config.Logs == nil {
		t.Fatal("expected logs sink")
	}
	if got, want := config.Logs.StreamPrefix, "events/logs/workspaces/workspace-123"; got != want {
		t.Fatalf("log prefix = %q, want %q", got, want)
	}
	if got, want := config.Logs.Destination, "events-basin"; got != want {
		t.Fatalf("log destination = %q, want %q", got, want)
	}
	if config.Events == nil {
		t.Fatal("expected events sink")
	}
	if got, want := config.Events.StreamPrefix, "events/workspaces/workspace-123"; got != want {
		t.Fatalf("event prefix = %q, want %q", got, want)
	}
	if got, want := config.Events.Destination, "events-basin"; got != want {
		t.Fatalf("event destination = %q, want %q", got, want)
	}
	if len(issuer.args) != 2 {
		t.Fatalf("expected two issued tokens, got %d", len(issuer.args))
	}

	wantOps := []string{s2.OperationAppend}
	for _, args := range issuer.args {
		if args.Scope.Basins == nil || args.Scope.Basins.Exact == nil || *args.Scope.Basins.Exact != "events-basin" {
			t.Fatalf("unexpected basin scope: %#v", args.Scope.Basins)
		}
		if !reflect.DeepEqual(args.Scope.Ops, wantOps) {
			t.Fatalf("ops = %#v, want %#v", args.Scope.Ops, wantOps)
		}
		for _, forbidden := range []string{s2.OperationCreateStream, s2.OperationRead, s2.OperationDeleteStream, s2.OperationListBasins, s2.OperationIssueAccessToken} {
			for _, op := range args.Scope.Ops {
				if op == forbidden {
					t.Fatalf("scoped token includes forbidden op %q", forbidden)
				}
			}
		}
	}
}
