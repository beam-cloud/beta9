package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
)

func (g *Group) listSecrets(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	secrets, err := g.backendRepo.ListSecrets(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(secrets))
	for _, s := range secrets {
		out = append(out, map[string]any{"name": s.Name, "updated_at": s.UpdatedAt})
	}
	return out, nil
}

func (g *Group) createSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.backendRepo.CreateSecret(ctx, a.Workspace, a.TokenId(), args.str("name"), args.rawString("value"), true); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "created": true}, nil
}

func (g *Group) updateSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.backendRepo.UpdateSecret(ctx, a.Workspace, a.TokenId(), args.str("name"), args.rawString("value")); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "updated": true}, nil
}

func (g *Group) deleteSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if err := g.backendRepo.DeleteSecret(ctx, a.Workspace, args.str("name")); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "deleted": true}, nil
}

func databaseView(info types.DatabaseServiceInfo) map[string]any {
	info.ConnectionString = ""
	info.PooledConnectionString = ""
	raw, _ := json.Marshal(info)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	for key, value := range out {
		if value == "" {
			delete(out, key)
		}
	}
	return out
}

func (g *Group) database(ctx context.Context, a *auth.AuthInfo, kind, name string) (*types.DatabaseServiceInfo, error) {
	services, err := g.gws.ListDatabaseServices(ctx, a)
	if err != nil {
		return nil, err
	}
	for i := range services {
		if services[i].Name == name && services[i].Kind == kind {
			return &services[i], nil
		}
	}
	return nil, fail("NOT_FOUND", "no %s database named %q", kind, name)
}

func (g *Group) listDatabases(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	services, err := g.gws.ListDatabaseServices(ctx, a)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(services))
	for _, s := range services {
		out = append(out, databaseView(s))
	}
	return out, nil
}

func (g *Group) createDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, apiv1.DatabaseCreateTimeout)
	defer cancel()
	info, err := g.gws.CreateDatabaseService(ctx, a, types.CreateDatabaseParams{
		Kind:       args.str("kind"),
		Name:       args.str("name"),
		AlwaysOn:   args.boolean("always_on"),
		Size:       args.str("size"),
		Cpu:        int64(args.num("cpu", 0)),
		Memory:     int64(args.num("memory", 0)),
		Pool:       args.str("pool"),
		SnapshotID: args.str("snapshot_id"),
		Username:   args.str("username"),
		Database:   args.str("database"),
	})
	if err != nil {
		return nil, err
	}
	return databaseView(*info), nil
}

func (g *Group) databaseReadiness(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.gws.CheckDatabaseReadiness(ctx, a, args.str("name"), args.str("deployment_id"))
}

func (g *Group) databaseCredentials(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	info, err := g.database(ctx, a, args.str("kind"), args.str("name"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"name":                     info.Name,
		"kind":                     info.Kind,
		"tls":                      true,
		"connection_string_secret": info.ConnectionStringSecret,
	}
	for field, name := range map[string]string{
		"username":                 info.UsernameSecret,
		"password":                 info.PasswordSecret,
		"database":                 info.DatabaseSecret,
		"connection_string":        info.ConnectionStringSecret,
		"pooled_connection_string": info.PooledConnectionStringSecret,
	} {
		if name == "" {
			continue
		}
		value, err := g.gws.SecretValue(ctx, a.Workspace, name)
		if err != nil {
			return nil, fmt.Errorf("read %s credential: %w", field, err)
		}
		out[field] = value
	}
	connection, err := url.Parse(out["connection_string"].(string))
	if err != nil || connection.Hostname() == "" {
		return nil, fail("INVALID_CONNECTION", "stored database connection URL has no valid host")
	}
	port := 443
	if connection.Port() != "" {
		port, err = strconv.Atoi(connection.Port())
		if err != nil {
			return nil, fail("INVALID_CONNECTION", "stored database connection URL has an invalid port")
		}
	}
	out["host"], out["port"] = connection.Hostname(), port
	out["tls_verification"] = connection.Query().Get("sslmode") == "verify-full" || connection.Query().Get("ssl_cert_reqs") == "required"
	return out, nil
}

func (g *Group) rotateDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.database(ctx, a, args.str("kind"), args.str("name")); err != nil {
		return nil, err
	}
	info, err := g.gws.RotateDatabaseCredentials(ctx, a, args.str("name"))
	if err != nil {
		return nil, err
	}
	out := databaseView(*info)
	out["effects"] = "Database and deployments bound to its credential secrets were recycled; active connections may drop. Readiness must be reverified."
	return out, nil
}

func (g *Group) deleteDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.database(ctx, a, args.str("kind"), args.str("name")); err != nil {
		return nil, err
	}
	if err := g.gws.DeleteDatabaseService(ctx, a, args.str("name")); err != nil {
		return nil, err
	}
	return map[string]any{"deleted": args.str("name"), "kind": args.str("kind"), "disk_deleted": true}, nil
}

func (g *Group) listVolumes(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	volumes, err := g.backendRepo.ListVolumesWithRelated(ctx, a.Workspace.Id)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(volumes))
	for _, v := range volumes {
		out = append(out, map[string]any{"name": v.Name, "id": v.ExternalId, "created_at": v.CreatedAt.Time})
	}
	return out, nil
}

func (g *Group) createVolume(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	v, err := g.backendRepo.GetOrCreateVolume(ctx, a.Workspace.Id, args.str("name"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"name": v.Name, "id": v.ExternalId}, nil
}
