package mcp

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"k8s.io/utils/ptr"
)

func (g *Group) listApps(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	page, err := g.backendRepo.ListAppsPaginated(ctx, a.Workspace.Id, types.AppFilter{Name: args.str("name"), Cursor: args.str("cursor"), Limit: uint32(args.num("limit", 50))})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(page.Data))
	for _, app := range page.Data {
		item := map[string]any{"app_id": app.ExternalId, "name": app.Name}
		deployments, err := g.deployments(ctx, a.Workspace, types.DeploymentFilter{AppId: app.ExternalId, BaseFilter: types.BaseFilter{Limit: 1}})
		if err != nil {
			return nil, err
		}
		if len(deployments) > 0 {
			item = g.deploymentView(&deployments[0])
		}
		out = append(out, item)
	}
	return map[string]any{"items": out, "next_cursor": page.Next}, nil
}

func (g *Group) getApp(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	out := g.deploymentView(d)
	out["config"] = appConfigView(d.Stub.Type, cfg)
	return out, nil
}

func appConfigView(stubType types.StubType, cfg *types.StubConfigV1) map[string]any {
	raw, _ := json.Marshal(cfg)
	var full map[string]any
	_ = json.Unmarshal(raw, &full)
	view := make(map[string]any, len(configView)+1)
	for _, key := range configView {
		if stubType.Kind() == types.StubTypePod && slices.Contains(podUnusedConfig, key) {
			continue
		}
		if v, ok := full[key]; ok {
			view[key] = v
		}
	}
	bindings := make([]map[string]string, 0, len(cfg.Secrets))
	for _, s := range cfg.Secrets {
		bindings = append(bindings, map[string]string{"name": s.Name, "env_name": cmp.Or(s.EnvName, s.Name)})
	}
	view["secrets"] = bindings
	return view
}

func (g *Group) deleteApp(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	app, err := g.appByName(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	// The REST handler stops deployments and containers before removing the record.
	res, err := g.serve(ctx, http.MethodDelete, g.config.GatewayService.HTTP.GetExternalURL()+apiv1.HttpServerBaseRoute+"/app/"+a.Workspace.ExternalId+"/"+app.ExternalId, nil, nil)
	if err != nil {
		return nil, err
	}
	if status := res.(map[string]any)["status"].(int); status >= 300 {
		return nil, fmt.Errorf("delete app: HTTP %d", status)
	}
	return map[string]any{"deleted": app.Name, "app_id": app.ExternalId}, nil
}

func (g *Group) listDeployments(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	filter := types.DeploymentFilter{
		Name:       args.str("name"),
		BaseFilter: types.BaseFilter{Limit: uint32(args.num("limit", 50)), Offset: int(args.num("offset", 0))},
	}
	if v, ok := args["active"].(bool); ok {
		filter.Active = ptr.To(v)
	}
	list, err := g.deployments(ctx, a.Workspace, filter)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(list))
	for i := range list {
		if filter.Name == "" || list[i].Name == filter.Name {
			out = append(out, g.deploymentView(&list[i]))
		}
	}
	return out, nil
}

func (g *Group) deployment(ctx context.Context, a *auth.AuthInfo, id string) (*types.DeploymentWithRelated, error) {
	d, err := g.backendRepo.GetDeploymentByExternalId(ctx, a.Workspace.Id, id)
	if err != nil || d == nil {
		return nil, fail("NOT_FOUND", "no deployment %s", id)
	}
	return d, nil
}

// target is the deployment a tool acts on: a specific deployment_id, or the
// newest active version of the app called name.
func (g *Group) target(ctx context.Context, a *auth.AuthInfo, args toolArgs) (*types.DeploymentWithRelated, error) {
	if id := args.str("deployment_id"); id != "" {
		return g.deployment(ctx, a, id)
	}
	if name := args.str("name"); name != "" {
		d, err := g.gws.ActiveDeploymentByName(ctx, a.Workspace, name)
		if err == nil {
			return d, nil
		}
		list, listErr := g.deployments(ctx, a.Workspace, types.DeploymentFilter{Name: name, BaseFilter: types.BaseFilter{Limit: 1000}})
		if listErr != nil {
			return nil, listErr
		}
		for i := range list {
			if list[i].Name == name && (d == nil || list[i].Version > d.Version) {
				d = &list[i]
			}
		}
		if d == nil {
			return nil, fail("NOT_FOUND", "no deployment named %q", name)
		}
		return d, nil
	}
	return nil, fail("INVALID_ARGS", "name or deployment_id is required")
}

func (g *Group) getDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	return g.getApp(ctx, a, toolArgs{"deployment_id": d.ExternalId})
}

func (g *Group) redeploy(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	// Rebuilding a managed database's stub moves it to the current lifecycle.
	if config, err := d.Stub.UnmarshalConfig(); err == nil && config.EffectiveDatabaseConfig() != nil {
		res, err := g.gws.RedeployStub(ctx, a, d, func(*types.StubConfigV1) error { return nil })
		return deployed(res, err, d.Name)
	}
	res, err := g.gws.DeployStub(ctx, &pb.DeployStubRequest{StubId: d.Stub.ExternalId, Name: d.Name, Rollout: args.str("rollout")})
	return deployed(res, err, d.Name)
}

func (g *Group) stopDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.StopDeployment(ctx, &pb.StopDeploymentRequest{Id: d.ExternalId})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": d.ExternalId, "name": d.Name, "active": false})
}

func (g *Group) startDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.StartDeployment(ctx, &pb.StartDeploymentRequest{Id: d.ExternalId})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": d.ExternalId, "name": d.Name, "active": true})
}

func (g *Group) deleteDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.DeleteDeployment(ctx, &pb.DeleteDeploymentRequest{Id: d.ExternalId})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deleted": d.ExternalId, "name": d.Name})
}

func (g *Group) scaleDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	containers := uint32(args.num("containers", 1))
	res, err := g.gws.ScaleDeployment(ctx, &pb.ScaleDeploymentRequest{Id: d.ExternalId, Containers: containers})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": d.ExternalId, "name": d.Name, "containers": containers})
}

// okOrErr turns a gateway {ok, err_msg} response into a tool result.
func okOrErr(ok bool, errMsg string, err error, value any) (any, error) {
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New(errMsg)
	}
	return value, nil
}

// deployed is the result of every tool that ends in a new version.
func deployed(res *pb.DeployStubResponse, err error, name string) (any, error) {
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": res.GetDeploymentId(), "version": res.GetVersion(), "name": name})
}

func (g *Group) updateConfig(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	fields, _ := args["fields"].(map[string]any)
	if len(fields) == 0 {
		return nil, fail("INVALID_ARGS", "fields is required")
	}
	res, err := g.gws.RedeployWithConfig(ctx, a, args.str("name"), func(config *types.StubConfigV1) error {
		for path, value := range fields {
			if err := apiv1.SetConfigField(config, path, value); err != nil {
				return fail("INVALID_ARGS", "%s", err)
			}
		}
		return nil
	})
	return deployed(res, err, args.str("name"))
}

func (g *Group) setEnv(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	set, unset := args.stringMap("env"), args.strings("unset")
	if len(set) == 0 && len(unset) == 0 {
		return nil, fail("INVALID_ARGS", "env or unset is required")
	}
	res, err := g.gws.SetDeploymentEnv(ctx, a, args.str("name"), set, unset)
	return deployed(res, err, args.str("name"))
}

var regexpNonAlnum = regexp.MustCompile(`[^A-Z0-9]+`)

// connectionReferences mirrors the SDK's connection_references: the standard
// variables an app reads for a database of the given kind, or an app's URL.
func connectionReferences(source, kind, envName string) map[string]string {
	ref := func(field string) string { return fmt.Sprintf("${{db.%s.%s}}", source, field) }
	if kind == "" {
		key := envName
		if key == "" {
			key = strings.Trim(regexpNonAlnum.ReplaceAllString(strings.ToUpper(source), "_"), "_") + "_URL"
			if key[0] >= '0' && key[0] <= '9' {
				key = "APP_" + key
			}
		}
		return map[string]string{key: fmt.Sprintf("${{app.%s.URL}}", source)}
	}
	urlName := map[string]string{"redis": "REDIS_URL"}[kind]
	if urlName == "" {
		urlName = "DATABASE_URL"
	}
	if envName != "" {
		return map[string]string{envName: ref(urlName)}
	}
	prefix := map[string]string{"postgres": "PG", "redis": "REDIS", "mysql": "MYSQL_", "mongo": "MONGO_"}[kind]
	out := map[string]string{
		urlName:             ref(urlName),
		prefix + "HOST":     ref("HOST"),
		prefix + "PORT":     ref("PORT"),
		prefix + "USER":     ref("USERNAME"),
		prefix + "PASSWORD": ref("PASSWORD"),
	}
	if kind != "redis" {
		out[prefix+"DATABASE"] = ref("DATABASE")
	}
	return out
}

func (g *Group) connectServices(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	source, target := args.str("source"), args.str("target")
	src, err := g.gws.ActiveDeploymentByName(ctx, a.Workspace, source)
	if err != nil {
		return nil, fail("NOT_FOUND", "%s", err)
	}
	kind := ""
	cfg, err := src.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if cfg.Serving != nil && cfg.Serving.Database != nil {
		kind = cfg.Serving.Database.NormalizedKind()
	}
	env := connectionReferences(source, kind, args.str("env_name"))
	var authentication map[string]any
	if kind == "" && cfg.Authorized {
		secretName, err := g.serviceCredential(ctx, a, src.App.ExternalId, target, args.boolean("rotate_credentials"))
		if err != nil {
			return nil, err
		}

		var tokenEnv string
		for urlEnv := range env {
			tokenEnv = strings.TrimSuffix(urlEnv, "_URL") + "_TOKEN"
		}
		env[tokenEnv] = "${{secret." + secretName + "}}"
		authentication = map[string]any{
			"type":        "bearer",
			"token_env":   tokenEnv,
			"secret_name": secretName,
			"app_id":      src.App.ExternalId,
			"usage":       "Send Authorization: Bearer <token_env value>. Deleting the secret revokes access immediately.",
		}
	}

	res, err := g.gws.SetDeploymentEnv(ctx, a, target, env, nil)
	out, err := deployed(res, err, target)
	if err != nil {
		return nil, err
	}
	out.(map[string]any)["env"] = env
	if authentication != nil {
		out.(map[string]any)["authentication"] = authentication
	}
	return out, nil
}

func (g *Group) serviceCredential(ctx context.Context, a *auth.AuthInfo, appID, consumerName string, rotate bool) (string, error) {
	consumer, err := g.appByName(ctx, a.Workspace, consumerName)
	if err != nil {
		return "", err
	}
	name, value, err := auth.NewServiceCredential(a.Workspace.ExternalId, appID, consumer.ExternalId)
	if err != nil {
		return "", err
	}

	_, err = g.backendRepo.GetSecretByName(ctx, a.Workspace, name)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = g.backendRepo.CreateSecret(ctx, a.Workspace, a.TokenId(), name, value, true, nil)
		if err != nil {
			// A concurrent connection may have created the same binding.
			_, err = g.backendRepo.GetSecretByName(ctx, a.Workspace, name)
		}
	}
	if err == nil && rotate {
		_, err = g.backendRepo.UpdateSecret(ctx, a.Workspace, a.TokenId(), name, value, nil)
	}
	return name, err
}
