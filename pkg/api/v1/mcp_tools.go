package apiv1

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/utils/ptr"
)

func protoMap(m proto.Message) map[string]any {
	raw, _ := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

// configView is the part of a stub config an agent acts on.
var configView = []string{"runtime", "autoscaler", "keep_warm_seconds", "workers", "concurrent_requests", "max_pending_tasks", "task_policy", "env", "ports", "tcp", "authorized", "entry_point", "volumes", "disks", "pool"}

func (g *MCPGroup) deploymentURL(d *types.DeploymentWithRelated) string {
	externalURL := g.config.GatewayService.HTTP.GetExternalURL()
	urlType := g.config.GatewayService.InvokeURLType
	if d.Stub.Type.Kind() == types.StubTypePod {
		cfg, err := d.Stub.UnmarshalConfig()
		if err != nil {
			return ""
		}
		if cfg.TCP {
			return common.BuildPodDeploymentURL(g.config.Abstractions.Pod.TCP.GetExternalURL(), common.InvokeUrlTypeHost, &d.Deployment, cfg)
		}
		return common.BuildPodDeploymentURL(externalURL, urlType, &d.Deployment, cfg)
	}
	return common.BuildDeploymentLatestURL(externalURL, urlType, &d.Stub, &d.Deployment)
}

func (g *MCPGroup) deploymentView(d *types.DeploymentWithRelated) map[string]any {
	return map[string]any{
		"name":          d.Name,
		"deployment_id": d.ExternalId,
		"app_id":        d.App.ExternalId,
		"stub_id":       d.Stub.ExternalId,
		"stub_type":     d.StubType,
		"version":       d.Version,
		"active":        d.Active,
		"created_at":    d.CreatedAt.Time,
		"url":           g.deploymentURL(d),
	}
}

func (g *MCPGroup) deployments(ctx context.Context, ws *types.Workspace, filter types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	filter.WorkspaceID = ws.Id
	if filter.Limit == 0 {
		filter.Limit = 1000
	}
	return g.backendRepo.ListDeploymentsWithRelated(ctx, filter)
}

// latestByApp is the newest active deployment for every app that has one.
func (g *MCPGroup) latestByApp(ctx context.Context, ws *types.Workspace) (map[string]*types.DeploymentWithRelated, error) {
	all, err := g.deployments(ctx, ws, types.DeploymentFilter{Active: ptr.To(true)})
	if err != nil {
		return nil, err
	}
	latest := map[string]*types.DeploymentWithRelated{}
	for i := range all {
		d := &all[i]
		if cur, ok := latest[d.Name]; !ok || d.Version > cur.Version {
			latest[d.Name] = d
		}
	}
	return latest, nil
}

func (g *MCPGroup) appByName(ctx context.Context, ws *types.Workspace, name string) (*types.App, error) {
	page, err := g.backendRepo.ListAppsPaginated(ctx, ws.Id, types.AppFilter{Name: name, Limit: 50})
	if err != nil {
		return nil, err
	}
	for i := range page.Data {
		if page.Data[i].Name == name {
			return &page.Data[i], nil
		}
	}
	return nil, fail("NOT_FOUND", "no app named %q", name)
}

// --- catalog -----------------------------------------------------------------------------

func (g *MCPGroup) catalog() []mcpTool {
	target := schema(props{"name": str("App name: its newest active version"), "deployment_id": str("A specific version instead")})
	targetConfirm := schema(props{"name": str("App name: its newest active version"), "deployment_id": str("A specific version instead"), "confirm": boolean()})
	name := schema(props{"name": str("")}, "name")
	nameConfirm := schema(props{"name": str(""), "confirm": boolean()}, "name")
	database := schema(props{"kind": databaseKind, "name": str("")}, "kind", "name")
	window := schema(props{"name": str("App name"), "stub_id": str("Or a stub id"), "window_minutes": integer(60)})
	request := props{"name": str("App name: its newest active version"), "deployment_id": str("A specific version instead"), "path": str("Path under the app URL, e.g. /predict"), "method": str("HTTP method; default POST"), "body": map[string]any{"description": "JSON body"}, "headers": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}}

	return []mcpTool{
		// workspace
		{Name: "whoami", Description: "Workspace id, name and gateway URL for this token.", Schema: schema(props{}), Run: g.whoami},
		{Name: "list_apps", Description: "Apps in the workspace with their newest active deployment and URL.", Schema: schema(props{}), Run: g.listApps},
		{Name: "get_app", Description: "One app: its config (resources, scaling, env, secret bindings, ports, disks) and URL.", Schema: name, Run: g.getApp},
		{Name: "delete_app", Description: "Delete an app and every version of it. Requires confirm=true.", Schema: nameConfirm, Confirm: "delete_app removes every deployment of the app.", Run: g.deleteApp},
		// deployments
		{Name: "list_deployments", Description: "Deployment versions; filter by name and active.", Schema: schema(props{"name": str(""), "active": boolean(), "limit": integer(50)}), Run: g.listDeployments},
		{Name: "get_deployment", Description: "One deployment version: active, URL, stub.", Schema: target, Run: g.getDeployment},
		{Name: "redeploy", Description: "Deploy a new version from an existing version's config: a restart with fresh containers, or a rollback when deployment_id is an older version.", Schema: target, Destructive: true, Run: g.redeploy},
		{Name: "stop_deployment", Description: "Stop a deployment version; its containers drain and requests fail until started.", Schema: target, Destructive: true, Run: g.stopDeployment},
		{Name: "start_deployment", Description: "Start a stopped deployment version.", Schema: target, Destructive: true, Run: g.startDeployment},
		{Name: "delete_deployment", Description: "Delete one deployment version. Requires confirm=true.", Schema: targetConfirm, Confirm: "delete_deployment is irreversible.", Run: g.deleteDeployment},
		{Name: "scale_deployment", Description: "Set the replica count of a pod deployment.", Schema: schema(props{"name": str(""), "deployment_id": str(""), "containers": integer(1)}, "containers"), Destructive: true, Run: g.scaleDeployment},
		{Name: "invoke", Description: "Call a deployed app through the gateway with this token (works for authorized apps). Returns status and body.", Schema: schema(request), Destructive: true, Run: g.invoke},
		// settings
		{Name: "update_config", Description: "Change settings and deploy a new version: dotted paths such as runtime.cpu (millicores), runtime.memory (MB), runtime.gpu, runtime.gpu_count, autoscaler.max_containers, autoscaler.min_containers, autoscaler.tasks_per_container, keep_warm_seconds, concurrent_requests, workers, max_pending_tasks, task_policy.timeout, task_policy.max_retries, authorized, ports, entry_point. Use set_env for variables.", Schema: schema(props{"name": str("App name"), "fields": map[string]any{"type": "object", "description": "path -> value"}}, "name", "fields"), Destructive: true, Run: g.updateConfig},
		{Name: "set_env", Description: "Set or remove environment variables on an app and deploy a new version. Values may be ${{secret.NAME}}, ${{db.NAME.DATABASE_URL}} (or HOST, PORT, USERNAME, PASSWORD, DATABASE) or ${{app.NAME.URL}}.", Schema: schema(props{"name": str("App name"), "env": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}, "unset": strList("Variables to remove")}, "name"), Destructive: true, Run: g.setEnv},
		{Name: "connect_services", Description: "Wire `target` to `source`: a database's URL and parts, or an app's URL, as env references on target; deploys a new version of target.", Schema: schema(props{"source": str("Database or app name"), "target": str("App that receives the variables"), "env_name": str("Single variable name instead of the standard set")}, "source", "target"), Destructive: true, Run: g.connectServices},
		// secrets
		{Name: "list_secrets", Description: "Workspace secret names.", Schema: schema(props{}), Run: g.listSecrets},
		{Name: "create_secret", Description: "Create a workspace secret; reference it with ${{secret.NAME}}.", Schema: schema(props{"name": str(""), "value": str("")}, "name", "value"), Destructive: true, Run: g.createSecret},
		{Name: "update_secret", Description: "Change a secret's value; deployments pick it up on their next container start.", Schema: schema(props{"name": str(""), "value": str("")}, "name", "value"), Destructive: true, Run: g.updateSecret},
		{Name: "delete_secret", Description: "Delete a workspace secret. Requires confirm=true.", Schema: nameConfirm, Confirm: "delete_secret breaks deployments still bound to it.", Run: g.deleteSecret},
		// databases
		{Name: "list_databases", Description: "Managed database services and their state.", Schema: schema(props{}), Run: g.listDatabases},
		{Name: "create_database", Description: "Create a managed Postgres, Redis, MySQL or MongoDB service on a durable disk. Credentials become secrets; reference them with ${{db.<name>.DATABASE_URL}}.", Schema: schema(props{"kind": databaseKind, "name": str(""), "always_on": boolean()}, "kind", "name"), Destructive: true, Run: g.createDatabase},
		{Name: "database_credentials", Description: "Connection string and parts for a database service.", Schema: database, Run: g.databaseCredentials},
		{Name: "rotate_database_credentials", Description: "Rotate a database's password; the database and every app bound to it restart with the new credentials.", Schema: database, Destructive: true, Run: g.rotateDatabase},
		{Name: "delete_database", Description: "Delete a database service and its credential secrets. Requires confirm=true.", Schema: schema(props{"kind": databaseKind, "name": str(""), "confirm": boolean()}, "kind", "name"), Confirm: "delete_database removes the service and its data.", Run: g.deleteDatabase},
		// storage
		{Name: "list_volumes", Description: "Persistent volumes (mount with Volume(name, mount_path) in app code).", Schema: schema(props{}), Run: g.listVolumes},
		{Name: "create_volume", Description: "Create a persistent volume.", Schema: name, Destructive: true, Run: g.createVolume},
		// stacks
		{Name: "list_stacks", Description: "Stacks: named groups of apps shown together on the dashboard board.", Schema: schema(props{}), Run: g.listStacks},
		{Name: "create_stack", Description: "Create a stack, optionally with apps (by name).", Schema: schema(props{"name": str(""), "apps": strList("App names")}, "name"), Destructive: true, Run: g.createStack},
		{Name: "update_stack", Description: "Add or remove apps (by name) on a stack; the apps themselves are untouched.", Schema: schema(props{"name": str(""), "add": strList(""), "remove": strList("")}, "name"), Destructive: true, Run: g.updateStack},
		{Name: "delete_stack", Description: "Delete a stack; its apps are untouched.", Schema: name, Destructive: true, Run: g.deleteStack},
		// observe
		{Name: "logs", Description: "Recent logs for an app (by name), deployment, stub, task or container.", Schema: schema(props{"name": str("App name"), "deployment_id": str(""), "stub_id": str(""), "task_id": str(""), "container_id": str(""), "tail": integer(100), "search": str("Substring filter")}), Run: g.logs},
		{Name: "list_tasks", Description: "Recent tasks (invocations), newest first.", Schema: schema(props{"stub_id": str(""), "status": str("Comma-separated: pending, running, complete, error, cancelled, timeout"), "limit": integer(20)}), Run: g.listTasks},
		{Name: "get_task", Description: "Status, timing and container of one task.", Schema: schema(props{"task_id": str("")}, "task_id"), Run: g.getTask},
		{Name: "stop_task", Description: "Stop a running or pending task.", Schema: schema(props{"task_id": str("")}, "task_id"), Destructive: true, Run: g.stopTask},
		{Name: "request_stats", Description: "Request count, 5xx share and p50/p95/p99 latency for an endpoint over a window (upper bounds from a fixed histogram).", Schema: window, Run: g.requestStats},
		{Name: "list_webhooks", Description: "Workspace webhooks (URL, event types, enabled).", Schema: schema(props{}), Run: g.listWebhooks},
		{Name: "create_webhook", Description: "Register a signed HTTP webhook for workspace events (stub.*, task.*, endpoint.request_stats). Returns the signing secret once.", Schema: schema(props{"url": str(""), "event_types": strList(""), "description": str("")}, "url"), Destructive: true, Run: g.createWebhook},
		// everything else
		{Name: "api_routes", Description: "Every gateway REST route, for use with `api`: containers, metrics timeseries, event history, tokens, webhooks, volumes, disks, pods and more.", Schema: schema(props{}), Run: g.apiRoutes},
		{Name: "api", Description: "Call any gateway REST route with this token. Methods other than GET need confirm=true. {ws} in the path becomes your workspace id.", Schema: schema(props{"method": str("Default GET"), "path": str("e.g. /api/v1/container/{ws}"), "body": map[string]any{"description": "JSON body"}, "headers": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}, "confirm": boolean()}, "path"), Destructive: true, Run: g.api},
	}
}

// --- workspace ----------------------------------------------------------------------------

func (g *MCPGroup) whoami(_ context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	return map[string]any{
		"workspace_id":   a.Workspace.ExternalId,
		"workspace_name": a.Workspace.Name,
		"gateway_http":   g.config.GatewayService.HTTP.GetExternalURL(),
	}, nil
}

func (g *MCPGroup) listApps(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	latest, err := g.latestByApp(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(latest))
	for n := range latest {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, g.deploymentView(latest[n]))
	}
	return out, nil
}

func (g *MCPGroup) getApp(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.gws.ActiveDeploymentByName(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, fail("NOT_FOUND", "%s", err)
	}
	var full map[string]any
	if err := json.Unmarshal([]byte(d.Stub.Config), &full); err != nil {
		return nil, err
	}
	view := map[string]any{}
	for _, key := range configView {
		if v, ok := full[key]; ok {
			view[key] = v
		}
	}
	bindings := []map[string]string{}
	if secrets, ok := full["secrets"].([]any); ok {
		for _, s := range secrets {
			if m, ok := s.(map[string]any); ok {
				n, _ := m["name"].(string)
				e, _ := m["env_name"].(string)
				if e == "" {
					e = n
				}
				bindings = append(bindings, map[string]string{"name": n, "env_name": e})
			}
		}
	}
	view["secrets"] = bindings
	out := g.deploymentView(d)
	out["config"] = view
	return out, nil
}

func (g *MCPGroup) deleteApp(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	app, err := g.appByName(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	if err := g.backendRepo.DeleteApp(ctx, app.ExternalId); err != nil {
		return nil, err
	}
	return map[string]any{"deleted": app.Name, "app_id": app.ExternalId}, nil
}

// --- deployments ------------------------------------------------------------------------

func (g *MCPGroup) listDeployments(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	filter := types.DeploymentFilter{Name: args.str("name"), BaseFilter: types.BaseFilter{Limit: uint32(args.num("limit", 50))}}
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

func (g *MCPGroup) deployment(ctx context.Context, a *auth.AuthInfo, id string) (*types.DeploymentWithRelated, error) {
	d, err := g.backendRepo.GetDeploymentByExternalId(ctx, a.Workspace.Id, id)
	if err != nil || d == nil {
		return nil, fail("NOT_FOUND", "no deployment %s", id)
	}
	return d, nil
}

// target is the deployment a tool acts on: a specific deployment_id, or the
// newest active version of the app called name.
func (g *MCPGroup) target(ctx context.Context, a *auth.AuthInfo, args toolArgs) (*types.DeploymentWithRelated, error) {
	if id := args.str("deployment_id"); id != "" {
		return g.deployment(ctx, a, id)
	}
	if name := args.str("name"); name != "" {
		d, err := g.gws.ActiveDeploymentByName(ctx, a.Workspace, name)
		if err != nil {
			return nil, fail("NOT_FOUND", "%s", err)
		}
		return d, nil
	}
	return nil, fail("INVALID_ARGS", "name or deployment_id is required")
}

func (g *MCPGroup) getDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	return g.deploymentView(d), nil
}

func (g *MCPGroup) redeploy(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.DeployStub(ctx, &pb.DeployStubRequest{StubId: d.Stub.ExternalId, Name: d.Name})
	if err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("%s", res.ErrMsg)
	}
	return map[string]any{"deployment_id": res.DeploymentId, "version": res.Version, "name": d.Name}, nil
}

func (g *MCPGroup) stopDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.StopDeployment(ctx, &pb.StopDeploymentRequest{Id: d.ExternalId})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": d.ExternalId, "name": d.Name, "active": false})
}

func (g *MCPGroup) startDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.StartDeployment(ctx, &pb.StartDeploymentRequest{Id: d.ExternalId})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": d.ExternalId, "name": d.Name, "active": true})
}

func (g *MCPGroup) deleteDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	res, err := g.gws.DeleteDeployment(ctx, &pb.DeleteDeploymentRequest{Id: d.ExternalId})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deleted": d.ExternalId, "name": d.Name})
}

func (g *MCPGroup) scaleDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	containers := uint32(args.num("containers", 1))
	res, err := g.gws.ScaleDeployment(ctx, &pb.ScaleDeploymentRequest{Id: d.ExternalId, Containers: containers})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"deployment_id": d.ExternalId, "name": d.Name, "containers": containers})
}

func okOrErr(ok bool, errMsg string, err error, value any) (any, error) {
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s", errMsg)
	}
	return value, nil
}

// --- settings -------------------------------------------------------------------------------

func deployed(res *pb.DeployStubResponse, err error, name string) (any, error) {
	if err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("%s", res.ErrMsg)
	}
	return map[string]any{"deployment_id": res.DeploymentId, "version": res.Version, "name": name}, nil
}

func (g *MCPGroup) updateConfig(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	fields, _ := args["fields"].(map[string]any)
	if len(fields) == 0 {
		return nil, fail("INVALID_ARGS", "fields is required")
	}
	res, err := g.gws.RedeployWithConfig(ctx, a, args.str("name"), func(config *types.StubConfigV1) error {
		for path, value := range fields {
			if err := setConfigField(config, path, value); err != nil {
				return fail("INVALID_ARGS", "%s", err)
			}
		}
		return nil
	})
	return deployed(res, err, args.str("name"))
}

func (g *MCPGroup) setEnv(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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
			key = strings.Trim(strings.ToUpper(regexpNonAlnum.ReplaceAllString(source, "_")), "_") + "_URL"
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

func (g *MCPGroup) connectServices(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	source, target := args.str("source"), args.str("target")
	src, err := g.gws.ActiveDeploymentByName(ctx, a.Workspace, source)
	if err != nil {
		return nil, fail("NOT_FOUND", "%s", err)
	}
	kind := ""
	if cfg, err := src.Stub.UnmarshalConfig(); err == nil && cfg.Serving != nil && cfg.Serving.Database != nil {
		kind = cfg.Serving.Database.NormalizedKind()
	}
	env := connectionReferences(source, kind, args.str("env_name"))
	res, err := g.gws.SetDeploymentEnv(ctx, a, target, env, nil)
	out, err := deployed(res, err, target)
	if err != nil {
		return nil, err
	}
	out.(map[string]any)["env"] = env
	return out, nil
}

// --- secrets --------------------------------------------------------------------------------

func (g *MCPGroup) listSecrets(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
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

func (g *MCPGroup) createSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.backendRepo.CreateSecret(ctx, a.Workspace, a.TokenId(), args.str("name"), args.str("value"), true); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "created": true}, nil
}

func (g *MCPGroup) updateSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.backendRepo.UpdateSecret(ctx, a.Workspace, a.TokenId(), args.str("name"), args.str("value")); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "updated": true}, nil
}

func (g *MCPGroup) deleteSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if err := g.backendRepo.DeleteSecret(ctx, a.Workspace, args.str("name")); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "deleted": true}, nil
}

// --- databases ------------------------------------------------------------------------------

func databaseView(info types.DatabaseServiceInfo) map[string]any {
	info.ConnectionString = ""
	raw, _ := json.Marshal(info)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

func (g *MCPGroup) database(ctx context.Context, a *auth.AuthInfo, kind, name string) (*types.DatabaseServiceInfo, error) {
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

func (g *MCPGroup) listDatabases(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
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

func (g *MCPGroup) createDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, databaseCreateTimeout)
	defer cancel()
	info, err := g.gws.CreateDatabaseService(ctx, a, types.CreateDatabaseParams{Kind: args.str("kind"), Name: args.str("name"), AlwaysOn: args.boolean("always_on")})
	if err != nil {
		return nil, err
	}
	return databaseView(*info), nil
}

func (g *MCPGroup) databaseCredentials(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	info, err := g.database(ctx, a, args.str("kind"), args.str("name"))
	if err != nil {
		return nil, err
	}
	secret := func(name string) string {
		if name == "" {
			return ""
		}
		value, _ := g.gws.SecretValue(ctx, a.Workspace, name)
		return value
	}
	out := map[string]any{
		"name":                     info.Name,
		"kind":                     info.Kind,
		"username":                 secret(info.UsernameSecret),
		"connection_string":        secret(info.ConnectionStringSecret),
		"connection_string_secret": info.ConnectionStringSecret,
	}
	if info.DatabaseSecret != "" {
		out["database"] = secret(info.DatabaseSecret)
	}
	return out, nil
}

func (g *MCPGroup) rotateDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.database(ctx, a, args.str("kind"), args.str("name")); err != nil {
		return nil, err
	}
	info, err := g.gws.RotateDatabaseCredentials(ctx, a, args.str("name"))
	if err != nil {
		return nil, err
	}
	return databaseView(*info), nil
}

func (g *MCPGroup) deleteDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.database(ctx, a, args.str("kind"), args.str("name")); err != nil {
		return nil, err
	}
	if err := g.gws.DeleteDatabaseService(ctx, a, args.str("name")); err != nil {
		return nil, err
	}
	return map[string]any{"deleted": args.str("name"), "kind": args.str("kind")}, nil
}

// --- volumes ---------------------------------------------------------------------------------

func (g *MCPGroup) listVolumes(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
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

func (g *MCPGroup) createVolume(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	v, err := g.backendRepo.GetOrCreateVolume(ctx, a.Workspace.Id, args.str("name"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"name": v.Name, "id": v.ExternalId}, nil
}

// --- stacks -------------------------------------------------------------------------------------

type stackSpec struct {
	AppIds    []string                   `json:"appIds"`
	Positions map[string]json.RawMessage `json:"positions,omitempty"`
	Pending   json.RawMessage            `json:"pending,omitempty"`
}

func (g *MCPGroup) appNames(ctx context.Context, ws *types.Workspace) (map[string]string, map[string]string, error) {
	page, err := g.backendRepo.ListAppsPaginated(ctx, ws.Id, types.AppFilter{Limit: 1000})
	if err != nil {
		return nil, nil, err
	}
	byID, byName := map[string]string{}, map[string]string{}
	for _, app := range page.Data {
		byID[app.ExternalId] = app.Name
		byName[app.Name] = app.ExternalId
	}
	return byID, byName, nil
}

func (g *MCPGroup) stackView(ctx context.Context, ws *types.Workspace, s *types.Stack) (map[string]any, error) {
	byID, _, err := g.appNames(ctx, ws)
	if err != nil {
		return nil, err
	}
	var spec stackSpec
	_ = json.Unmarshal(s.Spec, &spec)
	apps := make([]string, 0, len(spec.AppIds))
	for _, id := range spec.AppIds {
		if name, ok := byID[id]; ok {
			apps = append(apps, name)
		}
	}
	return map[string]any{"name": s.Name, "id": s.ExternalId, "apps": apps}, nil
}

func (g *MCPGroup) stack(ctx context.Context, ws *types.Workspace, name string) (*types.Stack, error) {
	stacks, err := g.backendRepo.ListStacks(ctx, ws.Id)
	if err != nil {
		return nil, err
	}
	for i := range stacks {
		if stacks[i].Name == name {
			return &stacks[i], nil
		}
	}
	return nil, fail("NOT_FOUND", "no stack named %q", name)
}

func (g *MCPGroup) listStacks(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	stacks, err := g.backendRepo.ListStacks(ctx, a.Workspace.Id)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(stacks))
	for i := range stacks {
		view, err := g.stackView(ctx, a.Workspace, &stacks[i])
		if err != nil {
			return nil, err
		}
		out = append(out, view)
	}
	return out, nil
}

func (g *MCPGroup) resolveApps(ctx context.Context, ws *types.Workspace, names []string) ([]string, error) {
	_, byName, err := g.appNames(ctx, ws)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := byName[n]
		if !ok {
			return nil, fail("NOT_FOUND", "no app named %q", n)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (g *MCPGroup) createStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	ids, err := g.resolveApps(ctx, a.Workspace, args.strings("apps"))
	if err != nil {
		return nil, err
	}
	spec, _ := json.Marshal(stackSpec{AppIds: ids})
	s, err := g.backendRepo.CreateStack(ctx, a.Workspace.Id, args.str("name"), spec)
	if err != nil {
		return nil, err
	}
	return g.stackView(ctx, a.Workspace, s)
}

func (g *MCPGroup) updateStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	s, err := g.stack(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	add, err := g.resolveApps(ctx, a.Workspace, args.strings("add"))
	if err != nil {
		return nil, err
	}
	remove, err := g.resolveApps(ctx, a.Workspace, args.strings("remove"))
	if err != nil {
		return nil, err
	}
	var spec stackSpec
	_ = json.Unmarshal(s.Spec, &spec)
	drop := map[string]bool{}
	for _, id := range remove {
		drop[id] = true
	}
	ids := make([]string, 0, len(spec.AppIds)+len(add))
	seen := map[string]bool{}
	for _, id := range append(spec.AppIds, add...) {
		if !drop[id] && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	spec.AppIds = ids
	for id := range spec.Positions {
		if !seen[id] {
			delete(spec.Positions, id)
		}
	}
	raw, _ := json.Marshal(spec)
	updated, err := g.backendRepo.UpdateStack(ctx, a.Workspace.Id, s.ExternalId, s.Name, raw)
	if err != nil {
		return nil, err
	}
	return g.stackView(ctx, a.Workspace, updated)
}

func (g *MCPGroup) deleteStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	s, err := g.stack(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	if err := g.backendRepo.DeleteStack(ctx, a.Workspace.Id, s.ExternalId); err != nil {
		return nil, err
	}
	return map[string]any{"deleted": s.Name}, nil
}

// --- observe ------------------------------------------------------------------------------------

func (g *MCPGroup) logs(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	query := types.LogQuery{WorkspaceID: a.Workspace.ExternalId, Limit: uint64(args.num("tail", 100)), Query: args.str("search")}
	switch {
	case args.str("deployment_id") != "" || args.str("name") != "":
		d, err := g.target(ctx, a, args)
		if err != nil {
			return nil, err
		}
		query.ObjectType, query.ObjectID, query.StubID, query.AppID = types.GatewayObjectTypeDeployment, d.ExternalId, d.Stub.ExternalId, d.App.ExternalId
	case args.str("stub_id") != "":
		stub, err := g.backendRepo.GetStubByExternalId(ctx, args.str("stub_id"), types.QueryFilter{Field: "workspace_id", Value: a.Workspace.ExternalId})
		if err != nil || stub == nil || stub.ExternalId == "" {
			return nil, fail("NOT_FOUND", "no stub %s", args.str("stub_id"))
		}
		query.ObjectType, query.ObjectID, query.StubID = types.GatewayObjectTypeStub, stub.ExternalId, stub.ExternalId
		if stub.App != nil {
			query.AppID = stub.App.ExternalId
		}
	case args.str("task_id") != "":
		task, err := g.backendRepo.GetTaskWithRelated(ctx, args.str("task_id"))
		if err != nil || task == nil || task.Workspace.ExternalId != a.Workspace.ExternalId {
			return nil, fail("NOT_FOUND", "no task %s", args.str("task_id"))
		}
		query.ObjectType, query.ObjectID, query.StubID, query.AppID, query.TaskID, query.ContainerID = types.GatewayObjectTypeTask, task.ExternalId, task.Stub.ExternalId, task.App.ExternalId, task.ExternalId, task.ContainerId
	case args.str("container_id") != "":
		query.ObjectType, query.ObjectID, query.ContainerID = types.GatewayObjectTypeContainer, args.str("container_id"), args.str("container_id")
	default:
		return nil, fail("INVALID_ARGS", "pass one of name, deployment_id, stub_id, task_id, container_id")
	}
	res, err := g.eventRepo.GetLogs(ctx, query)
	if err != nil {
		return nil, err
	}
	return res.Logs, nil
}

func (g *MCPGroup) listTasks(ctx context.Context, _ *auth.AuthInfo, args toolArgs) (any, error) {
	filters := map[string]*pb.StringList{}
	if v := args.str("stub_id"); v != "" {
		filters["stub-id"] = &pb.StringList{Values: []string{v}}
	}
	if v := args.str("status"); v != "" {
		filters["status"] = &pb.StringList{Values: strings.Split(strings.ToUpper(strings.ReplaceAll(v, " ", "")), ",")}
	}
	res, err := g.gws.ListTasks(ctx, &pb.ListTasksRequest{Filters: filters, Limit: uint32(args.num("limit", 20))})
	if err != nil {
		return nil, err
	}
	if !res.Ok {
		return nil, fmt.Errorf("%s", res.ErrMsg)
	}
	out := make([]map[string]any, 0, len(res.Tasks))
	for _, t := range res.Tasks {
		out = append(out, protoMap(t))
	}
	return out, nil
}

func (g *MCPGroup) getTask(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	task, err := g.backendRepo.GetTaskWithRelated(ctx, args.str("task_id"))
	if err != nil || task == nil || task.Workspace.ExternalId != a.Workspace.ExternalId {
		return nil, fail("NOT_FOUND", "no task %s", args.str("task_id"))
	}
	return map[string]any{
		"task_id":      task.ExternalId,
		"status":       task.Status,
		"container_id": task.ContainerId,
		"stub_id":      task.Stub.ExternalId,
		"created_at":   task.CreatedAt.Time,
		"started_at":   task.StartedAt.Time,
		"ended_at":     task.EndedAt.Time,
	}, nil
}

func (g *MCPGroup) stopTask(ctx context.Context, _ *auth.AuthInfo, args toolArgs) (any, error) {
	res, err := g.gws.StopTasks(ctx, &pb.StopTasksRequest{TaskIds: []string{args.str("task_id")}})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"task_id": args.str("task_id"), "stopped": true})
}

func (g *MCPGroup) requestStats(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	stubID := args.str("stub_id")
	if stubID == "" {
		d, err := g.target(ctx, a, args)
		if err != nil {
			return nil, err
		}
		stubID = d.Stub.ExternalId
	}
	minutes := int(args.num("window_minutes", 60))
	end := time.Now().UTC()
	start := end.Add(-time.Duration(minutes) * time.Minute)
	history, err := g.eventRepo.GetEventHistory(ctx, types.EventQuery{
		WorkspaceID: a.Workspace.ExternalId,
		StubID:      stubID,
		EventTypes:  []string{types.EventEndpointRequestStats},
		StartTime:   &start,
		EndTime:     &end,
		Limit:       5000,
	})
	if err != nil {
		return nil, err
	}
	var total types.EventEndpointRequestStatsSchema
	var buckets []int64
	for _, record := range history.Events {
		var envelope struct {
			Data types.EventEndpointRequestStatsSchema `json:"data"`
		}
		if json.Unmarshal(record.CloudEvent, &envelope) != nil {
			continue
		}
		d := envelope.Data
		total.Requests += d.Requests
		total.Status4xx += d.Status4xx
		total.Status5xx += d.Status5xx
		total.DurationSumMs += d.DurationSumMs
		total.DurationMaxMs = max(total.DurationMaxMs, d.DurationMaxMs)
		if len(d.LatencyBuckets) > len(buckets) {
			buckets = append(buckets, make([]int64, len(d.LatencyBuckets)-len(buckets))...)
		}
		for i, c := range d.LatencyBuckets {
			buckets[i] += c
		}
	}
	percentile := func(p float64) int64 {
		target, seen := float64(total.Requests)*p/100, int64(0)
		for i, c := range buckets {
			seen += c
			if float64(seen) >= target {
				if i < len(types.RequestLatencyBoundsMs) {
					return min(types.RequestLatencyBoundsMs[i], total.DurationMaxMs)
				}
				return total.DurationMaxMs
			}
		}
		return 0
	}
	errorRate := 0.0
	if total.Requests > 0 {
		errorRate = float64(total.Status5xx) / float64(total.Requests)
	}
	return map[string]any{
		"stub_id":        stubID,
		"window_minutes": minutes,
		"requests":       total.Requests,
		"per_minute":     float64(total.Requests) / float64(minutes),
		"status_4xx":     total.Status4xx,
		"status_5xx":     total.Status5xx,
		"error_rate":     errorRate,
		"p50_ms":         percentile(50),
		"p95_ms":         percentile(95),
		"p99_ms":         percentile(99),
		"max_ms":         total.DurationMaxMs,
	}, nil
}

func (g *MCPGroup) listWebhooks(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	webhooks, err := g.workspaceRepo.ListWebhooks(ctx, a.Workspace.ExternalId)
	if err != nil {
		return nil, err
	}
	out := make([]types.WorkspaceWebhook, 0, len(webhooks))
	for _, w := range webhooks {
		out = append(out, redact(w))
	}
	return out, nil
}

func (g *MCPGroup) createWebhook(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if err := validateWebhookURL(args.str("url")); err != nil {
		return nil, fail("INVALID_ARGS", "url must be an absolute http(s) URL")
	}
	id, err := common.GenerateObjectId()
	if err != nil {
		return nil, err
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	eventTypes := args.strings("event_types")
	if len(eventTypes) == 0 {
		eventTypes = []string{"stub.*", "task.*"}
	}
	webhook := types.WorkspaceWebhook{
		ExternalId:  id,
		URL:         args.str("url"),
		EventTypes:  eventTypes,
		Secret:      "whsec_" + hex.EncodeToString(secret),
		Description: args.str("description"),
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	}
	if err := g.workspaceRepo.SetWebhook(ctx, a.Workspace.ExternalId, webhook); err != nil {
		return nil, err
	}
	return webhook, nil
}
