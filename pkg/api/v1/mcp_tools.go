package apiv1

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
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
	url, _ := g.gws.DeploymentURL(d)
	return url
}

// exactDeploymentURL never uses the mutable latest alias.
func (g *MCPGroup) exactDeploymentURL(d *types.DeploymentWithRelated, port int) (string, error) {
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return "", err
	}
	stub := &types.StubWithRelated{Stub: d.Stub, Workspace: d.Workspace, App: &d.App}
	host := g.config.GatewayService.HTTP.GetExternalURL()
	urlType := g.config.GatewayService.InvokeURLType
	if d.Stub.Type.Kind() != types.StubTypePod {
		return common.BuildDeploymentURL(host, urlType, stub, &d.Deployment), nil
	}
	if len(cfg.Ports) == 0 {
		return "", fail("NO_HTTP_PORT", "deployment has no exposed port")
	}
	if port == 0 && len(cfg.Ports) == 1 {
		port = int(cfg.Ports[0])
	}
	if !slices.Contains(cfg.Ports, uint32(port)) {
		return "", fail("INVALID_ARGS", "select an exposed port from %v", cfg.Ports)
	}
	cfg.Ports = []uint32{uint32(port)}
	if cfg.TCP {
		host, urlType = g.config.Abstractions.Pod.TCP.GetExternalURL(), common.InvokeUrlTypeHost
	}
	return common.BuildPodURL(host, urlType, stub, cfg), nil
}

func (g *MCPGroup) deploymentView(d *types.DeploymentWithRelated) map[string]any {
	exactURL, _ := g.exactDeploymentURL(d, 0)
	return map[string]any{
		"name":          d.Name,
		"deployment_id": d.ExternalId,
		"app_id":        d.App.ExternalId,
		"stub_id":       d.Stub.ExternalId,
		"stub_type":     d.StubType,
		"version":       d.Version,
		"active":        d.Active,
		"created_at":    d.CreatedAt.Time,
		"url":           exactURL,
		"latest_url":    g.deploymentURL(d),
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
	cursor := ""
	for {
		page, err := g.backendRepo.ListAppsPaginated(ctx, ws.Id, types.AppFilter{Name: name, Limit: 100, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		for i := range page.Data {
			if page.Data[i].Name == name {
				return &page.Data[i], nil
			}
		}
		if page.Next == "" || page.Next == cursor {
			break
		}
		cursor = page.Next
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
	request := props{
		"name":          str("App name: its newest active version"),
		"deployment_id": str("A specific version instead"),
		"path":          str("Path under the app URL, e.g. /predict"),
		"method":        str("HTTP method; default POST"),
		"body":          map[string]any{"description": "JSON body"},
		"headers":       map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
		"body_base64":   str("Binary request body, exclusive with body"),
		"port":          integer(0),
	}

	return []mcpTool{
		// workspace
		{
			Name:        "capabilities",
			Description: "Live workspace limits, GPU inventory and transport capabilities. Availability is advisory, not a reservation.",
			Schema:      schema(props{}),
			Run:         g.capabilities,
		},
		{
			Name:        "wait_deployment",
			Description: "Verify HTTP application readiness on the exact revision using a safe health path. TCP and portless workloads require their protocol-specific check.",
			Schema: schema(props{
				"name":          str("App name"),
				"deployment_id": str("Exact revision"),
				"path":          str("Safe GET health endpoint; default /health"),
				"port":          integer(0),
				"wait_seconds":  integer(20),
			}),
			Run: g.waitDeployment,
		},
		{Name: "whoami", Description: "Workspace id, name and gateway URL for this token. Where prepaid credit applies, `credit.ok` says whether work can run and `credit.message` where to add credits when it cannot.", Schema: schema(props{}), Run: g.whoami},
		{
			Name:        "list_apps",
			Description: "Apps in the workspace with their newest active deployment and URL.",
			Schema:      schema(props{"name": str("Filter by name"), "cursor": str("Next page cursor"), "limit": integer(50)}),
			Run:         g.listApps,
		},
		{
			Name:        "get_app",
			Description: "One app: its config (resources, scaling, env, secret bindings, ports, disks) and URL.",
			Schema:      target,
			Run:         g.getApp,
		},
		{Name: "delete_app", Description: "Delete an app and every version of it. Requires confirm=true.", Schema: nameConfirm, Confirm: "delete_app removes every deployment of the app.", Run: g.deleteApp},
		// deployments
		{
			Name:        "list_deployments",
			Description: "Deployment versions; filter by name and active.",
			Schema:      schema(props{"name": str(""), "active": boolean(), "limit": integer(50), "offset": integer(0)}),
			Run:         g.listDeployments,
		},
		{Name: "get_deployment", Description: "One deployment version: active, URL, stub.", Schema: target, Run: g.getDeployment},
		{
			Name:        "redeploy",
			Description: "Deploy an existing configuration again, or roll back using an older deployment_id. Choose rollout=replace to retire previous revisions; auto preserves them.",
			Schema: schema(props{
				"name":          str("App name"),
				"deployment_id": str("Specific configuration to deploy"),
				"rollout":       map[string]any{"type": "string", "enum": []string{"auto", "blue-green", "replace"}},
			}),
			Destructive: true,
			Run:         g.redeploy,
		},
		{Name: "stop_deployment", Description: "Stop a deployment version; its containers drain and requests fail until started.", Schema: target, Destructive: true, Run: g.stopDeployment},
		{Name: "start_deployment", Description: "Start a stopped deployment version.", Schema: target, Destructive: true, Run: g.startDeployment},
		{Name: "delete_deployment", Description: "Delete one deployment version. Requires confirm=true.", Schema: targetConfirm, Confirm: "delete_deployment is irreversible.", Run: g.deleteDeployment},
		{Name: "scale_deployment", Description: "Set the replica count of a pod deployment.", Schema: schema(props{"name": str(""), "deployment_id": str(""), "containers": integer(1)}, "containers"), Destructive: true, Run: g.scaleDeployment},
		{Name: "invoke", Description: "Call an exact HTTP revision. Beam credentials authenticate private apps; public apps receive only caller-supplied Authorization headers. Returns status, headers, and body.", Schema: schema(request), Destructive: true, Run: g.invoke},
		// settings
		{Name: "update_config", Description: "Change settings and deploy a new version: dotted paths such as runtime.cpu (millicores), runtime.memory (MB), runtime.gpu, runtime.gpu_count, autoscaler.max_containers, autoscaler.min_containers, autoscaler.tasks_per_container, keep_warm_seconds, concurrent_requests, workers, max_pending_tasks, task_policy.timeout, task_policy.max_retries, authorized, ports, entry_point. Use set_env for variables.", Schema: schema(props{"name": str("App name"), "fields": map[string]any{"type": "object", "description": "path -> value"}}, "name", "fields"), Destructive: true, Run: g.updateConfig},
		{Name: "set_env", Description: "Set or remove environment variables on an app and deploy a new version. Values may be ${{secret.NAME}}, ${{db.NAME.DATABASE_URL}} (or HOST, PORT, USERNAME, PASSWORD, DATABASE) or ${{app.NAME.URL}} (one port of a multi-port app: ${{app.NAME.URL.<port>}}; its TLS host:port on the TCP gateway: ${{app.NAME.TCP.<port>}}).", Schema: schema(props{"name": str("App name"), "env": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}, "unset": strList("Variables to remove")}, "name"), Destructive: true, Run: g.setEnv},
		{Name: "connect_services", Description: "Wire `target` to `source`: a database's URL and parts, or an app's URL, as env references on target; deploys a new version of target.", Schema: schema(props{"source": str("Database or app name"), "target": str("App that receives the variables"), "env_name": str("URL variable name instead of the standard set; authenticated applications also receive <prefix>_TOKEN"), "rotate_credentials": boolean()}, "source", "target"), Destructive: true, Run: g.connectServices},
		// secrets
		{Name: "list_secrets", Description: "Workspace secret names.", Schema: schema(props{}), Run: g.listSecrets},
		{Name: "create_secret", Description: "Create a workspace secret; reference it with ${{secret.NAME}}.", Schema: schema(props{"name": str(""), "value": str("")}, "name", "value"), Destructive: true, Run: g.createSecret},
		{Name: "update_secret", Description: "Change a secret's value; deployments pick it up on their next container start.", Schema: schema(props{"name": str(""), "value": str("")}, "name", "value"), Destructive: true, Run: g.updateSecret},
		{Name: "delete_secret", Description: "Delete a workspace secret. Requires confirm=true.", Schema: nameConfirm, Confirm: "delete_secret breaks deployments still bound to it.", Run: g.deleteSecret},
		// databases
		{Name: "list_databases", Description: "Managed database services and their state.", Schema: schema(props{}), Run: g.listDatabases},
		{
			Name:        "create_database",
			Description: "Create a managed Postgres, Redis, MySQL or MongoDB service on a durable disk. Credentials become secrets; reference them with ${{db.<name>.DATABASE_URL}}.",
			Schema: schema(props{
				"kind":        databaseKind,
				"name":        str(""),
				"always_on":   boolean(),
				"size":        str("Disk capacity, e.g. 10Gi"),
				"cpu":         integer(1000),
				"memory":      integer(512),
				"pool":        str("Optional worker pool"),
				"snapshot_id": str("Restore an available qcow snapshot into a new database; the source is retained"),
				"username":    str("Original Postgres role when restoring a snapshot"),
				"database":    str("Original Postgres database name when restoring a snapshot"),
			}, "kind", "name"),
			Destructive: true,
			Run:         g.createDatabase,
		},
		{Name: "database_credentials", Description: "Connection string and parts for a database service.", Schema: database, Run: g.databaseCredentials},
		{Name: "database_readiness", Description: "Probe Postgres or Redis through its TLS endpoint with stored credentials (up to five seconds). May wake a serverless database. Repeat while ready=false; error explains the last failure. Pin deployment_id to reject a replaced revision.", Schema: schema(props{"name": str(""), "deployment_id": str("Expected deployment revision")}, "name"), Run: g.databaseReadiness},
		{Name: "rotate_database_credentials", Description: "Rotate a database's password; the database and every app bound to it restart with the new credentials.", Schema: database, Destructive: true, Run: g.rotateDatabase},
		{
			Name:        "delete_database",
			Description: "Delete a database service, its credential secrets and its durable disk. Requires confirm=true.",
			Schema:      schema(props{"kind": databaseKind, "name": str(""), "confirm": boolean()}, "kind", "name"),
			Confirm:     "delete_database removes the service, its credentials and its durable disk.",
			Run:         g.deleteDatabase,
		},
		// storage
		{Name: "list_volumes", Description: "Persistent volumes (mount with Volume(name, mount_path) in app code).", Schema: schema(props{}), Run: g.listVolumes},
		{Name: "create_volume", Description: "Create a persistent volume.", Schema: name, Destructive: true, Run: g.createVolume},
		// stacks
		{Name: "list_stacks", Description: "Stacks: named groups of apps shown together on the dashboard board.", Schema: schema(props{}), Run: g.listStacks},
		{Name: "create_stack", Description: "Create a stack, optionally with apps (by name).", Schema: schema(props{"name": str(""), "apps": strList("App names")}, "name"), Destructive: true, Run: g.createStack},
		{
			Name:        "update_stack",
			Description: "Add or remove apps (by name) on a stack; the apps themselves are untouched.",
			Schema: schema(props{
				"name":              str(""),
				"add":               strList(""),
				"remove":            strList(""),
				"spec":              map[string]any{"type": "object", "description": "Merge fields into the existing stack spec; preserves dashboard fields"},
				"expected_revision": str("Revision from list_stacks; prevents stale updates"),
			}, "name"),
			Destructive: true,
			Run:         g.updateStack,
		},
		{Name: "delete_stack", Description: "Delete a stack; its apps are untouched.", Schema: name, Destructive: true, Run: g.deleteStack},
		// observe
		{Name: "logs", Description: "Recent logs, newest last. By app name (every version), or one deployment, stub, task or container. Each line carries its stream: stdout, stderr or system (container lifecycle: image pulls, mounts, exits); `stream` keeps one of them out of the `tail` newest lines.", Schema: schema(props{"name": str("App name"), "deployment_id": str(""), "stub_id": str(""), "task_id": str(""), "container_id": str(""), "tail": integer(100), "since_minutes": integer(0), "search": str("Substring filter"), "stream": logStream}), Run: g.logs},
		{Name: "list_tasks", Description: "Recent tasks (invocations), newest first.", Schema: schema(props{"stub_id": str(""), "status": str("Comma-separated: pending, running, complete, error, cancelled, timeout"), "limit": integer(20)}), Run: g.listTasks},
		{
			Name:        "get_task",
			Description: "Status, results, artifacts and failure details of a task. Optionally wait up to 55 seconds.",
			Schema:      schema(props{"task_id": str(""), "wait_seconds": integer(0)}, "task_id"),
			Run:         g.getTask,
		},
		{Name: "stop_task", Description: "Stop a running or pending task.", Schema: schema(props{"task_id": str("")}, "task_id"), Destructive: true, Run: g.stopTask},
		{Name: "metrics", Description: "CPU, memory, GPU memory, network and container count for an app over a window, per 1m or 1h bucket, plus the latest bucket as `now`. Averages are per container; memory_limit and cpu_limit are the configured resources.", Schema: schema(props{"name": str("App name"), "deployment_id": str(""), "stub_id": str(""), "window_minutes": integer(60), "interval": str("1m (default) or 1h")}), Run: g.metrics},
		{Name: "request_stats", Description: "Request count, 5xx share and p50/p95/p99 latency for an endpoint over a window (upper bounds from a fixed histogram). Endpoints only: pods, functions and queues have `metrics` and `logs`.", Schema: window, Run: g.requestStats},
		{Name: "list_webhooks", Description: "Workspace webhooks (URL, event types, enabled).", Schema: schema(props{}), Run: g.listWebhooks},
		{Name: "create_webhook", Description: "Register a signed HTTP webhook for workspace events (stub.*, task.*, endpoint.request_stats). Returns the signing secret once.", Schema: schema(props{"url": str(""), "event_types": strList(""), "description": str("")}, "url"), Destructive: true, Run: g.createWebhook},
		// everything else
		{
			Name:        "api_routes",
			Description: "Every gateway REST route, for use with `api`: containers, metrics timeseries, event history, tokens, webhooks, volumes, disks, pods and more.",
			Schema:      schema(props{"path": str("Filter path or RPC name; includes schemas when supplied")}),
			Run:         g.apiRoutes,
		},
		{
			Name:        "api",
			Description: "Call any gateway REST route with this token. Methods other than GET need confirm=true. {ws} in the path becomes your workspace id.",
			Schema: schema(props{
				"method":      str("Default GET"),
				"path":        str("e.g. /api/v1/container/{ws}"),
				"body":        map[string]any{"description": "JSON body"},
				"headers":     map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
				"body_base64": str("Binary request body"),
				"confirm":     boolean(),
			}, "path"),
			Destructive: true,
			Run:         g.api,
		},
	}
}

// --- workspace ----------------------------------------------------------------------------

func (g *MCPGroup) whoami(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	out := map[string]any{
		"workspace_id":   a.Workspace.ExternalId,
		"workspace_name": a.Workspace.Name,
		"gateway_http":   g.config.GatewayService.HTTP.GetExternalURL(),
	}
	if credit := g.gws.WorkspaceCredit(ctx, a.Workspace); credit != nil {
		out["credit"] = credit
	}
	return out, nil
}

func (g *MCPGroup) listApps(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *MCPGroup) getApp(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(cfg)
	var full map[string]any
	_ = json.Unmarshal(raw, &full)
	view := make(map[string]any, len(configView)+1)
	for _, key := range configView {
		if v, ok := full[key]; ok {
			view[key] = v
		}
	}
	bindings := make([]map[string]string, 0, len(cfg.Secrets))
	for _, s := range cfg.Secrets {
		bindings = append(bindings, map[string]string{"name": s.Name, "env_name": cmp.Or(s.EnvName, s.Name)})
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
	// The REST handler stops deployments and containers before removing the record.
	res, err := g.serve(ctx, http.MethodDelete, g.config.GatewayService.HTTP.GetExternalURL()+HttpServerBaseRoute+"/app/"+a.Workspace.ExternalId+"/"+app.ExternalId, nil, nil)
	if err != nil {
		return nil, err
	}
	if status := res.(map[string]any)["status"].(int); status >= 300 {
		return nil, fmt.Errorf("delete app: HTTP %d", status)
	}
	return map[string]any{"deleted": app.Name, "app_id": app.ExternalId}, nil
}

// --- deployments ------------------------------------------------------------------------

func (g *MCPGroup) listDeployments(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *MCPGroup) getDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	return g.getApp(ctx, a, toolArgs{"deployment_id": d.ExternalId})
}

func (g *MCPGroup) redeploy(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

// --- settings -------------------------------------------------------------------------------

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

func (g *MCPGroup) connectServices(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *MCPGroup) serviceCredential(ctx context.Context, a *auth.AuthInfo, appID, consumerName string, rotate bool) (string, error) {
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
		_, err = g.backendRepo.CreateSecret(ctx, a.Workspace, a.TokenId(), name, value, true)
		if err != nil {
			// A concurrent connection may have created the same binding.
			_, err = g.backendRepo.GetSecretByName(ctx, a.Workspace, name)
		}
	}
	if err == nil && rotate {
		_, err = g.backendRepo.UpdateSecret(ctx, a.Workspace, a.TokenId(), name, value)
	}
	return name, err
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
	if _, err := g.backendRepo.CreateSecret(ctx, a.Workspace, a.TokenId(), args.str("name"), args.rawString("value"), true); err != nil {
		return nil, err
	}
	return map[string]any{"name": args.str("name"), "created": true}, nil
}

func (g *MCPGroup) updateSecret(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.backendRepo.UpdateSecret(ctx, a.Workspace, a.TokenId(), args.str("name"), args.rawString("value")); err != nil {
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
	info.PooledConnectionString = ""
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

func (g *MCPGroup) databaseReadiness(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.gws.CheckDatabaseReadiness(ctx, a, args.str("name"), args.str("deployment_id"))
}

func (g *MCPGroup) databaseCredentials(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *MCPGroup) rotateDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *MCPGroup) deleteDatabase(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if _, err := g.database(ctx, a, args.str("kind"), args.str("name")); err != nil {
		return nil, err
	}
	if err := g.gws.DeleteDatabaseService(ctx, a, args.str("name")); err != nil {
		return nil, err
	}
	return map[string]any{"deleted": args.str("name"), "kind": args.str("kind"), "disk_deleted": true}, nil
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

// appNames maps app ids to names and back, once per call.
func (g *MCPGroup) appNames(ctx context.Context, ws *types.Workspace) (byID, byName map[string]string, err error) {
	byID, byName = map[string]string{}, map[string]string{}
	cursor := ""
	for {
		page, err := g.backendRepo.ListAppsPaginated(ctx, ws.Id, types.AppFilter{Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, nil, err
		}
		for _, app := range page.Data {
			byID[app.ExternalId] = app.Name
			byName[app.Name] = app.ExternalId
		}
		if page.Next == "" || page.Next == cursor {
			break
		}
		cursor = page.Next
	}
	return byID, byName, nil
}

func stackView(s *types.Stack, byID map[string]string) map[string]any {
	var spec stackSpec
	_ = json.Unmarshal(s.Spec, &spec)
	apps := make([]string, 0, len(spec.AppIds))
	for _, id := range spec.AppIds {
		if name, ok := byID[id]; ok {
			apps = append(apps, name)
		}
	}
	return map[string]any{
		"name":     s.Name,
		"id":       s.ExternalId,
		"apps":     apps,
		"spec":     json.RawMessage(s.Spec),
		"revision": fmt.Sprintf("%x", sha256.Sum256(s.Spec)),
	}
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
	byID, _, err := g.appNames(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(stacks))
	for i := range stacks {
		out = append(out, stackView(&stacks[i], byID))
	}
	return out, nil
}

func resolveApps(byName map[string]string, names []string) ([]string, error) {
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
	byID, byName, err := g.appNames(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	ids, err := resolveApps(byName, args.strings("apps"))
	if err != nil {
		return nil, err
	}
	spec, _ := json.Marshal(stackSpec{AppIds: ids})
	s, err := g.backendRepo.CreateStack(ctx, a.Workspace.Id, args.str("name"), spec)
	if err != nil {
		return nil, err
	}
	return stackView(s, byID), nil
}

func (g *MCPGroup) updateStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	s, err := g.stack(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	if expected := args.str("expected_revision"); expected != "" && expected != fmt.Sprintf("%x", sha256.Sum256(s.Spec)) {
		return nil, fail("STALE_PLAN", "stack changed; reload it before applying")
	}
	byID, byName, err := g.appNames(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	add, err := resolveApps(byName, args.strings("add"))
	if err != nil {
		return nil, err
	}
	remove, err := resolveApps(byName, args.strings("remove"))
	if err != nil {
		return nil, err
	}
	var spec stackSpec
	_ = json.Unmarshal(s.Spec, &spec)
	drop := map[string]bool{}
	for _, id := range remove {
		drop[id] = true
	}
	keep := map[string]bool{}
	ids := make([]string, 0, len(spec.AppIds)+len(add))
	for _, id := range append(spec.AppIds, add...) {
		if !drop[id] && !keep[id] {
			ids = append(ids, id)
			keep[id] = true
		}
	}
	spec.AppIds = ids
	for id := range spec.Positions {
		if !keep[id] {
			delete(spec.Positions, id)
		}
	}
	// Merge owned fields into the original object rather than dropping desired
	// configuration or fields written by a newer dashboard.
	full := map[string]any{}
	_ = json.Unmarshal(s.Spec, &full)
	if patch, ok := args["spec"].(map[string]any); ok {
		for key, value := range patch {
			full[key] = value
		}
	}
	full["appIds"] = ids
	full["positions"] = spec.Positions
	raw, err := json.Marshal(full)
	if err != nil {
		return nil, err
	}
	if len(raw) > stackSpecMaxBytes {
		return nil, fail("INVALID_ARGS", "stack spec exceeds 256 KiB")
	}
	writer, ok := g.backendRepo.(interface {
		UpdateStackIfUnchanged(context.Context, uint, string, string, json.RawMessage, json.RawMessage) (*types.Stack, error)
	})
	if !ok {
		return nil, fail("UNSUPPORTED", "atomic stack updates unavailable")
	}
	updated, err := writer.UpdateStackIfUnchanged(ctx, a.Workspace.Id, s.ExternalId, s.Name, s.Spec, raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fail("STALE_PLAN", "stack changed during update; reload before retrying: %s", err)
	}
	if err != nil {
		return nil, err
	}
	return stackView(updated, byID), nil
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
	stream := args.str("stream")
	if stream != "" && !slices.Contains(logStreams, stream) {
		return nil, fail("INVALID_ARGS", "stream must be one of %s", strings.Join(logStreams, ", "))
	}
	query := types.LogQuery{WorkspaceID: a.Workspace.ExternalId, Limit: uint64(args.num("tail", 100)), Query: args.str("search")}
	if minutes := args.num("since_minutes", 0); minutes > 0 {
		query.StartTime = ptr.To(time.Now().UTC().Add(-time.Duration(minutes) * time.Minute))
	}
	switch {
	case args.str("deployment_id") != "":
		d, err := g.target(ctx, a, args)
		if err != nil {
			return nil, err
		}
		query.ObjectType, query.ObjectID, query.StubID, query.AppID = types.GatewayObjectTypeDeployment, d.ExternalId, d.Stub.ExternalId, d.App.ExternalId
	case args.str("name") != "":
		app, err := g.appByName(ctx, a.Workspace, args.str("name"))
		if err != nil {
			return nil, err
		}
		query.ObjectType, query.ObjectID, query.AppID = types.GatewayObjectTypeApp, app.ExternalId, app.ExternalId
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
	requested := int(query.Limit)
	if stream != "" {
		query.Limit = 10000
	}
	res, err := g.eventRepo.GetLogs(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(res.Logs))
	for _, l := range res.Logs {
		if stream != "" && l.Stream != stream {
			continue
		}
		line := map[string]any{"time": l.Timestamp, "message": l.Message, "container_id": l.ContainerID}
		if l.Stream != "" {
			line["stream"] = l.Stream
		}
		if l.TaskID != "" {
			line["task_id"] = l.TaskID
		}
		out = append(out, line)
	}
	complete := stream == "" || len(out) >= requested || len(res.Logs) < int(query.Limit)
	if len(out) > requested {
		out = out[len(out)-requested:]
	}
	return map[string]any{"items": out, "complete": complete, "scanned": len(res.Logs), "scan_limit": query.Limit}, nil
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
	if _, err := okOrErr(res.GetOk(), res.GetErrMsg(), err, nil); err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(res.Tasks))
	for _, t := range res.Tasks {
		out = append(out, protoMap(t))
	}
	return out, nil
}

func (g *MCPGroup) getTask(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	deadline := time.Now().Add(time.Duration(args.num("wait_seconds", 0)) * time.Second)
	for {
		task, err := g.backendRepo.GetTaskWithRelated(ctx, args.str("task_id"))
		if err != nil || task == nil || task.Workspace.ExternalId != a.Workspace.ExternalId {
			return nil, fail("NOT_FOUND", "no task %s", args.str("task_id"))
		}
		status := strings.ToLower(string(task.Status))
		if (status != "pending" && status != "running" && status != "retry") || !time.Now().Before(deadline) {
			response, err := g.serve(ctx, http.MethodGet, g.config.GatewayService.HTTP.GetExternalURL()+"/api/v1/task/"+a.Workspace.ExternalId+"/"+task.ExternalId, nil, nil)
			if err != nil {
				return nil, err
			}
			envelope := response.(map[string]any)
			if body, ok := envelope["body"].(map[string]any); ok && envelope["is_error"] == false {
				body["task_id"] = task.ExternalId
				return body, nil
			}
			return response, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (g *MCPGroup) stopTask(ctx context.Context, _ *auth.AuthInfo, args toolArgs) (any, error) {
	res, err := g.gws.StopTasks(ctx, &pb.StopTasksRequest{TaskIds: []string{args.str("task_id")}})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"task_id": args.str("task_id"), "stopped": true})
}

// stubID is the stub a metrics tool reads: given directly, or the target deployment's.
func (g *MCPGroup) stubID(ctx context.Context, a *auth.AuthInfo, args toolArgs) (string, error) {
	if id := args.str("stub_id"); id != "" {
		return id, nil
	}
	d, err := g.target(ctx, a, args)
	if err != nil {
		return "", err
	}
	return d.Stub.ExternalId, nil
}

func (g *MCPGroup) metrics(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	stubID, err := g.stubID(ctx, a, args)
	if err != nil {
		return nil, err
	}
	interval := args.str("interval")
	if interval == "" {
		interval = "1m"
	}
	if interval != "1m" && interval != "1h" {
		return nil, fail("INVALID_ARGS", "interval must be 1m or 1h")
	}
	end := time.Now().UTC()
	start := end.Add(-time.Duration(args.num("window_minutes", 60)) * time.Minute)
	res, err := g.eventRepo.GetStubMetricsTimeseries(ctx, types.EventQuery{WorkspaceID: a.Workspace.ExternalId, StubID: stubID, EventTypes: []string{types.EventContainerMetrics}}, start, end, interval)
	if err != nil {
		return nil, err
	}
	const mb = 1 << 20
	points := make([]map[string]any, 0, len(res.Timeseries.AggregationBuckets))
	for _, b := range res.Timeseries.AggregationBuckets {
		if b.DocCount == 0 {
			continue
		}
		points = append(points, map[string]any{
			"time":                time.UnixMilli(b.Key).UTC(),
			"containers":          b.ContainerCount.Value,
			"cpu_pct":             b.CPUPercentAvg.Value,
			"cpu_used":            b.CPUUsedAvg.Value,
			"cpu_limit":           b.CPUTotalAvg.Value,
			"memory_mb":           b.MemoryRSSBytesAvg.Value / mb,
			"memory_limit_mb":     b.MemoryTotalBytesAvg.Value / mb,
			"gpu_memory_mb":       b.GPUMemoryUsedBytesAvg.Value / mb,
			"gpu_memory_total_mb": b.GPUMemoryTotalBytesAvg.Value / mb,
			"net_in_bps":          b.NetworkRecvBytesRateAvg.Value,
			"net_out_bps":         b.NetworkSentBytesRateAvg.Value,
		})
	}
	out := map[string]any{"stub_id": stubID, "interval": interval, "points": points}
	if len(points) > 0 {
		out["now"] = points[len(points)-1] // retained for compatibility; this is a historical bucket
		out["latest"] = points[len(points)-1]
		latest := points[len(points)-1]["time"].(time.Time)
		out["sample_age_seconds"] = end.Sub(latest).Seconds()
		threshold := 2 * time.Minute
		if interval == "1h" {
			threshold = 2 * time.Hour
		}
		out["stale"] = end.Sub(latest) > threshold
	} else {
		out["stale"] = true
	}
	out["observed_at"] = end
	return out, nil
}

func (g *MCPGroup) requestStats(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	stubID, err := g.stubID(ctx, a, args)
	if err != nil {
		return nil, err
	}
	stub, err := g.backendRepo.GetStubByExternalId(ctx, stubID, types.QueryFilter{Field: "workspace_id", Value: a.Workspace.ExternalId})
	if err != nil || stub == nil || stub.ExternalId == "" {
		return nil, fail("NOT_FOUND", "no stub %s", stubID)
	}
	if !servesRequests(stub.Type) {
		return nil, fail("INVALID_ARGS", "request_stats covers endpoints; %s is a %s, which has metrics and logs", cmp.Or(args.str("name"), stubID), stub.Type.Kind())
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
		"complete":       len(history.Events) < 5000,
		"event_limit":    5000,
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

func (g *MCPGroup) capabilities(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	out := map[string]any{
		"serverless_default":                      true,
		"always_on":                               "autoscaler.min_containers=1",
		"inline_response_limit_bytes":             maxInProcessBody,
		"local_bridge_required_for_source_deploy": true,
		"gpu_inventory_is_reservation":            false,
		"gateway_operations":                      gatewayOperations(""),
	}
	// Available recovery workflows and durability qualification are separate capabilities.
	out["database_recovery"] = map[string]any{
		"mode":                        "object-store-flush for new Postgres/Redis; snapshots for legacy disks",
		"conditional_writes_required": true,
		"single_machine_failure_rpo_zero_qualified": false,
	}
	out["disk_capacity"] = map[string]any{
		"qcow":     "bounded ext4 block device; growth on writable host reattachment",
		"snapshot": "directory driver does not enforce configured size",
	}
	out["service_scoped_auth"] = true
	for key, path := range map[string]string{
		"gpu_inventory": "/api/v1/machine/" + a.Workspace.ExternalId + "/gpus",
		"limits":        "/api/v1/workspace/" + a.Workspace.ExternalId + "/limits",
	} {
		response, err := g.serve(ctx, http.MethodGet, g.config.GatewayService.HTTP.GetExternalURL()+path, nil, nil)
		if err != nil {
			return nil, err
		}
		value := response.(map[string]any)
		if value["is_error"] == true {
			out[key] = value
		} else {
			out[key] = value["body"]
		}
	}
	return out, nil
}

func (g *MCPGroup) waitDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if cfg.TCP || (d.Stub.Type.Kind() == types.StubTypePod && len(cfg.Ports) == 0) {
		return nil, fail("UNSUPPORTED_PROTOCOL", "verify this workload using its native protocol or task status")
	}
	wait := time.Duration(args.num("wait_seconds", 20)) * time.Second
	if wait == 0 {
		wait = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	call := toolArgs{
		"deployment_id": d.ExternalId,
		"method":        "GET",
		"path":          cmp.Or(args.str("path"), "/health"),
		"port":          args["port"],
	}

	for {
		response, err := g.invoke(ctx, a, call)
		if err != nil {
			return nil, err
		}
		value := response.(map[string]any)
		if status, ok := value["status"].(int); ok && status >= 200 && status < 300 && value["is_error"] == false {
			return map[string]any{"deployment_id": d.ExternalId, "ready": true, "health": value}, nil
		}
		select {
		case <-ctx.Done():
			return map[string]any{"deployment_id": d.ExternalId, "ready": false, "health": value, "is_error": true, "code": "NOT_READY"}, nil
		case <-time.After(time.Second):
		}
	}
}
