package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
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

// Configuration fields exposed by app tools.
var configView = []string{
	"runtime",
	"autoscaler",
	"keep_warm_seconds",
	"workers",
	"concurrent_requests",
	"max_pending_tasks",
	"task_policy",
	"env",
	"ports",
	"tcp",
	"authorized",
	"entry_point",
	"volumes",
	"disks",
	"pool",
}

// Pods serve ports without the task-runner settings.
var podUnusedConfig = []string{"workers", "concurrent_requests", "max_pending_tasks", "task_policy"}

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
	latestURL := g.deploymentURL(d)
	view := map[string]any{
		"name":          d.Name,
		"deployment_id": d.ExternalId,
		"app_id":        d.App.ExternalId,
		"stub_id":       d.Stub.ExternalId,
		"stub_type":     d.StubType,
		"version":       d.Version,
		"active":        d.Active,
		"created_at":    d.CreatedAt.Time,
		"url":           exactURL,
		"latest_url":    latestURL,
	}
	if urls := portURLs(d, latestURL); urls != nil {
		view["port_urls"] = urls
	}
	return view
}

// portURLs resolves the URL template for each exposed port.
func portURLs(d *types.DeploymentWithRelated, template string) map[string]string {
	if !strings.Contains(template, common.PortPlaceholder) {
		return nil
	}
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return nil
	}
	urls := make(map[string]string, len(cfg.Ports))
	for _, port := range cfg.Ports {
		key := strconv.Itoa(int(port))
		urls[key] = strings.ReplaceAll(template, common.PortPlaceholder, key)
	}
	return urls
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
