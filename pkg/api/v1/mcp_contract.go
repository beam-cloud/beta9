package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/google/jsonschema-go/jsonschema"
)

type mcpSelection struct {
	Features []string
	Tools    []string
	ReadOnly bool
}

type mcpSelectionKey struct{}

func compileToolSchema(schema map[string]any) (func(any) error, error) {
	raw, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	var contract jsonschema.Schema
	if err := json.Unmarshal(raw, &contract); err != nil {
		return nil, err
	}
	resolved, err := contract.Resolve(nil)
	if err != nil {
		return nil, err
	}
	return resolved.Validate, nil
}

func (g *MCPGroup) prepareTools() {
	g.byName = make(map[string]*mcpTool, len(g.tools))
	for i := range g.tools {
		tool := &g.tools[i]
		validate, err := compileToolSchema(tool.Schema)
		if err != nil {
			panic(fmt.Sprintf("invalid MCP schema %s: %v", tool.Name, err))
		}
		tool.validate = validate
		if _, exists := g.byName[tool.Name]; exists {
			panic("duplicate MCP tool: " + tool.Name)
		}
		g.byName[tool.Name] = tool
	}
}

// withToolPolicies supplies default categories, scopes, and annotations.
func withToolPolicies(tools []mcpTool) []mcpTool {
	reads := strings.Fields("capabilities wait_deployment whoami list_apps get_app list_deployments get_deployment " +
		"list_secrets list_databases database_credentials database_readiness list_volumes list_stacks " +
		"logs list_tasks get_task metrics request_stats list_webhooks api_routes")
	creations := strings.Fields("create_secret create_volume create_stack create_database create_webhook start_deployment")
	credentials := strings.Fields("database_credentials rotate_database_credentials")
	for i := range tools {
		t := &tools[i]
		// VM tools declare their policies in the catalog.
		if t.Category != "" {
			continue
		}
		t.Category = "apps"
		for _, group := range []struct{ category, words string }{
			{"core", "whoami capabilities api api_routes"},
			{"databases", "list_databases create_database database_credentials database_readiness rotate_database_credentials delete_database"},
			{"secrets", "list_secrets create_secret update_secret delete_secret"},
			{"storage", "list_volumes create_volume"},
			{"stacks", "list_stacks create_stack update_stack delete_stack"},
			{"observe", "logs metrics request_stats list_tasks get_task stop_task list_webhooks create_webhook"},
		} {
			if slices.Contains(strings.Fields(group.words), t.Name) {
				t.Category = group.category
			}
		}
		t.ReadOnly = slices.Contains(reads, t.Name)
		t.Idempotent = t.ReadOnly
		t.OpenWorld = t.Name == "invoke" || t.Name == "wait_deployment" || t.Name == "database_readiness"
		if slices.Contains(creations, t.Name) {
			t.Destructive = false
		}
		t.Scope = "beam:write"
		if t.ReadOnly {
			t.Scope = "beam:read"
		}
		if slices.Contains(credentials, t.Name) {
			t.Scope = "beam:credentials"
		}
		if t.Name == "invoke" {
			t.Scope = "beam:exec"
		}
		if t.Name == "api" {
			t.Scope = "beam:admin"
		}
	}
	return tools
}

func (g *MCPGroup) allowed(info *auth.AuthInfo, t *mcpTool) bool {
	if t.Scope == "" {
		return true
	}
	if !auth.HasPermission(info) {
		return false
	}
	if (t.Category == "vm" || t.Scope == "beam:exec" || t.Scope == "beam:credentials") && !auth.HasInteractivePermission(info) {
		return false
	}
	if info.OAuth == nil {
		return true
	}
	// OAuth grants cannot use API routes that mint unscoped credentials.
	return t.Scope != "beam:admin" && slices.Contains(info.OAuth.Scopes, t.Scope)
}

func (g *MCPGroup) selected(ctx context.Context, tool *mcpTool) bool {
	selection, _ := ctx.Value(mcpSelectionKey{}).(mcpSelection)
	if selection.ReadOnly && !tool.ReadOnly {
		return false
	}
	if len(selection.Features) == 0 && len(selection.Tools) == 0 {
		return true
	}
	return slices.Contains(selection.Features, tool.Category) || slices.Contains(selection.Tools, tool.Name)
}

func (g *MCPGroup) instructions() string {
	return "Beam CPU/GPU apps, managed databases, stacks and persistent CPU VMs in one workspace. " +
		"Start with whoami and capabilities. Use list_apps or list_vms to inspect existing resources. " +
		"Deploy local files through the Beam CLI or local MCP bridge; remote MCP cannot read a laptop directory. " +
		"VM tools manage lifecycle, ports, snapshots, templates, files, commands, Docker and desktop controls. " +
		"VM commands return process IDs; use vm_process_status and vm_process_output to follow long work. " +
		"Use environment references to wire apps and databases. Confirmation-required tools name their effect. " +
		"Apps and stack services default to serverless: min_replicas=0, keep_warm_seconds=0; databases use always_on=false. " +
		"Keep these defaults unless the user explicitly requests continuous compute; a cold start is " +
		"not a reason to add an idle floor. " +
		"Use a finite idle timeout and auto-resume for persistent VMs. Pin container_id for process " +
		"operations across VM restarts. " +
		"Filter discovery with ?features=vm,apps,observe or ?tools=whoami,list_vms; ?readonly=true exposes read-only operations."
}
