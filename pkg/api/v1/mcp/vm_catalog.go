package mcp

import (
	"context"
	"net/http"
	"net/url"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
)

func vmTool(name, description string, input map[string]any, readOnly, idempotent bool, scope string, run toolFn) mcpTool {
	t := mcpTool{
		Name:        name,
		Description: description,
		Schema:      input,
		ReadOnly:    readOnly,
		Idempotent:  idempotent,
		Category:    "vm",
		Scope:       scope,
		Run:         run,
	}
	switch name {
	case "vm_exec", "vm_docker", "vm_prompt", "vm_desktop_launch", "vm_desktop_click", "vm_desktop_press", "vm_desktop_write", "vm_desktop_drag", "vm_desktop_scroll":
		t.OpenWorld = true
	}
	return t
}

func stringMapSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}
}

func enum(values ...string) map[string]any { return map[string]any{"type": "string", "enum": values} }

func boundedInt(low, high, fallback int) map[string]any {
	return map[string]any{"type": "integer", "minimum": low, "maximum": high, "default": fallback}
}

func vmAllowListSchema() map[string]any {
	return strList("Up to 64 outbound hostnames, *.wildcards (excluding apex), IPs or CIDRs. Hostnames match TLS SNI on 443 / HTTP Host on 80; other TCP needs an IP or CIDR. Proxied destinations must be public, even with an explicit IP/CIDR. Explicit hostname rules must cover every bound credential host.")
}

func vmBlockNetworkSchema() map[string]any {
	return map[string]any{"type": "boolean", "description": "Block outbound traffic except bound credential hosts. Cannot combine with allow_list; inbound port access is configured separately."}
}

func vmSpecSchema() map[string]any {
	command := map[string]any{"type": "array", "items": str("Argument"), "minItems": 1, "maxItems": types.VMKioskMaxCommandArgs}
	spec := schema(props{
		"image_id":        str("VM image built with beam vm image build or the local vm_build_image tool"),
		"cpu":             map[string]any{"type": "integer", "minimum": 100, "maximum": 64000, "default": 4000, "description": "CPU millicores (1000 = 1 CPU); fresh VM default is 4000."},
		"memory":          map[string]any{"type": "integer", "minimum": 256, "maximum": 262144, "default": 8192, "description": "RAM in MiB; desktop requires at least 1024. Fresh VM default is 8192."},
		"disk_size":       str("Writable root capacity; fresh VM default 50Gi, maximum 100Gi"),
		"pool":            str("VM-capable worker pool"),
		"env":             strList("KEY=value entries"),
		"secrets":         strList("Workspace secret names with approved hosts; guest env receives placeholders for HTTPS request headers. BETA9_TOKEN/BEAM_TOKEN are explicit plaintext opt-ins."),
		"ports":           map[string]any{"type": "array", "items": boundedInt(1, 65535, 8000)},
		"private_ports":   map[string]any{"type": "array", "items": boundedInt(1, 65535, 8000), "description": "Tunnel-only ports; no public URL."},
		"protected_ports": map[string]any{"type": "array", "items": boundedInt(1, 65535, 8000), "description": "Published HTTP ports requiring a VM traffic token or vm_access_url browser session."},
		"public_ports":    map[string]any{"type": "array", "items": boundedInt(1, 65535, 7681), "description": "Publish without a traffic token; terminal and desktop ports are protected by default."},
		"desktop":         map[string]any{"type": "boolean", "description": "Enable a desktop already installed in image_id (Ubuntu 24.04 or 22.04); opt-in for fresh MCP specs, at least 1024 MiB RAM."},
		"kiosk": schema(props{
			"transport": enum(string(types.VMKioskTransportWebRTC), string(types.VMKioskTransportWebSockets)),
			"command":   command,
			"cwd":       str("Absolute guest working directory; defaults to /workspace"),
			"env":       stringMapSchema(),
			"mcp": schema(props{
				"command": command,
				"port":    boundedInt(1, 65535, 8000),
				"path":    str("Streamable HTTP endpoint path; defaults to /mcp"),
			}, "command", "port"),
		}, "command"),
		"docker_enabled":     map[string]any{"type": "boolean", "description": "Start the guest Docker daemon; disabled for fresh MCP specs. Wait for docker info before vm_docker."},
		"ssh":                map[string]any{"type": "boolean", "description": "Enable SSH on port 2222; opt-in for fresh MCP specs and requires ssh_public_key."},
		"ssh_public_key":     str("SSH public key; never a private key"),
		"idle_timeout":       boundedInt(0, 365*24*60*60, 0),
		"idle_action":        enum("stop", "pause"),
		"auto_resume":        boolean(),
		"block_network":      vmBlockNetworkSchema(),
		"allow_list":         vmAllowListSchema(),
		"disks":              map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"volumes":            map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"source_snapshot_id": str("Root disk snapshot ID"),
	})
	spec["allOf"] = []map[string]any{{
		"if": map[string]any{
			"required":   []string{"desktop"},
			"properties": props{"desktop": map[string]any{"const": true}},
		},
		"then": map[string]any{"properties": props{"memory": map[string]any{"minimum": 1024}}},
	}}
	return spec
}

func vmSchema(extra props, required ...string) map[string]any {
	properties := props{"name": str("VM name or immutable VM ID")}
	for key, value := range extra {
		properties[key] = value
	}
	return schema(properties, append([]string{"name"}, required...)...)
}

func (g *Group) vmCatalog() []mcpTool {
	tools := g.vmLifecycleCatalog()
	tools = append(tools, g.vmProcessCatalog()...)
	tools = append(tools, g.vmFileCatalog()...)
	return append(tools, g.vmDesktopCatalog()...)
}

func (g *Group) vmLifecycleCatalog() []mcpTool {
	get := func(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) { return g.vmGet(ctx, a, args) }
	tools := []mcpTool{
		vmTool(
			"list_vms",
			"Persistent CPU VMs in this workspace; optional metadata/status filters. Management responses "+
				"redact environment and credentials.",
			schema(props{
				"all":      boolean(),
				"metadata": stringMapSchema(),
				"status":   enum("running", "starting", "stopped", "stopping", "paused", "pausing", "error", "deleted"),
			}),
			true, true, "beam:read",
			g.vmList,
		),
		vmTool(
			"get_vm",
			"VM configuration, lifecycle, container ID, public_ipv6 and stable port URLs. Protected URLs "+
				"require vm_access_url for browser access or a VM traffic token header. Use the ID to pin subsequent operations.",
			vmSchema(nil),
			true, true, "beam:read",
			get,
		),
		vmTool(
			"create_vm",
			"Create a persistent CPU VM from a prepared image, template or snapshot. Use request_id (UUID) "+
				"for safe retries. Omitted template/snapshot settings inherit. For lifecycle state poll get_vm; "+
				"verify exec readiness with vm_exec command=[\"true\"], wait=true. SSH, desktop and Docker may still be starting.",
			schema(props{
				"name":       str("Optional VM name"),
				"spec":       vmSpecSchema(),
				"template":   str("Template name or ID"),
				"snapshot":   str("VM snapshot name or ID"),
				"metadata":   stringMapSchema(),
				"request_id": str("Idempotency UUID"),
			}),
			false, false, "beam:write",
			g.vmCreate,
		),
		vmTool(
			"update_vm",
			"Change idle timeout/action, auto-resume, metadata or outbound network policy. metadata "+
				"replaces the existing map.",
			vmSchema(props{
				"idle_timeout":  boundedInt(0, 365*24*60*60, 0),
				"idle_action":   enum("stop", "pause"),
				"auto_resume":   boolean(),
				"metadata":      stringMapSchema(),
				"block_network": vmBlockNetworkSchema(),
				"allow_list":    vmAllowListSchema(),
			}),
			false, true, "beam:write",
			g.vmUpdate,
		),
		vmTool(
			"vm_ports",
			"Stable port URLs and public/private/protected configuration. For a protected browser URL, call vm_access_url with name and port; metadata URLs contain no temporary credentials.",
			vmSchema(nil),
			true, true, "beam:read",
			get,
		),
		vmTool(
			"vm_metrics",
			"CPU, memory and network metrics for the VM's current runtime.",
			vmSchema(props{"window_minutes": boundedInt(1, 10080, 60)}),
			true, true, "beam:read",
			g.vmMetrics,
		),
	}
	for _, operation := range []struct {
		name, action, description, scope, confirm string
		extra                                     props
		required                                  []string
		read, idempotent                          bool
	}{
		{"start_vm", "start", "Start or resume a VM; cold=true discards saved RAM and starts from disk. Poll get_vm, then verify exec readiness with vm_exec; services may still be starting.", "beam:write", "", props{"cold": boolean()}, nil, false, true},
		{"pause_vm", "pause", "Pause a VM and preserve RAM. Requires confirmation.", "beam:write", "Pausing the VM interrupts its applications.", nil, nil, false, true},
		{"stop_vm", "stop", "Stop a VM and snapshot its writable root unless no_snapshot=true. Requires confirmation.", "beam:write", "Stopping the VM interrupts its applications.", props{"no_snapshot": boolean()}, nil, false, true},
		{"fork_vm", "fork", "Fork the writable root into a new independent VM. An SSH-enabled source requires ssh_public_key matching your local identity.", "beam:write", "", props{"new_name": str("New VM name"), "ssh_public_key": str("New SSH public key")}, nil, false, false},
		{"create_vm_snapshot", "snapshot", "Create a durable VM root snapshot.", "beam:write", "", props{"snapshot_name": str("Snapshot name")}, nil, false, false},
		{"create_vm_template", "template", "Create a reusable VM template from its writable root.", "beam:write", "", props{"template_name": str("Template name"), "description": str("Template description")}, nil, false, false},
		{"vm_expose_port", "expose", "Publish a VM port; protected=true requires a VM traffic token or vm_access_url browser link. Other application ports default public. For terminal 7681 or desktop 8080, pass protected explicitly to toggle access.", "beam:write", "", props{"port": boundedInt(1, 65535, 8000), "protected": boolean()}, []string{"port"}, false, true},
		{"vm_unexpose_port", "unexpose", "Remove the public URL for a VM port. Requires confirmation.", "beam:write", "Removing the port URL interrupts users of that URL.", props{"port": boundedInt(1, 65535, 8000)}, []string{"port"}, false, true},
		{"vm_bind_port", "bind", "Bind a port privately for authenticated SSH/tunnel access without a public URL.", "beam:write", "", props{"port": boundedInt(1, 65535, 8000)}, []string{"port"}, false, true},
		{"vm_access_token", "access-token", "Read the VM traffic token. It grants protected-port access, not workspace management.", "beam:credentials", "", nil, nil, true, true},
		{"vm_rotate_access_token", "rotate-access-token", "Rotate the VM traffic token and invalidate existing traffic sessions. Requires confirmation.", "beam:credentials", "Rotating the traffic token invalidates existing access.", nil, nil, false, false},
		{"vm_access_url", "access-session", "Create a browser link for one protected published HTTP port on its VM subdomain. Opening it sets a Secure HttpOnly host-scoped cookie and removes the credential from the URL. TTL is 1–3600 seconds (default 600); anyone holding the link can access that port until expiry. Mint a fresh link after expiry; rotation revokes sessions. Not for raw IPv6 or SSH.", "beam:credentials", "", props{"port": boundedInt(1, 65535, 8000), "ttl": boundedInt(1, 3600, 600)}, []string{"port"}, false, false},
		{"vm_keep_alive", "touch", "Renew activity to defer the VM idle policy.", "beam:write", "", nil, nil, false, true},
	} {
		op := operation
		extra := props{}
		for k, v := range op.extra {
			extra[k] = v
		}
		if op.confirm != "" {
			extra["confirm"] = boolean()
		}
		t := vmTool(
			op.name,
			op.description,
			vmSchema(extra, op.required...),
			op.read, op.idempotent, op.scope,
			func(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
				body := map[string]any{}
				for k, v := range args {
					if k != "name" && k != "confirm" {
						body[k] = v
					}
				}
				for _, key := range []string{"new_name", "snapshot_name", "template_name"} {
					if v, ok := body[key]; ok {
						body["name"] = v
						delete(body, key)
					}
				}
				if op.action == "access-session" {
					if _, ok := body["ttl"]; !ok {
						body["ttl"] = 600
					}
				}
				return g.vmRequest(ctx, a, http.MethodPost, vmPath(args)+"/"+op.action, body, nil)
			},
		)
		t.Confirm = op.confirm
		t.Destructive = op.confirm != ""
		tools = append(tools, t)
	}
	remove := vmTool(
		"delete_vm",
		"Delete a VM and its writable root. Requires confirmation.",
		vmSchema(props{"confirm": boolean()}),
		false, true, "beam:write",
		func(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
			return g.vmRequest(ctx, a, http.MethodDelete, vmPath(args), nil, nil)
		},
	)
	remove.Confirm = "Deleting the VM removes its writable root."
	tools = append(tools, remove)
	for _, kind := range []string{"snapshot", "template"} {
		kind := kind
		tools = append(tools, vmTool(
			"list_vm_"+kind+"s",
			"VM "+kind+"s in this workspace.",
			schema(props{}),
			true, true, "beam:read",
			func(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
				return g.vmRequest(ctx, a, http.MethodGet, "/artifacts/"+kind, nil, nil)
			},
		))
		t := vmTool(
			"delete_vm_"+kind,
			"Delete a VM "+kind+". Requires confirmation.",
			schema(props{"artifact": str("Artifact name or ID"), "confirm": boolean()}, "artifact"),
			false, true, "beam:write",
			func(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
				return g.vmRequest(ctx, a, http.MethodDelete, "/artifacts/"+kind+"/"+url.PathEscape(args.str("artifact")), nil, nil)
			},
		)
		t.Confirm = "Deleting the " + kind + " is irreversible."
		tools = append(tools, t)
	}
	return tools
}

func (g *Group) vmProcessCatalog() []mcpTool {
	var tools []mcpTool
	process := props{
		"pid":          boundedInt(1, 2147483647, 1),
		"container_id": str("Container ID from vm_exec; rejects a process from a previous VM boot"),
	}
	tools = append(tools,
		vmTool(
			"vm_exec",
			"Run an argv array inside a running VM. Defaults to returning a process ID immediately; "+
				"wait=true waits at most 30 seconds, then follow vm_process_status/output. Never flatten argv "+
				"yourself.",
			vmSchema(props{
				"command": map[string]any{"type": "array", "items": str(""), "minItems": 1},
				"cwd":     str("Working directory"),
				"env":     stringMapSchema(),
				"secrets": strList("Workspace secret names with approved hosts; env receives stable placeholders for literal HTTPS request headers, not raw values. The VM's explicit allow_list must cover each credential hostname."),
				"wait":    boolean(),
			}, "command"),
			false, false, "beam:exec",
			g.vmExec,
		),
		vmTool(
			"vm_process_status",
			"Status and exit code of a managed VM process.",
			vmSchema(process, "pid"),
			true, true, "beam:exec",
			g.vmProcessStatus,
		),
		vmTool(
			"vm_process_output",
			"Read a process's stdout/stderr. Output may advance its stream; do not retry a lost response "+
				"automatically.",
			vmSchema(process, "pid"),
			true, false, "beam:exec",
			g.vmProcessOutput,
		),
		vmTool(
			"vm_list_processes",
			"Managed processes in the running VM.",
			vmSchema(nil),
			true, true, "beam:exec",
			g.vmListProcesses,
		),
		vmTool(
			"vm_kill_process",
			"Kill a managed process. Requires confirmation; pin container_id to avoid targeting a PID after "+
				"reboot.",
			vmSchema(props{"pid": process["pid"], "container_id": process["container_id"], "confirm": boolean()}, "pid", "container_id"),
			false, false, "beam:exec",
			g.vmKillProcess,
		),
		vmTool(
			"vm_logs",
			"Read recent systemd journal or a durable agent session log. For managed process logs use "+
				"vm_process_output.",
			vmSchema(props{"unit": str("Systemd unit"), "session": str("Agent session ID"), "tail": boundedInt(1, 1000, 100)}),
			true, true, "beam:exec",
			g.vmLogs,
		),
		vmTool(
			"vm_prompt",
			"Run an installed Codex or Claude agent as a background systemd service. Returns a durable "+
				"session log ID; read it using vm_logs.",
			vmSchema(props{
				"prompt": str("Agent task"),
				"agent":  enum("codex", "claude"),
				"cwd":    str("Working directory; default /workspace"),
			}, "prompt"),
			false, false, "beam:exec",
			g.vmPrompt,
		),
		vmTool(
			"vm_docker",
			"Run Docker CLI argv inside a Docker-enabled VM after docker info succeeds via vm_exec. The returned PID can be followed using the "+
				"process tools.",
			vmSchema(props{"args": map[string]any{"type": "array", "items": str(""), "minItems": 1}, "wait": boolean()}, "args"),
			false, false, "beam:exec",
			func(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
				info, err := g.vmGet(ctx, a, args)
				if err != nil {
					return nil, err
				}
				spec, _ := info["spec"].(map[string]any)
				if spec["docker_enabled"] != true {
					return nil, fail("DOCKER_DISABLED", "VM was not created with Docker enabled")
				}
				return g.vmExecCommand(ctx, a, args, append([]string{"docker"}, args.strings("args")...), args.boolean("wait"))
			},
		),
	)
	for i := range tools {
		if tools[i].Name == "vm_kill_process" {
			tools[i].Confirm = "Killing the process interrupts its work."
		}
	}
	return tools
}

func (g *Group) vmFileCatalog() []mcpTool {
	var tools []mcpTool
	for _, op := range []struct {
		name, path, method, description string
		read, idempotent                bool
	}{
		{"vm_list_files", "/files", http.MethodGet, "List VM files in a directory.", true, true},
		{"vm_stat_file", "/files/stat", http.MethodGet, "File metadata inside a VM.", true, true},
		{"vm_create_directory", "/directories", http.MethodPost, "Create a VM directory.", false, true},
		{"vm_delete_file", "/files/", http.MethodDelete, "Delete a file inside a VM; requires confirmation.", false, true},
		{"vm_delete_directory", "/directories/", http.MethodDelete, "Delete a VM directory; requires confirmation.", false, true},
	} {
		op := op
		p := props{"path": str("Absolute path inside the VM")}
		if op.name == "vm_create_directory" {
			p["mode"] = boundedInt(0, 511, 493)
		}
		if op.name == "vm_delete_file" || op.name == "vm_delete_directory" {
			p["confirm"] = boolean()
		}
		t := vmTool(
			op.name,
			op.description,
			vmSchema(p, "path"),
			op.read, op.idempotent, "beam:exec",
			func(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
				body := map[string]any{"container_path": args.str("path")}
				if v, ok := args["mode"]; ok {
					body["mode"] = v
				}
				query := url.Values{}
				if op.method == http.MethodGet {
					query.Set("container_path", args.str("path"))
					body = nil
				}
				path := op.path
				if op.method == http.MethodDelete {
					path += url.PathEscape(args.str("path"))
					body = nil
				}
				return g.vmPod(ctx, a, args, op.method, path, body, query)
			},
		)
		if _, ok := p["confirm"]; ok {
			t.Confirm = "Deleting VM files is irreversible."
		}
		tools = append(tools, t)
	}
	tools = append(tools,
		vmTool(
			"vm_read_file",
			"Read at most 512 KiB from a VM file. Returns base64 bytes; offset permits chunked downloads.",
			vmSchema(props{
				"path":   str("Absolute path"),
				"offset": boundedInt(0, 1000000000, 0),
				"length": boundedInt(1, 524288, 65536),
			}, "path"),
			true, true, "beam:exec",
			g.vmReadFile,
		),
		vmTool(
			"vm_write_file",
			"Write text or base64 bytes into a VM file. Offset 0 creates/replaces the file; other offsets "+
				"write a chunk. Requires confirmation.",
			vmSchema(props{
				"path":        str("Absolute path"),
				"text":        str("UTF-8 text; exclusive with data_base64"),
				"data_base64": str("Base64 bytes; exclusive with text"),
				"mode":        boundedInt(0, 511, 420),
				"offset":      boundedInt(0, 1000000000, 0),
				"confirm":     boolean(),
			}, "path"),
			false, true, "beam:exec",
			g.vmWriteFile,
		),
	)
	for i := range tools {
		if tools[i].Name == "vm_write_file" {
			tools[i].Confirm = "Writing at offset zero replaces any existing file."
		}
	}
	tools = append(tools,
		vmTool(
			"vm_find_in_files",
			"Search VM files using a regular expression.",
			vmSchema(props{"path": str("Absolute path"), "pattern": str("Regular expression")}, "path", "pattern"),
			true, true, "beam:exec",
			g.vmFindInFiles,
		),
		vmTool(
			"vm_replace_in_files",
			"Replace matching content inside VM files. Requires confirmation.",
			vmSchema(props{
				"path":        str("Absolute path"),
				"pattern":     str("Search pattern"),
				"replacement": str("Replacement text"),
				"confirm":     boolean(),
			}, "path", "pattern", "replacement"),
			false, false, "beam:exec",
			g.vmReplaceInFiles,
		),
	)
	tools[len(tools)-1].Confirm = "Replacing file content may overwrite existing work."
	return tools
}
