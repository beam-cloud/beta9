package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/google/uuid"
)

func (g *MCPGroup) vmRequest(ctx context.Context, a *auth.AuthInfo, method, suffix string, body any, query url.Values) (any, error) {
	path := "/api/v1/vm/" + url.PathEscape(a.Workspace.ExternalId) + suffix
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	response, err := g.serve(ctx, method, g.config.GatewayService.HTTP.GetExternalURL()+path, raw, nil)
	return mcpAPIValue(response, err)
}

func mcpAPIValue(response any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	result, ok := response.(map[string]any)
	if !ok {
		return nil, fail("INVALID_RESPONSE", "expected a gateway response")
	}
	status, _ := result["status"].(int)
	if status < 200 || status >= 300 {
		return nil, fail("API_ERROR", "gateway HTTP %d: %v", status, result["body"])
	}
	body := result["body"]
	if obj, ok := body.(map[string]any); ok {
		if accepted, exists := obj["ok"].(bool); exists && !accepted {
			message := obj["error_msg"]
			if message == nil {
				message = obj["errorMsg"]
			}
			return nil, fail("VM_OPERATION_FAILED", "%v", message)
		}
	}
	return body, nil
}

func vmPath(args toolArgs) string { return "/" + url.PathEscape(args.str("name")) }

func (g *MCPGroup) vmGet(ctx context.Context, a *auth.AuthInfo, args toolArgs) (map[string]any, error) {
	value, err := g.vmRequest(ctx, a, http.MethodGet, vmPath(args), nil, nil)
	if err != nil {
		return nil, err
	}
	info, ok := value.(map[string]any)
	if !ok {
		return nil, fail("INVALID_RESPONSE", "expected VM details")
	}
	return info, nil
}

// Resolve the container through the workspace's VM identity on every call.
// Never accept an arbitrary container_id from an agent. Reattachable process
// calls can pin it to reject IDs from a previous boot.
func (g *MCPGroup) vmPod(ctx context.Context, a *auth.AuthInfo, args toolArgs, method, operation string, body map[string]any, query url.Values) (any, error) {
	info, err := g.vmGet(ctx, a, args)
	if err != nil {
		return nil, err
	}
	container, _ := info["container_id"].(string)
	if container == "" || info["desired_state"] != "running" {
		return nil, fail("VM_NOT_RUNNING", "VM is not running; call start_vm before this operation")
	}
	if pinned := args.str("container_id"); pinned != "" && pinned != container {
		return nil, fail("VM_RESTARTED", "VM has restarted; this process belongs to a previous container")
	}
	// Renew idle activity before the operation. Background processes are followed
	// explicitly rather than starting a permanent lease the agent may abandon.
	if _, err := g.vmRequest(ctx, a, http.MethodPost, vmPath(args)+"/touch", map[string]any{}, nil); err != nil {
		return nil, err
	}
	path := "/api/v1/gateway/pods/" + url.PathEscape(container) + operation
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	value, err := g.api(ctx, a, toolArgs{"method": method, "path": path, "body": body, "confirm": true})
	value, err = mcpAPIValue(value, err)
	value = vmRPCValue(value)
	if obj, ok := value.(map[string]any); ok {
		obj["container_id"] = container
		obj["vm_id"] = info["id"]
	}
	return value, err
}

// The gRPC HTTP bridge emits protobuf camelCase fields. VM management and the
// CLI use snake_case; keep the MCP contract consistent at this boundary.
func vmRPCValue(value any) any {
	fields := map[string]string{"errorMsg": "error_msg", "exitCode": "exit_code", "fileInfo": "file_info", "modTime": "mod_time", "isDir": "is_dir", "containerId": "container_id", "vmId": "vm_id", "startTime": "start_time", "lineNumber": "line_number", "filePath": "file_path"}
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			if name, ok := fields[key]; ok {
				key = name
			}
			out[key] = vmRPCValue(item)
		}
		return out
	case []any:
		for i := range v {
			v[i] = vmRPCValue(v[i])
		}
	}
	return value
}

func shellWord(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
func shellCommand(argv []string) string {
	words := make([]string, len(argv))
	for i, arg := range argv {
		words[i] = shellWord(arg)
	}
	return strings.Join(words, " ")
}

func (g *MCPGroup) vmExec(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	argv := args.strings("command")
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return nil, fail("INVALID_ARGS", "command must contain a program and optional arguments")
	}
	body := map[string]any{"command": shellCommand(argv), "cwd": args.str("cwd"), "env": args.stringMap("env"), "wait": false, "secrets": args.strings("secrets")}
	value, err := g.vmPod(ctx, a, args, http.MethodPost, "/exec", body, nil)
	if err != nil || !args.boolean("wait") {
		return value, err
	}
	result, ok := value.(map[string]any)
	if !ok {
		return nil, fail("INVALID_RESPONSE", "expected a process ID")
	}
	process := toolArgs{"name": args.str("name"), "pid": result["pid"], "container_id": result["container_id"]}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := g.vmPod(ctx, a, process, http.MethodGet, "/status", nil, url.Values{"pid": {strconv.Itoa(int(process.num("pid", 0)))}})
		if err != nil {
			return nil, err
		}
		state, _ := status.(map[string]any)
		if state["status"] == "exited" {
			result["done"], result["exit_code"] = true, state["exit_code"]
			output, err := g.vmProcessOutput(ctx, a, process)
			if err != nil {
				return nil, err
			}
			streams, _ := output.(map[string]any)
			for _, stream := range []string{"stdout", "stderr"} {
				if part, ok := streams[stream].(map[string]any); ok {
					result[stream] = part[stream]
				}
			}
			if toolArgs(state).num("exit_code", 0) != 0 {
				result["is_error"], result["code"] = true, "PROCESS_FAILED"
			}
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	result["done"], result["waiting_expired"] = false, true
	return result, nil
}

func (g *MCPGroup) vmExecCommand(ctx context.Context, a *auth.AuthInfo, args toolArgs, command []string, wait bool) (any, error) {
	copy := toolArgs{"name": args.str("name"), "cwd": "/workspace", "wait": wait, "command": command}
	items := make([]any, len(command))
	for i, arg := range command {
		items[i] = arg
	}
	copy["command"] = items
	value, err := g.vmExec(ctx, a, copy)
	if result, ok := value.(map[string]any); ok && result["is_error"] == true {
		return nil, fail("PROCESS_FAILED", "guest command exited %v: %v", result["exit_code"], result["stderr"])
	}
	return value, err
}

func (g *MCPGroup) vmProcessOutput(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	query := url.Values{"pid": {strconv.Itoa(int(args.num("pid", 0)))}}
	stdout, err := g.vmPod(ctx, a, args, http.MethodGet, "/stdout", nil, query)
	if err != nil {
		return nil, err
	}
	stderr, err := g.vmPod(ctx, a, args, http.MethodGet, "/stderr", nil, query)
	if err != nil {
		return nil, err
	}
	return map[string]any{"stdout": stdout, "stderr": stderr}, nil
}

func (g *MCPGroup) vmLogs(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	command := []string{"journalctl", "--no-pager", "-n", strconv.Itoa(int(args.num("tail", 100)))}
	if unit := args.str("unit"); unit != "" {
		command = append(command, "-u", unit)
	}
	if session := args.str("session"); session != "" {
		if args.str("unit") != "" {
			return nil, fail("INVALID_ARGS", "choose unit or session")
		}
		for _, c := range session {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
				return nil, fail("INVALID_ARGS", "invalid session name")
			}
		}
		command = []string{"tail", "-n", strconv.Itoa(int(args.num("tail", 100))), "/workspace/.beam-vm/logs/" + session + ".log"}
	}
	return g.vmExecCommand(ctx, a, args, command, true)
}

func (g *MCPGroup) vmPrompt(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	info, err := g.vmGet(ctx, a, args)
	if err != nil {
		return nil, err
	}
	session := "prompt-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	agent := "codex"
	command := []string{agent, "exec", args.rawString("prompt")}
	if args.str("agent") == "claude" {
		agent = "claude"
		command = []string{agent, "-p", args.rawString("prompt")}
	}
	if _, err := g.vmExecCommand(ctx, a, args, []string{"bash", "-lc", "command -v " + shellWord(agent)}, true); err != nil {
		if failure, ok := err.(*toolError); ok && failure.Code == "PROCESS_FAILED" {
			return nil, fail("AGENT_NOT_INSTALLED", "install %s and sign in inside the VM before running a prompt", agent)
		}
		return nil, err
	}
	logPath := "/workspace/.beam-vm/logs/" + session + ".log"
	shell := "mkdir -p /workspace/.beam-vm/logs; set -o pipefail; " + shellCommand(command) + " 2>&1 | tee " + shellWord(logPath)
	argv := []string{"systemd-run", "--collect", "--unit=" + session, "--working-directory=" + args.str("cwd"), "bash", "-lc", shell}
	if args.str("cwd") == "" {
		argv[3] = "--working-directory=/workspace"
	}
	result, err := g.vmExecCommand(ctx, a, args, argv, true)
	return map[string]any{"vm_id": info["id"], "session": session, "log_path": logPath, "launch": result}, err
}
