package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/google/uuid"
)

func (g *Group) vmRequest(ctx context.Context, a *auth.AuthInfo, method, suffix string, body any, query url.Values) (any, error) {
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
	for attempt := 0; ; attempt++ {
		response, err := g.serve(ctx, method, g.config.GatewayService.HTTP.GetExternalURL()+path, raw, nil)
		result, _ := response.(map[string]any)
		body, _ := result["body"].(map[string]any)
		if result["status"] != http.StatusConflict || body["message"] != "VM operation already in progress" || attempt == 7 {
			return mcpAPIValue(response, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
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

func (g *Group) vmGet(ctx context.Context, a *auth.AuthInfo, args toolArgs) (map[string]any, error) {
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

// vmPod resolves the workspace VM and rejects container IDs from previous boots.
func (g *Group) vmPod(ctx context.Context, a *auth.AuthInfo, args toolArgs, method, operation string, body map[string]any, query url.Values) (any, error) {
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
	// Renew activity for this operation without holding a permanent lease.
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

// vmRPCValue normalizes protobuf response fields to the VM tool schema.
func vmRPCValue(value any) any {
	fields := map[string]string{
		"errorMsg":    "error_msg",
		"exitCode":    "exit_code",
		"fileInfo":    "file_info",
		"modTime":     "mod_time",
		"isDir":       "is_dir",
		"containerId": "container_id",
		"vmId":        "vm_id",
		"startTime":   "start_time",
		"lineNumber":  "line_number",
		"filePath":    "file_path",
	}
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

func (g *Group) vmExec(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	argv := args.strings("command")
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return nil, fail("INVALID_ARGS", "command must contain a program and optional arguments")
	}
	body := map[string]any{
		"command": shellCommand(argv),
		"cwd":     args.str("cwd"),
		"env":     args.stringMap("env"),
		"wait":    false,
		"secrets": args.strings("secrets"),
	}
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

func (g *Group) vmExecCommand(ctx context.Context, a *auth.AuthInfo, args toolArgs, command []string, wait bool) (any, error) {
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

func (g *Group) vmProcessOutput(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *Group) vmLogs(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *Group) vmPrompt(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
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

func (g *Group) vmList(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	query := url.Values{}
	if args.boolean("all") {
		query.Set("all", "true")
	}
	if args.str("status") != "" {
		query.Set("status", args.str("status"))
	}
	if metadata, ok := args["metadata"]; ok {
		raw, _ := json.Marshal(metadata)
		query.Set("metadata", string(raw))
	}
	return g.vmRequest(ctx, a, http.MethodGet, "", nil, query)
}

func (g *Group) vmCreate(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmRequest(ctx, a, http.MethodPost, "", args, nil)
}

func (g *Group) vmUpdate(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	body := map[string]any{}
	for k, v := range args {
		if k != "name" {
			body[k] = v
		}
	}
	return g.vmRequest(ctx, a, http.MethodPatch, vmPath(args), body, nil)
}

func (g *Group) vmMetrics(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	info, err := g.vmGet(ctx, a, args)
	if err != nil {
		return nil, err
	}
	return g.metrics(ctx, a, toolArgs{"stub_id": info["stub_id"], "window_minutes": args.num("window_minutes", 60)})
}

func (g *Group) vmProcessStatus(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmPod(ctx, a, args, http.MethodGet, "/status", nil, url.Values{"pid": {strconv.Itoa(int(args.num("pid", 0)))}})
}

func (g *Group) vmListProcesses(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmPod(ctx, a, args, http.MethodGet, "/processes", nil, nil)
}

func (g *Group) vmKillProcess(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmPod(ctx, a, args, http.MethodPost, "/kill", map[string]any{"pid": args["pid"]}, nil)
}

func (g *Group) vmReadFile(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmPod(ctx, a, args, http.MethodGet, "/files/download/"+url.PathEscape(args.str("path")), nil, url.Values{
		"offset": {strconv.Itoa(int(args.num("offset", 0)))},
		"length": {strconv.Itoa(int(args.num("length", 65536)))},
	})
}

func (g *Group) vmWriteFile(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	text, hasText := args["text"].(string)
	encoded, hasData := args["data_base64"].(string)
	if hasText == hasData {
		return nil, fail("INVALID_ARGS", "provide exactly one of text or data_base64")
	}
	if hasText {
		encoded = base64.StdEncoding.EncodeToString([]byte(text))
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(data) > 524288 {
		return nil, fail("INVALID_ARGS", "file chunk must be valid base64 and at most 512 KiB")
	}
	return g.vmPod(ctx, a, args, http.MethodPost, "/files/upload", map[string]any{
		"container_path": args.str("path"),
		"data":           encoded,
		"mode":           args.num("mode", 420),
		"offset":         args.num("offset", 0),
	}, nil)
}

func (g *Group) vmFindInFiles(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmPod(ctx, a, args, http.MethodPost, "/files/find", map[string]any{"container_path": args.str("path"), "pattern": args.rawString("pattern")}, nil)
}

func (g *Group) vmReplaceInFiles(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmPod(ctx, a, args, http.MethodPost, "/files/replace", map[string]any{
		"container_path": args.str("path"),
		"pattern":        args.rawString("pattern"),
		"new_string":     args.rawString("replacement"),
	}, nil)
}
