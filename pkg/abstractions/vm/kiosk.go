package vm

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/net/http/httpguts"
)

func validateKiosk(spec *types.VMSpec) error {
	k := spec.Kiosk
	if k == nil {
		return nil
	}
	if err := validateKioskLaunch(k.Command, k.Cwd, k.Env); err != nil {
		return err
	}
	if k.Cwd == "" {
		k.Cwd = "/workspace"
	}
	if len(k.MCP) > types.VMKioskMaxMCPServers {
		return fmt.Errorf("too many kiosk MCP servers")
	}
	for name, server := range k.MCP {
		if err := validateKioskMCP(name, server); err != nil {
			return err
		}
	}

	spec.Desktop = true
	encoded, err := json.Marshal(k)
	if err != nil || len(encoded) > types.VMKioskMaxConfigBytes {
		return fmt.Errorf("kiosk configuration must fit in %d KiB of JSON", types.VMKioskMaxConfigBytes>>10)
	}
	return nil
}

var kioskMCPName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func validateKioskMCP(name string, server types.VMKioskMCPServer) error {
	if !kioskMCPName.MatchString(name) {
		return fmt.Errorf("invalid kiosk MCP server name %q", name)
	}
	if (server.Command == "") == (server.URL == "") {
		return fmt.Errorf("kiosk MCP server %q requires either command or url", name)
	}
	if server.Command != "" {
		if len(server.Headers) > 0 {
			return fmt.Errorf("stdio MCP servers cannot use HTTP headers")
		}
		return validateKioskLaunch(append([]string{server.Command}, server.Args...), server.Cwd, server.Env)
	}
	u, err := url.Parse(server.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("kiosk MCP server URL must be an HTTP endpoint without userinfo or fragment")
	}
	if len(server.Args) > 0 || len(server.Env) > 0 || server.Cwd != "" {
		return fmt.Errorf("HTTP MCP servers cannot use launch options")
	}
	for key, value := range server.Headers {
		if !httpguts.ValidHeaderFieldName(key) || !httpguts.ValidHeaderFieldValue(value) {
			return fmt.Errorf("invalid kiosk MCP header")
		}
	}
	return nil
}

func validateKioskLaunch(command []string, cwd string, env map[string]string) error {
	if len(command) == 0 || len(command) > types.VMKioskMaxCommandArgs || command[0] == "" {
		return fmt.Errorf("kiosk commands require an argv array with 1–%d entries", types.VMKioskMaxCommandArgs)
	}
	size := 0
	for _, arg := range command {
		size += len(arg)
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("kiosk command arguments cannot contain NUL")
		}
	}
	if size > types.VMKioskMaxCommandBytes {
		return fmt.Errorf("kiosk commands must fit in %d KiB", types.VMKioskMaxCommandBytes>>10)
	}
	if cwd != "" && (!path.IsAbs(cwd) || path.Clean(cwd) != cwd || strings.ContainsRune(cwd, 0)) {
		return fmt.Errorf("kiosk cwd must be a clean absolute guest path")
	}
	size = 0
	for key, value := range env {
		size += len(key) + len(value)
		if !validEnvKey.MatchString(key) || strings.HasPrefix(key, "BEAM_VM_") || strings.HasPrefix(key, "BETA9_") || strings.ContainsRune(value, 0) {
			return fmt.Errorf("invalid or reserved kiosk environment key %q", key)
		}
	}
	if size > types.VMKioskMaxEnvBytes {
		return fmt.Errorf("kiosk environment must fit in %d KiB", types.VMKioskMaxEnvBytes>>10)
	}
	return nil
}
