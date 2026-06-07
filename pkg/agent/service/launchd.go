package service

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type Launchd struct {
	Runner Runner
}

func (m Launchd) Name() string {
	return types.AgentServiceManagerLaunchd
}

func (m Launchd) Available() bool {
	if runtime.GOOS != "darwin" {
		return false
	}
	_, err := m.runner().LookPath(types.AgentLaunchctlCommand)
	return err == nil
}

func (m Launchd) Install(ctx context.Context, spec Spec) error {
	spec, err := spec.normalized()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(spec.StateDir, 0755); err != nil {
		return err
	}

	path, domain, label, err := launchdTarget(spec.Name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(LaunchdPlist(spec)), 0644); err != nil {
		return err
	}

	runner := m.runner()
	_ = runner.Run(ctx, types.AgentLaunchctlCommand, "bootout", domain, path)
	if err := runner.Run(ctx, types.AgentLaunchctlCommand, "bootstrap", domain, path); err != nil {
		return fmt.Errorf("%s bootstrap: %w", types.AgentLaunchctlCommand, err)
	}
	if err := runner.Run(ctx, types.AgentLaunchctlCommand, "kickstart", "-k", domain+"/"+label); err != nil {
		return fmt.Errorf("%s kickstart: %w", types.AgentLaunchctlCommand, err)
	}
	return nil
}

func (m Launchd) runner() Runner {
	if m.Runner != nil {
		return m.Runner
	}
	return osRunner{}
}

func LaunchdPlist(spec Spec) string {
	spec, _ = spec.normalized()

	p := plist{}
	p.WriteString(xml.Header)
	p.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	p.WriteString(`<plist version="1.0">` + "\n<dict>\n")
	p.string("Label", launchdLabel(spec.Name))
	p.array("ProgramArguments", append([]string{spec.BinaryPath}, spec.Args...))
	p.dict("EnvironmentVariables", spec.Env)
	p.string("WorkingDirectory", spec.StateDir)
	p.string("StandardOutPath", filepath.Join(spec.StateDir, types.AgentServiceLogFile))
	p.string("StandardErrorPath", filepath.Join(spec.StateDir, types.AgentServiceErrorLogFile))
	p.bool("RunAtLoad", true)
	p.bool("KeepAlive", true)
	p.WriteString("</dict>\n</plist>\n")
	return p.String()
}

func launchdTarget(serviceName string) (path, domain, label string, err error) {
	label = launchdLabel(serviceName)
	if os.Geteuid() == 0 {
		return filepath.Join(types.AgentLaunchdSystemDir, label+".plist"), "system", label, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", "", err
	}
	domain = "gui/" + strconv.Itoa(os.Getuid())
	return filepath.Join(home, types.AgentLaunchdUserDir, label+".plist"), domain, label, nil
}

func launchdLabel(rawName string) string {
	name := serviceName(rawName)
	if name == types.DefaultAgentServiceName {
		return types.AgentLaunchdLabel
	}
	return types.AgentLaunchdLabel + "." + name
}

type plist struct {
	strings.Builder
}

func (p *plist) string(key, value string) {
	p.WriteString("  <key>" + xmlEscape(key) + "</key>\n")
	p.WriteString("  <string>" + xmlEscape(value) + "</string>\n")
}

func (p *plist) array(key string, values []string) {
	p.WriteString("  <key>" + xmlEscape(key) + "</key>\n")
	p.WriteString("  <array>\n")
	for _, value := range values {
		p.WriteString("    <string>" + xmlEscape(value) + "</string>\n")
	}
	p.WriteString("  </array>\n")
}

func (p *plist) dict(key string, values map[string]string) {
	p.WriteString("  <key>" + xmlEscape(key) + "</key>\n")
	p.WriteString("  <dict>\n")
	for _, envKey := range sortedEnvKeys(values) {
		p.string(envKey, values[envKey])
	}
	p.WriteString("  </dict>\n")
}

func (p *plist) bool(key string, value bool) {
	p.WriteString("  <key>" + xmlEscape(key) + "</key>\n")
	if value {
		p.WriteString("  <true/>\n")
		return
	}
	p.WriteString("  <false/>\n")
}

func xmlEscape(value string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(value))
	return b.String()
}
