package vm

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"golang.org/x/net/http/httpguts"
)

//go:embed kiosk.html
var kioskPage string

//go:embed kiosk-webrtc.js
var kioskWebRTC []byte

func validateKiosk(spec *types.VMSpec) error {
	k := spec.Kiosk
	if k == nil {
		return nil
	}
	for _, port := range slices.Concat(spec.Ports, spec.PublicPorts) {
		if err := validateKioskPort(k, port, slices.Contains(spec.PublicPorts, port)); err != nil {
			return err
		}
	}
	if k.Transport != "" && k.Transport != types.VMKioskTransportWebRTC && k.Transport != types.VMKioskTransportWebSockets {
		return fmt.Errorf("kiosk transport must be webrtc or websockets")
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
			if err := validateKioskLaunch(append([]string{server.Command}, server.Args...), server.Cwd, server.Env); err != nil {
				return err
			}
		} else {
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
		}
	}
	spec.Desktop = true
	for _, port := range k.PrivatePorts() {
		if !slices.Contains(spec.PrivatePorts, port) {
			spec.PrivatePorts = append(spec.PrivatePorts, port)
		}
	}
	encoded, err := json.Marshal(k)
	if err != nil || len(encoded) > types.VMKioskMaxConfigBytes {
		return fmt.Errorf("kiosk configuration must fit in %d KiB of JSON", types.VMKioskMaxConfigBytes>>10)
	}
	return nil
}

var kioskMCPName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

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

// Creation and port updates enforce the same private and protected endpoints.
func validateKioskPort(k *types.VMKioskSpec, port uint32, public bool) error {
	if k == nil {
		return nil
	}
	switch {
	case slices.Contains(k.PrivatePorts(), port):
		return fmt.Errorf("port %d is reserved for kiosk services", port)
	case public && port == types.VMDesktopPort:
		return fmt.Errorf("kiosk browser sessions must be protected")
	}
	return nil
}

type kioskContext struct {
	echo.Context
	*Service
	vm *types.VM
}

func (c *kioskContext) forward(port uint32, path string) error {
	return c.forwardVM(c, c.vm, port, path)
}

var kioskRoutes = newKioskRouter()

func newKioskRouter() *echo.Router {
	e := echo.New()
	e.Any(types.VMKioskControlPath+"/webrtc.js", func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		return c.Blob(http.StatusOK, "text/javascript; charset=utf-8", kioskWebRTC)
	})
	e.Any(types.VMKioskControlPath+"/webrtc/session", func(c echo.Context) error {
		session := c.(*kioskContext)
		return session.webrtc.register(c, session.vm)
	})
	e.Any(types.VMKioskControlPath+"/status", func(c echo.Context) error {
		if c.Request().Method != http.MethodGet {
			return echo.NewHTTPError(http.StatusMethodNotAllowed)
		}
		return c.(*kioskContext).forward(types.VMKioskStatusPort, "status")
	})
	e.Any(types.VMKioskControlPath+"/restart", func(c echo.Context) error {
		if c.Request().Method != http.MethodPost || c.Request().Header.Get(types.VMKioskActionHeader) != "restart" {
			return echo.NewHTTPError(http.StatusForbidden, "explicit kiosk restart required")
		}
		return c.(*kioskContext).forward(types.VMKioskStatusPort, "restart")
	})
	e.Any(types.VMKioskMCPPath, func(c echo.Context) error {
		return c.(*kioskContext).forward(types.VMKioskMCPPort, strings.TrimPrefix(types.VMKioskMCPPath, "/"))
	})
	for _, path := range []string{types.VMKioskStreamPath + "/*", "/*"} {
		e.Any(path, func(c echo.Context) error {
			return c.(*kioskContext).forward(types.VMDesktopPort, c.Param("*"))
		})
	}
	return e.Router()
}

// proxyKiosk runs after the published desktop port's authentication and VM
// lifecycle checks. Its private control routes inherit that same authority.
func (s *Service) proxyKiosk(c echo.Context, v *types.VM) error {
	path := "/" + c.Param("*")
	// Echo matches against its native context; attach the VM after selection.
	kioskRoutes.Find(c.Request().Method, path, c)
	return c.Handler()(&kioskContext{Context: c, Service: s, vm: v})
}
