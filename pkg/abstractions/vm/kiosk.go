package vm

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
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
	if k.Transport == types.VMKioskTransportWebRTC {
		if !slices.Contains(spec.PrivatePorts, types.VMKioskMediaPort) {
			spec.PrivatePorts = append(spec.PrivatePorts, types.VMKioskMediaPort)
		}
	}
	checkCommand := func(command []string) error {
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
		return nil
	}
	if err := checkCommand(k.Command); err != nil {
		return err
	}
	if k.Cwd == "" {
		k.Cwd = "/workspace"
	}
	if !path.IsAbs(k.Cwd) || path.Clean(k.Cwd) != k.Cwd || strings.ContainsRune(k.Cwd, 0) {
		return fmt.Errorf("kiosk cwd must be a clean absolute guest path")
	}
	size := 0
	for key, value := range k.Env {
		size += len(key) + len(value)
		if !validEnvKey.MatchString(key) || strings.HasPrefix(key, "BEAM_VM_") || strings.HasPrefix(key, "BETA9_") || strings.ContainsRune(value, 0) {
			return fmt.Errorf("invalid or reserved kiosk environment key %q", key)
		}
	}
	if size > types.VMKioskMaxEnvBytes {
		return fmt.Errorf("kiosk environment must fit in %d KiB", types.VMKioskMaxEnvBytes>>10)
	}
	if k.MCP != nil {
		if err := checkCommand(k.MCP.Command); err != nil {
			return err
		}
		p := k.MCP.Port
		if p == 0 || p > 65535 || p == types.VMSSHPort || p == types.VMTerminalPort || p == types.VMDesktopPort || p == types.VMKioskStatusPort || (k.Transport == types.VMKioskTransportWebRTC && p == types.VMKioskMediaPort) || p == uint32(types.WorkerSandboxProcessManagerPort) {
			return fmt.Errorf("kiosk MCP requires a non-reserved port between 1 and 65535")
		}
		if k.MCP.Path == "" {
			k.MCP.Path = "/mcp"
		}
		if !strings.HasPrefix(k.MCP.Path, "/") || path.Clean(k.MCP.Path) != k.MCP.Path || strings.ContainsAny(k.MCP.Path, "?#\\\x00\r\n") {
			return fmt.Errorf("kiosk MCP path must be an absolute URL path without a query or fragment")
		}
		if !slices.Contains(spec.Ports, p) {
			spec.Ports = append(spec.Ports, p)
		}
		if !slices.Contains(spec.ProtectedPorts, p) {
			spec.ProtectedPorts = append(spec.ProtectedPorts, p)
		}
	}
	spec.Desktop = true
	if !slices.Contains(spec.PrivatePorts, types.VMKioskStatusPort) {
		spec.PrivatePorts = append(spec.PrivatePorts, types.VMKioskStatusPort)
	}
	encoded, err := json.Marshal(k)
	if err != nil || len(encoded) > types.VMKioskMaxConfigBytes {
		return fmt.Errorf("kiosk configuration must fit in %d KiB of JSON", types.VMKioskMaxConfigBytes>>10)
	}
	return nil
}

// Creation and port updates enforce the same private and protected endpoints.
func validateKioskPort(k *types.VMKioskSpec, port uint32, public bool) error {
	if k == nil {
		return nil
	}
	switch {
	case port == types.VMKioskStatusPort:
		return fmt.Errorf("port %d is reserved for kiosk readiness", port)
	case port == types.VMKioskMediaPort && k.Transport == types.VMKioskTransportWebRTC:
		return fmt.Errorf("port %d is reserved for private kiosk media", port)
	case public && port == types.VMDesktopPort:
		return fmt.Errorf("kiosk browser sessions must be protected")
	case public && k.MCP != nil && port == k.MCP.Port:
		return fmt.Errorf("kiosk MCP ports must be protected")
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
