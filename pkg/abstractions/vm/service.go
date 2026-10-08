// Package vm owns persistent CPU VM identities and reuses the sandbox scheduler
// and durable disk driver for each cold boot.
package vm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	"golang.org/x/crypto/ssh"
	"io"
	"k8s.io/apimachinery/pkg/api/resource"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Runtime interface {
	pb.PodServiceServer
	RunVM(context.Context, *auth.AuthInfo, string, string, []uint32) error
	ForwardVM(echo.Context, string, string) error
	TunnelVM(echo.Context, string, uint32) error
}

type Gateway interface {
	GetOrCreateStub(context.Context, *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error)
	StopContainer(context.Context, *pb.StopContainerRequest) (*pb.StopContainerResponse, error)
}

type Service struct {
	repo        repository.VMRepository
	backend     repository.BackendRepository
	containers  repository.ContainerRepository
	runtime     Runtime
	gateway     Gateway
	domain      string
	baseURL     string
	defaultPool string
}

func New(ctx context.Context, config types.VMConfig, backend repository.BackendRepository, containers repository.ContainerRepository, runtime Runtime, gateway Gateway, api *echo.Group, server *echo.Echo) error {
	repo, ok := backend.(repository.VMRepository)
	if !ok {
		return fmt.Errorf("backend does not support persistent VMs")
	}
	s := &Service{repo: repo, backend: backend, containers: containers, runtime: runtime, gateway: gateway, domain: config.Domain, baseURL: strings.TrimSuffix(config.BaseURL, "/"), defaultPool: config.DefaultPool}
	api.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(s.list))
	api.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(s.create))
	api.GET("/:workspaceId/artifacts/:kind", auth.WithStrictWorkspaceAuth(s.artifacts))
	api.DELETE("/:workspaceId/artifacts/:kind/:artifact", auth.WithStrictWorkspaceAuth(s.removeArtifact))
	api.GET("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.get))
	api.GET("/:workspaceId/:name/tunnel/:port", auth.WithStrictWorkspaceAuth(s.tunnel))
	api.POST("/:workspaceId/:name/:action", auth.WithStrictWorkspaceAuth(s.action))
	api.DELETE("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.remove))
	server.Any("/vm/:handle/:port", s.proxy)
	server.Any("/vm/:handle/:port/*", s.proxy)
	if s.domain != "" {
		server.Pre(s.hostRoute)
	}
	go s.reconcile(ctx)
	return nil
}

var validName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,23}$`)
var validEnvKey = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validate(spec *types.VMSpec) error {
	if spec.CPU == 0 {
		spec.CPU = 1000
		if spec.Desktop {
			spec.CPU = 2000
		}
	}
	if spec.Memory == 0 {
		spec.Memory = 1024
		if spec.Desktop {
			spec.Memory = 2048
		}
	}
	if spec.DiskSize == "" {
		spec.DiskSize = "50GiB"
	}
	// The qcow driver takes integer bytes or Kubernetes units (Gi, not GiB).
	quantity, err := resource.ParseQuantity(strings.TrimSuffix(spec.DiskSize, "B"))
	if err != nil {
		return fmt.Errorf("invalid disk size %q", spec.DiskSize)
	}
	bytes, exact := quantity.AsInt64()
	if !exact || bytes < 1<<30 {
		return fmt.Errorf("disk size must be an integer number of bytes, at least 1 GiB")
	}
	spec.DiskSize = strconv.FormatInt(bytes, 10)
	if spec.CPU < 100 || spec.Memory < 256 || spec.IdleTimeout < 0 {
		return fmt.Errorf("CPU must be at least 0.1, memory at least 256 MiB, and idle timeout nonnegative")
	}
	if spec.ImageID == "" {
		return fmt.Errorf("image_id is required; the image must contain systemd and the Beam VM services")
	}
	if spec.Desktop && spec.Memory < 2048 {
		return fmt.Errorf("desktop requires at least 2048 MiB")
	}
	for _, env := range spec.Env {
		key, _, ok := strings.Cut(env, "=")
		if !ok || !validEnvKey.MatchString(key) || strings.HasPrefix(key, "BEAM_VM_") || strings.HasPrefix(key, "BETA9_") || strings.ContainsRune(env, 0) {
			return fmt.Errorf("invalid or reserved environment key %q", key)
		}
	}
	for _, name := range spec.Secrets {
		if strings.HasPrefix(name, "BEAM_VM_") || strings.HasPrefix(name, "BETA9_") {
			return fmt.Errorf("reserved secret name %q", name)
		}
	}
	if spec.SSH {
		_, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(spec.SSHPublicKey))
		if err != nil || len(options) != 0 || len(rest) != 0 || strings.ContainsAny(spec.SSHPublicKey, "\r\n") {
			return fmt.Errorf("SSH requires one valid public key without authorized_keys options")
		}
	}
	seen := map[uint32]bool{}
	ports := []uint32{7681}
	if spec.Desktop {
		ports = append(ports, 8080)
	}
	if spec.SSH {
		ports = append(ports, 2222)
	}
	ports = append(ports, spec.Ports...)
	spec.Ports = nil
	for _, port := range ports {
		if port == 0 || port > 65535 || (port == 2222 && !spec.SSH) {
			return fmt.Errorf("invalid port %d", port)
		}
		if !seen[port] {
			spec.Ports = append(spec.Ports, port)
			seen[port] = true
		}
	}
	for _, port := range spec.PrivatePorts {
		if port == 0 || port > 65535 || port == 2222 {
			return fmt.Errorf("invalid private port %d", port)
		}
	}
	return nil
}

// Bound request bodies, reject misspelled/GPU fields, and reject trailing JSON.
func decode(c echo.Context, dst any) error {
	d := json.NewDecoder(io.LimitReader(c.Request().Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return echo.NewHTTPError(400, err.Error())
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return echo.NewHTTPError(400, "expected one JSON object")
	}
	return nil
}

func requestContext(c echo.Context) (context.Context, *auth.AuthInfo) {
	info := c.(*auth.HttpAuthContext).AuthInfo
	return auth.ContextWithAuthInfo(c.Request().Context(), info), info
}

func apiError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(404, "VM not found")
	}
	var pg *pq.Error
	if errors.As(err, &pg) && pg.Code == "23505" {
		return echo.NewHTTPError(409, "name already exists")
	}
	if strings.Contains(err.Error(), "operation already in progress") {
		return echo.NewHTTPError(409, err.Error())
	}
	return echo.NewHTTPError(400, err.Error())
}

func (s *Service) urls(v *types.VM) {
	v.URLs = map[uint32]string{}
	for _, port := range v.Spec.Ports {
		if port == 2222 {
			continue
		}
		if s.domain != "" {
			scheme := "https"
			if strings.HasSuffix(strings.Split(s.domain, ":")[0], ".localhost") || strings.Split(s.domain, ":")[0] == "localhost" {
				scheme = "http"
			}
			v.URLs[port] = fmt.Sprintf("%s://%s-%d.%s/", scheme, v.Handle, port, s.domain)
		} else {
			v.URLs[port] = fmt.Sprintf("%s/vm/%s/%d/", s.baseURL, v.Handle, port)
		}
	}
	v.TerminalURL = v.URLs[7681]
	if v.Spec.Desktop {
		v.DesktopURL = v.URLs[8080]
	}
}

// Keep the complete launch spec in storage, but never return environment
// values in management responses or template listings.
func (s *Service) response(v *types.VM) types.VM {
	copy := *v
	copy.Spec.Env = nil
	s.urls(&copy)
	return copy
}

func artifactResponse(a types.VMArtifact) types.VMArtifact {
	a.Spec.Env = nil
	return a
}

func (s *Service) lockedVM(ctx context.Context, workspace uint, name string) (*types.VM, func(), error) {
	v, err := s.repo.GetVM(ctx, workspace, name)
	if err != nil {
		return nil, nil, err
	}
	unlock, err := s.repo.LockVM(ctx, v.ID)
	if err != nil {
		return nil, nil, err
	}
	v, err = s.repo.GetVM(ctx, workspace, v.ID)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return v, unlock, nil
}

func (s *Service) list(c echo.Context) error {
	ctx, info := requestContext(c)
	vms, err := s.repo.ListVMs(ctx, info.Workspace.Id)
	if err != nil {
		return apiError(err)
	}
	result := []types.VM{}
	for _, v := range vms {
		if v.DesiredState == "deleted" && c.QueryParam("all") != "true" {
			continue
		}
		result = append(result, s.response(v))
	}
	return c.JSON(200, result)
}

func (s *Service) get(c echo.Context) error {
	ctx, info := requestContext(c)
	v, err := s.repo.GetVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	if v.DesiredState == "deleted" {
		return echo.NewHTTPError(404, "VM removed")
	}
	return c.JSON(200, s.response(v))
}

type createRequest struct {
	Name     string          `json:"name"`
	Spec     json.RawMessage `json:"spec"`
	Template string          `json:"template,omitempty"`
	Snapshot string          `json:"snapshot,omitempty"`
}

func (s *Service) create(c echo.Context) error {
	ctx, info := requestContext(c)
	var req createRequest
	if err := decode(c, &req); err != nil {
		return err
	}
	if req.Name == "" {
		req.Name = "vm-" + uuid.NewString()[:8]
	}
	if !validName.MatchString(req.Name) {
		return echo.NewHTTPError(400, "name must begin with a letter and contain at most 24 lowercase letters, digits or hyphens")
	}
	if req.Template != "" && req.Snapshot != "" {
		return echo.NewHTTPError(400, "choose a template or snapshot")
	}
	seed, kind := req.Template, "template"
	if req.Snapshot != "" {
		seed, kind = req.Snapshot, "snapshot"
	}
	if seed != "" {
		items, err := s.repo.ListVMArtifacts(ctx, info.Workspace.Id, kind)
		if err != nil {
			return apiError(err)
		}
		found := false
		for _, a := range items {
			if a.ID == seed || a.Name == seed {
				base, _ := json.Marshal(a.Spec)
				var fields, overrides map[string]json.RawMessage
				if err := json.Unmarshal(base, &fields); err != nil {
					return apiError(err)
				}
				if len(req.Spec) > 0 {
					if err := json.Unmarshal(req.Spec, &overrides); err != nil {
						return apiError(err)
					}
				}
				for key, value := range overrides {
					fields[key] = value
				}
				fields["source_snapshot_id"], _ = json.Marshal(a.RootSnapshotID)
				req.Spec, _ = json.Marshal(fields)
				found = true
				break
			}
		}
		if !found {
			return echo.NewHTTPError(404, kind+" not found")
		}
	}
	var spec types.VMSpec
	d := json.NewDecoder(strings.NewReader(string(req.Spec)))
	d.DisallowUnknownFields()
	if err := d.Decode(&spec); err != nil {
		return apiError(err)
	}
	if spec.SourceSnapshotID != "" && seed == "" {
		return echo.NewHTTPError(400, "use a VM template or fork to initialize a root disk")
	}
	if err := validate(&spec); err != nil {
		return apiError(err)
	}
	v, err := s.createVM(ctx, info, req.Name, spec)
	if err != nil {
		return apiError(err)
	}
	return c.JSON(201, s.response(v))
}

func (s *Service) createVM(ctx context.Context, info *auth.AuthInfo, name string, spec types.VMSpec) (*types.VM, error) {
	if spec.Pool == "" {
		spec.Pool = s.defaultPool
	}
	now := time.Now().UTC()
	id := uuid.NewString()
	v := &types.VM{ID: id, WorkspaceID: info.Workspace.Id, WorkspaceExternalID: info.Workspace.ExternalId, TokenID: info.Token.ExternalId, Name: name, Handle: name + "-" + strings.ReplaceAll(uuid.NewString(), "-", ""), Spec: spec, DesiredState: "running", Status: "starting", CreatedAt: now, UpdatedAt: now, LastActiveAt: now}
	unlock, err := s.repo.LockVM(ctx, id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	// Hold the lifecycle lock before publishing the row to the reconciler.
	if err := s.repo.CreateVM(ctx, v); err != nil {
		return nil, err
	}
	if err := s.start(ctx, info, v); err != nil {
		s.failed(ctx, v, err)
		return v, nil
	}
	return v, nil
}

type actionRequest struct {
	Name         string `json:"name,omitempty"`
	NoSnapshot   bool   `json:"no_snapshot,omitempty"`
	Port         uint32 `json:"port,omitempty"`
	SSHPublicKey string `json:"ssh_public_key,omitempty"`
	Description  string `json:"description,omitempty"`
}

func (s *Service) action(c echo.Context) error {
	ctx, info := requestContext(c)
	var req actionRequest
	if err := decode(c, &req); err != nil {
		return err
	}
	v, unlock, err := s.lockedVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	defer unlock()
	if v.DesiredState == "deleted" {
		return echo.NewHTTPError(404, "VM removed")
	}
	v.TokenID = info.Token.ExternalId
	switch c.Param("action") {
	case "start", "resume":
		// Finish an interrupted stop before creating a replacement runtime.
		if v.DesiredState == "stopped" && (v.ContainerID != "" || v.StopSnapshotID != "") {
			if err := s.stop(ctx, v, false); err != nil {
				return apiError(err)
			}
		}
		v.DesiredState = "running"
		v.LaunchAttempts = 0
		v.Error = ""
		if err := s.repo.TouchVM(ctx, v.ID); err != nil {
			return apiError(err)
		}
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return apiError(err)
		}
		err = s.start(ctx, info, v)
	case "stop":
		v.DesiredState = "stopped"
		if !req.NoSnapshot && v.StopSnapshotID == "" && v.ContainerID != "" {
			v.StopSnapshotID = uuid.NewString()
		}
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return apiError(err)
		}
		err = s.stop(ctx, v, !req.NoSnapshot)
	case "snapshot", "template":
		kind := c.Param("action")
		a, err := s.capture(ctx, v, kind, req.Name, req.Description)
		if err != nil {
			return apiError(err)
		}
		return c.JSON(201, artifactResponse(*a))
	case "fork":
		if req.Name == "" {
			req.Name = "vm-" + uuid.NewString()[:8]
		}
		if !validName.MatchString(req.Name) {
			return echo.NewHTTPError(400, "valid fork name required")
		}
		if err := s.snapshotRoot(ctx, v); err != nil {
			return apiError(err)
		}
		spec := v.Spec
		spec.SourceSnapshotID = v.RootSnapshotID
		spec.SSHPublicKey = req.SSHPublicKey
		if err := validate(&spec); err != nil {
			return apiError(err)
		}
		child, err := s.createVM(ctx, info, req.Name, spec)
		if err != nil {
			return apiError(err)
		}
		return c.JSON(201, s.response(child))
	case "expose", "unexpose", "bind":
		if req.Port == 0 || req.Port > 65535 || req.Port == 2222 || req.Port == 7681 || (req.Port == 8080 && v.Spec.Desktop) {
			return echo.NewHTTPError(400, "invalid or reserved port")
		}
		if v.ContainerID != "" && c.Param("action") != "unexpose" {
			resp, err := s.runtime.SandboxExposePort(ctx, &pb.PodSandboxExposePortRequest{ContainerId: v.ContainerID, StubId: v.StubID, Port: int32(req.Port)})
			if err != nil {
				return apiError(err)
			}
			if !resp.Ok {
				return echo.NewHTTPError(400, "unable to "+c.Param("action")+" port")
			}
		}
		if c.Param("action") == "bind" {
			if !slices.Contains(v.Spec.PrivatePorts, req.Port) {
				v.Spec.PrivatePorts = append(v.Spec.PrivatePorts, req.Port)
			}
		} else {
			ports := slices.DeleteFunc(slices.Clone(v.Spec.Ports), func(port uint32) bool { return port == req.Port })
			if c.Param("action") == "expose" {
				ports = append(ports, req.Port)
			}
			v.Spec.Ports = ports
		}
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return apiError(err)
		}
	case "touch":
		if err := s.repo.TouchVM(ctx, v.ID); err != nil {
			return apiError(err)
		}
	default:
		return echo.NewHTTPError(404, "unknown VM action")
	}
	if err != nil {
		s.failed(ctx, v, err)
		return apiError(err)
	}
	return c.JSON(200, s.response(v))
}

func (s *Service) remove(c echo.Context) error {
	ctx, info := requestContext(c)
	v, unlock, err := s.lockedVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	defer unlock()
	v.DesiredState = "deleted"
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return apiError(err)
	}
	if err := s.stopOrDelete(ctx, v); err != nil {
		return apiError(err)
	}
	return c.NoContent(204)
}

func (s *Service) artifacts(c echo.Context) error {
	ctx, info := requestContext(c)
	kind := c.Param("kind")
	if kind != "snapshot" && kind != "template" {
		return echo.NewHTTPError(400, "invalid artifact kind")
	}
	a, err := s.repo.ListVMArtifacts(ctx, info.Workspace.Id, kind)
	if err != nil {
		return apiError(err)
	}
	for i := range a {
		a[i] = artifactResponse(a[i])
	}
	return c.JSON(200, a)
}

func (s *Service) removeArtifact(c echo.Context) error {
	ctx, info := requestContext(c)
	if c.Param("kind") != "template" && c.Param("kind") != "snapshot" {
		return echo.NewHTTPError(400, "invalid artifact kind")
	}
	if err := s.repo.DeleteVMArtifact(ctx, info.Workspace.Id, c.Param("kind"), c.Param("artifact")); err != nil {
		return apiError(err)
	}
	return c.NoContent(204)
}

func (s *Service) hostRoute(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		host := strings.ToLower(c.Request().Host)
		domain := strings.ToLower(s.domain)
		if strings.HasSuffix(host, "."+domain) {
			label := strings.TrimSuffix(host, "."+domain)
			handle, port, ok := splitHost(label)
			if ok {
				// Route through Echo normally so host-based access retains
				// the gateway's recovery, tracing and other middleware.
				u := c.Request().URL
				path, rawPath := u.Path, u.RawPath
				prefix := "/vm/" + handle + "/" + port
				u.Path = prefix + "/" + strings.TrimPrefix(path, "/")
				if rawPath != "" {
					u.RawPath = prefix + "/" + strings.TrimPrefix(rawPath, "/")
				}
				defer func() { u.Path, u.RawPath = path, rawPath }()
			}
		}
		return next(c)
	}
}

func splitHost(label string) (string, string, bool) {
	i := strings.LastIndex(label, "-")
	if i < 0 {
		return "", "", false
	}
	p, err := strconv.Atoi(label[i+1:])
	return label[:i], label[i+1:], err == nil && p > 0 && p <= 65535
}

func (s *Service) proxy(c echo.Context) error {
	ctx := c.Request().Context()
	v, err := s.repo.GetVMByHandle(ctx, c.Param("handle"))
	if err != nil || v.DesiredState == "deleted" {
		return echo.NewHTTPError(404, "VM not found")
	}
	port, err := strconv.Atoi(c.Param("port"))
	if err != nil || port == 2222 {
		return echo.NewHTTPError(404)
	}
	if !slices.Contains(v.Spec.Ports, uint32(port)) {
		return echo.NewHTTPError(404)
	}
	token, err := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(503, "VM authorization unavailable")
	}
	if token == nil || !token.Active || token.DisabledByClusterAdmin {
		return echo.NewHTTPError(403, "VM access is revoked")
	}
	release, err := s.keepActive(ctx, v.ID)
	if err != nil {
		return echo.NewHTTPError(503, "VM activity unavailable")
	}
	defer release()
	// Capability URLs contain only the VM's random identity, never a workspace
	// token. Authenticated management and raw SSH retain workspace auth.
	if v.Status != "running" {
		return echo.NewHTTPError(503, "VM is not running; start it to use this URL")
	}
	// Hold activity while a desktop/terminal websocket is open, even when the
	// user is watching without sending input.
	subPath := c.Param("*")
	c.SetParamNames("port", "subPath")
	c.SetParamValues(strconv.Itoa(port), subPath)
	return s.runtime.ForwardVM(c, v.StubID, v.ContainerID)
}

func (s *Service) tunnel(c echo.Context) error {
	ctx, info := requestContext(c)
	v, err := s.repo.GetVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	port, err := strconv.Atoi(c.Param("port"))
	if err != nil || port < 1 || port > 65535 {
		return echo.NewHTTPError(400, "invalid port")
	}
	if port == 2222 && !v.Spec.SSH {
		return echo.NewHTTPError(400, "SSH is disabled")
	}
	if v.DesiredState != "running" || v.Status != "running" {
		return echo.NewHTTPError(409, "VM is not running")
	}
	if !slices.Contains(v.Spec.RuntimePorts(), uint32(port)) {
		return echo.NewHTTPError(400, "expose the port before opening a tunnel")
	}
	release, err := s.keepActive(ctx, v.ID)
	if err != nil {
		return apiError(err)
	}
	defer release()
	return s.runtime.TunnelVM(c, v.ContainerID, uint32(port))
}

func (s *Service) keepActive(ctx context.Context, id string) (func(), error) {
	if err := s.repo.TouchVM(ctx, id); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				_ = s.repo.TouchVM(ctx, id)
			}
		}
	}()
	return func() { close(done) }, nil
}
