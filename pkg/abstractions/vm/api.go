package vm

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	"google.golang.org/protobuf/proto"
)

// Bound request bodies, reject misspelled/GPU fields, and reject trailing JSON.
func decode(c echo.Context, dst any) error {
	d := json.NewDecoder(io.LimitReader(c.Request().Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if err := d.Decode(new(any)); err != io.EOF {
		return echo.NewHTTPError(http.StatusBadRequest, "expected one JSON object")
	}

	return nil
}

func requestContext(c echo.Context) (context.Context, *auth.AuthInfo) {
	info := c.(*auth.HttpAuthContext).AuthInfo
	return auth.ContextWithAuthInfo(c.Request().Context(), info), info
}

func apiError(err error) error {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) {
		return httpErr
	}

	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusNotFound, "VM not found")
	}

	var pg *pq.Error
	if errors.As(err, &pg) && pg.Code == "23505" {
		return echo.NewHTTPError(http.StatusConflict, "name already exists")
	}

	if strings.Contains(err.Error(), "operation already in progress") {
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	}

	return echo.NewHTTPError(http.StatusBadRequest, err.Error())
}

// Keep the complete launch spec in storage, but never return environment
// values in management responses or template listings.
func (s *Service) response(v *types.VM) types.VM {
	copy := *v
	copy.Spec = redactedSpec(v.Spec)
	copy.CreationDigest = ""
	copy.TrafficAccessToken = ""
	s.urls(&copy)
	return copy
}

// Blocking launch responses verify the same process-manager readiness as
// SandboxConnect, saving a client round trip without persisting lifecycle state.
func (s *Service) launchResponse(c echo.Context, status int, v *types.VM) error {
	response := s.response(v)
	if c.QueryParam("wait") != "exec" || v.DesiredState != "running" || v.ContainerID == "" || v.Status == "error" {
		return c.JSON(status, response)
	}

	ctx, _ := requestContext(c)
	ready, err := s.runtime.SandboxConnect(ctx, &pb.PodSandboxConnectRequest{ContainerId: v.ContainerID})
	if err != nil {
		return apiError(err)
	}

	if ready == nil || !ready.Ok {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "VM process manager is not ready")
	}

	response.Status = "running"
	return c.JSON(status, struct {
		types.VM
		ExecReady bool `json:"exec_ready"`
	}{response, true})
}

func artifactResponse(a types.VMArtifact) types.VMArtifact {
	a.Spec = redactedSpec(a.Spec)
	return a
}

func redactedSpec(spec types.VMSpec) types.VMSpec {
	spec.Env = nil
	volumes := spec.Volumes
	spec.Volumes = make([]*pb.Volume, len(volumes))
	for i, volume := range volumes {
		if volume == nil {
			continue
		}

		copy := proto.Clone(volume).(*pb.Volume)
		if copy.Config != nil {
			config := types.NewMountPointConfigFromProto(copy.Config).WithoutCredentials()
			copy.Config = config.ToProto()
		}

		spec.Volumes[i] = copy
	}

	return spec
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
	var metadata map[string]string
	if filter := c.QueryParam("metadata"); filter != "" {
		if err := json.Unmarshal([]byte(filter), &metadata); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "metadata filter must be a JSON object of strings")
		}
	}

	for _, v := range vms {
		if v.DesiredState == "deleted" && c.QueryParam("all") != "true" {
			continue
		}

		status, err := s.launchStatus(v)
		if err != nil {
			return apiError(err)
		}

		if filter := c.QueryParam("status"); filter != "" && status != filter {
			continue
		}

		match := true
		for key, value := range metadata {
			actual, exists := v.Metadata[key]
			match = match && exists && actual == value
		}

		if !match {
			continue
		}

		response := s.response(v)
		response.Status = status
		result = append(result, response)
	}

	return c.JSON(http.StatusOK, result)
}

func (s *Service) get(c echo.Context) error {
	ctx, info := requestContext(c)
	v, err := s.repo.GetVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}

	if v.DesiredState == "deleted" {
		return echo.NewHTTPError(http.StatusNotFound, "VM removed")
	}

	response := s.response(v)
	response.Status, err = s.launchStatus(v)
	if err != nil {
		return apiError(err)
	}

	return c.JSON(http.StatusOK, response)
}

type createRequest struct {
	Name      string            `json:"name"`
	Spec      json.RawMessage   `json:"spec"`
	Template  string            `json:"template,omitempty"`
	Snapshot  string            `json:"snapshot,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

func (s *Service) create(c echo.Context) error {
	ctx, info := requestContext(c)
	var req createRequest
	if err := decode(c, &req); err != nil {
		return err
	}

	if req.RequestID != "" {
		parsed, err := uuid.Parse(req.RequestID)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "request_id must be a UUID")
		}

		req.RequestID = parsed.String()
	}

	if err := validateMetadata(req.Metadata); err != nil {
		return apiError(err)
	}

	if req.Name != "" && !validName.MatchString(req.Name) {
		return echo.NewHTTPError(http.StatusBadRequest, "name must begin with a letter and contain at most 24 lowercase letters, digits or hyphens")
	}

	if req.Template != "" && req.Snapshot != "" {
		return echo.NewHTTPError(http.StatusBadRequest, "choose a template or snapshot")
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
			return echo.NewHTTPError(http.StatusNotFound, kind+" not found")
		}
	}

	var spec types.VMSpec
	d := json.NewDecoder(strings.NewReader(string(req.Spec)))
	d.DisallowUnknownFields()
	if err := d.Decode(&spec); err != nil {
		return apiError(err)
	}

	if spec.SourceSnapshotID != "" && seed == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "use a VM template or fork to initialize a root disk")
	}

	if err := validate(&spec); err != nil {
		return apiError(err)
	}

	v, err := s.createVMWithRequest(ctx, info, req.Name, spec, req.Metadata, req.RequestID)
	if err != nil {
		return apiError(err)
	}

	return s.launchResponse(c, http.StatusCreated, v)
}

func (s *Service) createVM(ctx context.Context, info *auth.AuthInfo, name string, spec types.VMSpec) (*types.VM, error) {
	return s.createVMWithRequest(ctx, info, name, spec, nil, "")
}

func (s *Service) createVMWithRequest(ctx context.Context, info *auth.AuthInfo, name string, spec types.VMSpec, metadata map[string]string, requestID string) (*types.VM, error) {
	if spec.Pool == "" {
		spec.Pool = s.defaultPool
	}

	now := time.Now().UTC()
	id := vmID(info.Workspace.Id, requestID)
	if name == "" {
		name = defaultVMName(id)
	}

	creation, _ := json.Marshal(struct {
		Name     string
		Spec     types.VMSpec
		Metadata map[string]string
	}{name, spec, metadata})
	digest := fmt.Sprintf("%x", sha256.Sum256(creation))
	v := &types.VM{
		ID:                  id,
		Name:                name,
		Handle:              name + "-" + randomHexID(),
		WorkspaceID:         info.Workspace.Id,
		WorkspaceExternalID: info.Workspace.ExternalId,
		TokenID:             info.Token.ExternalId,
		TrafficAccessToken:  randomHexID(),
		Spec:                spec,
		Metadata:            metadata,
		CreationDigest:      digest,
		DesiredState:        "running",
		Status:              "starting",
		CreatedAt:           now,
		UpdatedAt:           now,
		LastActiveAt:        now,
	}

	unlock, err := s.repo.LockVM(ctx, id)
	if err != nil {
		return nil, err
	}

	defer unlock()
	if requestID != "" {
		existing, err := s.repo.GetVM(ctx, info.Workspace.Id, id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}

		if err == nil {
			if existing.CreationDigest != digest {
				return nil, echo.NewHTTPError(http.StatusConflict, "request_id was already used with different VM settings")
			}

			if existing.DesiredState == "deleted" {
				return nil, echo.NewHTTPError(http.StatusGone, "the VM for this request_id was removed")
			}

			return existing, nil
		}
	}

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
	Protected    *bool  `json:"protected,omitempty"`
	TTL          int64  `json:"ttl,omitempty"`
	Cold         bool   `json:"cold,omitempty"`
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

	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	if v.DesiredState == "deleted" {
		return echo.NewHTTPError(http.StatusNotFound, "VM removed")
	}

	switch c.Param("action") {
	case "start", "resume":
		if req.Cold {
			// Finish terminal checkpoint finalization before abandoning RAM.
			if v.DesiredState == "paused" && v.MemoryCheckpointID != "" {
				if err := s.stop(ctx, v, false); err != nil {
					return apiError(err)
				}
			}

			v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
			if v.DesiredState == "paused" {
				v.DesiredState = "stopped"
			}
		}

		err = s.activate(ctx, info, v)
	case "pause":
		err = s.pause(ctx, v)
	case "wake":
		if !v.Spec.AutoResume && v.DesiredState != "running" {
			return echo.NewHTTPError(http.StatusConflict, "VM is stopped; call start() or enable auto_resume")
		}

		err = s.activate(ctx, info, v)
	case "stop":
		v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
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

		return c.JSON(http.StatusCreated, artifactResponse(*a))
	case "fork":
		if req.Name != "" && !validName.MatchString(req.Name) {
			return echo.NewHTTPError(http.StatusBadRequest, "valid fork name required")
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

		return c.JSON(http.StatusCreated, s.response(child))
	case "expose", "unexpose", "bind":
		if err := s.configurePort(ctx, v, c.Param("action"), req.Port, req.Protected); err != nil {
			return apiError(err)
		}
	case "touch":
		if err := s.repo.TouchVM(ctx, v.ID); err != nil {
			return apiError(err)
		}
	case "access-session":
		return s.createAccessSession(c, v, req.Port, req.TTL)
	case "rotate-access-token":
		v.TrafficAccessToken = randomHexID()
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return apiError(err)
		}

		fallthrough
	case "access-token":
		return c.JSON(http.StatusOK, map[string]string{"token": v.TrafficAccessToken})
	default:
		return echo.NewHTTPError(http.StatusNotFound, "unknown VM action")
	}

	if err != nil {
		s.failed(ctx, v, err)
		return apiError(err)
	}

	// The operation is persisted; readiness must not hold the lifecycle lock.
	unlock()
	unlock = nil
	return s.launchResponse(c, http.StatusOK, v)
}

func (s *Service) remove(c echo.Context) error {
	ctx, info := requestContext(c)
	v, unlock, err := s.lockedVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}

	defer unlock()
	v.DesiredState = "deleted"
	v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return apiError(err)
	}

	if err := s.stopOrDelete(ctx, v); err != nil {
		return apiError(err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (s *Service) artifacts(c echo.Context) error {
	ctx, info := requestContext(c)
	kind := c.Param("kind")
	if kind != "snapshot" && kind != "template" {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid artifact kind")
	}

	a, err := s.repo.ListVMArtifacts(ctx, info.Workspace.Id, kind)
	if err != nil {
		return apiError(err)
	}

	for i := range a {
		a[i] = artifactResponse(a[i])
	}

	return c.JSON(http.StatusOK, a)
}

func (s *Service) removeArtifact(c echo.Context) error {
	ctx, info := requestContext(c)
	if c.Param("kind") != "template" && c.Param("kind") != "snapshot" {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid artifact kind")
	}

	if err := s.repo.DeleteVMArtifact(ctx, info.Workspace.Id, c.Param("kind"), c.Param("artifact")); err != nil {
		return apiError(err)
	}

	return c.NoContent(http.StatusNoContent)
}
