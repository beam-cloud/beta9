package vm

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/ssh"
	"k8s.io/apimachinery/pkg/api/resource"
)

var validName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,23}$`)
var validEnvKey = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func normalizeDiskSize(size string) (string, error) {
	if size == "" {
		size = strconv.FormatInt(types.DefaultVMRootSizeBytes, 10)
	}
	quantity, err := resource.ParseQuantity(strings.TrimSuffix(size, "B"))
	if err != nil {
		return "", fmt.Errorf("invalid disk size %q", size)
	}
	bytes, exact := quantity.AsInt64()
	if !exact || bytes < 1<<30 {
		return "", fmt.Errorf("disk size must be an integer number of bytes, at least 1 GiB")
	}
	return strconv.FormatInt(bytes, 10), nil
}

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
	var err error
	spec.DiskSize, err = normalizeDiskSize(spec.DiskSize)
	if err != nil {
		return err
	}
	if spec.CPU < 100 || spec.Memory < 256 || spec.IdleTimeout < 0 || spec.IdleTimeout > 365*24*60*60 {
		return fmt.Errorf("CPU must be at least 0.1, memory at least 256 MiB, and idle timeout nonnegative")
	}
	if spec.BlockNetwork && len(spec.AllowList) > 0 {
		return fmt.Errorf("block_network and allow_list cannot both be set")
	}
	if spec.IdleAction != "" && spec.IdleAction != "stop" && spec.IdleAction != "pause" {
		return fmt.Errorf("idle_action must be stop or pause")
	}
	if err := common.ValidateAllowList(spec.AllowList); err != nil {
		return err
	}
	seenMounts := map[string]bool{"/": true}
	seenDisks := map[string]bool{}
	for _, disk := range spec.Disks {
		if disk == nil || !validName.MatchString(disk.Name) || strings.HasPrefix(disk.Name, "vm-") || seenDisks[disk.Name] {
			return fmt.Errorf("additional disks require unique names; the vm- prefix is reserved for VM roots")
		}
		if disk.Driver != "" && disk.Driver != "qcow" {
			return fmt.Errorf("microVM disks require the qcow driver")
		}
		if disk.Filesystem != "" && disk.Filesystem != "ext4" {
			return fmt.Errorf("microVM disks require ext4")
		}
		disk.Driver, disk.Filesystem = "qcow", "ext4"
		disk.Size, err = normalizeDiskSize(disk.Size)
		if err != nil {
			return err
		}
		if err := validateMountPath(disk.MountPath, seenMounts); err != nil {
			return err
		}
		seenDisks[disk.Name] = true
	}
	for _, volume := range spec.Volumes {
		if err := abstractions.ValidateVolume(volume); err != nil {
			return err
		}
		if err := validateMountPath(volume.MountPath, seenMounts); err != nil {
			return err
		}
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
		if port == 0 || port > 65535 || port == uint32(types.WorkerSandboxProcessManagerPort) || (port == 2222 && !spec.SSH) {
			return fmt.Errorf("invalid port %d", port)
		}
		if !seen[port] {
			spec.Ports = append(spec.Ports, port)
			seen[port] = true
		}
	}
	for _, port := range spec.PrivatePorts {
		if port == 0 || port > 65535 || port == 2222 || port == uint32(types.WorkerSandboxProcessManagerPort) {
			return fmt.Errorf("invalid private port %d", port)
		}
	}
	for _, port := range spec.ProtectedPorts {
		if port == 2222 || !slices.Contains(spec.Ports, port) {
			return fmt.Errorf("protected port %d must be published", port)
		}
	}
	return nil
}

func validateMountPath(mount string, seen map[string]bool) error {
	if !path.IsAbs(mount) || path.Clean(mount) != mount || seen[mount] {
		return fmt.Errorf("VM mount paths must be unique absolute paths outside the root")
	}
	for other := range seen {
		if other != "/" && (strings.HasPrefix(mount, other+"/") || strings.HasPrefix(other, mount+"/")) {
			return fmt.Errorf("VM mount paths cannot overlap: %s and %s", mount, other)
		}
	}
	for _, reserved := range []string{"/dev", "/proc", "/sys", "/run", "/.beam"} {
		if mount == reserved || strings.HasPrefix(mount, reserved+"/") {
			return fmt.Errorf("reserved VM mount path %q", mount)
		}
	}
	seen[mount] = true
	return nil
}

func validateMetadata(metadata map[string]string) error {
	if len(metadata) > 64 {
		return fmt.Errorf("metadata supports at most 64 entries")
	}
	size := 0
	for key, value := range metadata {
		if len(key) == 0 || len(key) > 128 || len(value) > 1024 || strings.ContainsRune(key+value, 0) {
			return fmt.Errorf("metadata keys must be 1–128 bytes and values at most 1024 bytes, without NUL")
		}
		size += len(key) + len(value)
	}
	if size > 16384 {
		return fmt.Errorf("metadata must fit in 16 KiB")
	}
	return nil
}

type updateRequest struct {
	IdleTimeout  *int64             `json:"idle_timeout,omitempty"`
	IdleAction   *string            `json:"idle_action,omitempty"`
	AutoResume   *bool              `json:"auto_resume,omitempty"`
	Metadata     *map[string]string `json:"metadata,omitempty"`
	BlockNetwork *bool              `json:"block_network,omitempty"`
	AllowList    *[]string          `json:"allow_list,omitempty"`
}

func (s *Service) update(c echo.Context) error {
	ctx, info := requestContext(c)
	var req updateRequest
	if err := decode(c, &req); err != nil {
		return err
	}
	if req.Metadata != nil {
		if err := validateMetadata(*req.Metadata); err != nil {
			return apiError(err)
		}
	}
	v, unlock, err := s.lockedVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	defer unlock()
	if v.DesiredState == "deleted" {
		return echo.NewHTTPError(404, "VM removed")
	}
	oldSpec := v.Spec
	if req.IdleTimeout != nil {
		v.Spec.IdleTimeout = *req.IdleTimeout
	}
	if req.IdleAction != nil {
		v.Spec.IdleAction = *req.IdleAction
	}
	if req.AutoResume != nil {
		v.Spec.AutoResume = *req.AutoResume
	}
	if req.BlockNetwork != nil {
		v.Spec.BlockNetwork = *req.BlockNetwork
	}
	if req.AllowList != nil {
		v.Spec.AllowList = *req.AllowList
	}
	if err := validate(&v.Spec); err != nil {
		return apiError(err)
	}
	if req.Metadata != nil {
		v.Metadata = *req.Metadata
	}
	// Apply through the existing worker firewall, and carry this same policy
	// on every cold boot. Updating a stopping guest would race disk shutdown.
	if (req.BlockNetwork != nil || req.AllowList != nil) && v.ContainerID != "" {
		if v.Status != "running" {
			return echo.NewHTTPError(409, "wait for the VM to finish starting or stopping before updating its network")
		}
		if err := s.applyNetwork(ctx, v, v.Spec); err != nil {
			return apiError(err)
		}
	}
	if err := s.repo.SaveVM(ctx, v); err != nil {
		if (req.BlockNetwork != nil || req.AllowList != nil) && v.ContainerID != "" {
			rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = s.applyNetwork(rollback, v, oldSpec)
		}
		return apiError(err)
	}
	// A new idle timeout starts at the update, rather than unexpectedly
	// stopping a VM immediately because it was previously inactive.
	if req.IdleTimeout != nil {
		if err := s.repo.TouchVM(ctx, v.ID); err != nil {
			return apiError(err)
		}
	}
	return c.JSON(200, s.response(v))
}

func (s *Service) applyNetwork(ctx context.Context, v *types.VM, spec types.VMSpec) error {
	resp, err := s.runtime.SandboxUpdateNetworkPermissions(ctx, &pb.PodSandboxUpdateNetworkPermissionsRequest{ContainerId: v.ContainerID, StubId: v.StubID, BlockNetwork: spec.BlockNetwork, AllowList: spec.AllowList})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("update network permissions: %s", resp.ErrorMsg)
	}
	return nil
}

// Shared by explicit start and access-triggered cold boots. Preserve the stop
// barrier and reset launch retries only when a caller asks to activate the VM.
func (s *Service) activate(ctx context.Context, info *auth.AuthInfo, v *types.VM) error {
	if v.DesiredState == "paused" && v.MemoryCheckpointID == "" {
		return echo.NewHTTPError(409, "memory pause did not complete; use start with cold=true to recover from disk")
	}
	if v.MemoryCheckpointID != "" && v.TokenID != info.Token.ExternalId {
		return echo.NewHTTPError(409, "memory resume requires the original owner credential; use cold=true with a new credential")
	}
	if (v.DesiredState == "stopped" || v.DesiredState == "paused") && (v.ContainerID != "" || v.StopSnapshotID != "") {
		if err := s.stop(ctx, v, false); err != nil {
			return err
		}
	}
	if err := s.validateMemoryDisks(ctx, v); err != nil {
		return err
	}
	v.DesiredState, v.Error, v.TokenID = "running", "", info.Token.ExternalId
	v.LaunchAttempts = 0
	if err := s.repo.TouchVM(ctx, v.ID); err != nil {
		return err
	}
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	return s.start(ctx, info, v)
}

func (s *Service) accessOwner(ctx context.Context, v *types.VM) (*auth.AuthInfo, error) {
	token, tokenErr := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
	workspace, workspaceErr := s.backend.GetWorkspaceByExternalId(ctx, v.WorkspaceExternalID)
	if tokenErr != nil || workspaceErr != nil {
		return nil, echo.NewHTTPError(503, "VM authorization unavailable")
	}
	if token == nil || !token.Active || token.DisabledByClusterAdmin {
		return nil, echo.NewHTTPError(403, "VM access is revoked")
	}
	return &auth.AuthInfo{Workspace: &workspace, Token: token}, nil
}

// One attempt owns the lifecycle lock; the outer loop owns its retry deadline.
func (s *Service) resumeAccessAttempt(ctx context.Context, record *types.VM, port uint32) (*types.VM, bool, error) {
	v, unlock, err := s.lockedVM(ctx, record.WorkspaceID, record.ID)
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	if v.DesiredState == "deleted" || !v.Spec.AutoResume || !slices.Contains(v.Spec.Ports, port) {
		return nil, false, echo.NewHTTPError(409, "VM access changed while resuming")
	}
	info, err := s.accessOwner(ctx, v)
	if err != nil {
		return nil, false, err
	}
	ctx = auth.ContextWithAuthInfo(ctx, info)
	if v.DesiredState != "running" {
		if err := s.activate(ctx, info, v); err != nil {
			return nil, false, echo.NewHTTPError(503, "unable to resume VM: "+err.Error())
		}
	}
	state, err := s.runtimeState(v)
	if err != nil {
		return nil, false, echo.NewHTTPError(503, "unable to resume VM: "+err.Error())
	}
	if state == nil || state.Status != types.ContainerStatusRunning {
		return v, false, nil
	}
	v.Status = "running"
	// Probe the forwarding route before sending the original request once.
	ready, err := s.runtime.VMPortReady(ctx, v.StubID, v.ContainerID, port)
	if err != nil {
		return nil, false, echo.NewHTTPError(503, "VM readiness unavailable: "+err.Error())
	}
	return v, ready, nil
}

func (s *Service) wakeForAccess(parent context.Context, record *types.VM, port uint32) (*types.VM, error) {
	ctx, cancel := context.WithTimeout(parent, 180*time.Second)
	defer cancel()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		v, ready, err := s.resumeAccessAttempt(ctx, record, port)
		if err != nil && !strings.Contains(err.Error(), "operation already in progress") {
			return nil, apiError(err)
		}
		if ready {
			return v, nil
		}
		select {
		case <-ctx.Done():
			return nil, echo.NewHTTPError(503, "VM did not become ready before the resume deadline")
		case <-tick.C:
		}
	}
}
