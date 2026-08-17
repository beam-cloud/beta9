package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/resource"
)

const stateVolumeHostRoot = "/var/lib/beta9/state-volumes"

func stateVolumeSizeBytes(size string) (int64, error) {
	quantity, err := resource.ParseQuantity(strings.TrimSpace(size))
	if err != nil {
		return 0, fmt.Errorf("parse state volume size %q: %w", size, err)
	}
	bytes := quantity.Value()
	if bytes <= 0 {
		return 0, fmt.Errorf("state volume size %q must be positive", size)
	}
	return bytes, nil
}

func uninitializedStateVolumeSpec(containerID, volumeID, name, containerMountPath, size string, root, readOnly, containerScoped bool) (StateVolumeSpec, error) {
	sizeBytes, err := stateVolumeSizeBytes(size)
	if err != nil {
		return StateVolumeSpec{}, err
	}
	containerToken := stateVolumeToken("container-", containerID)
	volumeToken := stateVolumeToken("volume-", volumeID)
	backingDir := filepath.Join(stateVolumeHostRoot, "volumes", volumeToken, "graph")
	if containerScoped {
		backingDir = filepath.Join(stateVolumeHostRoot, "containers", containerToken, "volumes", volumeToken)
	}
	// Initialization is exclusive: an unowned layer is never reformatted. A
	// persisted journal or explicit generation restore must adopt existing data.
	basePath := filepath.Join(backingDir, "base.qcow2")
	if _, err := os.Lstat(basePath); err == nil {
		return StateVolumeSpec{}, fmt.Errorf("state volume %q already exists and must be recovered from its journal", volumeID)
	} else if !os.IsNotExist(err) {
		return StateVolumeSpec{}, fmt.Errorf("inspect state volume %q: %w", volumeID, err)
	}
	return StateVolumeSpec{
		ID: volumeID, Name: name, ContainerMountPath: containerMountPath, Root: root, ReadOnly: readOnly,
		BackingDir: backingDir, MountPath: filepath.Join(stateVolumeHostRoot, "mounts", containerToken, volumeToken),
		SizeBytes: sizeBytes, Format: true,
	}, nil
}

func (h *StateVolumeGroupHandle) PersistentOverlayPaths() (upper, work string, ok bool) {
	if h == nil || h.RootVolumeID == "" {
		return "", "", false
	}
	root := h.MountPaths[h.RootVolumeID]
	if root == "" {
		return "", "", false
	}
	return filepath.Join(root, "overlay", "upper"), filepath.Join(root, "overlay", "work"), true
}

func requestHasStateVolumes(request *types.ContainerRequest) bool {
	if request == nil {
		return false
	}
	if request.PersistentRoot != nil {
		return true
	}
	for i := range request.Mounts {
		if request.Mounts[i].MountType == types.StorageModeDurableDisk || request.Mounts[i].DurableDisk != nil {
			return true
		}
	}
	return false
}

func uninitializedStateVolumeGroupSpec(request *types.ContainerRequest) (StateVolumeGroupSpec, error) {
	if request == nil || strings.TrimSpace(request.ContainerId) == "" {
		return StateVolumeGroupSpec{}, fmt.Errorf("container request and ID are required")
	}
	spec := StateVolumeGroupSpec{ContainerID: request.ContainerId}
	if request.PersistentRoot != nil {
		rootState := request.RootState
		if rootState == nil {
			return StateVolumeGroupSpec{}, fmt.Errorf("persistent root requires scheduler-authored root_state")
		}
		volumeID := strings.TrimSpace(rootState.VolumeId)
		if parsed, err := uuid.Parse(volumeID); err != nil || parsed.String() != strings.ToLower(volumeID) {
			return StateVolumeGroupSpec{}, fmt.Errorf("persistent root requires a canonical UUID volume_id")
		}
		if rootState.Initialize == (rootState.SourceGenerationId != "") {
			return StateVolumeGroupSpec{}, fmt.Errorf("persistent root must specify exactly one of initialize or source_generation_id")
		}
		if rootState.CloneSource && rootState.SourceGenerationId == "" {
			return StateVolumeGroupSpec{}, fmt.Errorf("persistent root clone_source requires source_generation_id")
		}
		if rootState.Size != request.PersistentRoot.Size {
			return StateVolumeGroupSpec{}, fmt.Errorf("persistent root scheduler size %q does not match request size %q", rootState.Size, request.PersistentRoot.Size)
		}
		if err := validateStateVolumeWriterLeaseIdentity(rootState.AttachmentToken, rootState.FencingToken, rootState.LeaseExpiresAtUnix); err != nil {
			return StateVolumeGroupSpec{}, fmt.Errorf("persistent root: %w", err)
		}
		if rootState.SourceGenerationId == "" {
			root, err := uninitializedStateVolumeSpec(request.ContainerId, volumeID, "root", "/", rootState.Size, true, false, false)
			if err != nil {
				return StateVolumeGroupSpec{}, err
			}
			root.AttachmentToken = rootState.AttachmentToken
			root.FencingToken = rootState.FencingToken
			spec.Volumes = append(spec.Volumes, root)
		}
	}
	seenNames := map[string]struct{}{}
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.MountType != types.StorageModeDurableDisk && mount.DurableDisk == nil {
			continue
		}
		if mount.DurableDisk == nil {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable mount %q has no disk configuration", mount.MountPath)
		}
		name := strings.TrimSpace(mount.DurableDisk.Name)
		if name == "" {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable mount %q has no disk name", mount.MountPath)
		}
		if _, exists := seenNames[name]; exists {
			return StateVolumeGroupSpec{}, fmt.Errorf("duplicate durable disk name %q", name)
		}
		seenNames[name] = struct{}{}
		if mount.ReadOnly && mount.DurableDisk.SourceGenerationId == "" {
			return StateVolumeGroupSpec{}, fmt.Errorf("read-only durable disk %q requires a source generation", name)
		}
		volumeID := strings.TrimSpace(mount.DurableDisk.VolumeId)
		if parsed, err := uuid.Parse(volumeID); err != nil || parsed.String() != strings.ToLower(volumeID) {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable disk %q requires a canonical UUID volume_id", name)
		}
		if mount.DurableDisk.Initialize == (mount.DurableDisk.SourceGenerationId != "") {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable disk %q must specify exactly one of initialize or source_generation_id", name)
		}
		if mount.DurableDisk.CloneSource && mount.DurableDisk.SourceGenerationId == "" {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable disk %q clone_source requires source_generation_id", name)
		}
		if mount.ReadOnly {
			if mount.DurableDisk.AttachmentToken != "" || mount.DurableDisk.FencingToken != 0 {
				return StateVolumeGroupSpec{}, fmt.Errorf("read-only durable disk %q must not carry a writer lease", name)
			}
		} else if err := validateStateVolumeWriterLease(mount.DurableDisk); err != nil {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable disk %q: %w", name, err)
		}
		if mount.DurableDisk.SourceGenerationId != "" {
			continue
		}
		volume, err := uninitializedStateVolumeSpec(request.ContainerId, volumeID, name, mount.MountPath, mount.DurableDisk.Size, false, mount.ReadOnly, false)
		if err != nil {
			return StateVolumeGroupSpec{}, fmt.Errorf("durable disk %q: %w", name, err)
		}
		mount.MountType = types.StorageModeDurableDisk
		mount.LocalPath = volume.MountPath
		volume.AttachmentToken = mount.DurableDisk.AttachmentToken
		volume.FencingToken = mount.DurableDisk.FencingToken
		spec.Volumes = append(spec.Volumes, volume)
	}
	return spec, nil
}

func (s *Worker) prepareStateVolumes(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance) (*StateVolumeGroupHandle, error) {
	if !requestHasStateVolumes(request) {
		return nil, nil
	}
	if instance != nil && instance.StateVolumes != nil {
		return instance.StateVolumes, nil
	}
	if s.stateVolumeManager != nil {
		if handle, specs, ok := s.stateVolumeManager.ExistingGroup(request.ContainerId); ok {
			if request.StateSnapshotId != "" && handle.SourceStateSnapshotID != request.StateSnapshotId {
				return nil, fmt.Errorf("reconciled state volume group belongs to snapshot %q, not %q", handle.SourceStateSnapshotID, request.StateSnapshotId)
			}
			if err := bindReconciledStateVolumeGroup(request, specs); err != nil {
				return nil, fmt.Errorf("bind reconciled state volume group: %w", err)
			}
			if err := prepareStateVolumeOverlayWork(handle); err != nil {
				return nil, err
			}
			if request.StateSnapshotId != "" {
				response, err := s.backendRepoClient.GetStateSnapshot(ctx, &pb.GetStateSnapshotRequest{
					WorkspaceId: request.Workspace.ExternalId, StateSnapshotId: request.StateSnapshotId,
				})
				if err != nil || response == nil || !response.Ok || response.Snapshot == nil {
					if err == nil {
						err = fmt.Errorf("state snapshot metadata is unavailable for reconciled group")
					}
					return nil, err
				}
				if err := bindStateMemoryCheckpoint(request, instance, response.Snapshot); err != nil {
					return nil, err
				}
			}
			return handle, nil
		}
	}
	if request.StateSnapshotId != "" {
		return s.restoreStateVolumes(ctx, request, instance, request.StateSnapshotId)
	}
	if s.stateVolumeManager == nil {
		return nil, fmt.Errorf("state volumes requested but state volume manager is unavailable")
	}
	spec, err := uninitializedStateVolumeGroupSpec(request)
	if err != nil {
		return nil, err
	}
	if err := s.appendSourceGenerationVolumes(ctx, request, &spec); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := stateVolumePreparationFence(instance); err != nil {
		return nil, err
	}
	handle, err := s.stateVolumeManager.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	if err := prepareStateVolumeOverlayWork(handle); err != nil {
		_ = s.stateVolumeManager.Stop(context.Background(), request.ContainerId)
		return nil, err
	}
	return handle, nil
}

func bindReconciledStateVolumeGroup(request *types.ContainerRequest, specs []StateVolumeSpec) error {
	if request == nil || len(specs) == 0 {
		return fmt.Errorf("reconciled state volume group is empty")
	}
	byName := make(map[string]StateVolumeSpec, len(specs))
	rootCount := 0
	for _, spec := range specs {
		if spec.Root {
			rootCount++
			if spec.Name != "root" || spec.ContainerMountPath != "/" || spec.ReadOnly || request.PersistentRoot == nil || request.RootState == nil ||
				spec.ID != request.RootState.VolumeId || spec.AttachmentToken != request.RootState.AttachmentToken || spec.FencingToken != request.RootState.FencingToken {
				return fmt.Errorf("reconciled root does not match request")
			}
			if request.RootState.SourceGenerationId != "" {
				if err := validateReconciledStateVolumeLineage(spec, request.RootState.SourceGenerationId, request.RootState.CloneSource); err != nil {
					return fmt.Errorf("reconciled root lineage does not match request: %w", err)
				}
			}
			continue
		}
		if _, duplicate := byName[spec.Name]; duplicate {
			return fmt.Errorf("duplicate reconciled member %q", spec.Name)
		}
		byName[spec.Name] = spec
	}
	if (request.PersistentRoot != nil && rootCount != 1) || (request.PersistentRoot == nil && rootCount != 0) {
		return fmt.Errorf("reconciled root membership does not match request")
	}
	matched := 0
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.DurableDisk == nil {
			continue
		}
		spec, ok := byName[mount.DurableDisk.Name]
		if !ok || spec.ID != mount.DurableDisk.VolumeId || spec.ContainerMountPath != mount.MountPath || spec.ReadOnly != mount.ReadOnly {
			return fmt.Errorf("reconciled member %q does not match request", mount.DurableDisk.Name)
		}
		if mount.ReadOnly && spec.CurrentGenerationID != mount.DurableDisk.SourceGenerationId {
			return fmt.Errorf("reconciled read-only generation for %q does not match request", spec.Name)
		}
		if !mount.ReadOnly && mount.DurableDisk.SourceGenerationId != "" {
			if err := validateReconciledStateVolumeLineage(spec, mount.DurableDisk.SourceGenerationId, mount.DurableDisk.CloneSource); err != nil {
				return fmt.Errorf("reconciled writable lineage for %q does not match request: %w", spec.Name, err)
			}
		}
		mount.MountType = types.StorageModeDurableDisk
		mount.LocalPath = spec.MountPath
		matched++
	}
	if matched != len(byName) {
		return fmt.Errorf("reconciled group has members absent from request")
	}
	return nil
}

func validateReconciledStateVolumeLineage(spec StateVolumeSpec, sourceGenerationID string, cloneSource bool) error {
	if sourceGenerationID == "" || spec.LineageSourceGenerationID != sourceGenerationID {
		return fmt.Errorf("immutable source generation changed")
	}
	if spec.CurrentGenerationID == "" {
		if cloneSource {
			if spec.CloneParentGenerationID != sourceGenerationID || spec.ParentGenerationID != "" || spec.Generation != 0 {
				return fmt.Errorf("uncommitted clone anchor is inconsistent")
			}
		} else if spec.ParentGenerationID != sourceGenerationID || spec.CloneParentGenerationID != "" {
			return fmt.Errorf("uncommitted parent anchor is inconsistent")
		}
		return nil
	}
	if spec.Generation <= 0 || spec.ParentGenerationID != spec.CurrentGenerationID || spec.CloneParentGenerationID != "" {
		return fmt.Errorf("advanced current head is inconsistent")
	}
	return nil
}

func (s *Worker) appendSourceGenerationVolumes(ctx context.Context, request *types.ContainerRequest, group *StateVolumeGroupSpec) error {
	if request == nil || group == nil {
		return fmt.Errorf("state volume request and group are required")
	}
	type sourceVolume struct {
		volumeID, name, mountPath, size, generationID string
		root, readOnly, clone                         bool
		attachmentToken                               string
		fencingToken                                  int64
		mount                                         *types.Mount
	}
	sources := make([]sourceVolume, 0, len(request.Mounts)+1)
	if request.PersistentRoot != nil && request.RootState != nil && request.RootState.SourceGenerationId != "" {
		sources = append(sources, sourceVolume{
			volumeID: request.RootState.VolumeId, name: "root", mountPath: "/",
			size: request.RootState.Size, generationID: request.RootState.SourceGenerationId,
			root: true, clone: request.RootState.CloneSource,
			attachmentToken: request.RootState.AttachmentToken, fencingToken: request.RootState.FencingToken,
		})
	}
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.DurableDisk != nil && mount.DurableDisk.SourceGenerationId != "" {
			sources = append(sources, sourceVolume{
				volumeID: mount.DurableDisk.VolumeId, name: mount.DurableDisk.Name,
				mountPath: mount.MountPath, size: mount.DurableDisk.Size,
				generationID: mount.DurableDisk.SourceGenerationId,
				readOnly:     mount.ReadOnly, clone: mount.DurableDisk.CloneSource,
				attachmentToken: mount.DurableDisk.AttachmentToken,
				fencingToken:    mount.DurableDisk.FencingToken, mount: mount,
			})
		}
	}
	if len(sources) == 0 {
		return nil
	}
	if s.backendRepoClient == nil || !request.StorageAvailable() {
		return fmt.Errorf("source generation restore requires repository and workspace storage")
	}
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return err
	}
	cas := s.workspaceBlockV1CAS(storageClient)
	resolver := &repositoryBlockV1Resolver{workspaceID: request.Workspace.ExternalId, repository: s.backendRepoClient, cas: cas}
	containerToken := stateVolumeToken("container-", request.ContainerId)
	for _, source := range sources {
		generationID := source.generationID
		response, err := s.backendRepoClient.GetVolumeGeneration(ctx, &pb.GetVolumeGenerationRequest{
			WorkspaceId: request.Workspace.ExternalId, GenerationId: generationID,
		})
		if err != nil || response == nil || !response.Ok || response.Generation == nil {
			if err != nil {
				return err
			}
			return fmt.Errorf("source generation %q is unavailable", generationID)
		}
		record := response.Generation
		if record.Name != source.name {
			return fmt.Errorf("source generation %q belongs to disk %q, not %q", generationID, record.Name, source.name)
		}
		if source.readOnly && source.clone {
			return fmt.Errorf("read-only state volume %q cannot clone/rekey its source", source.name)
		}
		if !source.clone && source.volumeID != record.VolumeId {
			return fmt.Errorf("source generation %q belongs to volume %q, not requested volume %q", generationID, record.VolumeId, source.volumeID)
		}
		if source.clone && source.volumeID == record.VolumeId {
			return fmt.Errorf("cloned state volume %q must use a rekeyed destination volume", source.name)
		}
		declaredSize, err := stateVolumeSizeBytes(source.size)
		if err != nil {
			return fmt.Errorf("source generation %q has invalid requested disk size %q", generationID, source.size)
		}
		graphPath, manifest, err := RestoreBlockV1ChainForVolume(ctx, record.VolumeId, generationID,
			filepath.Join(stateVolumeHostRoot, "block-cache"), resolver, cas, s.stateVolumeManager.Images, declaredSize)
		if err != nil {
			return err
		}
		if declaredSize != manifest.VirtualSizeBytes {
			return fmt.Errorf("source generation %q virtual size does not match requested disk size", generationID)
		}
		volumeToken := stateVolumeToken("volume-", source.volumeID)
		backingDir := filepath.Join(stateVolumeHostRoot, "containers", containerToken, "volumes", volumeToken)
		if !source.readOnly {
			backingDir = filepath.Join(stateVolumeHostRoot, "volumes", volumeToken, "graph")
		}
		mountPath := filepath.Join(stateVolumeHostRoot, "mounts", containerToken, volumeToken)
		activePath, activeBackingPath, parentID, currentID, depth := graphPath, "", manifest.ParentGenerationID, generationID, manifest.Depth
		generation := record.Generation
		cloneParentID := ""
		if !source.readOnly {
			activePath = filepath.Join(backingDir, "active", uuid.NewString()+".qcow2")
			activeBackingPath = graphPath
			parentID, currentID, depth = generationID, "", manifest.Depth+1
			if source.clone {
				generation = 0
				parentID = ""
				cloneParentID = generationID
			}
		}
		if source.mount != nil {
			source.mount.MountType, source.mount.LocalPath = types.StorageModeDurableDisk, mountPath
		}
		group.Volumes = append(group.Volumes, StateVolumeSpec{
			ID: source.volumeID, Name: source.name, ContainerMountPath: source.mountPath,
			Root: source.root, ReadOnly: source.readOnly, Generation: generation, CurrentGenerationID: currentID,
			LineageSourceGenerationID: generationID,
			BackingDir:                backingDir, MountPath: mountPath, SizeBytes: manifest.VirtualSizeBytes,
			ActiveLayerPath: activePath, ActiveBackingPath: activeBackingPath,
			ReadOnlyLayerRoot:  filepath.Join(stateVolumeHostRoot, "block-cache"),
			ParentGenerationID: parentID, CloneParentGenerationID: cloneParentID, Depth: depth,
			AttachmentToken: source.attachmentToken, FencingToken: source.fencingToken,
			CreateLayer: !source.readOnly,
		})
	}
	return nil
}

func prepareStateVolumeOverlayWork(handle *StateVolumeGroupHandle) error {
	if handle == nil || handle.RootVolumeID == "" {
		return nil
	}
	root := filepath.Clean(handle.MountPaths[handle.RootVolumeID])
	allowedRoot := filepath.Join(stateVolumeHostRoot, "mounts")
	canonicalAllowed, err := canonicalStateVolumePath(allowedRoot)
	if err != nil {
		return err
	}
	canonicalRoot, err := canonicalStateVolumePath(root)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(canonicalAllowed, canonicalRoot)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("persistent overlay root %q is outside worker state-volume mounts", root)
	}
	if !storage.IsMounted(root) {
		return fmt.Errorf("persistent overlay root %q is not a mounted state volume", root)
	}
	work := filepath.Join(root, "overlay", "work")
	if info, err := os.Lstat(work); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("persistent overlay work path %q is not a real directory", work)
		}
		if err := os.RemoveAll(work); err != nil {
			return fmt.Errorf("reset persistent overlay work directory: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(work, 0755); err != nil {
		return fmt.Errorf("create persistent overlay work directory: %w", err)
	}
	return nil
}

type stateVolumeRestoreDestination struct {
	volumeID           string
	size               string
	cloneSource        bool
	attachmentToken    string
	fencingToken       int64
	leaseExpiresAtUnix int64
	durableMount       *types.Mount
}

type stateVolumeRestoredLineage struct {
	generation              int64
	currentGenerationID     string
	parentGenerationID      string
	cloneParentGenerationID string
	depth                   int
}

func resolveStateVolumeRestoredLineage(member *pb.StateGeneration, manifest BlockV1Manifest, destination stateVolumeRestoreDestination) (stateVolumeRestoredLineage, error) {
	if member == nil || member.GenerationId == "" || member.Generation <= 0 {
		return stateVolumeRestoredLineage{}, fmt.Errorf("state restore generation is incomplete")
	}
	if manifest.GenerationID != member.GenerationId || manifest.Generation != member.Generation {
		return stateVolumeRestoredLineage{}, fmt.Errorf("state restore generation identity does not match its authenticated manifest")
	}
	lineage := stateVolumeRestoredLineage{
		generation:          member.Generation,
		currentGenerationID: member.GenerationId,
		parentGenerationID:  manifest.ParentGenerationID,
		depth:               manifest.Depth,
	}
	if member.ReadOnly {
		return lineage, nil
	}
	lineage.currentGenerationID = ""
	lineage.parentGenerationID = member.GenerationId
	lineage.depth++
	if destination.cloneSource {
		lineage.generation = 0
		lineage.parentGenerationID = ""
		lineage.cloneParentGenerationID = member.GenerationId
	}
	return lineage, nil
}

func forcedColdStateRestoreReason(request *types.ContainerRequest, snapshot *pb.StateSnapshot) string {
	if request == nil || snapshot == nil || snapshot.RestoreMode != stateRestoreModeMemory {
		return ""
	}
	if request.StateFork || request.Stub.ExternalId != snapshot.SourceStubExternalId || snapshot.Mode != string(StateSnapshotModeTerminal) {
		return "memory restore is forbidden for a fork/template or non-terminal source; restored exact block state cold"
	}
	return ""
}

func resolveStateVolumeRestoreDestination(
	request *types.ContainerRequest,
	member *pb.StateGeneration,
	durableMounts map[string]*types.Mount,
) (stateVolumeRestoreDestination, error) {
	if request == nil || member == nil {
		return stateVolumeRestoreDestination{}, fmt.Errorf("state restore request and generation are required")
	}
	destination := stateVolumeRestoreDestination{}
	sourceGenerationID := ""
	initialize := false
	if member.Root {
		if request.PersistentRoot == nil || request.RootState == nil || member.Name != "root" || member.MountPath != "/" || member.ReadOnly {
			return destination, fmt.Errorf("state snapshot root membership does not match destination request")
		}
		rootState := request.RootState
		if rootState.Size != request.PersistentRoot.Size {
			return destination, fmt.Errorf("persistent root scheduler size %q does not match request size %q", rootState.Size, request.PersistentRoot.Size)
		}
		destination.volumeID = strings.TrimSpace(rootState.VolumeId)
		destination.size = rootState.Size
		sourceGenerationID = strings.TrimSpace(rootState.SourceGenerationId)
		destination.cloneSource = rootState.CloneSource
		destination.attachmentToken = rootState.AttachmentToken
		destination.fencingToken = rootState.FencingToken
		destination.leaseExpiresAtUnix = rootState.LeaseExpiresAtUnix
		initialize = rootState.Initialize
	} else {
		if member.Name == "root" || member.MountPath == "/" {
			return destination, fmt.Errorf("state snapshot uses reserved root identity for a non-root member")
		}
		mount := durableMounts[member.Name]
		if mount == nil || mount.DurableDisk == nil || mount.MountPath != member.MountPath || mount.ReadOnly != member.ReadOnly {
			return destination, fmt.Errorf("state snapshot volume %q does not match requested mount", member.Name)
		}
		destination.volumeID = strings.TrimSpace(mount.DurableDisk.VolumeId)
		destination.size = mount.DurableDisk.Size
		sourceGenerationID = strings.TrimSpace(mount.DurableDisk.SourceGenerationId)
		destination.cloneSource = mount.DurableDisk.CloneSource
		destination.attachmentToken = mount.DurableDisk.AttachmentToken
		destination.fencingToken = mount.DurableDisk.FencingToken
		destination.leaseExpiresAtUnix = mount.DurableDisk.LeaseExpiresAtUnix
		destination.durableMount = mount
		initialize = mount.DurableDisk.Initialize
	}
	if initialize || sourceGenerationID == "" {
		return destination, fmt.Errorf("state snapshot volume %q requires an exact source generation, not initialization", member.Name)
	}
	if sourceGenerationID != member.GenerationId {
		return destination, fmt.Errorf("state snapshot volume %q source generation does not match destination attachment", member.Name)
	}
	if parsed, err := uuid.Parse(destination.volumeID); err != nil || parsed.String() != strings.ToLower(destination.volumeID) {
		return destination, fmt.Errorf("state snapshot volume %q has invalid destination volume_id", member.Name)
	}
	if member.ReadOnly {
		if destination.cloneSource || destination.volumeID != member.VolumeId || destination.attachmentToken != "" || destination.fencingToken != 0 {
			return destination, fmt.Errorf("read-only state snapshot volume %q must pin the exact immutable source", member.Name)
		}
		return destination, nil
	}
	if request.StateFork != destination.cloneSource {
		return destination, fmt.Errorf("writable state snapshot volume %q clone intent does not match destination fork", member.Name)
	}
	if destination.cloneSource == (destination.volumeID == member.VolumeId) {
		return destination, fmt.Errorf("writable state snapshot volume %q has invalid destination lineage", member.Name)
	}
	if err := validateStateVolumeWriterLeaseIdentity(destination.attachmentToken, destination.fencingToken, destination.leaseExpiresAtUnix); err != nil {
		return destination, fmt.Errorf("state snapshot volume %q: %w", member.Name, err)
	}
	return destination, nil
}

func (s *Worker) restoreStateVolumes(ctx context.Context, request *types.ContainerRequest, instance *ContainerInstance, stateSnapshotID string) (*StateVolumeGroupHandle, error) {
	if s == nil || s.stateVolumeManager == nil || s.backendRepoClient == nil {
		return nil, fmt.Errorf("state restore services are unavailable")
	}
	if request == nil || request.Workspace.ExternalId == "" || request.Stub.ExternalId == "" || !request.StorageAvailable() {
		return nil, fmt.Errorf("state restore requires a workspace, stub, and workspace storage")
	}
	response, err := s.backendRepoClient.GetStateSnapshot(ctx, &pb.GetStateSnapshotRequest{
		WorkspaceId: request.Workspace.ExternalId, StateSnapshotId: stateSnapshotID,
	})
	if err != nil {
		return nil, err
	}
	if response == nil || !response.Ok || response.Snapshot == nil {
		message := "state snapshot not found"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return nil, fmt.Errorf("restore state snapshot %q: %s", stateSnapshotID, message)
	}
	snapshot := response.Snapshot
	if snapshot.ExternalId != stateSnapshotID || snapshot.Status != string(types.StateSnapshotStatusAvailable) {
		return nil, fmt.Errorf("state snapshot %q is not available", stateSnapshotID)
	}
	if !request.StateFork && snapshot.Mode == string(StateSnapshotModeLive) {
		return nil, fmt.Errorf("same-lineage restore from a live snapshot is forbidden; fork/rekey the destination")
	}
	if request.PersistentRoot != nil && request.RootState == nil {
		return nil, fmt.Errorf("persistent root restore requires scheduler-authored root_state")
	}
	if _, err := stateVolumeWriterLeases(request); err != nil {
		return nil, fmt.Errorf("validate restore writer leases: %w", err)
	}
	if err := bindStateMemoryCheckpoint(request, instance, snapshot); err != nil {
		return nil, err
	}
	forcedColdReason := forcedColdStateRestoreReason(request, snapshot)
	if snapshot.ImageId != "" && snapshot.ImageId != request.ImageId {
		return nil, fmt.Errorf("state snapshot image %q does not match request image %q", snapshot.ImageId, request.ImageId)
	}
	if strings.TrimSpace(request.StateImageDigest) == "" || strings.TrimSpace(request.StateRuntimeProfile) == "" {
		return nil, fmt.Errorf("scheduled state image digest and runtime profile are required")
	}
	if snapshot.ImageDigest != request.StateImageDigest {
		return nil, fmt.Errorf("state snapshot image digest does not match scheduled state")
	}
	if snapshot.RuntimeProfile != request.StateRuntimeProfile {
		return nil, fmt.Errorf("state snapshot runtime profile does not match scheduled state")
	}
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		return nil, err
	}
	cas := s.workspaceBlockV1CAS(storageClient)
	resolver := &repositoryBlockV1Resolver{workspaceID: request.Workspace.ExternalId, repository: s.backendRepoClient, cas: cas}
	containerToken := stateVolumeToken("container-", request.ContainerId)
	durableMounts := make(map[string]*types.Mount)
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.MountType == types.StorageModeDurableDisk || mount.DurableDisk != nil {
			if mount.DurableDisk == nil || strings.TrimSpace(mount.DurableDisk.Name) == "" {
				return nil, fmt.Errorf("durable mount %q has no disk configuration", mount.MountPath)
			}
			durableMounts[mount.DurableDisk.Name] = mount
		}
	}
	seenDurable := make(map[string]struct{})
	seenRoot := false
	seenVolumeIDs := make(map[string]struct{}, len(snapshot.Generations))
	seenDestinationVolumeIDs := make(map[string]struct{}, len(snapshot.Generations))
	seenGenerationIDs := make(map[string]struct{}, len(snapshot.Generations))
	seenNames := make(map[string]struct{}, len(snapshot.Generations))
	seenMountPaths := make(map[string]struct{}, len(snapshot.Generations))
	group := StateVolumeGroupSpec{ContainerID: request.ContainerId, SourceStateSnapshotID: stateSnapshotID}
	type restoredMountBinding struct {
		mount     *types.Mount
		localPath string
	}
	mountBindings := make([]restoredMountBinding, 0, len(durableMounts))
	for _, member := range snapshot.Generations {
		if member == nil || strings.TrimSpace(member.VolumeId) == "" || strings.TrimSpace(member.GenerationId) == "" ||
			strings.TrimSpace(member.Name) == "" || member.Generation <= 0 || !filepath.IsAbs(member.MountPath) ||
			filepath.Clean(member.MountPath) != member.MountPath {
			return nil, fmt.Errorf("state snapshot %q contains an incomplete generation", stateSnapshotID)
		}
		if _, duplicate := seenVolumeIDs[member.VolumeId]; duplicate {
			return nil, fmt.Errorf("state snapshot %q contains duplicate volume %q", stateSnapshotID, member.VolumeId)
		}
		if _, duplicate := seenGenerationIDs[member.GenerationId]; duplicate {
			return nil, fmt.Errorf("state snapshot %q contains duplicate generation %q", stateSnapshotID, member.GenerationId)
		}
		if _, duplicate := seenNames[member.Name]; duplicate {
			return nil, fmt.Errorf("state snapshot %q contains duplicate member name %q", stateSnapshotID, member.Name)
		}
		if _, duplicate := seenMountPaths[member.MountPath]; duplicate {
			return nil, fmt.Errorf("state snapshot %q contains duplicate mount path %q", stateSnapshotID, member.MountPath)
		}
		seenVolumeIDs[member.VolumeId] = struct{}{}
		seenGenerationIDs[member.GenerationId] = struct{}{}
		seenNames[member.Name] = struct{}{}
		seenMountPaths[member.MountPath] = struct{}{}
		if member.Root {
			if seenRoot {
				return nil, fmt.Errorf("state snapshot %q root membership does not match request", stateSnapshotID)
			}
			seenRoot = true
		} else {
			if _, duplicate := seenDurable[member.Name]; duplicate {
				return nil, fmt.Errorf("state snapshot contains duplicate volume %q", member.Name)
			}
			seenDurable[member.Name] = struct{}{}
		}
		destination, err := resolveStateVolumeRestoreDestination(request, member, durableMounts)
		if err != nil {
			return nil, fmt.Errorf("state snapshot %q: %w", stateSnapshotID, err)
		}
		if _, duplicate := seenDestinationVolumeIDs[destination.volumeID]; duplicate {
			return nil, fmt.Errorf("state snapshot %q maps multiple members to destination volume %q", stateSnapshotID, destination.volumeID)
		}
		seenDestinationVolumeIDs[destination.volumeID] = struct{}{}
		volumeToken := stateVolumeToken("volume-", destination.volumeID)
		backingDir := filepath.Join(stateVolumeHostRoot, "containers", containerToken, "volumes", volumeToken)
		if !member.ReadOnly {
			backingDir = filepath.Join(stateVolumeHostRoot, "volumes", volumeToken, "graph")
		}
		declaredSizeBytes, err := stateVolumeSizeBytes(destination.size)
		if err != nil {
			return nil, fmt.Errorf("state snapshot volume %q has invalid requested size %q", member.Name, destination.size)
		}
		graphPath, manifest, err := RestoreBlockV1ChainForVolume(
			ctx, member.VolumeId, member.GenerationId,
			filepath.Join(stateVolumeHostRoot, "block-cache"), resolver, cas, s.stateVolumeManager.Images, declaredSizeBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("restore state volume %q: %w", member.Name, err)
		}
		if manifest.Generation != member.Generation || manifest.ParentGenerationID != member.ParentGenerationId ||
			manifest.CloneParentGenerationID != member.CloneParentGenerationId {
			return nil, fmt.Errorf("state snapshot volume %q membership ancestry does not match its authenticated manifest", member.Name)
		}
		if declaredSizeBytes != manifest.VirtualSizeBytes {
			return nil, fmt.Errorf("state snapshot volume %q virtual size %d does not match requested size %q", member.Name, manifest.VirtualSizeBytes, destination.size)
		}
		lineage, err := resolveStateVolumeRestoredLineage(member, manifest, destination)
		if err != nil {
			return nil, fmt.Errorf("state snapshot volume %q: %w", member.Name, err)
		}
		activePath := graphPath
		activeBackingPath := ""
		if !member.ReadOnly {
			activePath = filepath.Join(backingDir, "active", uuid.NewString()+".qcow2")
			activeBackingPath = graphPath
		}
		mountPath := filepath.Join(stateVolumeHostRoot, "mounts", containerToken, volumeToken)
		volume := StateVolumeSpec{
			ID: destination.volumeID, Name: member.Name, ContainerMountPath: member.MountPath,
			Root: member.Root, ReadOnly: member.ReadOnly, Generation: lineage.generation, CurrentGenerationID: lineage.currentGenerationID,
			LineageSourceGenerationID: member.GenerationId,
			SourceVolumeID:            member.VolumeId, SourceGeneration: manifest.Generation,
			SourceParentGenerationID:      manifest.ParentGenerationID,
			SourceCloneParentGenerationID: manifest.CloneParentGenerationID,
			SourceDepth:                   manifest.Depth,
			BackingDir:                    backingDir, MountPath: mountPath, SizeBytes: manifest.VirtualSizeBytes,
			ActiveLayerPath: activePath, ActiveBackingPath: activeBackingPath,
			ReadOnlyLayerRoot:  filepath.Join(stateVolumeHostRoot, "block-cache"),
			ParentGenerationID: lineage.parentGenerationID, CloneParentGenerationID: lineage.cloneParentGenerationID, Depth: lineage.depth,
			AttachmentToken: destination.attachmentToken, FencingToken: destination.fencingToken,
			CreateLayer: !member.ReadOnly,
		}
		if !member.Root {
			mountBindings = append(mountBindings, restoredMountBinding{mount: destination.durableMount, localPath: mountPath})
		}
		group.Volumes = append(group.Volumes, volume)
	}
	if (request.PersistentRoot != nil) != seenRoot || len(seenDurable) != len(durableMounts) {
		return nil, fmt.Errorf("state snapshot %q volume membership does not exactly match request", stateSnapshotID)
	}
	if len(group.Volumes) == 0 {
		return nil, fmt.Errorf("state snapshot %q has no volumes", stateSnapshotID)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := stateVolumePreparationFence(instance); err != nil {
		return nil, err
	}
	handle, err := s.stateVolumeManager.Start(ctx, group)
	if err != nil {
		return nil, err
	}
	for _, binding := range mountBindings {
		binding.mount.MountType = types.StorageModeDurableDisk
		binding.mount.LocalPath = binding.localPath
	}
	if err := prepareStateVolumeOverlayWork(handle); err != nil {
		_ = s.stateVolumeManager.Stop(context.Background(), request.ContainerId)
		return nil, err
	}
	request.StateSnapshotId = stateSnapshotID
	instance.StateRestoreFallbackReason = forcedColdReason
	s.containerInstances.Set(request.ContainerId, instance)
	return handle, nil
}

func bindStateMemoryCheckpoint(request *types.ContainerRequest, instance *ContainerInstance, snapshot *pb.StateSnapshot) error {
	if request == nil || instance == nil || snapshot == nil {
		return fmt.Errorf("state memory checkpoint binding context is incomplete")
	}
	instance.StateMemoryCheckpoint = nil
	switch snapshot.RestoreMode {
	case stateRestoreModeMemory:
		if snapshot.CheckpointId == "" || snapshot.CheckpointDigest == "" || snapshot.CheckpointCacheHash == "" ||
			snapshot.CheckpointSizeBytes <= 0 || snapshot.CheckpointOriginKey == "" {
			return fmt.Errorf("state snapshot %q has incomplete memory checkpoint metadata", snapshot.ExternalId)
		}
		if forcedColdStateRestoreReason(request, snapshot) == "" {
			instance.StateMemoryCheckpoint = &StateMemoryCheckpoint{
				ID: snapshot.CheckpointId, Digest: snapshot.CheckpointDigest,
				CacheHash: snapshot.CheckpointCacheHash, SizeBytes: snapshot.CheckpointSizeBytes,
				OriginKey: snapshot.CheckpointOriginKey, Locality: snapshot.CheckpointLocality,
				Accelerator: snapshot.CheckpointAccelerator, Runtime: snapshot.RuntimeProfile,
			}
		}
	case stateRestoreModeCold:
		if snapshot.CheckpointId != "" || snapshot.CheckpointDigest != "" || snapshot.CheckpointCacheHash != "" ||
			snapshot.CheckpointSizeBytes != 0 || snapshot.CheckpointOriginKey != "" {
			return fmt.Errorf("cold state snapshot %q unexpectedly contains memory checkpoint metadata", snapshot.ExternalId)
		}
	default:
		return fmt.Errorf("state snapshot %q has unsupported restore mode %q", snapshot.ExternalId, snapshot.RestoreMode)
	}
	return nil
}
