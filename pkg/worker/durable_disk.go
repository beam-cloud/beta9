package worker

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	durableDiskDriverEnv    = "BETA9_DURABLE_DISK_DRIVER"
	durableDiskMarkerFile   = ".beta9-durable-disk"
	durableDiskReplicaDir   = ".beta9-durable-disk-replicas"
	defaultDevReplicaCopies = 3

	drbdStateDirEnv       = "BETA9_DRBD_STATE_DIR"
	drbdConfigDirEnv      = "BETA9_DRBD_CONFIG_DIR"
	drbdNodeAddressesEnv  = "BETA9_DRBD_NODE_ADDRESSES"
	drbdNodeNamesEnv      = "BETA9_DRBD_NODE_NAMES"
	drbdLocalAddressEnv   = "BETA9_DRBD_LOCAL_ADDRESS"
	drbdLocalNodeNameEnv  = "BETA9_DRBD_LOCAL_NODE_NAME"
	drbdPortBaseEnv       = "BETA9_DRBD_PORT_BASE"
	drbdAttachTimeoutEnv  = "BETA9_DRBD_ATTACH_TIMEOUT"
	defaultDRBDStateDir   = "/var/lib/beta9/drbd"
	defaultDRBDConfigDir  = "/etc/drbd.d"
	defaultDRBDPortBase   = 7790
	defaultDRBDAttachWait = 30 * time.Second
)

type durableDiskCommandRunner func(ctx context.Context, name string, args ...string) (string, error)

var (
	durableDiskLookPath                          = exec.LookPath
	durableDiskEUID                              = os.Geteuid
	durableDiskHostname                          = os.Hostname
	durableDiskRun      durableDiskCommandRunner = runDurableDiskCommand
)

func (s *Worker) prepareDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if mount == nil || mount.DurableDisk == nil {
		return fmt.Errorf("durable disk mount is missing metadata")
	}
	if mount.LocalPath == "" {
		return fmt.Errorf("durable disk %q has no local path", mount.DurableDisk.Name)
	}

	driver := durableDiskDriver(mount.DurableDisk.Driver)
	switch driver {
	case "", types.DurableDiskDriverDev:
		return prepareDevDurableDiskMount(mount)
	case types.DurableDiskDriverDRBD:
		return s.prepareDRBDDurableDiskMount(request, mount)
	default:
		return fmt.Errorf("durable disk %q requested unsupported driver %q", mount.DurableDisk.Name, driver)
	}
}

func durableDiskDriver(configured string) string {
	if driver := types.NormalizeDurableDiskDriver(configured); driver != "" {
		return driver
	}
	return types.NormalizeDurableDiskDriver(os.Getenv(durableDiskDriverEnv))
}

func prepareDevDurableDiskMount(mount *types.Mount) error {
	if err := restoreDevDurableDiskPrimary(mount); err != nil {
		return err
	}
	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return fmt.Errorf("create dev durable disk path %s: %w", mount.LocalPath, err)
	}
	if err := writeDevDurableDiskMarker(mount.LocalPath, mount); err != nil {
		return err
	}

	return ensureDevDurableDiskReplicas(mount)
}

func (s *Worker) syncDurableDiskMounts(request *types.ContainerRequest) {
	if request == nil {
		return
	}

	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount == nil || mount.DurableDisk == nil {
			continue
		}
		switch durableDiskDriver(mount.DurableDisk.Driver) {
		case types.DurableDiskDriverDev:
			if err := syncDevDurableDiskMount(mount); err != nil {
				log.Warn().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Err(err).Msg("failed to sync dev durable disk replicas")
			}
		case types.DurableDiskDriverDRBD:
			if err := s.teardownDRBDDurableDiskMount(request, mount); err != nil {
				log.Warn().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Err(err).Msg("failed to demote drbd durable disk")
			}
		}
	}
}

func restoreDevDurableDiskPrimary(mount *types.Mount) error {
	if devDurableDiskHasPayload(mount.LocalPath) {
		return nil
	}

	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if !devDurableDiskHasPayload(replicaPath) {
			continue
		}

		if err := os.RemoveAll(mount.LocalPath); err != nil {
			return fmt.Errorf("clear dev durable disk primary %s: %w", mount.LocalPath, err)
		}
		if err := copyDurableDiskDir(replicaPath, mount.LocalPath); err != nil {
			return fmt.Errorf("restore dev durable disk primary %s from replica %s: %w", mount.LocalPath, replicaPath, err)
		}
		return writeDevDurableDiskMarker(mount.LocalPath, mount)
	}

	return nil
}

func ensureDevDurableDiskReplicas(mount *types.Mount) error {
	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if devDurableDiskHasPayload(mount.LocalPath) {
			if err := os.MkdirAll(replicaPath, 0755); err != nil {
				return fmt.Errorf("create dev durable disk replica %s: %w", replicaPath, err)
			}
			if err := writeDevDurableDiskMarker(replicaPath, mount); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(replicaPath, 0755); err != nil {
			return fmt.Errorf("create dev durable disk replica %s: %w", replicaPath, err)
		}
		if err := writeDevDurableDiskMarker(replicaPath, mount); err != nil {
			return err
		}
	}
	return nil
}

func syncDevDurableDiskMount(mount *types.Mount) error {
	if !devDurableDiskHasPayload(mount.LocalPath) {
		return nil
	}

	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if err := copyDurableDiskDirAtomic(mount.LocalPath, replicaPath); err != nil {
			return fmt.Errorf("sync dev durable disk replica %s: %w", replicaPath, err)
		}
	}
	return nil
}

func devDurableDiskReplicaPaths(mount *types.Mount) []string {
	if mount == nil || mount.DurableDisk == nil {
		return nil
	}

	replicas := int(mount.DurableDisk.Replicas)
	if replicas == 0 {
		replicas = defaultDevReplicaCopies
	}
	if replicas <= 1 {
		return nil
	}

	base := filepath.Base(filepath.Clean(mount.LocalPath))
	root := filepath.Join(filepath.Dir(filepath.Clean(mount.LocalPath)), durableDiskReplicaDir, base)
	paths := make([]string, 0, replicas-1)
	for replica := 1; replica < replicas; replica++ {
		paths = append(paths, filepath.Join(root, fmt.Sprintf("replica-%d", replica)))
	}
	return paths
}

func devDurableDiskHasPayload(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.Name() == durableDiskMarkerFile {
			continue
		}
		return true
	}
	return false
}

func writeDevDurableDiskMarker(path string, mount *types.Mount) error {
	replicas := uint32(defaultDevReplicaCopies)
	if mount != nil && mount.DurableDisk != nil && mount.DurableDisk.Replicas > 0 {
		replicas = mount.DurableDisk.Replicas
	}

	marker := fmt.Sprintf("driver=dev\nreplicas=%d\n", replicas)
	if err := os.WriteFile(filepath.Join(path, durableDiskMarkerFile), []byte(marker), 0644); err != nil {
		return fmt.Errorf("write dev durable disk marker %s: %w", path, err)
	}
	return nil
}

func copyDurableDiskDirAtomic(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	suffix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	tmp := fmt.Sprintf("%s.tmp.%s", dst, suffix)
	backup := fmt.Sprintf("%s.old.%s", dst, suffix)
	movedExisting := false
	defer os.RemoveAll(tmp)

	if err := copyDurableDiskDir(src, tmp); err != nil {
		return err
	}

	if _, err := os.Lstat(dst); err == nil {
		if err := os.Rename(dst, backup); err != nil {
			return err
		}
		movedExisting = true
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(tmp, dst); err != nil {
		if movedExisting {
			_ = os.Rename(backup, dst)
		}
		return err
	}

	if movedExisting {
		_ = os.RemoveAll(backup)
	}
	return nil
}

func copyDurableDiskDir(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", src)
	}
	if err := os.MkdirAll(dst, info.Mode().Perm()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		info, err := os.Lstat(srcPath)
		if err != nil {
			return err
		}

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(srcPath)
			if err != nil {
				return err
			}
			if err := os.Symlink(target, dstPath); err != nil {
				return err
			}
		case info.IsDir():
			if err := copyDurableDiskDir(srcPath, dstPath); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			if err := copyDurableDiskFile(srcPath, dstPath, info.Mode().Perm()); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyDurableDiskFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Worker) prepareDRBDDurableDiskMount(_ *types.ContainerRequest, mount *types.Mount) error {
	config, err := s.newDRBDDiskConfig(mount, true)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if s != nil && s.ctx != nil {
		ctx = s.ctx
	}
	ctx, cancel := context.WithTimeout(ctx, drbdAttachTimeout())
	defer cancel()

	if err := validateDRBDHost(mount.DurableDisk.Name); err != nil {
		return err
	}
	if err := config.prepareBackingDevice(ctx); err != nil {
		return err
	}
	if err := config.bringUpResource(ctx, true); err != nil {
		return err
	}
	if err := config.mountPrimary(ctx, mount.LocalPath); err != nil {
		return err
	}

	return nil
}

func (s *Worker) teardownDRBDDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	config, err := s.newDRBDDiskConfig(mount, false)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if s != nil && s.ctx != nil {
		ctx = s.ctx
	}
	ctx, cancel := context.WithTimeout(ctx, drbdAttachTimeout())
	defer cancel()

	if err := errors.Join(config.unmount(ctx, mount.LocalPath), config.demote(ctx)); err != nil {
		if request != nil && request.ContainerId != "" {
			return fmt.Errorf("container %s drbd teardown: %w", request.ContainerId, err)
		}
		return fmt.Errorf("drbd teardown: %w", err)
	}
	return nil
}

func (s *Worker) prepareDRBDPeerMount(mount *types.Mount) error {
	config, err := s.newDRBDDiskConfig(mount, false)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if s != nil && s.ctx != nil {
		ctx = s.ctx
	}
	ctx, cancel := context.WithTimeout(ctx, drbdAttachTimeout())
	defer cancel()

	if err := validateDRBDHost(mount.DurableDisk.Name); err != nil {
		return err
	}
	if err := config.prepareBackingDevice(ctx); err != nil {
		return err
	}
	return config.bringUpResource(ctx, false)
}

type drbdDiskConfig struct {
	Resource    string
	Device      string
	SizeBytes   int64
	Filesystem  string
	ConfigPath  string
	StatePath   string
	BackingPath string
	LocalNode   string
	Nodes       []drbdNodeConfig
}

type drbdNodeConfig struct {
	Name          string
	Address       string
	AddressFamily string
	Disk          string
}

func (s *Worker) newDRBDDiskConfig(mount *types.Mount, requirePrimary bool) (*drbdDiskConfig, error) {
	if mount == nil || mount.DurableDisk == nil {
		return nil, fmt.Errorf("durable disk mount is missing metadata")
	}
	disk := mount.DurableDisk
	if disk.Name == "" {
		return nil, fmt.Errorf("durable disk is missing name")
	}
	if disk.PrimaryWorkerID == "" {
		return nil, fmt.Errorf("durable disk %q requested DRBD without a primary worker placement", disk.Name)
	}
	if s == nil || s.workerId == "" {
		return nil, fmt.Errorf("durable disk %q requested DRBD on a worker with no worker id", disk.Name)
	}
	localStorageNodeID := s.storageNodeID()
	if localStorageNodeID == "" {
		return nil, fmt.Errorf("durable disk %q requested DRBD on a worker with no storage node id", disk.Name)
	}
	if requirePrimary && disk.PrimaryWorkerID != localStorageNodeID {
		return nil, fmt.Errorf("durable disk %q primary is storage node %s, refusing attach on storage node %s", disk.Name, disk.PrimaryWorkerID, localStorageNodeID)
	}

	replicaWorkerIDs := normalizedDRBDReplicaWorkerIDs(disk)
	if len(replicaWorkerIDs) < 2 {
		return nil, fmt.Errorf("durable disk %q requested DRBD with %d replica storage node(s); at least 2 are required", disk.Name, len(replicaWorkerIDs))
	}
	if !drbdHasQuorum(disk, replicaWorkerIDs) {
		return nil, fmt.Errorf("durable disk %q requested quorum %q without a majority-capable replica set", disk.Name, disk.Quorum)
	}
	if !slices.Contains(replicaWorkerIDs, localStorageNodeID) {
		return nil, fmt.Errorf("durable disk %q is not placed on storage node %s", disk.Name, localStorageNodeID)
	}

	sizeBytes, err := durableDiskSizeBytes(disk.Size)
	if err != nil {
		return nil, fmt.Errorf("durable disk %q has invalid size %q: %w", disk.Name, disk.Size, err)
	}
	filesystem, err := drbdFilesystem(disk.Filesystem)
	if err != nil {
		return nil, fmt.Errorf("durable disk %q: %w", disk.Name, err)
	}

	addresses, err := stringMapFromEnv(drbdNodeAddressesEnv)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("DRBD replica addresses are required; set %s as a JSON object of worker id to host or host:port", drbdNodeAddressesEnv)
	}
	nodeNames, err := stringMapFromEnv(drbdNodeNamesEnv)
	if err != nil {
		return nil, err
	}
	localNodeName, err := drbdLocalNodeName(localStorageNodeID, nodeNames)
	if err != nil {
		return nil, err
	}

	id := drbdStableID(mount)
	minor := 1000 + int(drbdHashModulo(id, 30000))
	port := drbdPort(id)
	statePath := filepath.Join(firstNonEmpty(os.Getenv(drbdStateDirEnv), defaultDRBDStateDir), id)
	backingPath := filepath.Join(statePath, "backing.img")
	resource := "beta9_" + id
	configPath := filepath.Join(firstNonEmpty(os.Getenv(drbdConfigDirEnv), defaultDRBDConfigDir), resource+".res")

	nodes := make([]drbdNodeConfig, 0, len(replicaWorkerIDs))
	for _, replicaStorageNodeID := range replicaWorkerIDs {
		address := strings.TrimSpace(addresses[replicaStorageNodeID])
		if replicaStorageNodeID == localStorageNodeID {
			address = firstNonEmpty(os.Getenv(drbdLocalAddressEnv), address)
		}
		if address == "" {
			return nil, fmt.Errorf("durable disk %q missing DRBD address for storage node %s; set %s", disk.Name, replicaStorageNodeID, drbdNodeAddressesEnv)
		}
		netAddress := drbdAddressWithPort(address, port)

		name := strings.TrimSpace(nodeNames[replicaStorageNodeID])
		if replicaStorageNodeID == localStorageNodeID {
			name = localNodeName
		}
		if name == "" {
			name = drbdNodeNameFromAddress(netAddress.Value)
		}
		if name == "" {
			return nil, fmt.Errorf("durable disk %q missing DRBD node name for storage node %s; set %s", disk.Name, replicaStorageNodeID, drbdNodeNamesEnv)
		}

		nodes = append(nodes, drbdNodeConfig{
			Name:          name,
			Address:       netAddress.Value,
			AddressFamily: netAddress.Family,
			Disk:          "none",
		})
	}

	return &drbdDiskConfig{
		Resource:    resource,
		Device:      fmt.Sprintf("/dev/drbd%d", minor),
		SizeBytes:   sizeBytes,
		Filesystem:  filesystem,
		ConfigPath:  configPath,
		StatePath:   statePath,
		BackingPath: backingPath,
		LocalNode:   localNodeName,
		Nodes:       nodes,
	}, nil
}

func validateDRBDHost(diskName string) error {
	if durableDiskEUID() != 0 {
		return fmt.Errorf("durable disk %q requires root to configure DRBD, loop devices, and mounts", diskName)
	}

	required := []string{"drbdadm", "drbdsetup", "losetup", "truncate", "mount", "findmnt", "blkid", "umount"}
	for _, name := range required {
		if _, err := durableDiskLookPath(name); err != nil {
			return fmt.Errorf("durable disk %q requires %s on this worker: %w", diskName, name, err)
		}
	}
	return nil
}

func (c *drbdDiskConfig) prepareBackingDevice(ctx context.Context) error {
	if err := os.MkdirAll(c.StatePath, 0755); err != nil {
		return fmt.Errorf("create drbd state path %s: %w", c.StatePath, err)
	}
	if err := c.ensureBackingFile(ctx); err != nil {
		return err
	}

	loopPath, err := c.findLoopDevice(ctx)
	if err != nil {
		return err
	}
	if loopPath == "" {
		out, err := durableDiskRun(ctx, "losetup", "--find", "--show", c.BackingPath)
		if err != nil {
			return fmt.Errorf("attach drbd backing file %s: %w", c.BackingPath, err)
		}
		loopPath = strings.TrimSpace(out)
	}
	if loopPath == "" {
		return fmt.Errorf("attach drbd backing file %s returned no loop device", c.BackingPath)
	}

	localNodeFound := false
	for i := range c.Nodes {
		c.Nodes[i].Disk = loopPath
		if c.Nodes[i].Name == c.LocalNode {
			localNodeFound = true
		}
	}
	if !localNodeFound {
		return fmt.Errorf("drbd local node %q is not present in resource %s", c.LocalNode, c.Resource)
	}
	return c.writeResourceFile()
}

func (c *drbdDiskConfig) ensureBackingFile(ctx context.Context) error {
	if info, err := os.Stat(c.BackingPath); err == nil {
		if info.Size() > c.SizeBytes {
			return fmt.Errorf("refusing to shrink drbd backing file %s from %d to %d bytes", c.BackingPath, info.Size(), c.SizeBytes)
		}
		if info.Size() == c.SizeBytes {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat drbd backing file %s: %w", c.BackingPath, err)
	}

	if _, err := durableDiskRun(ctx, "truncate", "-s", strconv.FormatInt(c.SizeBytes, 10), c.BackingPath); err != nil {
		return fmt.Errorf("create drbd backing file %s: %w", c.BackingPath, err)
	}
	return nil
}

func (c *drbdDiskConfig) findLoopDevice(ctx context.Context) (string, error) {
	out, err := durableDiskRun(ctx, "losetup", "-j", c.BackingPath)
	if err != nil {
		return "", fmt.Errorf("inspect drbd backing loop for %s: %w", c.BackingPath, err)
	}
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); line == "" {
			continue
		}
		if idx := strings.Index(line, ":"); idx > 0 {
			return strings.TrimSpace(line[:idx]), nil
		}
	}
	return "", nil
}

func (c *drbdDiskConfig) bringUpResource(ctx context.Context, promote bool) error {
	_, _ = durableDiskRun(ctx, "modprobe", "drbd")

	if _, err := durableDiskRun(ctx, "drbdadm", "create-md", c.Resource); err != nil && !isIgnorableDRBDError(err) {
		return fmt.Errorf("create drbd metadata for %s: %w", c.Resource, err)
	}
	if _, err := durableDiskRun(ctx, "drbdadm", "up", c.Resource); err != nil && !isIgnorableDRBDError(err) {
		return fmt.Errorf("bring up drbd resource %s: %w", c.Resource, err)
	}
	if _, err := durableDiskRun(ctx, "drbdadm", "connect", c.Resource); err != nil && !isIgnorableDRBDError(err) {
		return fmt.Errorf("connect drbd resource %s: %w", c.Resource, err)
	}
	if !promote {
		return nil
	}
	if _, err := durableDiskRun(ctx, "drbdadm", "primary", "--force", c.Resource); err != nil && !isIgnorableDRBDError(err) {
		return fmt.Errorf("promote drbd resource %s: %w", c.Resource, err)
	}
	return nil
}

func (c *drbdDiskConfig) mountPrimary(ctx context.Context, mountPath string) error {
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return fmt.Errorf("create drbd mount path %s: %w", mountPath, err)
	}
	if drbdIsMounted(ctx, mountPath) {
		return nil
	}
	if !c.hasFilesystem(ctx) {
		if _, err := durableDiskRun(ctx, "mkfs."+c.Filesystem, "-F", c.Device); err != nil {
			return fmt.Errorf("create %s filesystem on %s: %w", c.Filesystem, c.Device, err)
		}
	}
	if _, err := durableDiskRun(ctx, "mount", c.Device, mountPath); err != nil {
		return fmt.Errorf("mount drbd device %s at %s: %w", c.Device, mountPath, err)
	}
	return nil
}

func (c *drbdDiskConfig) unmount(ctx context.Context, mountPath string) error {
	if !drbdIsMounted(ctx, mountPath) {
		return nil
	}
	if _, err := durableDiskRun(ctx, "umount", mountPath); err != nil {
		return fmt.Errorf("unmount drbd mount %s: %w", mountPath, err)
	}
	return nil
}

func (c *drbdDiskConfig) demote(ctx context.Context) error {
	if _, err := durableDiskRun(ctx, "drbdadm", "secondary", c.Resource); err != nil && !isIgnorableDRBDError(err) {
		return fmt.Errorf("demote drbd resource %s: %w", c.Resource, err)
	}
	return nil
}

func (c *drbdDiskConfig) hasFilesystem(ctx context.Context) bool {
	out, err := durableDiskRun(ctx, "blkid", "-o", "value", "-s", "TYPE", c.Device)
	return err == nil && strings.TrimSpace(out) != ""
}

func drbdIsMounted(ctx context.Context, mountPath string) bool {
	_, err := durableDiskRun(ctx, "findmnt", "--mountpoint", mountPath)
	return err == nil
}

func (c *drbdDiskConfig) writeResourceFile() error {
	if err := os.MkdirAll(filepath.Dir(c.ConfigPath), 0755); err != nil {
		return fmt.Errorf("create drbd config path %s: %w", filepath.Dir(c.ConfigPath), err)
	}

	tmp := fmt.Sprintf("%s.tmp.%d", c.ConfigPath, time.Now().UnixNano())
	if err := os.WriteFile(tmp, []byte(c.renderResource()), 0644); err != nil {
		return fmt.Errorf("write drbd config %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, c.ConfigPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("install drbd config %s: %w", c.ConfigPath, err)
	}
	return nil
}

func (c *drbdDiskConfig) renderResource() string {
	var b strings.Builder
	fmt.Fprintf(&b, "resource %s {\n", c.Resource)
	fmt.Fprintf(&b, "  protocol C;\n")
	fmt.Fprintf(&b, "  disk {\n")
	fmt.Fprintf(&b, "    fencing resource-only;\n")
	fmt.Fprintf(&b, "  }\n")
	fmt.Fprintf(&b, "  net {\n")
	fmt.Fprintf(&b, "    quorum majority;\n")
	fmt.Fprintf(&b, "    on-no-quorum io-error;\n")
	fmt.Fprintf(&b, "  }\n")
	for nodeID, node := range c.Nodes {
		fmt.Fprintf(&b, "  on %s {\n", node.Name)
		fmt.Fprintf(&b, "    node-id %d;\n", nodeID)
		fmt.Fprintf(&b, "    device %s;\n", c.Device)
		fmt.Fprintf(&b, "    disk %s;\n", firstNonEmpty(node.Disk, "none"))
		fmt.Fprintf(&b, "    address %s %s;\n", node.AddressFamily, node.Address)
		fmt.Fprintf(&b, "    meta-disk internal;\n")
		fmt.Fprintf(&b, "  }\n")
	}
	fmt.Fprintf(&b, "}\n")
	return b.String()
}

func normalizedDRBDReplicaWorkerIDs(disk *types.DurableDiskMountConfig) []string {
	if disk == nil {
		return nil
	}

	seen := map[string]struct{}{}
	var ids []string
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	add(disk.PrimaryWorkerID)
	for _, id := range disk.ReplicaWorkerIDs {
		add(id)
	}
	return ids
}

func drbdHasQuorum(disk *types.DurableDiskMountConfig, workerIDs []string) bool {
	if disk == nil {
		return false
	}
	if quorum := strings.TrimSpace(strings.ToLower(disk.Quorum)); quorum != "" && quorum != types.DurableDiskReplicationQuorumMajority {
		return true
	}
	replicas := int(disk.Replicas)
	if replicas == 0 {
		replicas = len(workerIDs)
	}
	return replicas >= 2 && len(workerIDs) >= replicas
}

func drbdFilesystem(filesystem string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(filesystem)) {
	case "", "ext4":
		return "ext4", nil
	case "xfs":
		return "xfs", nil
	default:
		return "", fmt.Errorf("unsupported DRBD filesystem %q", filesystem)
	}
}

func durableDiskSizeBytes(size string) (int64, error) {
	size = strings.TrimSpace(size)
	if size == "" {
		return 0, fmt.Errorf("size is required")
	}

	units := []struct {
		suffix string
		factor int64
	}{
		{"Ti", 1 << 40},
		{"Gi", 1 << 30},
		{"Mi", 1 << 20},
		{"Ki", 1 << 10},
		{"T", 1000 * 1000 * 1000 * 1000},
		{"G", 1000 * 1000 * 1000},
		{"M", 1000 * 1000},
		{"K", 1000},
	}

	for _, unit := range units {
		if strings.HasSuffix(size, unit.suffix) {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(size, unit.suffix)), 10, 64)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("invalid size")
			}
			return n * unit.factor, nil
		}
	}

	n, err := strconv.ParseInt(size, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid size")
	}
	return n, nil
}

func drbdStableID(mount *types.Mount) string {
	h := sha1.Sum([]byte(strings.Join([]string{mount.LocalPath, mount.MountPath, mount.DurableDisk.Name}, "\x00")))
	return hex.EncodeToString(h[:])[:12]
}

func drbdHashModulo(id string, mod uint64) uint64 {
	if mod == 0 {
		return 0
	}
	n, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return 0
	}
	return n % mod
}

func drbdPort(id string) int {
	base := defaultDRBDPortBase
	if raw := strings.TrimSpace(os.Getenv(drbdPortBaseEnv)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			base = n
		}
	}
	return base + int(drbdHashModulo(id, 1000))
}

func stringMapFromEnv(key string) (map[string]string, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return map[string]string{}, nil
	}
	values := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("parse %s: %w", key, err)
	}
	return values, nil
}

func drbdLocalNodeName(workerID string, names map[string]string) (string, error) {
	if name := strings.TrimSpace(os.Getenv(drbdLocalNodeNameEnv)); name != "" {
		return name, nil
	}
	if name := strings.TrimSpace(names[workerID]); name != "" {
		return name, nil
	}
	name, err := durableDiskHostname()
	if err != nil {
		return "", fmt.Errorf("get local hostname for DRBD: %w", err)
	}
	return strings.TrimSpace(name), nil
}

type drbdNetAddress struct {
	Value  string
	Family string
}

func drbdAddressWithPort(address string, port int) drbdNetAddress {
	address = strings.TrimSpace(address)
	if address == "" {
		return drbdNetAddress{}
	}
	if host, _, err := net.SplitHostPort(address); err == nil {
		return drbdNetAddress{Value: address, Family: drbdAddressFamily(host)}
	}
	host := strings.Trim(address, "[]")
	if strings.Contains(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(address, "["), "]")
	}
	return drbdNetAddress{
		Value:  net.JoinHostPort(host, strconv.Itoa(port)),
		Family: drbdAddressFamily(host),
	}
}

func drbdAddressFamily(host string) string {
	if strings.Contains(strings.Trim(host, "[]"), ":") {
		return "ipv6"
	}
	return "ipv4"
}

func drbdNodeNameFromAddress(address string) string {
	host := address
	if h, _, err := net.SplitHostPort(address); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	host = strings.ReplaceAll(host, ".", "-")
	host = strings.ReplaceAll(host, ":", "-")
	return strings.Trim(host, "-")
}

func drbdAttachTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv(drbdAttachTimeoutEnv))
	if raw == "" {
		return defaultDRBDAttachWait
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	return defaultDRBDAttachWait
}

func isIgnorableDRBDError(err error) bool {
	if err == nil {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already") ||
		strings.Contains(msg, "exists") ||
		strings.Contains(msg, "valid meta data") ||
		strings.Contains(msg, "in use") ||
		strings.Contains(msg, "is primary") ||
		strings.Contains(msg, "is secondary")
}

func runDurableDiskCommand(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s failed: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
