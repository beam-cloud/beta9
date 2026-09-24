// Package microvm holds the contract between the worker's microvm runtime
// (pkg/runtime/microvm.go) and the guest init (cmd/vminit): the VM spec the
// host writes into the shared canvas, the control messages exchanged over
// vsock, and the fixed paths both sides agree on. It must stay stdlib-only so
// the guest init remains a small static binary.
package microvm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

const (
	// CanvasDir is the directory the host creates at the root of the shared
	// canvas. The guest keeps it in the final root as well: the writable disk
	// stays mounted under it so the host can freeze the filesystem there.
	CanvasDir = "/.beam"
	// SpecFile is the VM spec the host writes into the canvas.
	SpecFile = CanvasDir + "/vm.json"
	// InitPath is where the host copies the guest init inside the canvas and
	// what the kernel is told to exec.
	InitPath = CanvasDir + "/init"
	// DiskMount is where the guest mounts the writable root disk.
	DiskMount = CanvasDir + "/disk"
	// ImageMount is the guest's non-recursive bind of the virtiofs root used
	// as the overlay lower.
	ImageMount = CanvasDir + "/image"
	// NewRoot is where the guest assembles the overlay before pivot_root.
	NewRoot = CanvasDir + "/newroot"
	// OldRoot is where pivot_root parks the virtiofs root inside the new root.
	OldRoot = CanvasDir + "/oldroot"
	// OCISpecFile is the bundle config.json; the worker writes it at the root
	// of the rootfs, which is the canvas root.
	OCISpecFile = "/config.json"

	// VirtiofsTag is the virtio-fs mount tag for the canvas share.
	VirtiofsTag = "beamfs"
	// ControlPort is the vsock port the guest connects to on the host (CID 2).
	ControlPort = 1024
	// HostCID is the well-known vsock CID of the host.
	HostCID = 2
	// GuestCID is the CID assigned to every VM (one VM per vsock device).
	GuestCID = 3

	// Disk layout on the writable root disk, shared with container sandboxes
	// so a durable disk restores into either kind.
	DiskOverlayUpper = "overlay/upper"
	DiskOverlayWork  = "overlay/work"
	DiskDockerDir    = "docker"

	// DockerDataRoot is where dockerd keeps state; bind-mounted from the disk
	// so overlay2 sits on a real block filesystem rather than the guest overlay.
	DockerDataRoot = "/var/lib/docker"
)

// Spec is what the host tells the guest init about this VM. It lives at
// SpecFile inside the canvas next to the OCI config.json.
type Spec struct {
	Hostname string  `json:"hostname,omitempty"`
	Network  Network `json:"network"`
	// RootDisk is the block device holding the overlay upper (and Docker
	// state). Always present.
	RootDisk string `json:"root_disk"`
	// Disks are additional block devices to mount inside the final root.
	Disks []Disk `json:"disks,omitempty"`
	// Docker asks the guest to bind the disk's docker directory over
	// /var/lib/docker.
	Docker bool `json:"docker,omitempty"`
	// Binds are OCI bind mounts the host applied into the canvas; the guest
	// re-applies them into the overlay because overlayfs does not follow
	// mounts inside a lower layer.
	Binds []Bind `json:"binds,omitempty"`
	// Tmpfs are OCI tmpfs mounts the guest creates fresh.
	Tmpfs []Tmpfs `json:"tmpfs,omitempty"`
	// ControlPort overrides ControlPort when non-zero.
	ControlPort uint32 `json:"control_port,omitempty"`
}

// Network is the static configuration of the guest's single NIC. Addresses
// are CIDR strings; gateways are bare addresses.
type Network struct {
	MAC      string `json:"mac"`
	MTU      int    `json:"mtu,omitempty"`
	IPv4     string `json:"ipv4,omitempty"`
	Gateway4 string `json:"gateway4,omitempty"`
	IPv6     string `json:"ipv6,omitempty"`
	Gateway6 string `json:"gateway6,omitempty"`
}

// Disk is an extra block device and where to mount it.
type Disk struct {
	Device    string `json:"device"`
	MountPath string `json:"mount_path"`
	ReadOnly  bool   `json:"readonly,omitempty"`
}

// Bind is a mount the guest binds from the virtiofs root into the new root.
type Bind struct {
	Destination string `json:"destination"`
	File        bool   `json:"file,omitempty"`
	ReadOnly    bool   `json:"readonly,omitempty"`
}

// Tmpfs is a tmpfs the guest mounts inside the new root.
type Tmpfs struct {
	Destination string   `json:"destination"`
	Options     []string `json:"options,omitempty"`
}

// Message types on the control stream. The guest connects to the host and
// both sides exchange newline-delimited JSON.
const (
	// Guest -> host.
	MsgStarted = "started" // Payload: Pid of the container process.
	MsgExit    = "exit"    // Payload: Code, the container process exit code.
	MsgAck     = "ack"     // Payload: ID of the command, OK, Error.
	MsgLog     = "log"     // Payload: Text; diagnostics from init.

	// Host -> guest.
	MsgSignal = "signal" // Payload: Signal to deliver to the container process.
	MsgFreeze = "freeze" // FIFREEZE the filesystem at Text (a guest mount path; empty means the root disk).
	MsgThaw   = "thaw"   // FITHAW the same.
)

// Message is one control frame.
type Message struct {
	Type   string `json:"type"`
	ID     uint64 `json:"id,omitempty"`
	Pid    int    `json:"pid,omitempty"`
	Code   int    `json:"code,omitempty"`
	Signal int    `json:"signal,omitempty"`
	OK     bool   `json:"ok,omitempty"`
	Error  string `json:"error,omitempty"`
	Text   string `json:"text,omitempty"`
}

// Encoder writes messages as one JSON object per line.
type Encoder struct{ w io.Writer }

func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w} }

func (e *Encoder) Encode(m Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = e.w.Write(append(data, '\n'))
	return err
}

// Decoder reads newline-delimited messages.
type Decoder struct{ r *bufio.Reader }

func NewDecoder(r io.Reader) *Decoder { return &Decoder{r: bufio.NewReader(r)} }

func (d *Decoder) Decode() (Message, error) {
	line, err := d.r.ReadBytes('\n')
	if err != nil {
		return Message{}, err
	}
	var m Message
	if err := json.Unmarshal(line, &m); err != nil {
		return Message{}, fmt.Errorf("decode control message %q: %w", line, err)
	}
	return m, nil
}
