package types

// The machine-root durable disk contract, shared between the gateway (which
// expands a persistent root request into a disk entry) and the worker (which
// recognizes the mount and hosts the container's overlay upper layer on it).
const (
	// DurableDiskRootMountPath marks a durable disk as the machine root
	// instead of an ordinary bind mount.
	DurableDiskRootMountPath = "/"
	// DurableDiskDefaultRootName names the root disk when the request does
	// not provide one.
	DurableDiskDefaultRootName = "root"
)

// DiskFilesystemExt4 is the filesystem qcow volumes are formatted with and
// the value recorded in snapshot rows and manifests.
const DiskFilesystemExt4 = "ext4"

// QMP command names the qcow disk engine drives against qemu-storage-daemon.
const (
	QMPCommandCapabilities         = "qmp_capabilities"
	QMPCommandQuit                 = "quit"
	QMPCommandBlockdevAdd          = "blockdev-add"
	QMPCommandBlockdevDel          = "blockdev-del"
	QMPCommandBlockdevSnapshot     = "blockdev-snapshot"
	QMPCommandTransaction          = "transaction"
	QMPCommandQueryBlockstats      = "query-blockstats"
	QMPCommandQueryNamedBlockNodes = "query-named-block-nodes"
)
