package types

const (
	DefaultGatewayServiceName          string = "gateway"
	DefaultDurableDisksPath            string = "/var/lib/beta9/durable-disks"
	DefaultObjectPrefix                string = "objects"
	DefaultVolumesPrefix               string = "volumes"
	DefaultOutputsPrefix               string = "outputs"
	DefaultCheckpointsPrefix           string = "checkpoints"
	DefaultDurableDisksPrefix          string = "durable-disks"
	DurableDiskChunksPrefix            string = "chunks"
	DurableDiskSnapshotsPrefix         string = "snapshots"
	FailedDeploymentContainerThreshold int    = 3
	FailedContainerThreshold           int    = 1
)

type ContainerEvent struct {
	ContainerId string
	Change      int
}

type AvailableHost struct {
	Hostname    string
	ContainerId string
}

// @go2proto
type FileInfo struct {
	Name        string
	IsDir       bool
	Size        int64
	Mode        int32
	ModTime     int64
	Owner       string
	Group       string
	Path        string
	Permissions uint32
}

// @go2proto
type FileSearchPosition struct {
	Line   int32
	Column int32
}

// @go2proto
type FileSearchRange struct {
	Start FileSearchPosition
	End   FileSearchPosition
}

// @go2proto
type FileSearchMatch struct {
	Range   FileSearchRange
	Content string
}

// @go2proto
type FileSearchResult struct {
	Path    string
	Matches []FileSearchMatch
}
