package types

const (
	DefaultGatewayServiceName          string = "gateway"
	DefaultExtractedObjectPath         string = "/data/unpacked"
	DefaultVolumesPath                 string = "/data/volumes"
	DefaultObjectPath                  string = "/data/objects"
	DefaultOutputsPath                 string = "/data/outputs"
	DefaultFilesystemName              string = "beta9-fs"
	DefaultFilesystemPath              string = "/data"
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
