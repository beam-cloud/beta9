package types

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/gofrs/uuid"
)

const (
	DefaultExtractedObjectPath string        = "/data/unpacked"
	DefaultObjectPath          string        = "/data/objects"
	DefaultFilesystemName      string        = "beam-fs"
	DefaultFilesystemPath      string        = "/data"
	FailedContainerThreshold   int           = 3
	RequestTimeoutDurationS    time.Duration = 175 * time.Second
)

type QueueResponse struct {
	TaskId string `json:"task_id,omitempty"`
	Error  string `json:"error_msg,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

const (
	LatestRequestedVersion RequestedVersionType = iota
	DefaultRequestedVersion
	AnyRequestedVersion
)

type RequestedVersionType uint

type RequestedVersion struct {
	Type  RequestedVersionType
	Value uint
}

type ContainerEvent struct {
	ContainerId string
	Change      int
}

// TaskMessage represents a JSON serializable message
// to be added to the task queue
type TaskMessage struct {
	ID      string                 `json:"id"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Expires *time.Time             `json:"expires"`
}

func (tm *TaskMessage) Reset() {
	tm.ID = uuid.Must(uuid.NewV4()).String()
	tm.Args = nil
	tm.Kwargs = nil
}

// Encode returns a binary representation of the TaskMessage
func (tm *TaskMessage) Encode() ([]byte, error) {
	if tm.Args == nil {
		tm.Args = make([]interface{}, 0)
	}

	encodedData, err := json.Marshal(tm)
	if err != nil {
		return nil, err
	}

	return encodedData, err
}

// Decode initializes the TaskMessage fields from a byte array
func (tm *TaskMessage) Decode(encodedData []byte) error {
	err := json.Unmarshal(encodedData, tm)
	if err != nil {
		return err
	}

	return nil
}

// Returns a regex that can be used to match version strings in a subroute
// e.g. /<appId>/v123/
func GetAppVersionRegex() (*regexp.Regexp, error) {
	appVersionRegex, err := regexp.Compile(`^/v([0-9]+|latest)(/.*)?$`)
	if err != nil {
		return nil, err
	}

	return appVersionRegex, nil
}

// Parses version string (e.g. 2, 3) into uint.
// Returns nil if version is empty or "latest".
// Returns error if version is invalid.
func ParseAppVersion(version string) (*RequestedVersion, error) {
	v := &RequestedVersion{
		Type:  AnyRequestedVersion,
		Value: 0,
	}

	switch version {
	case "":
		v.Type = DefaultRequestedVersion
	case "latest":
		v.Type = LatestRequestedVersion
	default:
		parsedInt, err := strconv.Atoi(version)
		if err != nil {
			return nil, fmt.Errorf("unable to convert version <%v> into int: %v", version, err)
		}
		v.Value = uint(parsedInt)
	}

	return v, nil
}

type AvailableHost struct {
	Hostname    string
	ContainerId string
}
