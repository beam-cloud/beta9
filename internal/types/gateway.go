package types

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofrs/uuid"
)

const (
	DefaultObjectPath        string        = "/data/objects"
	DefaultFilesystemPath    string        = "/data"
	FailedContainerThreshold int           = 3
	RequestTimeoutDurationS  time.Duration = 175 * time.Second
)

type RequestBucketType uint

const (
	RequestBucketTypeDeployment RequestBucketType = iota
	RequestBucketTypeServe
)

type InvalidAppResponse struct {
	AppId string `json:"app_id,omitempty"`
	Error string `json:"error_msg,omitempty"`
}

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

type RequestBucket interface {
	ForwardRequest(*gin.Context) error
	GetName() string
	GetContainerEventChan() chan ContainerEvent
	Close()
}

type ContainerEvent struct {
	ContainerId string
	Change      int
}

type RequestBucketState struct {
	RunningContainers  int
	PendingContainers  int
	StoppingContainers int
	FailedContainers   int
}

var ErrBucketNotInUse error = errors.New("bucket not in use")

// TaskMessage represents a JSON serializable message
// to be added to the task queue
type TaskMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     *string                `json:"eta"`
	Expires *time.Time             `json:"expires"`
}

func (tm *TaskMessage) Reset() {
	tm.ID = uuid.Must(uuid.NewV4()).String()
	tm.Task = ""
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
		// NOTE: this is needed for backwards compatibility with existing tasks
		// Once all queues have been emptied, this block becomes unnecessary
		log.Println("Decoding deprecated task message.")

		jsonData, err := base64.StdEncoding.DecodeString(string(encodedData))
		if err != nil {
			return err
		}

		err = json.Unmarshal(jsonData, tm)
		if err != nil {
			return err
		}
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
