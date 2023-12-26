package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// V1-V2 config format
type BeamUserAppConfig struct {
	AppSpecVersion string `json:"app_spec_version"`

	App struct {
		Name           string   `json:"name"`
		Cpu            string   `json:"cpu"`
		Gpu            GpuType  `json:"gpu"`
		Memory         string   `json:"memory"`
		PythonVersion  string   `json:"python_version"`
		PythonPackages []string `json:"python_packages"`
		Commands       []string `json:"commands,omitempty"`
		Workspace      string   `json:"workspace"`
	} `json:"app"`

	Triggers []TriggerDeprecated `json:"triggers"`

	Mounts []Mount `json:"mounts"`

	AutoScaling AutoScaling `json:"autoscaling"`
	AutoScaler  Autoscaler  `json:"autoscaler"`

	Outputs []Output `json:"outputs"`
}

func (c *BeamUserAppConfig) AsString() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return string(bytes)
}

type TriggerDeprecated struct {
	Inputs      map[string]map[string]any `json:"inputs"`
	Outputs     map[string]map[string]any `json:"outputs"`
	TriggerType string                    `json:"trigger_type"`
	Handler     string                    `json:"handler"`
	Loader      *string                   `json:"loader"`
	When        *string                   `json:"when"`

	// These fields are optional and depend on the particular trigger being used
	KeepWarmSeconds uint    `json:"keep_warm_seconds"`
	MaxPendingTasks uint    `json:"max_pending_tasks"`
	CallbackUrl     *string `json:"callback_url"`
}

func (t *TriggerDeprecated) AsString() string {
	bytes, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func ParseAppConfig(appConfigRaw string) (BeamUserAppConfig, error) {
	var appConfig BeamUserAppConfig

	configJson := strings.TrimSpace(string(appConfigRaw))

	err := json.Unmarshal([]byte(configJson), &appConfig)
	if err != nil {
		return appConfig, err
	}

	return appConfig, nil
}

func (r *BeamUserAppConfig) Scan(value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal string value:", value))
	}

	return json.Unmarshal([]byte(val), r)
}

func (r BeamUserAppConfig) Value() (driver.Value, error) {
	val, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// V3 config format
type BeamAppConfig struct {
	AppSpecVersion string     `json:"app_spec_version"`
	SdkVersion     *string    `json:"sdk_version"`
	Name           string     `json:"name"`
	Runtime        Runtime    `json:"runtime"`
	Mounts         []Mount    `json:"mounts"`
	Triggers       []Trigger  `json:"triggers"`
	Run            *RunConfig `json:"run"`
}

func (c *BeamAppConfig) AsString() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func (c *BeamAppConfig) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("Failed to unmarshal JSON value")
	}

	return json.Unmarshal(bytes, (*BeamAppConfig)(c))
}

type GpuType string

func (g *GpuType) UnmarshalJSON(data []byte) error {
	var gpuStr string
	err := json.Unmarshal(data, &gpuStr)
	if err == nil {
		*g = GpuType(gpuStr)
		return nil
	}

	var gpuInt int
	err = json.Unmarshal(data, &gpuInt)
	if err != nil {
		return err
	}

	if gpuInt == 0 {
		*g = GpuType("")
	} else if gpuInt > 0 {
		*g = GpuType("T4")
	}

	return nil
}

type Output struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	OutputType string `json:"output_type"`
}

type Runtime struct {
	Cpu    string  `json:"cpu"`
	Gpu    GpuType `json:"gpu"`
	Memory string  `json:"memory"`
	Image  Image   `json:"image"`
}

type TaskPolicy struct {
	MaxRetries uint `json:"max_retries"`
	Timeout    int  `json:"timeout"`
}

type RunConfig struct {
	Handler     string      `json:"handler"`
	CallbackURL *string     `json:"callback_url"`
	Outputs     []Output    `json:"outputs"`
	Runtime     *Runtime    `json:"runtime"`
	TaskPolicy  *TaskPolicy `json:"task_policy"`
}

type Image struct {
	Commands             []string `json:"commands"`
	PythonVersion        string   `json:"python_version"`
	PythonPackages       []string `json:"python_packages"`
	BaseImage            *string  `json:"base_image"`
	BaseImageCredentials *string  `json:"base_image_creds"`
}

type AutoScaling struct {
	AutoScalingType string  `json:"autoscaling_type"`
	DesiredLatency  float64 `json:"desired_latency"`
	MaxReplicas     uint    `json:"max_replicas"`
}

type RequestLatencyAutoscaler struct {
	DesiredLatency float64 `json:"desired_latency"`
	MaxReplicas    uint    `json:"max_replicas"`
}

type QueueDepthAutoscaler struct {
	MaxTasksPerReplica uint `json:"max_tasks_per_replica"`
	MaxReplicas        uint `json:"max_replicas"`
}

type Autoscaler struct {
	RequestLatency *RequestLatencyAutoscaler `json:"request_latency,omitempty"`
	QueueDepth     *QueueDepthAutoscaler     `json:"queue_depth,omitempty"`
}

func (a *Autoscaler) TakeNonNil() interface{} {
	if a.RequestLatency != nil {
		return a.RequestLatency
	}
	if a.QueueDepth != nil {
		return a.QueueDepth
	}
	return nil
}

type Trigger struct {
	Outputs     []Output    `json:"outputs"`
	TriggerType string      `json:"trigger_type"`
	Handler     string      `json:"handler"`
	Method      *string     `json:"method"`
	Path        *string     `json:"path"`
	Runtime     *Runtime    `json:"runtime"`
	Autoscaler  *Autoscaler `json:"autoscaler"`

	// This is deprecated
	AutoScaling *AutoScaling `json:"autoscaling"`

	// These fields are optional and depend on the particular trigger being used
	When            *string     `json:"when"`
	Loader          *string     `json:"loader"`
	KeepWarmSeconds *uint       `json:"keep_warm_seconds"`
	MaxPendingTasks *uint       `json:"max_pending_tasks"`
	CallbackUrl     *string     `json:"callback_url"`
	TaskPolicy      *TaskPolicy `json:"task_policy"`
	Workers         *uint       `json:"workers"`
	Authorized      *bool       `json:"authorized"`
}

func (t *Trigger) AsString() string {
	bytes, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(bytes)
}
