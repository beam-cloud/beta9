package image

import (
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	imageVerifyPhaseTotal          = "total"
	imageVerifyPhaseBuildOptions   = "build_options"
	imageVerifyPhasePrepareOptions = "prepare_options"
	imageVerifyPhaseGetImageID     = "get_image_id"
	imageVerifyPhaseExists         = "exists"
	imageVerifyPhaseMetadata       = "metadata"
	imageVerifyPhaseRegistryExists = "registry_exists"

	imageVerifyAttrMetadataAuthoritative = "metadata_authoritative"
)

type imageVerifyRecorder struct {
	service   *ContainerImageService
	request   *pb.VerifyImageBuildRequest
	startedAt time.Time
}

func (is *ContainerImageService) newImageVerifyRecorder(in *pb.VerifyImageBuildRequest) imageVerifyRecorder {
	return imageVerifyRecorder{
		service:   is,
		request:   in,
		startedAt: time.Now(),
	}
}

func (r imageVerifyRecorder) measure(phase string, fn func() error, attrs ...string) error {
	startedAt := time.Now()
	err := fn()
	r.record(phase, time.Since(startedAt), err == nil, nil, labelPairs(attrs...))
	return err
}

func measureImageVerifyValue[T any](r imageVerifyRecorder, phase string, fn func() (T, error), attrs ...string) (T, error) {
	startedAt := time.Now()
	value, err := fn()
	r.record(phase, time.Since(startedAt), err == nil, nil, labelPairs(attrs...))
	return value, err
}

func (r imageVerifyRecorder) total(result *imageVerifyResult, err error) {
	r.record(imageVerifyPhaseTotal, time.Since(r.startedAt), err == nil, result, nil)
}

func (r imageVerifyRecorder) record(phase string, duration time.Duration, success bool, result *imageVerifyResult, extra map[string]string) {
	if r.service == nil {
		return
	}

	labels := map[string]string{
		"registry_store":    r.service.config.ImageService.RegistryStore,
		"clip_version":      fmt.Sprintf("%d", r.service.config.ImageService.ClipVersion),
		"explicit_image_id": boolLabel(r.request != nil && r.request.ImageId != nil && *r.request.ImageId != ""),
		"success":           boolLabel(success),
	}
	if r.request != nil {
		labels["python_version"] = r.request.PythonVersion
		labels["custom_base_image"] = boolLabel(r.request.ExistingImageUri != "")
		labels["dockerfile"] = boolLabel(r.request.Dockerfile != "")
	}
	if result != nil {
		labels["exists"] = boolLabel(result.exists)
		labels["valid"] = boolLabel(result.valid)
	}
	for key, value := range extra {
		labels[key] = value
	}

	metrics.RecordImageVerifyPhase(phase, duration, labels)
}

func labelPairs(values ...string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	labels := make(map[string]string, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		labels[values[i]] = values[i+1]
	}
	return labels
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
