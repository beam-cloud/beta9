package image

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
)

type ImageService interface {
	VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error)
	BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error
}

type RuncImageService struct {
	pb.UnimplementedImageServiceServer
	builder   *Builder
	scheduler *scheduler.Scheduler
}

func NewRuncImageService(ctx context.Context, scheduler *scheduler.Scheduler) (*RuncImageService, error) {
	builder, err := NewBuilder(scheduler)
	if err != nil {
		return nil, err
	}

	return &RuncImageService{
		builder: builder,
	}, nil
}

func (is *RuncImageService) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	var valid bool = true
	var exists bool

	imageTag, err := is.builder.GetImageTag(&BuildOpts{
		BaseImageName:    common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
		BaseImageTag:     is.getBaseImageTag(in.PythonVersion),
		PythonVersion:    in.PythonVersion,
		PythonPackages:   in.PythonPackages,
		Commands:         in.Commands,
		ExistingImageUri: in.ExistingImageUri,
	})
	if err != nil {
		valid = false
	}

	exists = is.builder.Exists(ctx, imageTag)

	return &pb.VerifyImageBuildResponse{
		ImageTag: imageTag,
		Exists:   exists,
		Valid:    valid,
	}, nil
}

func (is *RuncImageService) BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error {
	log.Printf("incoming image build request: %+v", in)

	buildOptions := &BuildOpts{
		BaseImageName:    common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
		BaseImageTag:     is.getBaseImageTag(in.PythonVersion),
		UserImageTag:     in.ImageTag,
		PythonVersion:    in.PythonVersion,
		PythonPackages:   in.PythonPackages,
		Commands:         in.Commands,
		ExistingImageUri: in.ExistingImageUri,
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)
	go is.builder.Build(ctx, buildOptions, outputChan)

	var lastMessage common.OutputMsg
	for o := range outputChan {
		if err := stream.Send(&pb.BuildImageResponse{Msg: o.Msg, Done: o.Done, Success: o.Success}); err != nil {
			log.Println("failed to complete build: ", err)
			lastMessage = o
			break
		}

		if o.Done {
			lastMessage = o
			break
		}
	}

	log.Println("Success: ", lastMessage.Success)
	return nil
}

// Generate and upload archived version of the image for distribution
func (b *Builder) Archive(ctx context.Context, bundlePath string, containerId string, opts *BuildOpts, outputChan chan common.OutputMsg) error {
	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s", opts.UserImageTag, b.registry.ImageFileExtension)
	archivePath := filepath.Join(filepath.Dir(bundlePath), archiveName)

	var err error = nil
	archiveStore := common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_REGISTRY_STORE", "s3")
	switch archiveStore {
	case "s3":
		err = clip.CreateAndUploadArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		}, &clipCommon.S3StorageInfo{
			Bucket: common.Secrets().Get("BEAM_IMAGESERVICE_IMAGE_REGISTRY_S3_BUCKET"),
			Region: common.Secrets().Get("BEAM_IMAGESERVICE_IMAGE_REGISTRY_S3_REGION"),
			Key:    fmt.Sprintf("%s.clip", opts.UserImageTag),
		})
	case "local":
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Printf("unable to create archive: %v\n", err)
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to archive image."}
		return err
	}
	log.Printf("container <%v> archive took %v", containerId, time.Since(startTime))

	// Push the archive to a registry
	startTime = time.Now()
	err = b.registry.Push(ctx, archivePath, opts.UserImageTag)
	if err != nil {
		log.Printf("failed to push image for container <%v>: %v", containerId, err)
		outputChan <- common.OutputMsg{Done: true, Success: false, Msg: "Unable to push image."}
		return err
	}

	log.Printf("container <%v> push took %v", containerId, time.Since(startTime))
	log.Printf("container <%v> build completed successfully", containerId)
	return nil
}

// Get latest image tag from secrets
func (is *RuncImageService) getBaseImageTag(pythonVersion string) string {
	var baseImageTag string

	switch pythonVersion {
	case "python3.8":
		baseImageTag = fmt.Sprintf("py38-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))

	case "python3.9":
		baseImageTag = fmt.Sprintf("py39-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))

	case "python3.10":
		baseImageTag = fmt.Sprintf("py310-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))
	}

	return baseImageTag
}
