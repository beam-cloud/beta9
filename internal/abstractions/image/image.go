package image

import (
	"context"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	pb "github.com/beam-cloud/beam/proto"
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
		PythonVersion:    in.PythonVersion,
		PythonPackages:   in.PythonPackages,
		Commands:         in.Commands,
		ExistingImageUri: in.ExistingImageUri,
	}

	imageTag, err := is.builder.GetImageTag(buildOptions)
	if err != nil {
		return err
	}

	buildOptions.UserImageTag = imageTag

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

	go is.builder.Build(ctx, buildOptions, outputChan)

	var lastMessage common.OutputMsg
	for o := range outputChan {
		if err := stream.Send(&pb.BuildImageResponse{Msg: o.Msg, Done: o.Done, Success: o.Success, ImageTag: imageTag}); err != nil {
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
