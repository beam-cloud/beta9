package image

import (
	"context"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/pkg/errors"
)

type ImageService interface {
	pb.ImageServiceServer
	VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error)
	BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error
}

type RuncImageService struct {
	pb.UnimplementedImageServiceServer
	builder   *Builder
	scheduler *scheduler.Scheduler
	config    types.ImageServiceConfig
}

func NewRuncImageService(
	ctx context.Context,
	config types.ImageServiceConfig,
	scheduler *scheduler.Scheduler,
	containerRepo repository.ContainerRepository,
) (ImageService, error) {
	registry, err := common.NewImageRegistry(config)
	if err != nil {
		return nil, err
	}

	builder, err := NewBuilder(registry, scheduler, containerRepo)
	if err != nil {
		return nil, err
	}

	return &RuncImageService{
		builder: builder,
		config:  config,
	}, nil
}

func (is *RuncImageService) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	var valid bool = true

	imageId, err := is.builder.GetImageId(&BuildOpts{
		BaseImageTag:      is.config.Runner.Tags[in.PythonVersion],
		BaseImageName:     is.config.Runner.BaseImageName,
		BaseImageRegistry: is.config.Runner.BaseImageRegistry,
		PythonVersion:     in.PythonVersion,
		PythonPackages:    in.PythonPackages,
		Commands:          in.Commands,
		ExistingImageUri:  in.ExistingImageUri,
	})
	if err != nil {
		valid = false
	}

	return &pb.VerifyImageBuildResponse{
		ImageId: imageId,
		Exists:  is.builder.Exists(ctx, imageId),
		Valid:   valid,
	}, nil
}

func (is *RuncImageService) BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error {
	log.Printf("incoming image build request: %+v", in)

	buildOptions := &BuildOpts{
		BaseImageTag:      is.config.Runner.Tags[in.PythonVersion],
		BaseImageName:     is.config.Runner.BaseImageName,
		BaseImageRegistry: is.config.Runner.BaseImageRegistry,
		PythonVersion:     in.PythonVersion,
		PythonPackages:    in.PythonPackages,
		Commands:          in.Commands,
		ExistingImageUri:  in.ExistingImageUri,
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

	go is.builder.Build(ctx, buildOptions, outputChan)

	var lastMessage common.OutputMsg
	for o := range outputChan {
		if err := stream.Send(&pb.BuildImageResponse{Msg: o.Msg, Done: o.Done, Success: o.Success, ImageId: o.ImageId}); err != nil {
			log.Println("failed to complete build: ", err)
			lastMessage = o
			break
		}

		if o.Done {
			lastMessage = o
			break
		}
	}

	if !lastMessage.Success {
		log.Println("build failed")
		return errors.New("build failed")
	}

	log.Println("build completed successfully")
	return nil
}
