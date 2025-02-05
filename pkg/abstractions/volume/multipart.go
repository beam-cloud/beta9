package volume

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/beam-cloud/beta9/pkg/auth"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	presignedUrlDefaultExpires = 300    // 5 minutes
	presignedUrlMaxExpires     = 604800 // 1 week
	baseVolumeObjectPath       = "volumes"
)

func (s *GlobalVolumeService) getS3Client() *s3.Client {
	region := s.config.Region
	if region == "" {
		region = "-"
	}

	return s3.New(s3.Options{
		Credentials: credentials.NewStaticCredentialsProvider(
			s.config.AccessKey,
			s.config.SecretKey,
			s.config.Region,
		),
		BaseEndpoint: aws.String(s.config.EndpointURL),
		UsePathStyle: true,
		Region:       region,
	})
}

func joinCleanPath(parts ...string) string {
	for i, part := range parts {
		parts[i] = filepath.Clean(part)
	}

	return filepath.Join(parts...)
}

func (s *GlobalVolumeService) GetFileServiceInfo(ctx context.Context, in *pb.GetFileServiceInfoRequest) (*pb.GetFileServiceInfoResponse, error) {
	return &pb.GetFileServiceInfoResponse{
		Ok:      true,
		Enabled: s.config.Enabled && s.config.EndpointURL != "" && s.config.BucketName != "",
	}, nil
}

func (s *GlobalVolumeService) CreatePresignedURL(ctx context.Context, in *pb.CreatePresignedURLRequest) (*pb.CreatePresignedURLResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volume, err := s.backendRepo.GetVolume(ctx, authInfo.Workspace.Id, in.VolumeName)
	if err != nil {
		return &pb.CreatePresignedURLResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	if in.Expires == 0 {
		in.Expires = presignedUrlDefaultExpires
	}
	if in.Expires > presignedUrlMaxExpires {
		in.Expires = presignedUrlMaxExpires
	}

	if in.Params == nil {
		in.Params = &pb.PresignedURLParams{}
	}

	var (
		req           *v4.PresignedHTTPRequest
		key           = joinCleanPath(baseVolumeObjectPath, authInfo.Workspace.Name, volume.ExternalId, in.VolumePath)
		presignClient = s3.NewPresignClient(s.getS3Client())
		options       = []func(*s3.PresignOptions){
			s3.WithPresignExpires(time.Duration(in.Expires) * time.Second),
		}
	)

	switch in.Method {
	case pb.PresignedURLMethod_HeadObject:
		req, err = presignClient.PresignHeadObject(
			ctx, &s3.HeadObjectInput{
				Bucket: aws.String(s.config.BucketName),
				Key:    aws.String(key),
			},
			options...,
		)
	case pb.PresignedURLMethod_GetObject:
		req, err = presignClient.PresignGetObject(
			ctx, &s3.GetObjectInput{
				Bucket: aws.String(s.config.BucketName),
				Key:    aws.String(key),
			},
			options...,
		)
	case pb.PresignedURLMethod_PutObject:
		req, err = presignClient.PresignPutObject(
			ctx, &s3.PutObjectInput{
				Bucket:        aws.String(s.config.BucketName),
				Key:           aws.String(key),
				ContentType:   aws.String(in.Params.ContentType),
				ContentLength: aws.Int64(int64(in.Params.ContentLength)),
			},
			options...,
		)
	case pb.PresignedURLMethod_UploadPart:
		req, err = presignClient.PresignUploadPart(
			ctx, &s3.UploadPartInput{
				Bucket:     aws.String(s.config.BucketName),
				Key:        aws.String(key),
				PartNumber: aws.Int32(int32(in.Params.PartNumber)),
				UploadId:   aws.String(in.Params.UploadId),
			},
			options...,
		)
	default:
		err = errors.New("Unsupported Method")
	}

	if err != nil {
		return &pb.CreatePresignedURLResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.CreatePresignedURLResponse{
		Ok:  true,
		Url: req.URL,
	}, nil
}

func (s *GlobalVolumeService) CreateMultipartUpload(ctx context.Context, in *pb.CreateMultipartUploadRequest) (*pb.CreateMultipartUploadResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volume, err := s.backendRepo.GetVolume(ctx, authInfo.Workspace.Id, in.VolumeName)
	if err != nil {
		return &pb.CreateMultipartUploadResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Volume '%s' not found\n", in.VolumeName),
		}, nil
	}

	s3Client := s.getS3Client()

	response, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:  aws.String(s.config.BucketName),
		Key:     aws.String(joinCleanPath(baseVolumeObjectPath, authInfo.Workspace.Name, volume.ExternalId, in.VolumePath)),
		Expires: aws.Time(time.Now().Add(time.Minute)),
	})
	if err != nil {
		return &pb.CreateMultipartUploadResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	totalParts := math.Ceil(float64(in.FileSize) / float64(in.ChunkSize))
	if in.FileSize == 0 && in.ChunkSize == 0 {
		// When file and chunk size are both 0, we assume that the file is empty
		totalParts = 1
	}
	uploadParts := make([]*pb.FileUploadPart, int(totalParts))

	for i := range uploadParts {
		res, err := s.CreatePresignedURL(ctx, &pb.CreatePresignedURLRequest{
			VolumeName: in.VolumeName,
			VolumePath: in.VolumePath,
			Expires:    presignedUrlMaxExpires,
			Method:     pb.PresignedURLMethod_UploadPart,
			Params: &pb.PresignedURLParams{
				UploadId:      *response.UploadId,
				PartNumber:    uint32(i + 1),
				ContentLength: in.ChunkSize,
			},
		})
		if err != nil {
			s.AbortMultipartUpload(ctx, &pb.AbortMultipartUploadRequest{
				UploadId:   *response.UploadId,
				VolumeName: in.VolumeName,
				VolumePath: in.VolumePath,
			})

			return &pb.CreateMultipartUploadResponse{
				Ok:     false,
				ErrMsg: err.Error(),
			}, nil
		}

		uploadParts[i] = &pb.FileUploadPart{
			Number: uint32(i + 1),
			Start:  uint64(i) * in.ChunkSize,
			End:    uint64(math.Min(float64(uint64(i+1)*in.ChunkSize), float64(in.FileSize))),
			Url:    res.Url,
		}
	}

	return &pb.CreateMultipartUploadResponse{
		Ok:              true,
		UploadId:        *response.UploadId,
		FileUploadParts: uploadParts,
	}, nil
}

func (s *GlobalVolumeService) CompleteMultipartUpload(ctx context.Context, in *pb.CompleteMultipartUploadRequest) (*pb.CompleteMultipartUploadResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volume, err := s.backendRepo.GetVolume(ctx, authInfo.Workspace.Id, in.VolumeName)
	if err != nil {
		return &pb.CompleteMultipartUploadResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Volume '%s' not found\n", in.VolumeName),
		}, nil
	}

	s3Client := s.getS3Client()
	_, err = s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.config.BucketName),
		Key:      aws.String(joinCleanPath(baseVolumeObjectPath, authInfo.Workspace.Name, volume.ExternalId, in.VolumePath)),
		UploadId: aws.String(in.UploadId),
		MultipartUpload: &s3Types.CompletedMultipartUpload{
			Parts: func() []s3Types.CompletedPart {
				parts := make([]s3Types.CompletedPart, len(in.CompletedParts))
				for i, part := range in.CompletedParts {
					parts[i] = s3Types.CompletedPart{
						ETag:       aws.String(part.Etag),
						PartNumber: aws.Int32(int32(part.Number)),
					}
				}
				return parts
			}(),
		},
	})

	if err != nil {
		return &pb.CompleteMultipartUploadResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.CompleteMultipartUploadResponse{
		Ok: true,
	}, nil
}

func (s *GlobalVolumeService) AbortMultipartUpload(ctx context.Context, in *pb.AbortMultipartUploadRequest) (*pb.AbortMultipartUploadResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volume, err := s.backendRepo.GetVolume(ctx, authInfo.Workspace.Id, in.VolumeName)
	if err != nil {
		return &pb.AbortMultipartUploadResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Volume '%s' not found\n", in.VolumeName),
		}, nil
	}

	s3Client := s.getS3Client()

	_, err = s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.config.BucketName),
		Key:      aws.String(joinCleanPath(baseVolumeObjectPath, authInfo.Workspace.Name, volume.ExternalId, in.VolumePath)),
		UploadId: aws.String(in.UploadId),
	})
	if err != nil {
		return &pb.AbortMultipartUploadResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.AbortMultipartUploadResponse{
		Ok: true,
	}, nil
}
