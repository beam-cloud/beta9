package gatewayservices

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultObjectPutExpirationS    = 60 * 60 * 24
	workspaceObjectHashMetadataKey = "--content-sha256"

	// S3 multipart limits: parts (other than the last) must be >= 5 MiB, and
	// an upload has at most 10,000 parts.
	objectMultipartMinPartSize = 5 << 20
	objectMultipartMaxParts    = 10000
)

func (gws *GatewayService) ensureEmptyStubObject(ctx context.Context, workspace *types.Workspace) (types.Object, error) {
	return abstractions.EnsureEmptyStubObject(ctx, gws.backendRepo, workspace)
}

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	useWorkspaceStorage := authInfo.Workspace.StorageAvailable()
	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil {
		exists := true

		if !useWorkspaceStorage {
			objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
			if _, err := os.Stat(objectPath); os.IsNotExist(err) {
				exists = false
			}
		}

		if useWorkspaceStorage {
			storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
			if err != nil {
				return &pb.HeadObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create storage client",
				}, nil
			}
			if err := storageClient.EnsureLocalBucket(ctx); err != nil {
				return &pb.HeadObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to ensure workspace storage bucket",
				}, nil
			}

			objectExists, head, err := storageClient.Head(ctx, path.Join(types.DefaultObjectPrefix, existingObject.ExternalId))
			if err != nil {
				return &pb.HeadObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to check if object exists",
				}, nil
			}
			exists = objectExists
			if exists && in.SupportsPutHeaders && !workspaceObjectHasHashMetadata(head, existingObject.Hash) {
				exists = false
			}
		}

		if exists {
			return &pb.HeadObjectResponse{
				Ok:     true,
				Exists: true,
				ObjectMetadata: &pb.ObjectMetadata{
					Name: existingObject.Hash,
					Size: existingObject.Size,
				},
				ObjectId:            existingObject.ExternalId,
				UseWorkspaceStorage: useWorkspaceStorage,
			}, nil
		} else {
			return &pb.HeadObjectResponse{
				Ok:                  true,
				Exists:              false,
				UseWorkspaceStorage: useWorkspaceStorage,
			}, nil
		}
	}

	return &pb.HeadObjectResponse{
		Ok:                  true,
		Exists:              false,
		UseWorkspaceStorage: useWorkspaceStorage,
	}, nil
}

func workspaceObjectHasHashMetadata(head *s3.HeadObjectOutput, expectedHash string) bool {
	if head == nil || expectedHash == "" {
		return false
	}

	for key, value := range head.Metadata {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		normalizedKey = strings.TrimPrefix(normalizedKey, "x-amz-meta-")
		if (normalizedKey == workspaceObjectHashMetadataKey || normalizedKey == strings.TrimLeft(workspaceObjectHashMetadataKey, "-")) &&
			strings.TrimSpace(value) == expectedHash {
			return true
		}
	}

	return false
}

func (gws *GatewayService) CreateObject(ctx context.Context, in *pb.CreateObjectRequest) (*pb.CreateObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !authInfo.Workspace.StorageAvailable() {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Workspace storage is unavailable",
		}, nil
	}

	storageClient, err := clients.NewWorkspaceStorageClientWithDefaultPresignEndpoint(
		ctx,
		authInfo.Workspace.Name,
		authInfo.Workspace.Storage,
		gws.appConfig.Storage.WorkspaceStorage,
	)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create storage client",
		}, nil
	}
	if err := storageClient.EnsureLocalBucket(ctx); err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to ensure workspace storage bucket",
		}, nil
	}

	object, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil && !in.Overwrite {
		return &pb.CreateObjectResponse{
			Ok:       true,
			ObjectId: object.ExternalId,
		}, nil
	}

	if object == nil {
		object, err = gws.backendRepo.CreateObject(ctx, in.Hash, in.Size, authInfo.Workspace.Id)
		if err != nil {
			return &pb.CreateObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to create object",
			}, nil
		}
	}

	if in.MultipartPartSize > 0 && in.MultipartTotalSize > in.MultipartPartSize {
		uploadID, parts, err := gws.createMultipartObjectUpload(ctx, storageClient, object.ExternalId, in.Hash, in.MultipartTotalSize, in.MultipartPartSize)
		if err != nil {
			log.Warn().Err(err).Str("object_id", object.ExternalId).Msg("multipart object upload unavailable, falling back to single put")
		} else {
			return &pb.CreateObjectResponse{
				Ok:          true,
				ObjectId:    object.ExternalId,
				UploadId:    uploadID,
				UploadParts: parts,
			}, nil
		}
	}

	var (
		presignedURL string
		putHeaders   map[string]string
	)
	if in.SupportsPutHeaders && in.Hash != "" {
		presignedMetadata := map[string]string{}
		presignedMetadata[workspaceObjectHashMetadataKey] = in.Hash
		presignedURL, putHeaders, err = storageClient.GeneratePresignedPutURLWithMetadata(ctx, path.Join(types.DefaultObjectPrefix, object.ExternalId), defaultObjectPutExpirationS, presignedMetadata)
	} else {
		presignedURL, err = storageClient.GeneratePresignedPutURL(ctx, path.Join(types.DefaultObjectPrefix, object.ExternalId), defaultObjectPutExpirationS)
	}
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to generate presigned URL",
		}, nil
	}

	return &pb.CreateObjectResponse{
		Ok:           true,
		ObjectId:     object.ExternalId,
		PresignedUrl: presignedURL,
		PutHeaders:   putHeaders,
	}, nil
}

// createMultipartObjectUpload starts an S3 multipart upload for the object and
// presigns one UploadPart URL per part so the client can push the archive as
// concurrent streams. A single presigned PUT from a laptop runs at whatever one
// TCP stream to the bucket manages (a few MB/s on a typical uplink); parts in
// parallel fill the uplink.
func (gws *GatewayService) createMultipartObjectUpload(ctx context.Context, storageClient *clients.WorkspaceStorageClient, objectID, hash string, size, partSize int64) (string, []*pb.ObjectUploadPart, error) {
	if partSize < objectMultipartMinPartSize {
		partSize = objectMultipartMinPartSize
	}
	partCount, err := multipartPartCount(size, partSize)
	if err != nil {
		return "", nil, err
	}

	key := path.Join(types.DefaultObjectPrefix, objectID)
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(storageClient.BucketName()),
		Key:    aws.String(key),
	}
	if hash != "" {
		input.Metadata = map[string]string{workspaceObjectHashMetadataKey: hash}
	}
	created, err := storageClient.S3Client().CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", nil, fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := aws.ToString(created.UploadId)

	abort := func() {
		abortMultipartObjectUpload(ctx, storageClient.S3Client(), storageClient.BucketName(), key, uploadID)
	}

	parts := make([]*pb.ObjectUploadPart, 0, partCount)
	for i := int64(0); i < partCount; i++ {
		start := i * partSize
		end := min(start+partSize, size)
		presigned, err := storageClient.PresignClient().PresignUploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(storageClient.BucketName()),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(int32(i + 1)),
			ContentLength: aws.Int64(end - start),
		}, s3.WithPresignExpires(time.Duration(defaultObjectPutExpirationS)*time.Second))
		if err != nil {
			abort()
			return "", nil, fmt.Errorf("presign part %d: %w", i+1, err)
		}
		parts = append(parts, &pb.ObjectUploadPart{
			Number: uint32(i + 1),
			Start:  start,
			End:    end,
			Url:    presigned.URL,
		})
	}
	return uploadID, parts, nil
}

// multipartPartCount is the number of parts of partSize bytes needed to cover
// size bytes. Both come from the client, so the arithmetic must not overflow:
// size+partSize-1 wraps negative for a size near MaxInt64, and a negative
// count would panic in make.
func multipartPartCount(size, partSize int64) (int64, error) {
	if size <= 0 || partSize <= 0 {
		return 0, fmt.Errorf("invalid multipart upload of %d bytes in parts of %d", size, partSize)
	}
	partCount := size / partSize
	if size%partSize != 0 {
		partCount++
	}
	if partCount > objectMultipartMaxParts {
		return 0, fmt.Errorf("object of %d bytes needs %d parts of %d bytes, more than %d", size, partCount, partSize, objectMultipartMaxParts)
	}
	return partCount, nil
}

// abortMultipartObjectUpload discards an upload that will not complete, so
// its parts do not linger in the bucket. It runs detached from the request
// context: the request may already be canceled, and the abort must still
// reach the bucket.
func abortMultipartObjectUpload(ctx context.Context, client *s3.Client, bucket, key, uploadID string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	}); err != nil {
		log.Warn().Err(err).Str("key", key).Msg("abort multipart object upload failed")
	}
}

func (gws *GatewayService) CompleteObjectUpload(ctx context.Context, in *pb.CompleteObjectUploadRequest) (*pb.CompleteObjectUploadResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !authInfo.Workspace.StorageAvailable() {
		return &pb.CompleteObjectUploadResponse{Ok: false, ErrorMsg: "Workspace storage is unavailable"}, nil
	}
	if in.UploadId == "" || len(in.Parts) == 0 {
		return &pb.CompleteObjectUploadResponse{Ok: false, ErrorMsg: "Missing upload id or parts"}, nil
	}

	object, err := gws.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Workspace.Id)
	if err != nil {
		return &pb.CompleteObjectUploadResponse{Ok: false, ErrorMsg: "Object not found"}, nil
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return &pb.CompleteObjectUploadResponse{Ok: false, ErrorMsg: "Unable to create storage client"}, nil
	}

	completed := make([]s3types.CompletedPart, 0, len(in.Parts))
	for _, part := range in.Parts {
		completed = append(completed, s3types.CompletedPart{
			PartNumber: aws.Int32(int32(part.Number)),
			ETag:       aws.String(part.Etag),
		})
	}
	key := path.Join(types.DefaultObjectPrefix, object.ExternalId)
	_, err = storageClient.S3Client().CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(storageClient.BucketName()),
		Key:             aws.String(key),
		UploadId:        aws.String(in.UploadId),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		log.Warn().Err(err).Str("object_id", object.ExternalId).Msg("complete multipart object upload failed")
		// The client does not retry completion, so this upload is dead and
		// its parts should go.
		abortMultipartObjectUpload(ctx, storageClient.S3Client(), storageClient.BucketName(), key, in.UploadId)
		return &pb.CompleteObjectUploadResponse{Ok: false, ErrorMsg: "Unable to complete upload"}, nil
	}
	return &pb.CompleteObjectUploadResponse{Ok: true}, nil
}

func (gws *GatewayService) PutObjectStream(stream pb.GatewayService_PutObjectStreamServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return status.Error(codes.PermissionDenied, "Unauthorized Access")
	}

	objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
	if err := os.MkdirAll(objectPath, 0755); err != nil {
		return stream.SendAndClose(&pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create object directory",
		})
	}

	var size int
	var file *os.File
	var newObject *types.Object

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to receive stream of bytes",
			})
		}

		if file == nil {
			newObject, err = gws.backendRepo.CreateObject(ctx, request.Hash, 0, authInfo.Workspace.Id)
			if err != nil {
				return stream.SendAndClose(&pb.PutObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create object",
				})
			}

			file, err = os.Create(path.Join(objectPath, newObject.ExternalId))
			if err != nil {
				gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
				return stream.SendAndClose(&pb.PutObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create file",
				})
			}
			defer file.Close()
		}

		s, err := file.Write(request.ObjectContent)
		if err != nil {
			os.Remove(path.Join(objectPath, newObject.ExternalId))
			gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to write file content",
			})
		}
		size += s
	}

	if err := gws.backendRepo.UpdateObjectSizeByExternalId(ctx, newObject.ExternalId, size); err != nil {
		os.Remove(path.Join(objectPath, newObject.ExternalId))
		gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		return stream.SendAndClose(&pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to complete file upload",
		})
	}

	return stream.SendAndClose(&pb.PutObjectResponse{
		Ok:       true,
		ObjectId: newObject.ExternalId,
	})
}
