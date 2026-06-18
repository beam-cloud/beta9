package gatewayservices

import (
	"context"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultObjectPutExpirationS    = 60 * 60 * 24
	workspaceObjectHashMetadataKey = "--content-sha256"
)

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

	presignEndpointUrl := gws.defaultWorkspacePresignEndpointUrl(authInfo.Workspace.Storage)
	storageClient, err := clients.NewWorkspaceStorageClientWithPresignEndpoint(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage, presignEndpointUrl)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create storage client",
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

func (gws *GatewayService) defaultWorkspacePresignEndpointUrl(workspaceStorage *types.WorkspaceStorage) string {
	if workspaceStorage == nil || workspaceStorage.EndpointUrl == nil {
		return ""
	}

	storageConfig := gws.appConfig.Storage.WorkspaceStorage
	if storageConfig.DefaultPresignedEndpointUrl == "" {
		return ""
	}

	if !sameStorageEndpoint(*workspaceStorage.EndpointUrl, storageConfig.DefaultEndpointUrl) {
		return ""
	}

	return storageConfig.DefaultPresignedEndpointUrl
}

func sameStorageEndpoint(a, b string) bool {
	a = strings.TrimRight(strings.TrimSpace(a), "/")
	b = strings.TrimRight(strings.TrimSpace(b), "/")
	if a == "" || b == "" {
		return a == b
	}
	if a == b {
		return true
	}

	aURL, aErr := url.Parse(a)
	bURL, bErr := url.Parse(b)
	if aErr != nil || bErr != nil {
		return false
	}

	if !strings.EqualFold(aURL.Scheme, bURL.Scheme) {
		return false
	}

	return strings.EqualFold(aURL.Hostname(), bURL.Hostname()) &&
		effectiveURLPort(aURL) == effectiveURLPort(bURL)
}

func effectiveURLPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}

	switch strings.ToLower(u.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
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
