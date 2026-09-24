package worker

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/microvm"
	"github.com/beam-cloud/beta9/pkg/runtime"
	pb "github.com/beam-cloud/beta9/proto"
)

// guestFS returns the runtime's in-guest filesystem when the container's
// writable layer is not visible from the host (microVM root disks). The
// sandbox file RPCs then run inside the guest instead of on the host overlay.
func guestFS(instance *ContainerInstance) (runtime.GuestFilesystem, bool) {
	if instance == nil || instance.Runtime == nil {
		return nil, false
	}
	gfs, ok := instance.Runtime.(runtime.GuestFilesystem)
	return gfs, ok
}

func (s *ContainerRuntimeServer) guestUploadFile(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxUploadFileRequest, containerPath string) (*pb.ContainerSandboxUploadFileResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpWrite, Path: containerPath, Offset: in.Offset, Length: int64(len(in.Data)), Mode: uint32(in.Mode)}
	if _, err := gfs.GuestFS(ctx, in.ContainerId, req, bytes.NewReader(in.Data), nil); err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to write file to %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxUploadFileResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) guestDownloadFile(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxDownloadFileRequest, containerPath string) (*pb.ContainerSandboxDownloadFileResponse, error) {
	var buf bytes.Buffer
	req := microvm.FSRequest{Op: microvm.FSOpRead, Path: containerPath, Offset: in.Offset, Length: int64(in.Length)}
	if _, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, &buf); err != nil {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to read file from %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxDownloadFileResponse{Ok: true, Data: buf.Bytes()}, nil
}

func (s *ContainerRuntimeServer) guestCreateDirectory(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxCreateDirectoryRequest, containerPath string) (*pb.ContainerSandboxCreateDirectoryResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpMkdir, Path: containerPath, Mode: uint32(in.Mode)}
	if _, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil); err != nil {
		return &pb.ContainerSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to create directory %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxCreateDirectoryResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) guestDeleteDirectory(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxDeleteDirectoryRequest, containerPath string) (*pb.ContainerSandboxDeleteDirectoryResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpRemove, Path: containerPath}
	if _, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil); err != nil {
		return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to delete directory %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) guestDeleteFile(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxDeleteFileRequest, containerPath string) (*pb.ContainerSandboxDeleteFileResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpRemove, Path: containerPath}
	if _, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil); err != nil {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to delete file %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxDeleteFileResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) guestStatFile(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxStatFileRequest, containerPath string) (*pb.ContainerSandboxStatFileResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpStat, Path: containerPath}
	reply, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil)
	if err != nil || reply.Info == nil {
		if err == nil {
			err = fmt.Errorf("no file info returned")
		}
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to stat file %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxStatFileResponse{Ok: true, FileInfo: guestFileInfo(*reply.Info)}, nil
}

func (s *ContainerRuntimeServer) guestListFiles(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxListFilesRequest, containerPath string) (*pb.ContainerSandboxListFilesResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpList, Path: containerPath}
	reply, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil)
	if err != nil {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to list files in %s: %s", containerPath, err.Error())}, nil
	}
	files := make([]*pb.FileInfo, 0, len(reply.Entries))
	for _, entry := range reply.Entries {
		files = append(files, guestFileInfo(entry))
	}
	return &pb.ContainerSandboxListFilesResponse{Ok: true, Files: files}, nil
}

func (s *ContainerRuntimeServer) guestReplaceInFiles(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxReplaceInFilesRequest, containerPath string) (*pb.ContainerSandboxReplaceInFilesResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpReplace, Path: containerPath, Pattern: in.Pattern, Replacement: in.NewString}
	if _, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil); err != nil {
		return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to replace in files at %s: %s", containerPath, err.Error())}, nil
	}
	return &pb.ContainerSandboxReplaceInFilesResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) guestFindInFiles(ctx context.Context, gfs runtime.GuestFilesystem, in *pb.ContainerSandboxFindInFilesRequest, containerPath string) (*pb.ContainerSandboxFindInFilesResponse, error) {
	req := microvm.FSRequest{Op: microvm.FSOpFind, Path: containerPath, Pattern: in.Pattern}
	reply, err := gfs.GuestFS(ctx, in.ContainerId, req, nil, nil)
	if err != nil {
		return &pb.ContainerSandboxFindInFilesResponse{ErrorMsg: fmt.Sprintf("failed to search for '%s' in %s: %s", in.Pattern, containerPath, err.Error())}, nil
	}
	results := make([]*pb.FileSearchResult, 0, len(reply.Results))
	for _, result := range reply.Results {
		matches := make([]*pb.FileSearchMatch, 0, len(result.Matches))
		for _, m := range result.Matches {
			matches = append(matches, &pb.FileSearchMatch{
				Range: &pb.FileSearchRange{
					Start: &pb.FileSearchPosition{Line: m.Line, Column: m.StartCol},
					End:   &pb.FileSearchPosition{Line: m.Line, Column: m.EndCol},
				},
				Content: m.Content,
			})
		}
		results = append(results, &pb.FileSearchResult{Path: result.Path, Matches: matches})
	}
	return &pb.ContainerSandboxFindInFilesResponse{Ok: true, Results: results}, nil
}

func guestFileInfo(info microvm.FSFileInfo) *pb.FileInfo {
	return &pb.FileInfo{
		Name:        info.Name,
		IsDir:       info.IsDir,
		Size:        info.Size,
		Mode:        int32(info.Mode),
		ModTime:     info.ModTime,
		Permissions: info.Mode,
		Owner:       strconv.Itoa(int(info.UID)),
		Group:       strconv.Itoa(int(info.GID)),
	}
}
