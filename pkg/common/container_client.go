package common

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ContainerClient struct {
	ServiceUrl   string
	ServiceToken string
	conn         *grpc.ClientConn
	client       pb.ContainerServiceClient
	existingConn net.Conn
}

func NewContainerClient(serviceUrl, serviceToken string, existingConn net.Conn) (*ContainerClient, error) {
	client := &ContainerClient{
		ServiceUrl:   serviceUrl,
		ServiceToken: serviceToken,
		existingConn: existingConn,
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *ContainerClient) connect() error {
	grpcOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	isTLS := strings.HasSuffix(c.ServiceUrl, "443")
	if isTLS {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		grpcOption = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{grpcOption}

	// Use existingConn if provided
	if c.existingConn != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return c.existingConn, nil
		}))
	}

	maxMessageSize := 1 << 30 // 1Gi
	if c.ServiceToken != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(GRPCClientAuthInterceptor(c.ServiceToken)),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageSize),
				grpc.MaxCallSendMsgSize(maxMessageSize),
			))
	}

	conn, err := grpc.Dial(c.ServiceUrl, dialOpts...)
	if err != nil {
		return err
	}

	c.conn = conn
	c.client = pb.NewContainerServiceClient(conn)
	return nil
}

func (c *ContainerClient) Close() error {
	return c.conn.Close()
}

func (c *ContainerClient) Status(containerId string) (*pb.ContainerStatusResponse, error) {
	resp, err := c.client.ContainerStatus(context.TODO(), &pb.ContainerStatusRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) Exec(containerId, cmd string, env []string) (*pb.ContainerExecResponse, error) {
	resp, err := c.client.ContainerExec(context.TODO(), &pb.ContainerExecRequest{ContainerId: containerId, Cmd: cmd, Env: env})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxExec(containerId, cmd string, env map[string]string, cwd string) (*pb.ContainerSandboxExecResponse, error) {
	resp, err := c.client.ContainerSandboxExec(context.TODO(), &pb.ContainerSandboxExecRequest{ContainerId: containerId, Cmd: cmd, Env: env, Cwd: cwd})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxListExposedPorts(containerId string) (*pb.ContainerSandboxListExposedPortsResponse, error) {
	resp, err := c.client.ContainerSandboxListExposedPorts(context.TODO(), &pb.ContainerSandboxListExposedPortsRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxListProcesses(containerId string) (*pb.ContainerSandboxListProcessesResponse, error) {
	resp, err := c.client.ContainerSandboxListProcesses(context.TODO(), &pb.ContainerSandboxListProcessesRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxStatus(containerId string, pid int32) (*pb.ContainerSandboxStatusResponse, error) {
	resp, err := c.client.ContainerSandboxStatus(context.TODO(), &pb.ContainerSandboxStatusRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxStdout(containerId string, pid int32) (*pb.ContainerSandboxStdoutResponse, error) {
	resp, err := c.client.ContainerSandboxStdout(context.TODO(), &pb.ContainerSandboxStdoutRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxStderr(containerId string, pid int32) (*pb.ContainerSandboxStderrResponse, error) {
	resp, err := c.client.ContainerSandboxStderr(context.TODO(), &pb.ContainerSandboxStderrRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxKill(containerId string, pid int32) (*pb.ContainerSandboxKillResponse, error) {
	resp, err := c.client.ContainerSandboxKill(context.TODO(), &pb.ContainerSandboxKillRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (c *ContainerClient) SandboxUploadFile(containerId, path string, data []byte, mode int32) (*pb.ContainerSandboxUploadFileResponse, error) {
	resp, err := c.client.ContainerSandboxUploadFile(context.TODO(), &pb.ContainerSandboxUploadFileRequest{ContainerId: containerId, ContainerPath: path, Data: data, Mode: mode})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxDownloadFile(containerId, containerPath string) (*pb.ContainerSandboxDownloadFileResponse, error) {
	resp, err := c.client.ContainerSandboxDownloadFile(context.TODO(), &pb.ContainerSandboxDownloadFileRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxDeleteFile(containerId, containerPath string) (*pb.ContainerSandboxDeleteFileResponse, error) {
	resp, err := c.client.ContainerSandboxDeleteFile(context.TODO(), &pb.ContainerSandboxDeleteFileRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxCreateDirectory(containerId, containerPath string, mode int32) (*pb.ContainerSandboxCreateDirectoryResponse, error) {
	resp, err := c.client.ContainerSandboxCreateDirectory(context.TODO(), &pb.ContainerSandboxCreateDirectoryRequest{ContainerId: containerId, ContainerPath: containerPath, Mode: mode})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxDeleteDirectory(containerId, containerPath string) (*pb.ContainerSandboxDeleteDirectoryResponse, error) {
	resp, err := c.client.ContainerSandboxDeleteDirectory(context.TODO(), &pb.ContainerSandboxDeleteDirectoryRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxStatFile(containerId, containerPath string) (*pb.ContainerSandboxStatFileResponse, error) {
	resp, err := c.client.ContainerSandboxStatFile(context.TODO(), &pb.ContainerSandboxStatFileRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxListFiles(containerId, containerPath string) (*pb.ContainerSandboxListFilesResponse, error) {
	resp, err := c.client.ContainerSandboxListFiles(context.TODO(), &pb.ContainerSandboxListFilesRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxReplaceInFiles(containerId, containerPath, oldString, newString string) (*pb.ContainerSandboxReplaceInFilesResponse, error) {
	resp, err := c.client.ContainerSandboxReplaceInFiles(context.TODO(), &pb.ContainerSandboxReplaceInFilesRequest{ContainerId: containerId, ContainerPath: containerPath, Pattern: oldString, NewString: newString})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxFindInFiles(containerId, containerPath, pattern string) (*pb.ContainerSandboxFindInFilesResponse, error) {
	resp, err := c.client.ContainerSandboxFindInFiles(context.TODO(), &pb.ContainerSandboxFindInFilesRequest{ContainerId: containerId, ContainerPath: containerPath, Pattern: pattern})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) SandboxExposePort(containerId string, port int32) (*pb.ContainerSandboxExposePortResponse, error) {
	resp, err := c.client.ContainerSandboxExposePort(context.TODO(), &pb.ContainerSandboxExposePortRequest{ContainerId: containerId, Port: port})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) Kill(containerId string) (*pb.ContainerKillResponse, error) {
	resp, err := c.client.ContainerKill(context.TODO(), &pb.ContainerKillRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) StreamLogs(ctx context.Context, containerId string, outputChan chan OutputMsg) error {
	stream, err := c.client.ContainerStreamLogs(ctx, &pb.ContainerStreamLogsRequest{ContainerId: containerId})
	if err != nil {
		return fmt.Errorf("error creating log stream: %w", err)
	}

	// Keepalive for streaming logs
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				outputChan <- OutputMsg{Msg: ""}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			logEntry, err := stream.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				return fmt.Errorf("error receiving from log stream: %w", err)
			}

			if logEntry.Msg != "" {
				outputChan <- OutputMsg{Msg: logEntry.Msg}
			}
		}
	}
}

func generateProgressBar(progress int, total int) string {
	barWidth := 50
	progressWidth := (progress * barWidth) / total
	remainingWidth := barWidth - progressWidth

	progressBar := "[" +
		fmt.Sprintf("%s%s",
			strings.Repeat("=", progressWidth),
			strings.Repeat(" ", remainingWidth)) +
		"]"

	percent := (progress * 100) / total

	up := ""
	if percent > 0 {
		up = "\033[A"
	}

	return fmt.Sprintf("%s\r%s %d%%\n", up, progressBar, (progress*100)/total)
}

func (c *ContainerClient) Checkpoint(ctx context.Context, containerId string) (*pb.ContainerCheckpointResponse, error) {
	resp, err := c.client.ContainerCheckpoint(ctx, &pb.ContainerCheckpointRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *ContainerClient) Archive(ctx context.Context, containerId, imageId string, outputChan chan OutputMsg) error {
	outputChan <- OutputMsg{Archiving: true, Done: false, Success: false, Msg: "\nSaving image, this may take a few minutes...\n"}
	stream, err := c.client.ContainerArchive(ctx, &pb.ContainerArchiveRequest{ContainerId: containerId,
		ImageId: imageId})
	if err != nil {
		return fmt.Errorf("error creating archive stream: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			resp, err := stream.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				return fmt.Errorf("error receiving from archive stream: %w", err)
			}

			if resp.ErrorMsg != "" {
				outputChan <- OutputMsg{Msg: resp.ErrorMsg + "\n", Done: false, Archiving: true}
			}

			if !resp.Done && resp.ErrorMsg == "" {
				if resp.Progress == 0 {
					outputChan <- OutputMsg{Msg: ".", Done: false, Archiving: true}
				} else {
					progressBar := generateProgressBar(int(resp.Progress), 100)
					outputChan <- OutputMsg{Msg: progressBar, Done: false, Archiving: true}
				}
			}

			if resp.Done && resp.Success {
				return nil
			} else if resp.Done && !resp.Success {
				return errors.New("image archiving failed")
			}
		}
	}
}

func (c *ContainerClient) SyncWorkspace(ctx context.Context, request *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	resp, err := c.client.ContainerSyncWorkspace(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
