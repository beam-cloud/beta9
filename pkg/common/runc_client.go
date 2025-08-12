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

type RunCClient struct {
	ServiceUrl   string
	ServiceToken string
	conn         *grpc.ClientConn
	client       pb.RunCServiceClient
	existingConn net.Conn
}

func NewRunCClient(serviceUrl, serviceToken string, existingConn net.Conn) (*RunCClient, error) {
	client := &RunCClient{
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

func (c *RunCClient) connect() error {
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
	c.client = pb.NewRunCServiceClient(conn)
	return nil
}

func (c *RunCClient) Close() error {
	return c.conn.Close()
}

func (c *RunCClient) Status(containerId string) (*pb.RunCStatusResponse, error) {
	resp, err := c.client.RunCStatus(context.TODO(), &pb.RunCStatusRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) Exec(containerId, cmd string, env []string) (*pb.RunCExecResponse, error) {
	resp, err := c.client.RunCExec(context.TODO(), &pb.RunCExecRequest{ContainerId: containerId, Cmd: cmd, Env: env})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxExec(containerId, cmd string, env map[string]string, cwd string) (*pb.RunCSandboxExecResponse, error) {
	resp, err := c.client.RunCSandboxExec(context.TODO(), &pb.RunCSandboxExecRequest{ContainerId: containerId, Cmd: cmd, Env: env, Cwd: cwd})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxStatus(containerId string, pid int32) (*pb.RunCSandboxStatusResponse, error) {
	resp, err := c.client.RunCSandboxStatus(context.TODO(), &pb.RunCSandboxStatusRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxStdout(containerId string, pid int32) (*pb.RunCSandboxStdoutResponse, error) {
	resp, err := c.client.RunCSandboxStdout(context.TODO(), &pb.RunCSandboxStdoutRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxStderr(containerId string, pid int32) (*pb.RunCSandboxStderrResponse, error) {
	resp, err := c.client.RunCSandboxStderr(context.TODO(), &pb.RunCSandboxStderrRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxKill(containerId string, pid int32) (*pb.RunCSandboxKillResponse, error) {
	resp, err := c.client.RunCSandboxKill(context.TODO(), &pb.RunCSandboxKillRequest{ContainerId: containerId, Pid: pid})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxUploadFile(containerId, path string, data []byte, mode int32) (*pb.RunCSandboxUploadFileResponse, error) {
	resp, err := c.client.RunCSandboxUploadFile(context.TODO(), &pb.RunCSandboxUploadFileRequest{ContainerId: containerId, ContainerPath: path, Data: data, Mode: mode})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxDownloadFile(containerId, containerPath string) (*pb.RunCSandboxDownloadFileResponse, error) {
	resp, err := c.client.RunCSandboxDownloadFile(context.TODO(), &pb.RunCSandboxDownloadFileRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxDeleteFile(containerId, containerPath string) (*pb.RunCSandboxDeleteFileResponse, error) {
	resp, err := c.client.RunCSandboxDeleteFile(context.TODO(), &pb.RunCSandboxDeleteFileRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxCreateDirectory(containerId, containerPath string) (*pb.RunCSandboxCreateDirectoryResponse, error) {
	resp, err := c.client.RunCSandboxCreateDirectory(context.TODO(), &pb.RunCSandboxCreateDirectoryRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxDeleteDirectory(containerId, containerPath string) (*pb.RunCSandboxDeleteDirectoryResponse, error) {
	resp, err := c.client.RunCSandboxDeleteDirectory(context.TODO(), &pb.RunCSandboxDeleteDirectoryRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxStatFile(containerId, containerPath string) (*pb.RunCSandboxStatFileResponse, error) {
	resp, err := c.client.RunCSandboxStatFile(context.TODO(), &pb.RunCSandboxStatFileRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxListFiles(containerId, containerPath string) (*pb.RunCSandboxListFilesResponse, error) {
	resp, err := c.client.RunCSandboxListFiles(context.TODO(), &pb.RunCSandboxListFilesRequest{ContainerId: containerId, ContainerPath: containerPath})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxReplaceInFiles(containerId, containerPath, oldString, newString string) (*pb.RunCSandboxReplaceInFilesResponse, error) {
	resp, err := c.client.RunCSandboxReplaceInFiles(context.TODO(), &pb.RunCSandboxReplaceInFilesRequest{ContainerId: containerId, ContainerPath: containerPath, Pattern: oldString, NewString: newString})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxFindInFiles(containerId, containerPath, pattern string) (*pb.RunCSandboxFindInFilesResponse, error) {
	resp, err := c.client.RunCSandboxFindInFiles(context.TODO(), &pb.RunCSandboxFindInFilesRequest{ContainerId: containerId, ContainerPath: containerPath, Pattern: pattern})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) SandboxExposePort(containerId string, port int32) (*pb.RunCSandboxExposePortResponse, error) {
	resp, err := c.client.RunCSandboxExposePort(context.TODO(), &pb.RunCSandboxExposePortRequest{ContainerId: containerId, Port: port})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) Kill(containerId string) (*pb.RunCKillResponse, error) {
	resp, err := c.client.RunCKill(context.TODO(), &pb.RunCKillRequest{ContainerId: containerId})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (c *RunCClient) StreamLogs(ctx context.Context, containerId string, outputChan chan OutputMsg) error {
	stream, err := c.client.RunCStreamLogs(ctx, &pb.RunCStreamLogsRequest{ContainerId: containerId})
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

func (c *RunCClient) Archive(ctx context.Context, containerId, imageId string, outputChan chan OutputMsg) error {
	outputChan <- OutputMsg{Archiving: true, Done: false, Success: false, Msg: "\nSaving image, this may take a few minutes...\n"}
	stream, err := c.client.RunCArchive(ctx, &pb.RunCArchiveRequest{ContainerId: containerId,
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
				progressBar := generateProgressBar(int(resp.Progress), 100)
				outputChan <- OutputMsg{Msg: progressBar, Done: false, Archiving: true}
			}

			if resp.Done && resp.Success {
				return nil
			} else if resp.Done && !resp.Success {
				return errors.New("image archiving failed")
			}
		}
	}
}

func (c *RunCClient) SyncWorkspace(ctx context.Context, request *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	resp, err := c.client.RunCSyncWorkspace(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
