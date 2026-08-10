package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

func TestSetupThunderNodeInstallsWithCacheLocalityPrivateIP(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true, EnrollmentToken: "tr_node"}}
	restoreIP := stubThunderNodeIP("10.0.0.10", nil)
	defer restoreIP()
	var commands []string
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		commands = append(commands, command)
		return nil
	})
	defer restore()

	err := setupThunderNode(context.Background(), client, "agent-token", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 {
		t.Fatalf("install command count = %d", len(commands))
	}
	if !strings.Contains(commands[0], "THUNDER_INSTALL_MODE=thunderd") || !strings.Contains(commands[0], "THUNDERD_IP='10.0.0.10'") || !strings.Contains(commands[0], "THUNDER_ENROLLMENT_TOKEN='tr_node'") {
		t.Fatalf("install command = %q", commands[0])
	}
	if client.createAgentToken != "agent-token" {
		t.Fatalf("create agent token = %q", client.createAgentToken)
	}
	if client.deleteCalls != 0 {
		t.Fatalf("delete calls = %d", client.deleteCalls)
	}
}

func TestSetupThunderNodeSkipsInstallerWhenEnrollmentFails(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createErr: errors.New("gateway unavailable")}
	restoreIP := stubThunderNodeIP("10.0.0.10", nil)
	defer restoreIP()
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		t.Fatalf("installer should not run when enrollment fails: %s", command)
		return nil
	})
	defer restore()

	err := setupThunderNode(context.Background(), client, "agent-token", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "gateway unavailable") {
		t.Fatalf("setupThunderNode() error = %v", err)
	}
}

func TestSetupThunderNodeSkipsInstallerWhenAlreadyEnrolled(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true}}
	restoreIP := stubThunderNodeIP("10.0.0.10", nil)
	defer restoreIP()
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		t.Fatalf("installer should not run for an existing enrollment: %s", command)
		return nil
	})
	defer restore()

	if err := setupThunderNode(context.Background(), client, "agent-token", nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSetupThunderNodeDeletesEnrollmentWhenInstallFails(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true, EnrollmentToken: "tr_node"}, deleteResp: &pb.DeleteNodeEnrollmentResponse{Ok: true}}
	restoreIP := stubThunderNodeIP("10.0.0.10", nil)
	defer restoreIP()
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		return errors.New("install failed")
	})
	defer restore()

	err := setupThunderNode(context.Background(), client, "agent-token", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "install failed") {
		t.Fatalf("setupThunderNode() error = %v", err)
	}
	if client.deleteCalls != 1 || client.deleteAgentToken != "agent-token" {
		t.Fatalf("delete calls = %d token = %q", client.deleteCalls, client.deleteAgentToken)
	}
}

func TestSetupThunderNodeSkipsInstallerWhenPrivateIPMissing(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true, EnrollmentToken: "tr_node"}}
	restoreIP := stubThunderNodeIP("", errors.New("no private ip"))
	defer restoreIP()
	restoreInstall := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		t.Fatalf("installer should not run without a private IP: %s", command)
		return nil
	})
	defer restoreInstall()

	err := setupThunderNode(context.Background(), client, "agent-token", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no private ip") {
		t.Fatalf("setupThunderNode() error = %v", err)
	}
}

func TestDefaultRunThunderNodeInstallCommandPropagatesPipelineFailure(t *testing.T) {
	err := defaultRunThunderNodeInstallCommand(context.Background(), "printf download-failed >&2; false | true")
	if err == nil {
		t.Fatal("installer command succeeded despite pipeline failure")
	}
	if !strings.Contains(err.Error(), "download-failed") {
		t.Fatalf("installer command error = %v", err)
	}
}

func TestDefaultRunThunderNodeInstallCommandKillsProcessGroupOnTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := defaultRunThunderNodeInstallCommand(ctx, "sh -c 'sleep 10'")
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("installer command error = %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("installer command took %s after timeout", elapsed)
	}
}

func TestNewThunderNodeInstallCommandUsesPipefailAndProcessGroup(t *testing.T) {
	cmd := newThunderNodeInstallCommand(context.Background(), "true")
	if got := strings.Join(cmd.Args, " "); got != "bash -o pipefail -c true" {
		t.Fatalf("install command args = %q", got)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("install command should start a new process group")
	}
	if cmd.Cancel == nil {
		t.Fatalf("install command should have a process-group cancel hook")
	}
}

func stubThunderNodeIP(ip string, err error) func() {
	old := discoverThunderNodeIP
	discoverThunderNodeIP = func() (string, error) {
		return ip, err
	}
	return func() { discoverThunderNodeIP = old }
}

func stubThunderNodeInstallCommand(fn func(context.Context, string) error) func() {
	old := runThunderNodeInstallCommand
	runThunderNodeInstallCommand = fn
	return func() { runThunderNodeInstallCommand = old }
}

type fakeGatewayNodeEnrollmentClient struct {
	createResp *pb.CreateNodeEnrollmentResponse
	createErr  error
	deleteResp *pb.DeleteNodeEnrollmentResponse
	deleteErr  error

	createAgentToken string
	deleteAgentToken string
	deleteCalls      int
}

func (f *fakeGatewayNodeEnrollmentClient) CreateNodeEnrollment(ctx context.Context, in *pb.CreateNodeEnrollmentRequest, _ ...grpc.CallOption) (*pb.CreateNodeEnrollmentResponse, error) {
	f.createAgentToken = in.GetAgentToken()
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createResp != nil {
		return f.createResp, nil
	}
	return &pb.CreateNodeEnrollmentResponse{Ok: true}, nil
}

func (f *fakeGatewayNodeEnrollmentClient) DeleteNodeEnrollment(ctx context.Context, in *pb.DeleteNodeEnrollmentRequest, _ ...grpc.CallOption) (*pb.DeleteNodeEnrollmentResponse, error) {
	f.deleteCalls++
	f.deleteAgentToken = in.GetAgentToken()
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if f.deleteResp != nil {
		return f.deleteResp, nil
	}
	return &pb.DeleteNodeEnrollmentResponse{Ok: true}, nil
}
