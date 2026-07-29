package agent

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

func TestSetupThunderNodeInstallsWithTailscaleIP(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true, EnrollmentToken: "tr_node"}}
	var commands []string
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		commands = append(commands, command)
		return nil
	})
	defer restore()

	err := setupThunderNode(context.Background(), client, "agent-token", []netip.Addr{
		netip.MustParseAddr("fd7a:115c:a1e0::1"),
		netip.MustParseAddr("100.64.0.10"),
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 {
		t.Fatalf("install command count = %d", len(commands))
	}
	if !strings.Contains(commands[0], "THUNDER_INSTALL_MODE=thunderd") || !strings.Contains(commands[0], "THUNDERD_IP='100.64.0.10'") || !strings.Contains(commands[0], "THUNDER_ENROLLMENT_TOKEN='tr_node'") {
		t.Fatalf("install command = %q", commands[0])
	}
	if client.createAgentToken != "agent-token" {
		t.Fatalf("create agent token = %q", client.createAgentToken)
	}
	if client.deleteCalls != 0 {
		t.Fatalf("delete calls = %d", client.deleteCalls)
	}
}

func TestSetupThunderNodeSkipsInstallerWhenAlreadyEnrolled(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true}}
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		t.Fatalf("installer should not run for an existing enrollment: %s", command)
		return nil
	})
	defer restore()

	if err := setupThunderNode(context.Background(), client, "agent-token", []netip.Addr{netip.MustParseAddr("100.64.0.10")}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSetupThunderNodeDeletesEnrollmentWhenInstallFails(t *testing.T) {
	client := &fakeGatewayNodeEnrollmentClient{createResp: &pb.CreateNodeEnrollmentResponse{Ok: true, EnrollmentToken: "tr_node"}, deleteResp: &pb.DeleteNodeEnrollmentResponse{Ok: true}}
	restore := stubThunderNodeInstallCommand(func(ctx context.Context, command string) error {
		return errors.New("install failed")
	})
	defer restore()

	err := setupThunderNode(context.Background(), client, "agent-token", []netip.Addr{netip.MustParseAddr("100.64.0.10")}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "install failed") {
		t.Fatalf("setupThunderNode() error = %v", err)
	}
	if client.deleteCalls != 1 || client.deleteAgentToken != "agent-token" {
		t.Fatalf("delete calls = %d token = %q", client.deleteCalls, client.deleteAgentToken)
	}
}

func TestThunderReachableNodeIPPrefersIPv4(t *testing.T) {
	got, err := thunderReachableNodeIP([]netip.Addr{
		netip.MustParseAddr("fd7a:115c:a1e0::1"),
		netip.MustParseAddr("100.64.0.10"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "100.64.0.10" {
		t.Fatalf("reachable IP = %q", got)
	}
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
