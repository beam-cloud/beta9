package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os/exec"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

const thunderNodeInstallTimeout = 2 * time.Minute

var runThunderNodeInstallCommand = defaultRunThunderNodeInstallCommand

type nodeEnrollmentGatewayClient interface {
	CreateNodeEnrollment(context.Context, *pb.CreateNodeEnrollmentRequest, ...grpc.CallOption) (*pb.CreateNodeEnrollmentResponse, error)
	DeleteNodeEnrollment(context.Context, *pb.DeleteNodeEnrollmentRequest, ...grpc.CallOption) (*pb.DeleteNodeEnrollmentResponse, error)
}

func setupThunderNode(ctx context.Context, client nodeEnrollmentGatewayClient, agentToken string, tailscaleIPs []netip.Addr, stdout, stderr io.Writer) error {
	if client == nil {
		return fmt.Errorf("gateway client is required for Thunder node enrollment")
	}

	reachableIP, err := thunderReachableNodeIP(tailscaleIPs)
	if err != nil {
		return err
	}

	res, err := client.CreateNodeEnrollment(ctx, &pb.CreateNodeEnrollmentRequest{AgentToken: agentToken})
	if err != nil {
		return fmt.Errorf("create Thunder node enrollment: %w", err)
	}
	if res == nil || !res.Ok {
		return fmt.Errorf("create Thunder node enrollment: %s", firstNonEmpty(res.GetErrorMsg(), "gateway rejected request"))
	}

	enrollmentToken := strings.TrimSpace(res.EnrollmentToken)
	if enrollmentToken == "" {
		verbosef(stdout, "Thunder node enrollment already exists\n")
		return nil
	}

	cmd := thunderNodeInstallCommand(reachableIP, enrollmentToken)
	installCtx, cancel := context.WithTimeout(ctx, thunderNodeInstallTimeout)
	defer cancel()

	statusf(stdout, "Installing Thunder node")
	if err := runThunderNodeInstallCommand(installCtx, cmd); err != nil {
		deleteThunderNodeEnrollment(context.Background(), client, agentToken, stderr)
		return fmt.Errorf("install Thunder node: %w", err)
	}
	statusf(stdout, "Thunder node ready")
	return nil
}

func deleteThunderNodeEnrollment(ctx context.Context, client nodeEnrollmentGatewayClient, agentToken string, stderr io.Writer) {
	if client == nil {
		return
	}
	if stderr == nil {
		stderr = io.Discard
	}
	deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := client.DeleteNodeEnrollment(deleteCtx, &pb.DeleteNodeEnrollmentRequest{AgentToken: agentToken})
	if err != nil {
		fmt.Fprintf(stderr, "failed to delete Thunder node enrollment: %v\n", err)
		return
	}
	if res == nil || !res.Ok {
		fmt.Fprintf(stderr, "failed to delete Thunder node enrollment: %s\n", firstNonEmpty(res.GetErrorMsg(), "gateway rejected request"))
	}
}

func thunderReachableNodeIP(ips []netip.Addr) (string, error) {
	for _, ip := range ips {
		if ip.IsValid() && ip.Is4() {
			return ip.String(), nil
		}
	}
	for _, ip := range ips {
		if ip.IsValid() {
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("tailscale IP address is required for Thunder node enrollment")
}

func hostTailscaleIPs() []netip.Addr {
	iface, err := net.InterfaceByName("tailscale0")
	if err != nil {
		return nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}

	ips := make([]netip.Addr, 0, len(addrs))
	for _, addr := range addrs {
		prefix, err := netip.ParsePrefix(addr.String())
		if err == nil {
			ips = append(ips, prefix.Addr())
			continue
		}
		if ip, err := netip.ParseAddr(addr.String()); err == nil {
			ips = append(ips, ip)
		}
	}
	return ips
}

func thunderNodeInstallCommand(reachableIP, enrollmentToken string) string {
	return "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_INSTALL_MODE=thunderd THUNDERD_IP=" + common.ShellQuote(reachableIP) + " THUNDER_ENROLLMENT_TOKEN=" + common.ShellQuote(enrollmentToken) + " sh"
}

func defaultRunThunderNodeInstallCommand(ctx context.Context, command string) error {
	out, err := exec.CommandContext(ctx, "sh", "-c", command).CombinedOutput()
	if err != nil {
		output := strings.TrimSpace(string(out))
		if output != "" {
			return fmt.Errorf("%w: %s", err, output)
		}
		return err
	}
	return nil
}
