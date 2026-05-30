package worker

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type gatewayContainerEnvironment struct {
	grpcHost string
	grpcPort string
	httpHost string
	httpPort string
}

func (s *Worker) getContainerEnvironment(request *types.ContainerRequest, options *ContainerOptions) []string {
	gatewayEnv := s.gatewayContainerEnvironment()

	// Most of these env vars are required to communicate with the gateway and vice versa.
	env := []string{
		fmt.Sprintf("BIND_PORT=%d", containerInnerPort),
		fmt.Sprintf("%s=%s", types.ContainerEnvHostname, fmt.Sprintf("%s:%d", s.podAddr, options.BindPorts[0])),
		fmt.Sprintf("CONTAINER_ID=%s", request.ContainerId),
		fmt.Sprintf("%s=%s", types.ContainerEnvGatewayGRPCHost, gatewayEnv.grpcHost),
		fmt.Sprintf("%s=%s", types.ContainerEnvGatewayGRPCPort, gatewayEnv.grpcPort),
		fmt.Sprintf("%s=%s", types.ContainerEnvGatewayHTTPHost, gatewayEnv.httpHost),
		fmt.Sprintf("%s=%s", types.ContainerEnvGatewayHTTPPort, gatewayEnv.httpPort),
		fmt.Sprintf("STORAGE_AVAILABLE=%t", request.StorageAvailable()),
		"PYTHONUNBUFFERED=1",
	}

	env = append(request.Env, env...)
	if options.InitialSpec != nil {
		env = append(options.InitialSpec.Process.Env, env...)
	}

	return env
}

func (s *Worker) gatewayContainerEnvironment() gatewayContainerEnvironment {
	return gatewayContainerEnvironment{
		grpcHost: gatewayHostValue(
			os.Getenv(types.ContainerEnvGatewayGRPCHost),
			s.config.GatewayService.GRPC.ExternalHost,
			s.config.GatewayService.Host,
		),
		grpcPort: gatewayPortValue(
			os.Getenv(types.ContainerEnvGatewayGRPCPort),
			s.config.GatewayService.GRPC.ExternalPort,
			s.config.GatewayService.GRPC.Port,
			s.config.GatewayService.GRPC.TLS,
		),
		httpHost: gatewayHostValue(
			os.Getenv(types.ContainerEnvGatewayHTTPHost),
			s.config.GatewayService.HTTP.ExternalHost,
			s.config.GatewayService.Host,
		),
		httpPort: gatewayPortValue(
			os.Getenv(types.ContainerEnvGatewayHTTPPort),
			s.config.GatewayService.HTTP.ExternalPort,
			s.config.GatewayService.HTTP.Port,
			s.config.GatewayService.HTTP.TLS,
		),
	}
}

func gatewayHostValue(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if ip := net.ParseIP(value); ip != nil {
			return value
		}
		if strings.Contains(value, "://") {
			if u, err := url.Parse(value); err == nil && u.Hostname() != "" {
				return u.Hostname()
			}
		}
		host, _, err := net.SplitHostPort(value)
		if err == nil && host != "" {
			return strings.Trim(host, "[]")
		}
		return strings.Trim(value, "[]")
	}
	return ""
}

func gatewayPortValue(envValue string, configuredExternalPort, configuredPort int, tls bool) string {
	if envValue = strings.TrimSpace(envValue); envValue != "" {
		return envValue
	}
	for _, port := range []int{configuredExternalPort, configuredPort} {
		if port > 0 {
			return strconv.Itoa(port)
		}
	}
	if tls {
		return "443"
	}
	return "80"
}
