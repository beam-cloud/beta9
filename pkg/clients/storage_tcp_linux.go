//go:build linux

package clients

import (
	"context"
	"net"
	"os"
	"syscall"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

func init() {
	client, err := storageTCPHTTPClient(os.Getenv("BETA9_STORAGE_TCP_CONGESTION"))
	if err != nil {
		log.Warn().Err(err).Msg("storage TCP experiment unavailable; retaining default transport")
		return
	}
	storageBaseHTTPClient = client
}

// One shared pool, with an opt-in algorithm only on its own storage sockets.
// Neither the host default nor repository and guest RPC transports change.
func storageTCPHTTPClient(mode string) (*awshttp.BuildableClient, error) {
	base := common.AWSHTTPClient()
	if mode != "bbr" {
		return base, nil
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, unix.IPPROTO_TCP)
	if err != nil {
		return nil, err
	}
	err = unix.SetsockoptString(fd, unix.IPPROTO_TCP, unix.TCP_CONGESTION, "bbr")
	unix.Close(fd)
	if err != nil {
		return nil, err
	}
	return base.WithDialerOptions(func(d *net.Dialer) {
		previous := d.ControlContext
		d.ControlContext = func(ctx context.Context, network, address string, raw syscall.RawConn) error {
			if previous != nil {
				if err := previous(ctx, network, address, raw); err != nil {
					return err
				}
			}
			var optionErr error
			if err := raw.Control(func(fd uintptr) {
				optionErr = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, "bbr")
			}); err != nil {
				return err
			}
			return optionErr
		}
	}), nil
}
