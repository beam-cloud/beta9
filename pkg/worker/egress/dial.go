package egress

import (
	"context"
	"net"
	"syscall"
)

func checkPublicAddress(_ context.Context, _, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || !publicIP(net.ParseIP(host)) {
		return errPrivateAddress
	}
	return nil
}
