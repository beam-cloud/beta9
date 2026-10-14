//go:build !linux

package egress

import (
	"fmt"
	"net"
)

func originalDestination(net.Conn) (*net.TCPAddr, error) {
	return nil, fmt.Errorf("transparent egress requires Linux")
}
