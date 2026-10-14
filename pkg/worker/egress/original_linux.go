//go:build linux

package egress

import (
	"encoding/binary"
	"fmt"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

const originalDestinationOption = 80

// SO_ORIGINAL_DST returns the destination before PREROUTING REDIRECT.
func originalDestination(conn net.Conn) (*net.TCPAddr, error) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("original destination requires TCP")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return nil, err
	}
	var address [128]byte
	var sockErr error
	level := unix.SOL_IP
	if tcp.RemoteAddr().(*net.TCPAddr).IP.To4() == nil {
		level = unix.SOL_IPV6
	}
	err = raw.Control(func(fd uintptr) {
		size := uint32(len(address))
		_, _, errno := unix.Syscall6(unix.SYS_GETSOCKOPT, fd, uintptr(level), originalDestinationOption, uintptr(unsafe.Pointer(&address[0])), uintptr(unsafe.Pointer(&size)), 0)
		if errno != 0 {
			sockErr = errno
		}
	})
	if err != nil {
		return nil, err
	}
	if sockErr != nil {
		return nil, sockErr
	}
	port := int(binary.BigEndian.Uint16(address[2:4]))
	if level == unix.SOL_IPV6 {
		return &net.TCPAddr{IP: net.IP(append([]byte(nil), address[8:24]...)), Port: port}, nil
	}
	return &net.TCPAddr{IP: net.IP(append([]byte(nil), address[4:8]...)), Port: port}, nil
}
