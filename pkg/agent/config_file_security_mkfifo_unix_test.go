//go:build !windows

package agent

import "syscall"

func mkfifoForTest(path string) error {
	return syscall.Mkfifo(path, 0600)
}
