//go:build windows

package agent

import "errors"

func mkfifoForTest(path string) error {
	return errors.New("not supported")
}
