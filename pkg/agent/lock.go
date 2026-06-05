package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/types"
)

type agentLock struct {
	file *os.File
}

func acquireAgentLock() (*agentLock, error) {
	stateDir, err := agentStateDir()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(stateDir, types.AgentLockFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("another beam-agent is already running for state dir %s", stateDir)
		}
		return nil, err
	}

	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	_, _ = fmt.Fprintf(file, "pid=%d\n", os.Getpid())

	return &agentLock{file: file}, nil
}

func (l *agentLock) release() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}
