package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

var ErrStillRunning = errors.New("managed command still running")

type lockedBuffer struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (b lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

type ManagedCommand struct {
	cmd      *exec.Cmd
	done     chan struct{}
	errMu    sync.Mutex
	err      error
	outputMu sync.Mutex
	output   bytes.Buffer
}

func StartManagedCommand(name string, args []string, env []string) (*ManagedCommand, error) {
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if env != nil {
		cmd.Env = append(os.Environ(), env...)
	}

	c := &ManagedCommand{
		cmd:  cmd,
		done: make(chan struct{}),
	}
	w := lockedBuffer{mu: &c.outputMu, buf: &c.output}
	cmd.Stdout = w
	cmd.Stderr = w

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	go func() {
		err := cmd.Wait()
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		close(c.done)
	}()

	return c, nil
}

func (c *ManagedCommand) Wait(timeout time.Duration) error {
	if c == nil {
		return nil
	}

	if timeout <= 0 {
		<-c.done
		return c.result()
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c.done:
		return c.result()
	case <-timer.C:
		return ErrStillRunning
	}
}

func (c *ManagedCommand) DoneErr() (error, bool) {
	if c == nil {
		return nil, true
	}

	select {
	case <-c.done:
		return c.result(), true
	default:
		return nil, false
	}
}

func (c *ManagedCommand) result() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

func (c *ManagedCommand) Terminate(timeout time.Duration) error {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	if err, done := c.DoneErr(); done {
		return err
	}

	if err := c.signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal managed command: %w", err)
	}
	if err := c.Wait(timeout); !errors.Is(err, ErrStillRunning) {
		return nil
	}

	if err := c.signal(syscall.SIGKILL); err != nil {
		return fmt.Errorf("kill managed command: %w", err)
	}
	if err := c.Wait(timeout); errors.Is(err, ErrStillRunning) {
		return err
	}
	return nil
}

func (c *ManagedCommand) signal(sig syscall.Signal) error {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return nil
	}

	pid := c.cmd.Process.Pid
	if pgid, err := syscall.Getpgid(pid); err == nil {
		if err := syscall.Kill(-pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}

	if err := c.cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

func (c *ManagedCommand) OutputString() string {
	if c == nil {
		return ""
	}
	c.outputMu.Lock()
	defer c.outputMu.Unlock()
	return c.output.String()
}

func (c *ManagedCommand) PID() int {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return 0
	}
	return c.cmd.Process.Pid
}

func RunCommandWithTimeout(timeout time.Duration, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return output, ctx.Err()
	}
	return output, err
}
