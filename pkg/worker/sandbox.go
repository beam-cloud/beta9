package worker

import (
	"io"
	"os/exec"
	"time"
)

type SandboxProcessState struct {
	Pid       int
	ExitCode  int
	Stdin     io.WriteCloser
	Stdout    io.ReadCloser
	Stderr    io.ReadCloser
	StartTime time.Time
	EndTime   time.Time
	Status    string // "running", "exited", "error"
	Error     error
}

type SandboxProcessIO struct {
	stdinR  io.ReadCloser
	stdinW  io.WriteCloser
	stdoutR io.ReadCloser
	stdoutW io.WriteCloser
	stderrR io.ReadCloser
	stderrW io.WriteCloser
}

func NewSandboxProcessIO() *SandboxProcessIO {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	return &SandboxProcessIO{
		stdinR:  stdinR,
		stdinW:  stdinW,
		stdoutR: stdoutR,
		stdoutW: stdoutW,
		stderrR: stderrR,
		stderrW: stderrW,
	}
}

func (p *SandboxProcessIO) Close() error {
	p.stdinR.Close()
	p.stdinW.Close()
	p.stdoutR.Close()
	p.stdoutW.Close()
	p.stderrR.Close()
	p.stderrW.Close()
	return nil
}

func (p *SandboxProcessIO) Stdin() io.WriteCloser { return p.stdinW }
func (p *SandboxProcessIO) Stdout() io.ReadCloser { return p.stdoutR }
func (p *SandboxProcessIO) Stderr() io.ReadCloser { return p.stderrR }

func (p *SandboxProcessIO) Set(cmd *exec.Cmd) {
	cmd.Stdin = p.stdinR
	cmd.Stdout = p.stdoutW
	cmd.Stderr = p.stderrW
}
