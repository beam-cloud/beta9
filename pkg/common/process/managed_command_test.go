package process

import (
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestManagedCommandTerminateKillsAndWaits(t *testing.T) {
	cmd, err := StartManagedCommand("sh", []string{"-c", "trap '' TERM; sleep 60"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pid := cmd.PID()

	if err := cmd.Terminate(100 * time.Millisecond); err != nil {
		t.Fatalf("terminate failed: %v, output=%s", err, cmd.OutputString())
	}

	if err := syscall.Kill(pid, 0); err == nil || !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("expected process %d to be gone after terminate, kill(0) err=%v", pid, err)
	}
	if _, done := cmd.DoneErr(); !done {
		t.Fatal("managed command was not reaped")
	}
}
