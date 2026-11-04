//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
)

func TestBootTimelineHandoffAndLateEvents(t *testing.T) {
	previousStart := bootStart
	t.Cleanup(func() { bootStart = previousStart })
	parent := bootTimeline{enabled: true}
	parent.record("boot phase: init entered")
	parent.record("boot mount: index=0 destination=/volumes duration_us=12")
	child := bootTimeline{}
	child.restore(parent.handoff())
	if !child.enabled || len(child.events) != 2 || !bootStart.Equal(previousStart) {
		t.Fatal("startup measurements did not survive the systemd agent exec")
	}
	var messages []microvm.Message
	child.connect(func(msg microvm.Message) error { messages = append(messages, msg); return nil })
	child.record("boot phase: exec TCP listening")
	if len(messages) != 2 || len(messages[0].BootTrace) != 2 || len(messages[1].BootTrace) != 1 {
		t.Fatalf("expected one batch followed by the late readiness measurement: %+v", messages)
	}
	for _, message := range messages {
		for _, event := range message.BootTrace {
			if event.TimeUnixNano <= 0 || event.ElapsedUS < 0 {
				t.Fatal("missing timing boundary")
			}
		}
	}
}

func TestBootCPUAccounting(t *testing.T) {
	for _, invalid := range []string{"", "cpu0 1 2 3 4 5 6 7 8", "cpu 1 2 3 4", "cpu 1 bad 3 4 5 6 7 8"} {
		if parseBootCPU([]byte(invalid)) != nil {
			t.Fatalf("accepted invalid aggregate counters: %q", invalid)
		}
	}
	cpu := parseBootCPU([]byte("cpu 10 2 30 40 5 6 7 80 9 10\ncpu0 0 0 0 0 0 0 0 0\n"))
	if cpu == nil || cpu.UserTicks != 12 || cpu.SystemTicks != 43 || cpu.IdleTicks != 40 || cpu.IOWaitTicks != 5 || cpu.StealTicks != 80 {
		t.Fatalf("incorrect guest accounting: %+v", cpu)
	}
}

func TestBootTimelineBoundsAndDisabledTrace(t *testing.T) {
	b := bootTimeline{enabled: true}
	for i := 0; i < maxBootEvents+10; i++ {
		b.record(strings.Repeat("\x00", 512))
	}
	if len(b.events) != maxBootEvents {
		t.Fatal("unbounded boot measurements")
	}
	var wire bytes.Buffer
	b.connect(func(msg microvm.Message) error { return microvm.NewEncoder(&wire).Encode(msg) })
	if wire.Len() >= microvm.MaxLineBytes {
		t.Fatalf("boot trace exceeds control message limit: %d", wire.Len())
	}
	if _, err := microvm.NewDecoder(&wire).Decode(); err != nil {
		t.Fatal(err)
	}
	if len(b.handoff()) >= microvm.MaxLineBytes {
		t.Fatal("handoff exceeds limit")
	}
	disabled := bootTimeline{}
	disabled.record("boot phase: init entered")
	disabled.connect(func(microvm.Message) error { t.Fatal("disabled trace sent a message"); return nil })
	if disabled.handoff() != "" {
		t.Fatal("disabled trace inherited by agent")
	}
	for _, data := range []string{"invalid", `{"started":1,"events":[{"name":"` + strings.Repeat("x", maxBootEventName+1) + `"}]}`} {
		var next bootTimeline
		next.restore(data)
		if next.enabled {
			t.Fatal("invalid handoff accepted")
		}
	}
	var payload microvm.Message
	if err := json.Unmarshal([]byte(`{"type":"started","pid":1}`), &payload); err != nil || payload.Pid != 1 {
		t.Fatal("legacy control messages changed")
	}
}
