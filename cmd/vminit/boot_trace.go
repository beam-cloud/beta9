//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"golang.org/x/sys/unix"
)

const bootTraceEnv = "BEAM_VMINIT_BOOT_TRACE"

// Even fully JSON-escaped names keep a batch below MaxLineBytes.
const maxBootEvents = 64
const maxBootEventName = 128

var guestBootTrace = bootTimeline{}

type bootTimeline struct {
	mu      sync.Mutex
	enabled bool
	events  []microvm.BootEvent
	send    func(microvm.Message) error
}

func (b *bootTimeline) record(name string) {
	if len(name) > maxBootEventName {
		name = name[:maxBootEventName]
	}
	now := time.Now()
	event := microvm.BootEvent{Name: name, TimeUnixNano: now.UnixNano(), ElapsedUS: now.Sub(bootStart).Microseconds()}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == maxBootEvents {
		return
	}
	if b.enabled && strings.HasPrefix(name, "boot phase: ") {
		event.CPU = readBootCPU()
	}
	b.events = append(b.events, event)
	if b.enabled && b.send != nil {
		_ = b.send(microvm.Message{Type: microvm.MsgBootTrace, BootTrace: []microvm.BootEvent{event}})
	}
}

func readBootCPU() *microvm.BootCPU {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil
	}
	cpu := parseBootCPU(data)
	if cpu == nil {
		return nil
	}
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) == nil {
		cpu.ProcessUserUS = usage.Utime.Nano() / 1000
		cpu.ProcessSystemUS = usage.Stime.Nano() / 1000
	}
	return cpu
}

func parseBootCPU(data []byte) *microvm.BootCPU {
	line, _, _ := strings.Cut(string(data), "\n")
	fields := strings.Fields(line)
	if len(fields) < 9 || fields[0] != "cpu" {
		return nil
	}
	var ticks [8]uint64
	for i := range ticks {
		value, err := strconv.ParseUint(fields[i+1], 10, 64)
		if err != nil {
			return nil
		}
		ticks[i] = value
	}
	return &microvm.BootCPU{UserTicks: ticks[0] + ticks[1], SystemTicks: ticks[2] + ticks[5] + ticks[6], IdleTicks: ticks[3], IOWaitTicks: ticks[4], StealTicks: ticks[7]}
}

func (b *bootTimeline) connect(send func(microvm.Message) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.send = send
	if b.enabled && len(b.events) > 0 {
		_ = send(microvm.Message{Type: microvm.MsgBootTrace, BootTrace: b.events})
	}
}

func (b *bootTimeline) handoff() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.enabled {
		return ""
	}
	data, _ := json.Marshal(struct {
		Started int64               `json:"started"`
		Events  []microvm.BootEvent `json:"events"`
	}{bootStart.UnixNano(), b.events})
	return string(data)
}

func (b *bootTimeline) restore(data string) {
	if data == "" || len(data) > microvm.MaxLineBytes {
		return
	}
	var previous struct {
		Started int64               `json:"started"`
		Events  []microvm.BootEvent `json:"events"`
	}
	if json.Unmarshal([]byte(data), &previous) != nil || previous.Started <= 0 || len(previous.Events) > maxBootEvents {
		return
	}
	for _, event := range previous.Events {
		if len(event.Name) > maxBootEventName {
			return
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enabled, b.events = true, previous.Events
	bootStart = time.Unix(0, previous.Started)
}

func flushBootTraceConsole() {
	guestBootTrace.mu.Lock()
	defer guestBootTrace.mu.Unlock()
	for _, event := range guestBootTrace.events {
		fmt.Fprintf(os.Stdout, "vminit[+%.3fs]: %s\n", float64(event.ElapsedUS)/1e6, event.Name)
	}
}
