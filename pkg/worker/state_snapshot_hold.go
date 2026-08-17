package worker

import (
	"fmt"
	"sync"
)

// terminalStateSnapshotHold transfers teardown ownership from spawn to the
// terminal snapshot operation before CRIU can stop the runtime. spawn waits on
// done, so overlay/root state, the QSD group, and ContainerInstance remain
// available through durable publication and acknowledgement.
type terminalStateSnapshotHold struct {
	operationID   string
	mode          StateSnapshotMode
	includeMemory bool
	done          chan struct{}
	once          sync.Once

	mu             sync.RWMutex
	runtimeStopped bool
}

func (s *Worker) lockStateSnapshotOperation(containerID string) func() {
	value, _ := s.stateSnapshotOperationLocks.LoadOrStore(containerID, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func (i *ContainerInstance) beginTerminalStateSnapshot(operationID string, mode StateSnapshotMode, includeMemory bool) (*terminalStateSnapshotHold, error) {
	if i == nil || mode != StateSnapshotModeTerminal {
		return nil, nil
	}
	i.stateMu.Lock()
	defer i.stateMu.Unlock()
	if i.terminalStateSnapshot != nil {
		hold := i.terminalStateSnapshot
		if hold.operationID != operationID || hold.mode != mode || hold.includeMemory != includeMemory {
			return nil, fmt.Errorf("a different terminal state snapshot operation owns container teardown")
		}
		return hold, nil
	}
	hold := &terminalStateSnapshotHold{
		operationID: operationID, mode: mode, includeMemory: includeMemory, done: make(chan struct{}),
	}
	i.terminalStateSnapshot = hold
	return hold, nil
}

func (h *terminalStateSnapshotHold) markRuntimeStopped() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.runtimeStopped = true
	h.mu.Unlock()
}

func (h *terminalStateSnapshotHold) stopped() bool {
	if h == nil {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.runtimeStopped
}

func (i *ContainerInstance) finishTerminalStateSnapshot(hold *terminalStateSnapshotHold) {
	if i == nil || hold == nil {
		return
	}
	i.stateMu.Lock()
	if i.terminalStateSnapshot == hold {
		i.terminalStateSnapshot = nil
	}
	i.stateMu.Unlock()
	hold.once.Do(func() { close(hold.done) })
}

func (i *ContainerInstance) waitForTerminalStateSnapshot() {
	if i == nil {
		return
	}
	i.stateMu.RLock()
	hold := i.terminalStateSnapshot
	i.stateMu.RUnlock()
	if hold != nil {
		<-hold.done
	}
}
