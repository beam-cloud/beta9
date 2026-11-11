package microvm

import (
	"fmt"
	"strings"
	"time"
)

// Steps lists how long each step of an operation took, for its log line.
type Steps struct {
	list strings.Builder
	last time.Time
}

// StartSteps starts timing the first step.
func StartSteps() *Steps { return &Steps{last: time.Now()} }

// Took ends the step that was running, under name, and starts the next.
func (s *Steps) Took(name string) {
	now := time.Now()
	fmt.Fprintf(&s.list, "%s=%dms ", name, now.Sub(s.last).Milliseconds())
	s.last = now
}

func (s *Steps) String() string { return strings.TrimSpace(s.list.String()) }
