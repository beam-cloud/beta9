package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Option replaces one of the seams through which the scheduler reaches the
// outside world. Production passes none; tests and the simulator in
// pkg/scheduler/sim pass fakes.
type Option func(*seams)

type seams struct {
	clock         clock.Clock
	kube          kubernetes.Interface
	spawn         func(func())
	computeEvents func(types.EventComputeSchema)
}

func newSeams(opts []Option) seams {
	s := seams{}
	for _, opt := range opts {
		opt(&s)
	}
	s.clock = clock.OrReal(s.clock)
	return s
}

// WithClock sets the scheduler's source of time, including the time its pool
// controllers and their loops see.
func WithClock(c clock.Clock) Option {
	return func(s *seams) { s.clock = c }
}

// WithKubernetes sets the client Kubernetes pools create worker Jobs with.
// Without it they use the in-cluster client.
func WithKubernetes(client kubernetes.Interface) Option {
	return func(s *seams) { s.kube = client }
}

// WithInlineBackgroundWork runs work the scheduler would hand to a goroutine,
// such as adding a worker, on the calling goroutine instead. Each scheduling
// pass is then complete when it returns.
func WithInlineBackgroundWork() Option {
	return func(s *seams) { s.spawn = func(fn func()) { fn() } }
}

// WithComputeEvents receives the placement and pool events the scheduler
// would otherwise publish to the admin workspace's compute stream.
func WithComputeEvents(sink func(types.EventComputeSchema)) Option {
	return func(s *seams) { s.computeEvents = sink }
}
