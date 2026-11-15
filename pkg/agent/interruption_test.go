package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// awsMetadata stands in for the metadata service of an EC2 instance. It
// answers only in a session, as an instance that requires IMDSv2 does.
type awsMetadata struct {
	*httptest.Server
	mu       sync.Mutex
	sessions int
	token    string
	action   string
	asked    int
}

func newAWSMetadata(t *testing.T) *awsMetadata {
	t.Helper()
	m := &awsMetadata{}
	m.Server = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.Close)
	return m
}

func (m *awsMetadata) serve(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
		if r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") != "21600" {
			http.Error(w, "missing session lifetime", http.StatusBadRequest)
			return
		}
		m.sessions++
		m.token = fmt.Sprintf("session-%d", m.sessions)
		fmt.Fprint(w, m.token)
	case r.Method == http.MethodGet && r.URL.Path == "/latest/meta-data/spot/instance-action":
		if m.token == "" || r.Header.Get("X-aws-ec2-metadata-token") != m.token {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		m.asked++
		if m.action == "" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, m.action)
	default:
		http.NotFound(w, r)
	}
}

// notify makes AWS give the instance its notice.
func (m *awsMetadata) notify(action string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.action = action
}

// expireSession ends the session the instance was asked in.
func (m *awsMetadata) expireSession() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.token = ""
}

func (m *awsMetadata) source() *awsSpotNotice {
	return &awsSpotNotice{client: m.Client(), base: m.URL}
}

// gcpMetadata stands in for the metadata service of a Compute Engine VM.
type gcpMetadata struct {
	*httptest.Server
	mu        sync.Mutex
	preempted string
	status    int
	anonymous bool
}

func newGCPMetadata(t *testing.T) *gcpMetadata {
	t.Helper()
	m := &gcpMetadata{preempted: "FALSE"}
	m.Server = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.Close)
	return m
}

func (m *gcpMetadata) serve(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.Method != http.MethodGet || r.URL.Path != "/computeMetadata/v1/instance/preempted" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Metadata-Flavor") != "Google" {
		http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
		return
	}
	if !m.anonymous {
		w.Header().Set("Metadata-Flavor", "Google")
	}
	if m.status != 0 {
		w.WriteHeader(m.status)
		return
	}
	fmt.Fprintln(w, m.preempted)
}

func (m *gcpMetadata) set(change func(m *gcpMetadata)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	change(m)
}

func (m *gcpMetadata) source() *gcpPreemptionNotice {
	return &gcpPreemptionNotice{client: m.Client(), base: m.URL}
}

func TestAWSGivesNoNoticeUntilTheInstanceIsToBeTaken(t *testing.T) {
	aws := newAWSMetadata(t)
	source := aws.source()

	notice, err := source.check(context.Background())
	require.NoError(t, err)
	require.Nil(t, notice)

	aws.notify(`{"action": "terminate", "time": "2026-11-13T08:22:00Z"}`)
	notice, err = source.check(context.Background())
	require.NoError(t, err)
	require.Equal(t, &interruption{Detail: "terminate", At: time.Date(2026, 11, 13, 8, 22, 0, 0, time.UTC)}, notice)
	require.Equal(t, 1, aws.sessions, "one session is used for both")
}

func TestAWSNoticeOfAStopIsANoticeToo(t *testing.T) {
	aws := newAWSMetadata(t)
	aws.notify(`{"action": "stop", "time": "2026-11-13T08:22:00Z"}`)

	notice, err := aws.source().check(context.Background())
	require.NoError(t, err)
	require.Equal(t, "stop", notice.Detail)
}

// The item is missing until the instance is given notice, so its being there
// is the notice, whatever it holds.
func TestAWSNoticeThatCannotBeReadIsStillANotice(t *testing.T) {
	for _, body := range []string{"not json", `{}`, `{"time": "2026-11-13T08:22:00Z"}`} {
		aws := newAWSMetadata(t)
		aws.notify(body)

		notice, err := aws.source().check(context.Background())
		require.NoError(t, err, body)
		require.Equal(t, &interruption{Detail: "spot instance action"}, notice, body)
	}
}

func TestAWSOpensANewSessionWhenItsSessionRunsOut(t *testing.T) {
	aws := newAWSMetadata(t)
	source := aws.source()
	_, err := source.check(context.Background())
	require.NoError(t, err)

	aws.expireSession()
	aws.notify(`{"action": "terminate", "time": "2026-11-13T08:22:00Z"}`)
	notice, err := source.check(context.Background())
	require.NoError(t, err)
	require.Equal(t, "terminate", notice.Detail)
	require.Equal(t, 2, aws.sessions)
}

func TestAWSNoticeIsNotReadWhereThereIsNoAWS(t *testing.T) {
	t.Run("another cloud's metadata service", func(t *testing.T) {
		gcp := newGCPMetadata(t)
		source := &awsSpotNotice{client: gcp.Client(), base: gcp.URL}
		_, err := source.check(context.Background())
		require.ErrorContains(t, err, "no session (status 404)")
	})

	t.Run("a service that gives sessions without a token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		t.Cleanup(server.Close)
		source := &awsSpotNotice{client: server.Client(), base: server.URL}
		_, err := source.check(context.Background())
		require.ErrorContains(t, err, "no session (status 200)")
	})

	t.Run("nothing there", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		base := server.URL
		server.Close()
		source := &awsSpotNotice{client: http.DefaultClient, base: base}
		_, err := source.check(context.Background())
		require.Error(t, err)
	})

	t.Run("a metadata service in trouble", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				fmt.Fprint(w, "session")
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(server.Close)
		source := &awsSpotNotice{client: server.Client(), base: server.URL}
		notice, err := source.check(context.Background())
		require.ErrorContains(t, err, "status 503")
		require.Nil(t, notice)
	})
}

func TestGoogleGivesNoNoticeUntilTheVMIsPreempted(t *testing.T) {
	gcp := newGCPMetadata(t)
	source := gcp.source()

	notice, err := source.check(context.Background())
	require.NoError(t, err)
	require.Nil(t, notice)

	gcp.set(func(m *gcpMetadata) { m.preempted = "TRUE" })
	notice, err = source.check(context.Background())
	require.NoError(t, err)
	require.Equal(t, &interruption{Detail: "preempted"}, notice)
}

func TestGoogleNoticeIsNotReadWhereThereIsNoGoogle(t *testing.T) {
	t.Run("another cloud's metadata service", func(t *testing.T) {
		aws := newAWSMetadata(t)
		source := &gcpPreemptionNotice{client: aws.Client(), base: aws.URL}
		_, err := source.check(context.Background())
		require.ErrorContains(t, err, "status 404")
	})

	t.Run("a service that answers and is not Google's", func(t *testing.T) {
		gcp := newGCPMetadata(t)
		gcp.set(func(m *gcpMetadata) { m.anonymous, m.preempted = true, "TRUE" })
		notice, err := gcp.source().check(context.Background())
		require.ErrorContains(t, err, "status 200")
		require.Nil(t, notice)
	})

	t.Run("a metadata service in trouble", func(t *testing.T) {
		gcp := newGCPMetadata(t)
		gcp.set(func(m *gcpMetadata) { m.status = http.StatusServiceUnavailable })
		_, err := gcp.source().check(context.Background())
		require.ErrorContains(t, err, "status 503")
	})

	t.Run("an answer that is neither", func(t *testing.T) {
		gcp := newGCPMetadata(t)
		gcp.set(func(m *gcpMetadata) { m.preempted = "MAYBE" })
		notice, err := gcp.source().check(context.Background())
		require.ErrorContains(t, err, `preempted is "MAYBE"`)
		require.Nil(t, notice)
	})
}

// fakeGateway takes notices. Each report gets the next of its answers, and
// the last of them from then on.
type fakeGateway struct {
	mu       sync.Mutex
	answers  []func() (*pb.ReportAgentInterruptionResponse, error)
	requests []*pb.ReportAgentInterruptionRequest
}

func taken() (*pb.ReportAgentInterruptionResponse, error) {
	return &pb.ReportAgentInterruptionResponse{Ok: true}, nil
}

func (g *fakeGateway) ReportAgentInterruption(_ context.Context, in *pb.ReportAgentInterruptionRequest, _ ...grpc.CallOption) (*pb.ReportAgentInterruptionResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests = append(g.requests, in)
	if len(g.answers) == 0 {
		return taken()
	}
	answer := g.answers[min(len(g.requests), len(g.answers))-1]
	return answer()
}

func (g *fakeGateway) reports() []*pb.ReportAgentInterruptionRequest {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*pb.ReportAgentInterruptionRequest(nil), g.requests...)
}

// scriptedSource answers each check with the next of its answers, and with
// the last of them from then on.
type scriptedSource struct {
	provider string
	mu       sync.Mutex
	answers  []scriptedAnswer
	checks   int
}

type scriptedAnswer struct {
	notice *interruption
	err    error
}

var (
	notThisCloud = scriptedAnswer{err: errors.New("no such metadata service")}
	noNotice     = scriptedAnswer{}
)

func (s *scriptedSource) name() string { return s.provider }

func (s *scriptedSource) check(context.Context) (*interruption, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks++
	answer := s.answers[min(s.checks, len(s.answers))-1]
	return answer.notice, answer.err
}

func (s *scriptedSource) asked() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checks
}

// testWatcher is a watcher that does not wait between its questions.
func testWatcher(gateway interruptionReporter, sources ...noticeSource) (*interruptionWatcher, *bytes.Buffer) {
	var stderr bytes.Buffer
	return &interruptionWatcher{
		sources: sources, gateway: gateway, agentToken: "agent-token",
		stdout: io.Discard, stderr: &stderr,
		probes: 3, pollInterval: time.Millisecond, reportAttempts: 5, reportInterval: time.Millisecond,
	}, &stderr
}

// runWatcher runs the watcher to its end, which it has to reach on its own.
func runWatcher(t *testing.T, w *interruptionWatcher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.run(ctx)
	require.NoError(t, ctx.Err(), "the watcher did not stop on its own")
}

// The agent tells the gateway once the machine has its notice: which
// provider gave it, what it said, and when the machine goes.
func TestAgentPassesAnAWSNoticeOnToTheGateway(t *testing.T) {
	aws := newAWSMetadata(t)
	gateway := &fakeGateway{}
	watcher, _ := testWatcher(gateway, aws.source(), &gcpPreemptionNotice{client: aws.Client(), base: aws.URL})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); watcher.run(ctx) }()

	// It asks, and has nothing to say while there is no notice.
	require.Eventually(t, func() bool {
		aws.mu.Lock()
		defer aws.mu.Unlock()
		return aws.asked >= 3
	}, 5*time.Second, time.Millisecond)
	require.Empty(t, gateway.reports())

	before := time.Now()
	aws.notify(`{"action": "terminate", "time": "2026-11-13T08:22:00Z"}`)
	<-done
	require.NoError(t, ctx.Err(), "the watcher did not stop on its own")

	reports := gateway.reports()
	require.Len(t, reports, 1)
	require.Equal(t, "agent-token", reports[0].AgentToken)
	require.Equal(t, "aws", reports[0].Source)
	require.Equal(t, "terminate", reports[0].Detail)
	require.Equal(t, time.Date(2026, 11, 13, 8, 22, 0, 0, time.UTC).UnixNano(), reports[0].ExpectedAtUnixNano)
	require.GreaterOrEqual(t, reports[0].ObservedAtUnixNano, before.UnixNano())
	require.LessOrEqual(t, reports[0].ObservedAtUnixNano, time.Now().UnixNano())
}

// On a Google VM the AWS question gets no answer, and Google's is the one
// that is watched.
func TestAgentPassesAGoogleNoticeOnToTheGateway(t *testing.T) {
	gcp := newGCPMetadata(t)
	gcp.set(func(m *gcpMetadata) { m.preempted = "TRUE" })
	gateway := &fakeGateway{}
	watcher, _ := testWatcher(gateway, &awsSpotNotice{client: gcp.Client(), base: gcp.URL}, gcp.source())

	runWatcher(t, watcher)

	reports := gateway.reports()
	require.Len(t, reports, 1)
	require.Equal(t, "gcp", reports[0].Source)
	require.Equal(t, "preempted", reports[0].Detail)
	require.Zero(t, reports[0].ExpectedAtUnixNano, "Google does not say when")
}

// A machine that no provider answers on is given no notice, and is asked no
// more once that is clear.
func TestAgentStopsAskingAMachineThatIsGivenNoNotice(t *testing.T) {
	aws := &scriptedSource{provider: "aws", answers: []scriptedAnswer{notThisCloud}}
	gcp := &scriptedSource{provider: "gcp", answers: []scriptedAnswer{notThisCloud}}
	gateway := &fakeGateway{}
	watcher, stderr := testWatcher(gateway, aws, gcp)

	runWatcher(t, watcher)

	require.Equal(t, 3, aws.asked())
	require.Equal(t, 3, gcp.asked())
	require.Empty(t, gateway.reports())
	require.Empty(t, stderr.String(), "most machines are like this: it is not worth a line")
}

// A metadata service can be slow to come up. A machine that answers on a
// later try is watched.
func TestAgentWatchesAMachineThatAnswersOnALaterTry(t *testing.T) {
	aws := &scriptedSource{provider: "aws", answers: []scriptedAnswer{notThisCloud}}
	gcp := &scriptedSource{provider: "gcp", answers: []scriptedAnswer{notThisCloud, notThisCloud, noNotice, {notice: &interruption{Detail: "preempted"}}}}
	gateway := &fakeGateway{}
	watcher, _ := testWatcher(gateway, aws, gcp)

	runWatcher(t, watcher)

	reports := gateway.reports()
	require.Len(t, reports, 1)
	require.Equal(t, "gcp", reports[0].Source)
	require.Equal(t, 3, aws.asked(), "only the one that answered is asked from then on")
}

// A metadata service that stops answering for a while is asked again, and
// says so once.
func TestAgentKeepsWatchingThroughAMetadataOutage(t *testing.T) {
	outage := scriptedAnswer{err: errors.New("metadata service timed out")}
	aws := &scriptedSource{provider: "aws", answers: []scriptedAnswer{noNotice, noNotice, outage, outage, outage, noNotice, {notice: &interruption{Detail: "terminate"}}}}
	gateway := &fakeGateway{}
	watcher, stderr := testWatcher(gateway, aws)

	runWatcher(t, watcher)

	require.Len(t, gateway.reports(), 1)
	require.Equal(t, 1, bytes.Count(stderr.Bytes(), []byte("could not be read")), stderr.String())
	require.Contains(t, stderr.String(), "interruption notice from aws could not be read: metadata service timed out")
}

// The notice is told again until the gateway has taken it.
func TestAgentTellsTheGatewayAgainUntilItHasTheNotice(t *testing.T) {
	aws := &scriptedSource{provider: "aws", answers: []scriptedAnswer{{notice: &interruption{Detail: "terminate"}}}}
	gateway := &fakeGateway{answers: []func() (*pb.ReportAgentInterruptionResponse, error){
		func() (*pb.ReportAgentInterruptionResponse, error) {
			return nil, status.Error(codes.Unavailable, "gateway is restarting")
		},
		func() (*pb.ReportAgentInterruptionResponse, error) {
			return &pb.ReportAgentInterruptionResponse{Ok: false, ErrMsg: "redis is down"}, nil
		},
		taken,
	}}
	watcher, stderr := testWatcher(gateway, aws)

	runWatcher(t, watcher)

	reports := gateway.reports()
	require.Len(t, reports, 3)
	require.Same(t, reports[0], reports[2], "the same notice, observed when it was first seen")
	require.Contains(t, stderr.String(), "gateway is restarting")
	require.Contains(t, stderr.String(), "interruption notice was not delivered: redis is down")
}

func TestAgentGivesUpOnAGatewayThatNeverTakesTheNotice(t *testing.T) {
	aws := &scriptedSource{provider: "aws", answers: []scriptedAnswer{{notice: &interruption{Detail: "terminate"}}}}
	gateway := &fakeGateway{answers: []func() (*pb.ReportAgentInterruptionResponse, error){
		func() (*pb.ReportAgentInterruptionResponse, error) {
			return &pb.ReportAgentInterruptionResponse{Ok: false}, nil
		},
	}}
	watcher, stderr := testWatcher(gateway, aws)

	runWatcher(t, watcher)

	require.Len(t, gateway.reports(), 5)
	require.Contains(t, stderr.String(), "gateway rejected request")
}

// A gateway from before notices were passed on has nowhere to take one.
func TestAgentDoesNotRepeatANoticeToAGatewayThatCannotTakeIt(t *testing.T) {
	aws := &scriptedSource{provider: "aws", answers: []scriptedAnswer{{notice: &interruption{Detail: "terminate"}}}}
	gateway := &fakeGateway{answers: []func() (*pb.ReportAgentInterruptionResponse, error){
		func() (*pb.ReportAgentInterruptionResponse, error) {
			return nil, status.Error(codes.Unimplemented, "unknown method ReportAgentInterruption")
		},
	}}
	watcher, stderr := testWatcher(gateway, aws)

	runWatcher(t, watcher)

	require.Len(t, gateway.reports(), 1)
	require.Contains(t, stderr.String(), "gateway does not take interruption notices")
}

// The watcher ends with the agent, whatever it was doing.
func TestAgentStopsWatchingWhenItStops(t *testing.T) {
	for name, test := range map[string]struct {
		source  *scriptedSource
		gateway *fakeGateway
		// asked is how often the machine was asked by the end, where that
		// is known.
		asked int
	}{
		"while it looks for a notice to watch": {
			source:  &scriptedSource{provider: "aws", answers: []scriptedAnswer{notThisCloud}},
			gateway: &fakeGateway{},
			asked:   1,
		},
		"while it waits for the notice": {
			source:  &scriptedSource{provider: "aws", answers: []scriptedAnswer{noNotice}},
			gateway: &fakeGateway{},
		},
		"while it tells the gateway": {
			source: &scriptedSource{provider: "aws", answers: []scriptedAnswer{{notice: &interruption{Detail: "terminate"}}}},
			gateway: &fakeGateway{answers: []func() (*pb.ReportAgentInterruptionResponse, error){
				func() (*pb.ReportAgentInterruptionResponse, error) {
					return nil, status.Error(codes.Unavailable, "gateway is down")
				},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			watcher, _ := testWatcher(test.gateway, test.source)
			watcher.pollInterval, watcher.reportInterval = time.Hour, time.Hour

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); watcher.run(ctx) }()

			require.Eventually(t, func() bool { return test.source.asked() > 0 }, 5*time.Second, time.Millisecond)
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("the watcher went on after the agent stopped")
			}
			if test.asked > 0 {
				require.Equal(t, test.asked, test.source.asked(), "the machine is not asked again")
			}
		})
	}
}

// What a machine is asked is where its cloud keeps the notice.
func TestMachineNoticeSourcesAskTheMetadataService(t *testing.T) {
	sources := machineNoticeSources()
	require.Len(t, sources, 2)

	aws, ok := sources[0].(*awsSpotNotice)
	require.True(t, ok)
	require.Equal(t, "aws", aws.name())
	require.Equal(t, "http://169.254.169.254", aws.base)

	gcp, ok := sources[1].(*gcpPreemptionNotice)
	require.True(t, ok)
	require.Equal(t, "gcp", gcp.name())
	require.Equal(t, "http://169.254.169.254", gcp.base)

	for _, client := range []*http.Client{aws.client, gcp.client} {
		require.Equal(t, noticeRequestTimeout, client.Timeout)
		transport, ok := client.Transport.(*http.Transport)
		require.True(t, ok)
		require.Nil(t, transport.Proxy, "the metadata service is not behind the environment's proxy")
	}
}
