package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A spot machine can be taken back by its provider. AWS tells the machine
// two minutes before, and Google up to two minutes before, through the
// machine's metadata service. The agent asks for that notice and passes it
// on to the gateway, which stops giving the machine work, so that its pool
// can replace it while it still runs.
//
// A provider that gives no notice, or gives it by shutting the machine down,
// needs nothing here. A machine that is shut down stops its worker, and the
// gateway finds out for itself that a machine is gone.

const (
	// metadataURL is where a cloud machine reaches its metadata service.
	metadataURL = "http://169.254.169.254"

	// noticePollInterval is how often the machine is asked. AWS recommends
	// every five seconds for a notice that comes two minutes ahead.
	noticePollInterval = 5 * time.Second
	// noticeRequestTimeout bounds one question to the metadata service. It
	// is on the machine's own link, and answers at once if it is there.
	noticeRequestTimeout = 2 * time.Second
	// noticeProbes is how many times a machine is asked, when the agent
	// starts, before it is taken for one that is given no notice.
	noticeProbes = 3
	// noticeReportInterval is how long the agent waits before it tells the
	// gateway again, and noticeReportAttempts how many times it tries.
	noticeReportInterval = 2 * time.Second
	noticeReportAttempts = 30
	// noticeReportTimeout bounds one attempt to tell the gateway.
	noticeReportTimeout = 10 * time.Second

	// metadataAnswerLimit is how much of an answer is read.
	metadataAnswerLimit = 64 << 10
)

// interruption is a provider's notice that it is about to take the machine.
type interruption struct {
	// Detail is what the provider said.
	Detail string
	// At is when it takes the machine. Zero if it did not say.
	At time.Time
}

// noticeSource is one provider's way of giving a machine notice.
type noticeSource interface {
	// name is the provider, as the gateway is told.
	name() string
	// check asks the machine. It returns nil when there is no notice, and
	// an error when the machine is not this provider's or its metadata
	// service did not answer.
	check(ctx context.Context) (*interruption, error)
}

// machineNoticeSources are the notices a machine can be given.
func machineNoticeSources() []noticeSource {
	// The metadata service is on the machine's link. A proxy from the
	// environment could not reach it, and must not be asked to.
	client := &http.Client{
		Timeout: noticeRequestTimeout,
		Transport: &http.Transport{
			Proxy:       nil,
			DialContext: (&net.Dialer{Timeout: noticeRequestTimeout}).DialContext,
		},
	}
	return []noticeSource{
		&awsSpotNotice{client: client, base: metadataURL},
		&gcpPreemptionNotice{client: client, base: metadataURL},
	}
}

// metadataAnswer is what a metadata service said.
type metadataAnswer struct {
	status int
	header http.Header
	body   []byte
}

func askMetadata(ctx context.Context, client *http.Client, method, url string, header map[string]string) (metadataAnswer, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return metadataAnswer{}, err
	}
	for name, value := range header {
		request.Header.Set(name, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return metadataAnswer{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, metadataAnswerLimit))
	if err != nil {
		return metadataAnswer{}, err
	}
	return metadataAnswer{status: response.StatusCode, header: response.Header, body: body}, nil
}

// awsSpotNotice reads the notice AWS gives a Spot Instance: the metadata item
// spot/instance-action, which is there from two minutes before the instance
// is stopped or terminated and is missing until then.
type awsSpotNotice struct {
	client *http.Client
	base   string
	// token is the session the metadata service is asked in. It is fetched
	// when there is none, and again when the service stops taking it.
	token string
}

const (
	awsSessionPath       = "/latest/api/token"
	awsInstanceActionURL = "/latest/meta-data/spot/instance-action"
	// awsSessionSeconds is how long a session is asked to last: the longest
	// the service gives.
	awsSessionSeconds = "21600"
)

func (a *awsSpotNotice) name() string { return "aws" }

func (a *awsSpotNotice) check(ctx context.Context) (*interruption, error) {
	answer, err := a.ask(ctx, awsInstanceActionURL)
	if err == nil && answer.status == http.StatusUnauthorized {
		// The session ran out.
		a.token = ""
		answer, err = a.ask(ctx, awsInstanceActionURL)
	}
	if err != nil {
		return nil, err
	}

	switch answer.status {
	case http.StatusNotFound:
		return nil, nil
	case http.StatusOK:
		// The item being there is the notice, whatever it says.
		var action struct {
			Action string    `json:"action"`
			Time   time.Time `json:"time"`
		}
		if err := json.Unmarshal(answer.body, &action); err != nil || action.Action == "" {
			return &interruption{Detail: "spot instance action"}, nil
		}
		return &interruption{Detail: action.Action, At: action.Time}, nil
	default:
		return nil, fmt.Errorf("aws metadata: status %d", answer.status)
	}
}

// ask reads a metadata item in a session, which it opens if there is none.
func (a *awsSpotNotice) ask(ctx context.Context, path string) (metadataAnswer, error) {
	if a.token == "" {
		session, err := askMetadata(ctx, a.client, http.MethodPut, a.base+awsSessionPath, map[string]string{
			"X-aws-ec2-metadata-token-ttl-seconds": awsSessionSeconds,
		})
		if err != nil {
			return metadataAnswer{}, err
		}
		token := strings.TrimSpace(string(session.body))
		if session.status != http.StatusOK || token == "" {
			return metadataAnswer{}, fmt.Errorf("aws metadata: no session (status %d)", session.status)
		}
		a.token = token
	}
	return askMetadata(ctx, a.client, http.MethodGet, a.base+path, map[string]string{
		"X-aws-ec2-metadata-token": a.token,
	})
}

// gcpPreemptionNotice reads the notice Google gives a Spot VM: the metadata
// value instance/preempted, which turns TRUE when the VM is being preempted.
type gcpPreemptionNotice struct {
	client *http.Client
	base   string
}

const (
	gcpPreemptedPath  = "/computeMetadata/v1/instance/preempted"
	gcpMetadataFlavor = "Google"
)

func (g *gcpPreemptionNotice) name() string { return "gcp" }

func (g *gcpPreemptionNotice) check(ctx context.Context) (*interruption, error) {
	answer, err := askMetadata(ctx, g.client, http.MethodGet, g.base+gcpPreemptedPath, map[string]string{
		"Metadata-Flavor": gcpMetadataFlavor,
	})
	if err != nil {
		return nil, err
	}
	// Other clouds answer at the same address. Google's says who it is.
	if answer.status != http.StatusOK || answer.header.Get("Metadata-Flavor") != gcpMetadataFlavor {
		return nil, fmt.Errorf("gcp metadata: status %d", answer.status)
	}
	switch value := strings.TrimSpace(string(answer.body)); strings.ToUpper(value) {
	case "TRUE":
		return &interruption{Detail: "preempted"}, nil
	case "FALSE":
		return nil, nil
	default:
		return nil, fmt.Errorf("gcp metadata: preempted is %q", value)
	}
}

// interruptionReporter is the gateway, as far as a notice is concerned.
type interruptionReporter interface {
	ReportAgentInterruption(context.Context, *pb.ReportAgentInterruptionRequest, ...grpc.CallOption) (*pb.ReportAgentInterruptionResponse, error)
}

// interruptionWatcher waits for the machine to be given notice, and tells
// the gateway when it is.
type interruptionWatcher struct {
	sources    []noticeSource
	gateway    interruptionReporter
	agentToken string
	stdout     io.Writer
	stderr     io.Writer

	probes         int
	pollInterval   time.Duration
	reportAttempts int
	reportInterval time.Duration
}

func newInterruptionWatcher(gateway interruptionReporter, agentToken string, stdout, stderr io.Writer) *interruptionWatcher {
	return &interruptionWatcher{
		sources:        machineNoticeSources(),
		gateway:        gateway,
		agentToken:     agentToken,
		stdout:         stdout,
		stderr:         stderr,
		probes:         noticeProbes,
		pollInterval:   noticePollInterval,
		reportAttempts: noticeReportAttempts,
		reportInterval: noticeReportInterval,
	}
}

// run watches until the machine is given notice and the gateway has been
// told, or until ctx ends. On a machine that is given no notice it returns
// once it has found that out.
func (w *interruptionWatcher) run(ctx context.Context) {
	source := w.find(ctx)
	if source == nil {
		return
	}
	verbosef(w.stdout, "watching for an interruption notice from %s\n", source.name())

	notice := w.await(ctx, source)
	if notice == nil {
		return
	}
	statusf(w.stdout, "This machine is about to be taken back by its provider (%s: %s)", source.name(), notice.Detail)
	w.report(ctx, source.name(), *notice)
}

// find returns the source that answers on this machine, or nil if none
// does: the machine is not one that is given notice.
func (w *interruptionWatcher) find(ctx context.Context) noticeSource {
	for probe := range w.probes {
		if probe > 0 && !waitForRouteRetry(ctx, w.pollInterval) {
			return nil
		}
		for _, source := range w.sources {
			if _, err := source.check(ctx); err == nil {
				return source
			}
		}
	}
	return nil
}

// await asks the source until it gives notice. It returns nil if ctx ends
// first.
func (w *interruptionWatcher) await(ctx context.Context, source noticeSource) *interruption {
	failing := false
	for {
		notice, err := source.check(ctx)
		if notice != nil {
			return notice
		}
		// A metadata service that stops answering says so once.
		if err != nil && !failing && ctx.Err() == nil {
			fmt.Fprintf(w.stderr, "interruption notice from %s could not be read: %v\n", source.name(), err)
		}
		failing = err != nil
		if !waitForRouteRetry(ctx, w.pollInterval) {
			return nil
		}
	}
}

// report tells the gateway, again if it has to: the notice is short, and
// worth more the sooner the gateway has it.
func (w *interruptionWatcher) report(ctx context.Context, source string, notice interruption) {
	request := &pb.ReportAgentInterruptionRequest{
		AgentToken:         w.agentToken,
		Source:             source,
		Detail:             notice.Detail,
		ObservedAtUnixNano: time.Now().UnixNano(),
	}
	if !notice.At.IsZero() {
		request.ExpectedAtUnixNano = notice.At.UnixNano()
	}

	for attempt := range w.reportAttempts {
		if attempt > 0 && !waitForRouteRetry(ctx, w.reportInterval) {
			return
		}
		err := w.tell(ctx, request)
		if err == nil {
			return
		}
		if status.Code(err) == codes.Unimplemented {
			// A gateway from before notices were passed on.
			fmt.Fprintf(w.stderr, "gateway does not take interruption notices: %v\n", err)
			return
		}
		fmt.Fprintf(w.stderr, "interruption notice was not delivered: %v\n", err)
	}
}

func (w *interruptionWatcher) tell(ctx context.Context, request *pb.ReportAgentInterruptionRequest) error {
	ctx, cancel := context.WithTimeout(ctx, noticeReportTimeout)
	defer cancel()
	response, err := w.gateway.ReportAgentInterruption(ctx, request)
	if err != nil {
		return err
	}
	if !response.GetOk() {
		return errors.New(firstNonEmpty(response.GetErrMsg(), "gateway rejected request"))
	}
	return nil
}
