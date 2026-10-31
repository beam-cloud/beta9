package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
)

const (
	defaultEventHTTPTimeout = 5 * time.Second
	eventHTTPQueueSize      = 4096

	eventCallbackFormatCloudEvent   = "cloud_event"
	eventCallbackFormatBeta9Webhook = "beta9_webhook"
	eventCallbackFormatSlack        = "slack"
)

type eventHTTPSink struct {
	config types.EventCallbackConfig
	client *http.Client
	queue  chan cloudevents.Event
}

func newEventHTTPSink(config types.EventCallbackConfig) *eventHTTPSink {
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = defaultEventHTTPTimeout
	}

	sink := &eventHTTPSink{
		config: config,
		client: &http.Client{Timeout: timeout},
		queue:  make(chan cloudevents.Event, eventHTTPQueueSize),
	}
	go sink.run()
	return sink
}

func (s *eventHTTPSink) PushEvent(event cloudevents.Event) error {
	if !eventHTTPMatches(s.config.EventTypes, event.Type()) {
		return nil
	}

	select {
	case s.queue <- event:
		return nil
	default:
		return fmt.Errorf("event callback queue is full")
	}
}

func (s *eventHTTPSink) run() {
	for event := range s.queue {
		if err := s.deliver(event); err != nil {
			log.Debug().Err(err).Str("event_type", event.Type()).Str("url", s.config.URL).Msg("failed to deliver event http callback")
		}
	}
}

func (s *eventHTTPSink) deliver(event cloudevents.Event) error {
	body, err := s.payload(event)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, s.config.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range s.config.Headers {
		req.Header.Set(key, value)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("event callback returned status %d", resp.StatusCode)
	}
	return nil
}

func (s *eventHTTPSink) payload(event cloudevents.Event) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(s.config.Format)) {
	case "", eventCallbackFormatCloudEvent, "cloudevent", "json":
		return json.Marshal(event)
	case eventCallbackFormatBeta9Webhook, "internal_api":
		return beta9WebhookEventPayload(event)
	case eventCallbackFormatSlack:
		return json.Marshal(map[string]string{"text": slackEventText(event)})
	default:
		return nil, fmt.Errorf("unsupported event callback format %q", s.config.Format)
	}
}

func beta9WebhookEventPayload(event cloudevents.Event) ([]byte, error) {
	envelope, err := cloudEventJSONMap(event)
	if err != nil {
		return nil, err
	}
	envelope["date"] = eventUnixSeconds(event.Time())
	return json.Marshal([]map[string]interface{}{envelope})
}

func cloudEventJSONMap(event cloudevents.Event) (map[string]interface{}, error) {
	body, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	var envelope map[string]interface{}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

func eventUnixSeconds(t time.Time) float64 {
	if t.IsZero() {
		t = time.Now()
	}
	return float64(t.UnixNano()) / float64(time.Second)
}

func eventHTTPMatches(filters []string, eventType string) bool {
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		filter = strings.TrimSpace(filter)
		switch {
		case filter == "":
			continue
		case filter == "*", filter == eventType:
			return true
		case strings.HasSuffix(filter, "*") && strings.HasPrefix(eventType, strings.TrimSuffix(filter, "*")):
			return true
		}
	}
	return false
}

func slackEventText(event cloudevents.Event) string {
	if event.Type() == types.EventFeedbackSubmitted {
		return slackFeedbackText(event)
	}

	metadata := eventMetadataFromCloudEvent(event)
	parts := []string{fmt.Sprintf("beta9 event `%s`", event.Type())}
	if metadata.WorkspaceID != "" {
		parts = append(parts, "workspace `"+metadata.WorkspaceID+"`")
	}
	if metadata.StubID != "" {
		parts = append(parts, "stub `"+metadata.StubID+"`")
	}
	if metadata.ContainerID != "" {
		parts = append(parts, "container `"+metadata.ContainerID+"`")
	}
	if metadata.TaskID != "" {
		parts = append(parts, "task `"+metadata.TaskID+"`")
	}

	var data map[string]interface{}
	if err := json.Unmarshal(event.Data(), &data); err == nil {
		if id, ok := data["id"].(string); ok && id != "" {
			parts = append(parts, "id `"+id+"`")
		}
		if status, ok := data["status"].(string); ok && status != "" {
			parts = append(parts, "status `"+status+"`")
		}
		if reason, ok := data["reason"].(string); ok && reason != "" && reason != "UNKNOWN" {
			parts = append(parts, "reason `"+reason+"`")
		}
		if message, ok := data["message"].(string); ok && message != "" {
			parts = append(parts, message)
		}
	}

	return strings.Join(parts, " | ")
}

const slackFeedbackDetailsLimit = 1500

// slackFeedbackText renders a report for a person to read. Its fields are user input, so they
// are escaped to keep Slack from turning them into mentions or links.
func slackFeedbackText(event cloudevents.Event) string {
	var feedback types.EventFeedbackSchema
	_ = json.Unmarshal(event.Data(), &feedback)

	header := "Feedback `" + feedback.Kind + "`"
	if feedback.Blocked {
		header += " (blocked)"
	}
	parts := []string{header}
	if workspace := firstNonEmpty(feedback.WorkspaceName, feedback.WorkspaceID); workspace != "" {
		parts = append(parts, "workspace `"+slackEscape(workspace)+"`")
	}
	if feedback.Caller != "" {
		parts = append(parts, "`"+slackEscape(feedback.Caller)+"`")
	}
	if feedback.Model != "" {
		parts = append(parts, "model `"+slackEscape(feedback.Model)+"`")
	}
	if feedback.ID != "" {
		parts = append(parts, "id `"+feedback.ID+"`")
	}

	lines := []string{strings.Join(parts, " | "), "> " + slackEscape(feedback.Summary)}
	if feedback.Command != "" || feedback.ErrorCode != "" {
		lines = append(lines, "Command `"+slackEscape(feedback.Command)+"` "+slackEscape(feedback.ErrorCode))
	}
	if feedback.ErrorMessage != "" {
		lines = append(lines, "Error: "+slackEscape(truncateRunes(feedback.ErrorMessage, slackFeedbackDetailsLimit)))
	}
	if feedback.Details != "" {
		lines = append(lines, slackEscape(truncateRunes(feedback.Details, slackFeedbackDetailsLimit)))
	}
	if feedback.SuggestedImprovement != "" {
		lines = append(lines, "Suggested: "+slackEscape(truncateRunes(feedback.SuggestedImprovement, slackFeedbackDetailsLimit)))
	}
	return strings.Join(lines, "\n")
}

func slackEscape(text string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text)
}

func truncateRunes(text string, limit int) string {
	if runes := []rune(text); len(runes) > limit {
		return string(runes[:limit]) + "…"
	}
	return text
}

const (
	webhookQueueSize       = 2048
	webhookWorkers         = 4 // one slow receiver must not stall every workspace's deliveries
	webhookCacheTTL        = 15 * time.Second
	webhookHTTPTimeout     = 10 * time.Second
	webhookSignatureHeader = "X-Beta9-Signature"
	webhookEventHeader     = "X-Beta9-Event"
	webhookDeliveryHeader  = "X-Beta9-Delivery"
)

// Never fanned out to webhooks.
var webhookExcludedTypes = []string{
	types.EventContainerLog,
	types.EventPlatformLog,
	types.EventContainerMetrics,
	types.EventGatewayEndpointCalled,
	"compute.",
	"platform.",
	"router.",
	"feedback.",
}

// WebhookSource lists a workspace's webhooks; the sink caches results briefly.
type WebhookSource interface {
	ListWebhooks(ctx context.Context, workspaceId string) ([]types.WorkspaceWebhook, error)
}

type webhookDelivery struct {
	webhook types.WorkspaceWebhook
	event   cloudevents.Event
}

// workspaceWebhookSink fans events out to a workspace's webhooks, signed.
type workspaceWebhookSink struct {
	source WebhookSource
	queue  chan webhookDelivery
	client *http.Client

	mu    sync.Mutex
	cache map[string]webhookCacheEntry
}

type webhookCacheEntry struct {
	webhooks []types.WorkspaceWebhook
	expires  time.Time
}

func newWorkspaceWebhookSink(source WebhookSource, client *http.Client) *workspaceWebhookSink {
	sink := &workspaceWebhookSink{
		source: source,
		client: client,
		queue:  make(chan webhookDelivery, webhookQueueSize),
		cache:  map[string]webhookCacheEntry{},
	}
	for i := 0; i < webhookWorkers; i++ {
		go sink.run()
	}
	return sink
}

func (s *workspaceWebhookSink) PushEvent(event cloudevents.Event) error {
	if webhookExcluded(event.Type()) {
		return nil
	}
	workspaceId := extensionString(event.Extensions(), "workspaceid")
	if workspaceId == "" {
		return nil
	}
	for _, webhook := range s.webhooksFor(workspaceId) {
		if !webhook.Enabled || !eventHTTPMatches(webhook.EventTypes, event.Type()) {
			continue
		}
		select {
		case s.queue <- webhookDelivery{webhook: webhook, event: event}:
		default:
			return fmt.Errorf("webhook queue is full")
		}
	}
	return nil
}

func (s *workspaceWebhookSink) webhooksFor(workspaceId string) []types.WorkspaceWebhook {
	s.mu.Lock()
	entry, ok := s.cache[workspaceId]
	s.mu.Unlock()
	if ok && time.Now().Before(entry.expires) {
		return entry.webhooks
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	webhooks, err := s.source.ListWebhooks(ctx, workspaceId)
	if err != nil {
		log.Debug().Err(err).Str("workspace_id", workspaceId).Msg("list webhooks")
		webhooks = nil
	}
	s.mu.Lock()
	s.cache[workspaceId] = webhookCacheEntry{webhooks: webhooks, expires: time.Now().Add(webhookCacheTTL)}
	s.mu.Unlock()
	return webhooks
}

func (s *workspaceWebhookSink) run() {
	for delivery := range s.queue {
		if err := deliverWebhook(context.Background(), s.client, delivery.webhook, delivery.event); err != nil {
			log.Warn().Err(err).Str("workspace_id", extensionString(delivery.event.Extensions(), "workspaceid")).Str("event_type", delivery.event.Type()).Str("webhook_id", delivery.webhook.ExternalId).Msg("webhook delivery failed")
		}
	}
}

var webhookClient = newWebhookClient()

func newWebhookClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Resolve and connect directly so an environment proxy cannot bypass the
	// destination check. ControlContext checks each address after DNS resolution.
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{
		Timeout:        webhookHTTPTimeout,
		ControlContext: common.CheckPublicAddress,
	}).DialContext
	return &http.Client{
		Timeout:   webhookHTTPTimeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// DeliverWebhook posts one signed CloudEvent; non-2xx is an error.
func DeliverWebhook(ctx context.Context, webhook types.WorkspaceWebhook, event cloudevents.Event) error {
	return deliverWebhook(ctx, webhookClient, webhook, event)
}

func deliverWebhook(ctx context.Context, client *http.Client, webhook types.WorkspaceWebhook, event cloudevents.Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	req.Header.Set(webhookEventHeader, event.Type())
	req.Header.Set(webhookDeliveryHeader, event.ID())
	if webhook.Secret != "" {
		req.Header.Set(webhookSignatureHeader, signWebhookBody(webhook.Secret, body))
	}

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, common.ErrPrivateAddress) {
			return fmt.Errorf("webhook destination is not public; update the webhook URL to a public endpoint: %w", common.ErrPrivateAddress)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

// signWebhookBody returns the `sha256=<hex>` HMAC.
func signWebhookBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func webhookExcluded(eventType string) bool {
	for _, excluded := range webhookExcludedTypes {
		if eventType == excluded || (strings.HasSuffix(excluded, ".") && strings.HasPrefix(eventType, excluded)) {
			return true
		}
	}
	return false
}

const (
	workerLifecycleBatchSize     = 256
	workerLifecycleFlushInterval = 50 * time.Millisecond
	workerLifecycleQueueSize     = 16384
	workerLifecyclePushTimeout   = 5 * time.Second
	workerLifecycleRetryInterval = time.Second
	workerLifecycleRetryBudget   = 30 * time.Second
)

type workerLifecycleRelay struct {
	client        pb.WorkerRepositoryServiceClient
	workerID      string
	events        chan types.EventContainerLifecycleSchema
	retryInterval time.Duration
	retryBudget   time.Duration
}

func NewWorkerEventClientRepo(config types.AppConfig, client pb.WorkerRepositoryServiceClient, workerID string) EventRepository {
	events := NewEventClientRepo(config).(*EventClientRepo)
	relay := &workerLifecycleRelay{
		client:        client,
		workerID:      workerID,
		events:        make(chan types.EventContainerLifecycleSchema, workerLifecycleQueueSize),
		retryInterval: workerLifecycleRetryInterval,
		retryBudget:   workerLifecycleRetryBudget,
	}
	events.containerLifecyclePush = relay.push
	go relay.run()
	return events
}

func (r *workerLifecycleRelay) push(event types.EventContainerLifecycleSchema) bool {
	select {
	case r.events <- event:
		return true
	default:
		log.Warn().Str("worker_id", r.workerID).Msg("worker lifecycle event queue is full")
		return false
	}
}

func (r *workerLifecycleRelay) run() {
	ticker := time.NewTicker(workerLifecycleFlushInterval)
	defer ticker.Stop()

	batch := make([]types.EventContainerLifecycleSchema, 0, workerLifecycleBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Every attempt, including the last one, finishes inside the retry
		// budget: the RPC is bounded by the deadline and the backoff never
		// sleeps past it.
		deadline := time.Now().Add(r.retryBudget)
		for !r.flush(batch, deadline) {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				log.Warn().Str("worker_id", r.workerID).Int("events", len(batch)).Msg("dropping worker lifecycle events after retry budget")
				break
			}
			time.Sleep(min(r.retryInterval, remaining))
		}
		batch = batch[:0]
	}

	for {
		select {
		case event := <-r.events:
			batch = append(batch, event)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flush pushes one batch. The RPC is bounded by the push timeout and by
// deadline, whichever comes first.
func (r *workerLifecycleRelay) flush(events []types.EventContainerLifecycleSchema, deadline time.Time) bool {
	request := &pb.PushContainerLifecycleEventsRequest{
		WorkerId: r.workerID,
		Events:   make([][]byte, 0, len(events)),
	}
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			continue
		}
		request.Events = append(request.Events, data)
	}
	if len(request.Events) == 0 {
		return true
	}

	if timeout := time.Now().Add(workerLifecyclePushTimeout); timeout.Before(deadline) {
		deadline = timeout
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	response, err := r.client.PushContainerLifecycleEvents(ctx, request)
	if err != nil || response == nil || !response.Ok {
		logger := log.Debug().Err(err).Str("worker_id", r.workerID)
		if response != nil {
			logger = logger.Str("error_msg", response.ErrorMsg)
		}
		logger.Msg("failed to relay worker lifecycle events")
		return false
	}
	return true
}
