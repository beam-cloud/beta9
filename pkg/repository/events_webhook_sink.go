package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
)

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

	mu    sync.Mutex
	cache map[string]webhookCacheEntry
}

type webhookCacheEntry struct {
	webhooks []types.WorkspaceWebhook
	expires  time.Time
}

func newWorkspaceWebhookSink(source WebhookSource) *workspaceWebhookSink {
	sink := &workspaceWebhookSink{
		source: source,
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
		if err := DeliverWebhook(context.Background(), delivery.webhook, delivery.event); err != nil {
			log.Debug().Err(err).Str("event_type", delivery.event.Type()).Str("webhook_id", delivery.webhook.ExternalId).Msg("webhook delivery failed")
		}
	}
}

var webhookClient = &http.Client{Timeout: webhookHTTPTimeout}

// DeliverWebhook posts one signed CloudEvent; non-2xx is an error.
func DeliverWebhook(ctx context.Context, webhook types.WorkspaceWebhook, event cloudevents.Event) error {
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

	resp, err := webhookClient.Do(req)
	if err != nil {
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
