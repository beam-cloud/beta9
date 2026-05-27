package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/rs/zerolog/log"
)

const (
	defaultEventHTTPTimeout = 5 * time.Second
	eventHTTPQueueSize      = 4096
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
	case "", "cloud_event", "cloudevent", "json":
		return json.Marshal(event)
	case "slack":
		return json.Marshal(map[string]string{"text": slackEventText(event)})
	default:
		return nil, fmt.Errorf("unsupported event callback format %q", s.config.Format)
	}
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
