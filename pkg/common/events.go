package common

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	eventPrefix        string        = "event"
	eventChannelPrefix string        = "events"
	eventLockSuffix    string        = "lock"
	eventRetriesSuffix string        = "retries"
	eventLockTtlS      int           = 60
	eventTtlS          int           = 60
	eventRetryDelay    time.Duration = time.Second * time.Duration(5)
)

type EventType string

type EventBusSubscriber struct {
	Type     EventType
	Callback func(*Event) bool
}

type EventBus struct {
	EventCallbacks map[string]func(*Event) bool
	Subscribers    []EventBusSubscriber
	rdb            *RedisClient
}

type Event struct {
	Type          EventType      `json:"type"`
	Args          map[string]any `json:"args"`
	LockAndDelete bool           `json:"lock_and_delete"`
	Retries       uint           `json:"retries"`
}

const (
	EventTypeStopContainer       EventType = "STOP_CONTAINER"
	EventTypeReloadInstance      EventType = "RELOAD_INSTANCE"
	EventTypeCheckpointContainer EventType = "CHECKPOINT_CONTAINER"
)

func StopBuildEventType(containerId string) EventType {
	return EventType("stop-build-" + containerId)
}

// Send an event over the bus
func (eb *EventBus) Send(event *Event) (string, error) {
	serializedEvent, err := eb.serialize(event)
	if err != nil {
		return "", err
	}

	h := sha1.New()
	h.Write(serializedEvent)
	eventId := base64.URLEncoding.EncodeToString(h.Sum(nil))

	eventKey := fmt.Sprintf("%s:%s", eventPrefix, eventId)
	eventChannelKey := fmt.Sprintf("%s/%s", eventChannelPrefix, event.Type)

	res, err := eb.rdb.Exists(context.TODO(), eventKey).Result()
	if err != nil {
		return "", err
	}

	if res > 0 {
		return "", fmt.Errorf("event already exists: %v", eventKey)
	}

	err = eb.rdb.SetEx(context.TODO(), eventKey, serializedEvent, time.Duration(eventTtlS)*time.Second).Err()
	if err != nil {
		return "", err
	}

	err = eb.rdb.Publish(context.TODO(), eventChannelKey, eventId).Err()
	if err != nil {
		return "", fmt.Errorf("failed to send event <%v>: %v", eventId, err)
	}

	return eventId, nil
}

// Remove an event by ID
func (eb *EventBus) Delete(eventId string) error {
	eventKey := fmt.Sprintf("%s:%s", eventPrefix, eventId)
	err := eb.rdb.Del(context.TODO(), eventKey).Err()
	if err != nil {
		return err
	}

	return nil
}

// Claim an event for processing
func (eb *EventBus) Claim(eventId string) (*Event, *RedisLock, bool) {
	eventKey := fmt.Sprintf("%s:%s", eventPrefix, eventId)

	// Get event from cache
	res, err := eb.rdb.Get(context.TODO(), eventKey).Bytes()
	if err != nil {
		return nil, nil, false
	}

	// Deserialize event
	event, err := eb.deserialize(res)
	if err != nil {
		return nil, nil, false
	}

	// Lock on event lock key to prevent other consumers from processing this event
	var lock *RedisLock = nil
	if event.LockAndDelete {
		lockKey := fmt.Sprintf("%s:%s:%s", eventPrefix, eventId, eventLockSuffix)

		lock = NewRedisLock(eb.rdb)

		err = lock.Acquire(context.TODO(), lockKey, RedisLockOptions{
			TtlS: eventLockTtlS,
		})
		if err != nil {
			return nil, nil, false
		}
	}

	return event, lock, true
}

// Receive all subscribed events (blocking)
func (eb *EventBus) ReceiveEvents(ctx context.Context) {
	wg := sync.WaitGroup{}

	for _, s := range eb.Subscribers {
		wg.Add(1)
		go eb.receive(ctx, &wg, string(s.Type))
	}

	wg.Wait()
	<-ctx.Done()
}

// Receive events for a particular event type (blocking)
func (eb *EventBus) receive(ctx context.Context, wg *sync.WaitGroup, eventType string) {
	defer wg.Done()

	log.Info().Msgf("receiving %s events", eventType)

	eventChannelKey := fmt.Sprintf("%s/%s", eventChannelPrefix, eventType)
	messages, errs := eb.rdb.Subscribe(ctx, eventChannelKey)

retry:
	for {
		select {
		case m, ok := <-messages:
			if !ok {
				log.Info().Str("event_type", eventType).Msg("events subscription closed, retrying...")
				time.Sleep(eventRetryDelay)
				messages, errs = eb.rdb.Subscribe(ctx, eventChannelKey)
				continue retry
			}

			eventId := m.Payload

			event, lock, claimed := eb.Claim(eventId)
			if !claimed {
				continue
			}

			go eb.handleEvent(eventId, event, lock)

		case <-ctx.Done():
			return

		case err := <-errs:
			log.Error().Err(err).Msg("error with eventbus subscription")
			break retry
		}
	}
}

// Handle events in a go-routine so as not to block event channel
func (eb *EventBus) handleEvent(eventId string, event *Event, lock *RedisLock) {
	eventChannelKey := fmt.Sprintf("%s/%s", eventChannelPrefix, event.Type)
	lockKey := fmt.Sprintf("%s:%s:%s", eventPrefix, eventId, eventLockSuffix)
	if lock != nil {
		defer lock.Release(lockKey)
	}

	callback, ok := eb.EventCallbacks[string(event.Type)]
	if !ok {
		// If we receive an event, but don't have a callback defined for it, republish the event
		// so other consumers receive it.
		// This is not the same as a "retry" because the callback was never invoked.
		err := eb.rdb.Publish(context.TODO(), eventChannelKey, eventId).Err()
		if err != nil {
			log.Error().Str("event_id", eventId).Err(err).Msg("failed to republish event")
		}
		return
	}

	success := callback(event)
	if success {
		if event.LockAndDelete {
			eb.Delete(eventId)
		}
	} else {
		// If the callback fails, attempt to resend the event
		eb.resendEvent(eventChannelKey, eventId, event)
	}
}

// Re-send event if callback failed
func (eb *EventBus) resendEvent(eventChannelKey string, eventId string, event *Event) error {
	retryKey := fmt.Sprintf("%s:%s:%s", eventPrefix, eventId, eventRetriesSuffix)

	res, err := eb.rdb.Exists(context.TODO(), retryKey).Result()
	if err != nil {
		return err
	}

	keyExists := res > 0

	retries := 0
	if keyExists {
		res, _ := eb.rdb.Get(context.TODO(), retryKey).Result()
		retries, _ = strconv.Atoi(res)
	}

	if retries >= int(event.Retries) {
		log.Error().Str("event", fmt.Sprintf("%v", event)).Msg("hit event retry limit")
		return errors.New("hit event retry limit")
	}

	retries += 1
	err = eb.rdb.SetEx(context.TODO(), retryKey, retries, time.Duration(eventTtlS)*time.Second).Err()
	if err != nil {
		return err
	}

	// Wait a few seconds before sending the event again
	time.Sleep(eventRetryDelay)

	err = eb.rdb.Publish(context.TODO(), eventChannelKey, eventId).Err()
	if err != nil {
		return fmt.Errorf("failed to resend event <%v>: %v", eventId, err)
	}

	return nil
}

// Serialize an event object -> byte array
func (eb *EventBus) serialize(event *Event) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Deserialize an event from a byte array -> event object
func (eb *EventBus) deserialize(data []byte) (*Event, error) {
	var event Event
	err := json.Unmarshal(data, &event)
	if err != nil {
		return nil, err
	}
	return &event, nil
}

// Create a new event bus
func NewEventBus(rdb *RedisClient, subscribers ...EventBusSubscriber) *EventBus {
	eventCallbacks := make(map[string]func(*Event) bool)
	for _, s := range subscribers {
		eventCallbacks[string(s.Type)] = s.Callback
	}

	return &EventBus{
		EventCallbacks: eventCallbacks,
		Subscribers:    subscribers,
		rdb:            rdb,
	}
}
