package cache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/mitchellh/mapstructure"
	"github.com/redis/go-redis/v9"
)

var (
	ErrChannelClosed    = errors.New("redis: channel closed")
	ErrConnectionIssue  = errors.New("redis: connection issue")
	ErrUnknownRedisMode = errors.New("redis: unknown mode")
)

type RedisClient struct {
	redis.UniversalClient
}

func WithClientName(name string) func(*redis.UniversalOptions) {
	// Remove empty spaces and new lines
	name = strings.ReplaceAll(name, " ", "")
	name = strings.ReplaceAll(name, "\n", "")

	// Remove special characters using a regular expression
	reg := regexp.MustCompile("[^a-zA-Z0-9]+")
	name = reg.ReplaceAllString(name, "")

	return func(uo *redis.UniversalOptions) {
		uo.ClientName = name
	}
}

func NewRedisClient(config RedisConfig, options ...func(*redis.UniversalOptions)) (*RedisClient, error) {
	opts := &redis.UniversalOptions{}
	mapstructure.Decode(config, opts)

	for _, opt := range options {
		opt(opts)
	}

	if config.EnableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
		}
	}

	var client redis.UniversalClient
	switch config.Mode {
	case RedisModeSingle:
		client = redis.NewClient(opts.Simple())
	case RedisModeCluster:
		client = redis.NewClusterClient(opts.Cluster())
	case RedisModeSentinel:
		failoverOpts := opts.Failover()
		failoverOpts.RouteByLatency = true
		failoverOpts.UseDisconnectedReplicas = true
		client = redis.NewFailoverClusterClient(failoverOpts)
	default:
		return nil, ErrUnknownRedisMode
	}

	err := client.Ping(context.TODO()).Err()
	if err != nil {
		return nil, fmt.Errorf("%s: %s", ErrConnectionIssue, err)
	}

	return &RedisClient{UniversalClient: client}, nil
}

func (r *RedisClient) ToSlice(v interface{}) []interface{} {
	return ToSlice(v)
}

func (r *RedisClient) ToStruct(m map[string]string, out interface{}) error {
	return ToStruct(m, out)
}

func (r *RedisClient) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return r.UniversalClient.LRange(ctx, key, start, stop).Result()
}

func (r *RedisClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	client, ok := r.UniversalClient.(*redis.ClusterClient)
	if ok {
		return client.SPublish(ctx, channel, message)
	}

	return r.UniversalClient.Publish(ctx, channel, message)
}

func (r *RedisClient) PSubscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, <-chan error, func()) {
	outCh := make(chan *redis.Message)
	errCh := make(chan error, 1)
	onSubscribe := make(chan struct{})
	subCtx, cancel := context.WithCancel(ctx)
	var readyOnce sync.Once
	var closeOnce sync.Once

	signalReady := func() {
		readyOnce.Do(func() {
			close(onSubscribe)
		})
	}

	go func() {
		defer close(outCh)
		defer close(errCh)

		switch client := r.UniversalClient.(type) {
		case *redis.Client:
			r.handleChannelSubs(subCtx, client.PSubscribe, signalReady, outCh, errCh, channels...)

		case *redis.ClusterClient:
			// Shared pattern subscribe doesn't exist yet, use ForEachMaster here
			var wg sync.WaitGroup
			err := client.ForEachMaster(subCtx, func(ctx context.Context, rdb *redis.Client) error {
				wg.Add(1)
				go func(rdb *redis.Client) {
					defer wg.Done()
					r.handleChannelSubs(subCtx, rdb.PSubscribe, signalReady, outCh, errCh, channels...)
				}(rdb)
				return nil
			})

			if err != nil {
				sendRedisSubscriptionError(subCtx, errCh, err)
				signalReady()
			}
			wg.Wait()
		default:
			signalReady()
		}
	}()

	select {
	case <-onSubscribe:
	case <-ctx.Done():
		cancel()
	}

	stop := func() {
		closeOnce.Do(cancel)
	}

	return outCh, errCh, stop
}

func (r *RedisClient) Subscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, <-chan error) {
	outCh := make(chan *redis.Message)
	errCh := make(chan error, 1)
	onSubscribe := make(chan struct{})
	var readyOnce sync.Once

	signalReady := func() {
		readyOnce.Do(func() {
			close(onSubscribe)
		})
	}

	go func() {
		defer close(outCh)
		defer close(errCh)

		switch client := r.UniversalClient.(type) {
		case *redis.Client:
			r.handleChannelSubs(ctx, client.Subscribe, signalReady, outCh, errCh, channels...)

		case *redis.ClusterClient:
			r.handleChannelSubs(ctx, client.SSubscribe, signalReady, outCh, errCh, channels...)
		default:
			signalReady()
		}
	}()

	select {
	case <-onSubscribe:
	case <-ctx.Done():
	}

	return outCh, errCh
}

func sendRedisSubscriptionError(ctx context.Context, errCh chan<- error, err error) {
	select {
	case errCh <- err:
	case <-ctx.Done():
	default:
	}
}

func (r *RedisClient) handleChannelSubs(
	ctx context.Context,
	subFn func(ctx context.Context, channels ...string) *redis.PubSub,
	onSubscribe func(),
	outCh chan *redis.Message,
	errCh chan error,
	channels ...string,
) {
	pubsub := subFn(ctx, channels...)
	defer pubsub.Close()

	ch := pubsub.Channel(
		redis.WithChannelSendTimeout(3*time.Second),
		redis.WithChannelHealthCheckInterval(3*time.Second),
	)

	onSubscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-ch:
			if !ok {
				sendRedisSubscriptionError(ctx, errCh, ErrChannelClosed)
				return
			}

			select {
			case outCh <- message:
			case <-ctx.Done():
				return
			}
		}
	}

}

type RedisLockOptions struct {
	TtlS    int
	Retries int
}

type RedisLockOption func(*RedisLock)

type RedisLock struct {
	client *RedisClient
	locks  map[string]*redislock.Lock
	mu     sync.Mutex
}

func NewRedisLock(client *RedisClient, opts ...RedisLockOption) *RedisLock {
	l := RedisLock{
		client: client,
		locks:  make(map[string]*redislock.Lock),
	}

	for _, opt := range opts {
		opt(&l)
	}

	return &l
}

func (l *RedisLock) Acquire(ctx context.Context, key string, opts RedisLockOptions) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var retryStrategy redislock.RetryStrategy = nil
	if opts.Retries > 0 {
		retryStrategy = redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), opts.Retries)
	}

	lock, err := redislock.Obtain(ctx, l.client, key, time.Duration(opts.TtlS)*time.Second, &redislock.Options{
		RetryStrategy: retryStrategy,
	})
	if err != nil && err != redislock.ErrNotObtained {
		return err // unexpected error, return it
	}

	if err == nil {
		l.locks[key] = lock
		return nil
	}

	return redislock.ErrNotObtained
}

func (l *RedisLock) Release(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lock, ok := l.locks[key]; ok {
		if err := lock.Release(context.TODO()); err != nil {
			if errors.Is(err, redislock.ErrLockNotHeld) {
				delete(l.locks, key)
			}
			return err
		}
		delete(l.locks, key)
		return nil
	}

	return redislock.ErrLockNotHeld
}

func (l *RedisLock) Refresh(key string, opts RedisLockOptions) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lock, ok := l.locks[key]
	if !ok {
		return redislock.ErrLockNotHeld
	}

	return lock.Refresh(context.TODO(), time.Duration(opts.TtlS)*time.Second, &redislock.Options{
		RetryStrategy: redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), opts.Retries),
	})
}

// Copies the result of HGetAll to a provided struct.
// If a field cannot be parsed, we use Go's default value.
// Struct fields must have the redis tag on them otherwise they will be ignored.
func ToStruct(m map[string]string, out interface{}) error {
	val := reflect.ValueOf(out).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if !field.CanSet() {
			continue
		}

		tagVal := typ.Field(i).Tag.Get("redis")
		if tagVal == "" || tagVal == "-" {
			continue
		}

		if v, ok := m[tagVal]; ok {
			switch field.Kind() {

			case reflect.String:
				field.SetString(v)

			case reflect.Int:
				val, err := strconv.ParseInt(v, 10, 0)
				if err != nil {
					val = 0
				}
				field.SetInt(val)

			case reflect.Int32:
				val, err := strconv.ParseInt(v, 10, 32)
				if err != nil {
					val = 0
				}
				field.SetInt(val)

			case reflect.Int64:
				// Duration is recognized as an int64
				if field.Type() == reflect.ValueOf(time.Duration(0)).Type() {
					val, err := time.ParseDuration(v)
					if err != nil {
						val = 0
					}
					field.Set(reflect.ValueOf(val))
				} else {
					val, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						val = 0
					}
					field.SetInt(val)
				}

			case reflect.Uint32:
				val, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					val = 0
				}
				field.SetUint(val)

			case reflect.Uint64:
				val, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					val = 0
				}
				field.SetUint(val)

			case reflect.Float32:
				val, err := strconv.ParseFloat(v, 32)
				if err != nil {
					val = 0
				}
				field.SetFloat(val)

			case reflect.Float64:
				val, err := strconv.ParseFloat(v, 64)
				if err != nil {
					val = 0
				}
				field.SetFloat(val)

			case reflect.Bool:
				val, err := strconv.ParseBool(v)
				if err != nil {
					val = false
				}
				field.SetBool(val)
			}
		}
	}

	return nil
}

// Flattens a struct using its field tags so it can be used by HSet.
// Struct fields must have the redis tag on them otherwise they will be ignored.
func ToSlice(v interface{}) []interface{} {
	val := reflect.Indirect(reflect.ValueOf(v))
	typ := val.Type()

	// Prepare hash field-value pairs
	var fieldValues []interface{}
	for i := 0; i < val.NumField(); i++ {
		tagVal := typ.Field(i).Tag.Get("redis")
		if tagVal == "" || tagVal == "-" {
			continue
		}

		fieldValue := val.Field(i).Interface()
		fieldValues = append(fieldValues, tagVal, fmt.Sprintf("%v", fieldValue))
	}

	return fieldValues
}
