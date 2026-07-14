package common

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

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/go-viper/mapstructure/v2"
	"github.com/redis/go-redis/v9"
)

var (
	ErrNilMessage       = errors.New("redis: nil message")
	ErrChannelClosed    = errors.New("redis: channel closed")
	ErrConnectionIssue  = errors.New("redis: connection issue")
	ErrUnknownRedisMode = errors.New("redis: unknown mode")
)

func IsRedisLockNotObtained(err error) bool {
	return errors.Is(err, redislock.ErrNotObtained) ||
		(err != nil && strings.Contains(err.Error(), "redislock: not obtained"))
}

const redisPubSubChannelBufferSize = 65536

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

func NewRedisClient(config types.RedisConfig, options ...func(*redis.UniversalOptions)) (*RedisClient, error) {
	opts := &redis.UniversalOptions{}
	CopyStruct(&config, opts)

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
	case types.RedisModeSingle:
		client = redis.NewClient(opts.Simple())
	case types.RedisModeCluster:
		client = redis.NewClusterClient(opts.Cluster())
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

// Keys gets all keys using a pattern. Prefer explicit indexes for hot paths.
func (r *RedisClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	return r.Scan(ctx, pattern)
}

func (r *RedisClient) Scan(ctx context.Context, pattern string) ([]string, error) {
	var mu sync.Mutex
	seen := map[string]bool{}
	outCh := make(chan string)
	errCh := make(chan error)

	scanAndCollect := func(rdb *redis.Client) error {
		iter := rdb.Scan(ctx, 0, pattern, 10_000).Iterator()

		for iter.Next(ctx) {
			key := iter.Val()

			mu.Lock()
			if !seen[key] {
				seen[key] = true
				outCh <- key
			}
			mu.Unlock()
		}

		return iter.Err()
	}

	go func() {
		defer close(outCh)
		defer close(errCh)

		switch client := r.UniversalClient.(type) {
		case *redis.Client:
			if err := scanAndCollect(client); err != nil {
				errCh <- err
			}

		case *redis.ClusterClient:
			if err := client.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
				return scanAndCollect(rdb)
			}); err != nil {
				errCh <- err
			}
		}
	}()

	var keys []string
	for key := range outCh {
		keys = append(keys, key)
	}

	if err, ok := <-errCh; ok {
		return nil, err
	}

	return keys, nil
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
	outCh := make(chan *redis.Message, redisPubSubChannelBufferSize)
	errCh := make(chan error, 64)
	ctx, cancel := context.WithCancel(ctx)
	ready := make(chan error, 64)
	started := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer close(outCh)
		defer close(errCh)

		var subscriptions sync.WaitGroup
		start := func(subscribe func(context.Context, ...string) *redis.PubSub) {
			subscriptions.Add(1)
			go func() {
				defer subscriptions.Done()
				r.handlePatternSub(ctx, subscribe, ready, outCh, errCh, channels...)
			}()
		}

		count := 0
		switch client := r.UniversalClient.(type) {
		case *redis.Client:
			count = 1
			start(client.PSubscribe)

		case *redis.ClusterClient:
			// Keyspace notifications are node-local, so pattern subscriptions must
			// remain active on every master in the cluster.
			err := client.ForEachMaster(ctx, func(_ context.Context, master *redis.Client) error {
				count++
				start(master.PSubscribe)
				return nil
			})
			if err != nil {
				sendRedisSubscriptionError(ctx, errCh, err)
			}
		}

		for range count {
			if err := <-ready; err != nil {
				sendRedisSubscriptionError(ctx, errCh, err)
			}
		}
		close(started)
		subscriptions.Wait()
	}()

	<-started

	var closeOnce sync.Once
	close := func() {
		closeOnce.Do(func() {
			cancel()
			<-done
		})
	}

	return outCh, errCh, close
}

func (r *RedisClient) handlePatternSub(
	ctx context.Context,
	subscribe func(context.Context, ...string) *redis.PubSub,
	ready chan<- error,
	outCh chan<- *redis.Message,
	errCh chan<- error,
	channels ...string,
) {
	pubsub := subscribe(ctx, channels...)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		ready <- err
		return
	}
	ready <- nil

	messages := pubsub.Channel(
		redis.WithChannelSize(redisPubSubChannelBufferSize),
		redis.WithChannelSendTimeout(3*time.Second),
		redis.WithChannelHealthCheckInterval(3*time.Second),
	)
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-messages:
			if !ok {
				sendRedisSubscriptionError(ctx, errCh, ErrChannelClosed)
				return
			}
			if message == nil {
				sendRedisSubscriptionError(ctx, errCh, ErrNilMessage)
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

func sendRedisSubscriptionError(ctx context.Context, errCh chan<- error, err error) {
	select {
	case errCh <- err:
	case <-ctx.Done():
	}
}

func (r *RedisClient) Subscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, <-chan error) {
	outCh := make(chan *redis.Message, redisPubSubChannelBufferSize)
	errCh := make(chan error, 1)
	onSubscribe := make(chan bool, 1)

	go func() {
		defer close(outCh)
		defer close(errCh)

		switch client := r.UniversalClient.(type) {
		case *redis.Client:
			r.handleChannelSubs(ctx, client.Subscribe, onSubscribe, outCh, errCh, channels...)

		case *redis.ClusterClient:
			r.handleChannelSubs(ctx, client.SSubscribe, onSubscribe, outCh, errCh, channels...)
		}
	}()

	<-onSubscribe

	return outCh, errCh
}

func (r *RedisClient) handleChannelSubs(
	ctx context.Context,
	subFn func(ctx context.Context, channels ...string) *redis.PubSub,
	onSubscribe chan bool,
	outCh chan *redis.Message,
	errCh chan error,
	channels ...string,
) {
	pubsub := subFn(ctx, channels...)
	defer pubsub.Close()
	// Channel starts its receive loop asynchronously. Wait for Redis to confirm
	// the subscription so callers can safely publish as soon as we return.
	if _, err := pubsub.Receive(ctx); err != nil {
		errCh <- err
		onSubscribe <- true
		return
	}

	ch := pubsub.Channel(
		redis.WithChannelSize(redisPubSubChannelBufferSize),
		redis.WithChannelSendTimeout(3*time.Second),
		redis.WithChannelHealthCheckInterval(3*time.Second),
	)

	onSubscribe <- true

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-ch:
			if !ok {
				errCh <- ErrChannelClosed
				return
			}

			if message == nil {
				errCh <- ErrNilMessage
				return
			}

			outCh <- message
		}
	}

}

type RedisLockOptions struct {
	TtlS          int
	Retries       int
	RetryInterval time.Duration
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
	lock, err := l.obtain(ctx, key, opts)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.locks[key] = lock
	l.mu.Unlock()
	return nil
}

func (l *RedisLock) obtain(ctx context.Context, key string, opts RedisLockOptions) (*redislock.Lock, error) {
	if l == nil || l.client == nil {
		return nil, errors.New("redis lock client is unavailable")
	}
	if opts.TtlS <= 0 {
		return nil, errors.New("redis lock TTL must be positive")
	}
	var retryStrategy redislock.RetryStrategy = nil
	if opts.Retries > 0 {
		baseRetryStrategy := redislock.RetryStrategy(redislock.ExponentialBackoff(100*time.Millisecond, time.Duration(opts.TtlS)*time.Second))
		if opts.RetryInterval > 0 {
			baseRetryStrategy = redislock.LinearBackoff(opts.RetryInterval)
		}
		retryStrategy = redislock.LimitRetry(baseRetryStrategy, opts.Retries)
	}

	return redislock.Obtain(ctx, l.client, key, time.Duration(opts.TtlS)*time.Second, &redislock.Options{
		RetryStrategy: retryStrategy,
	})
}

func (l *RedisLock) WithLease(ctx context.Context, key string, opts RedisLockOptions, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("redis lock callback is required")
	}
	lock, err := l.obtain(ctx, key, opts)
	if err != nil {
		return err
	}

	ttl := time.Duration(opts.TtlS) * time.Second
	leaseCtx, cancel := context.WithCancel(ctx)
	stop := make(chan struct{})
	done := make(chan struct{})
	refreshErr := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				if err := lock.Refresh(leaseCtx, ttl, nil); err != nil {
					refreshErr <- err
					cancel()
					return
				}
			}
		}
	}()

	workErr := fn(leaseCtx)
	close(stop)
	<-done
	cancel()
	var leaseErr error
	select {
	case leaseErr = <-refreshErr:
	default:
	}
	releaseErr := lock.Release(context.Background())
	return errors.Join(workErr, leaseErr, releaseErr)
}

func (l *RedisLock) Token(key string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lock, ok := l.locks[key]; ok {
		return lock.Token(), nil
	}

	return "", redislock.ErrLockNotHeld
}

func (l *RedisLock) Refresh(ctx context.Context, key string, ttl time.Duration) error {
	l.mu.Lock()
	lock, ok := l.locks[key]
	l.mu.Unlock()
	if !ok {
		return redislock.ErrLockNotHeld
	}
	return lock.Refresh(ctx, ttl, nil)
}

func (l *RedisLock) ReleaseWithToken(key string, token string) error {
	rc := redislock.New(l.client)
	lock, err := rc.RetrieveLock(context.Background(), key, token)
	if err != nil {
		return err
	}

	return lock.Release(context.Background())
}

func (l *RedisLock) Release(key string) error {
	l.mu.Lock()
	lock, ok := l.locks[key]
	l.mu.Unlock()

	if !ok {
		return redislock.ErrLockNotHeld
	}
	if err := lock.Release(context.Background()); err != nil {
		return err
	}

	l.mu.Lock()
	if l.locks[key] == lock {
		delete(l.locks, key)
	}
	l.mu.Unlock()
	return nil
}

func CopyStruct(src, dst any) error {
	config := mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           dst,
	}

	decoder, err := mapstructure.NewDecoder(&config)
	if err != nil {
		return err
	}

	if err := decoder.Decode(src); err != nil {
		return err
	}

	return nil
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

			case reflect.Uint:
				val, err := strconv.ParseUint(v, 10, 0)
				if err != nil {
					val = 0
				}
				field.SetUint(val)

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
