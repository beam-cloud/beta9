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

// Gets all keys using a pattern
// Actually runs a scan since keys locks up the database.
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
			scanAndCollect(client)

		case *redis.ClusterClient:
			err := client.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
				return scanAndCollect(rdb)
			})

			if err != nil {
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
	outCh := make(chan *redis.Message)
	errCh := make(chan error)
	onSubscribe := make(chan bool, 1)

	go func() {
		switch client := r.UniversalClient.(type) {
		case *redis.Client:
			r.handleChannelSubs(ctx, client.PSubscribe, onSubscribe, outCh, errCh, channels...)

		case *redis.ClusterClient:
			// Shared pattern subscribe doesn't exist yet, use ForEachMaster here
			err := client.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
				r.handleChannelSubs(ctx, rdb.PSubscribe, onSubscribe, outCh, errCh, channels...)
				return nil
			})

			if err != nil {
				errCh <- err
			}
		}
	}()

	<-onSubscribe

	close := func() {
		defer close(outCh)
		defer close(errCh)
	}

	return outCh, errCh, close
}

func (r *RedisClient) Subscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, <-chan error) {
	outCh := make(chan *redis.Message)
	errCh := make(chan error)
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

	ch := pubsub.Channel(
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
	var retryStrategy redislock.RetryStrategy = nil
	if opts.Retries > 0 {
		retryStrategy = redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), opts.Retries)
	}
	lock, err := redislock.Obtain(ctx, l.client, key, time.Duration(opts.TtlS)*time.Second, &redislock.Options{
		RetryStrategy: retryStrategy,
	})
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.locks[key] = lock
	l.mu.Unlock()
	return nil
}

func (l *RedisLock) Token(key string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lock, ok := l.locks[key]; ok {
		return lock.Token(), nil
	}

	return "", redislock.ErrLockNotHeld
}

func (l *RedisLock) Release(key string, tokens ...string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lock, ok := l.locks[key]; ok {
		err := lock.Release(context.Background())
		if err != nil {
			return err
		}

		delete(l.locks, key)
		return nil
	} else if len(tokens) > 0 {
		// NOTE: we only ever use the first "token" here (if passed in), but the function
		// accepts a variadic argument to avoid changing all callers
		rc := redislock.New(l.client)
		lock, err := rc.RetrieveLock(context.Background(), key, tokens[0])
		if err != nil {
			return err
		}

		return lock.Release(context.Background())
	}

	return redislock.ErrLockNotHeld
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
