package common

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

var (
	ErrChannelClosed       = errors.New("redis: channel closed")
	ErrConnectionIssue     = errors.New("redis: connection issue")
	ErrUnknownRedisClient  = errors.New("redis: unknown client type")
	ErrUnknownRedisOptions = errors.New("redis: unknown options type")
	ErrLockNotReleased     = errors.New("redislock: lock not released")
	RedisModeCluster       = "cluster"
)

type redisCmdable interface {
	ClientGetName(ctx context.Context) *redis.StringCmd
	Close() error
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	LPop(ctx context.Context, key string) *redis.StringCmd
	Ping(ctx context.Context) *redis.StatusCmd
	PSubscribe(ctx context.Context, channels ...string) *redis.PubSub
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZCard(ctx context.Context, key string) *redis.IntCmd
	ZPopMin(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	Decr(ctx context.Context, key string) *redis.IntCmd
}

type RedisClient struct {
	// Embedded Redis client. All functions defined on interface will be available
	// to this struct.
	redisCmdable
}

func WithAddress(a string) func(*RedisOptions) {
	return func(ro *RedisOptions) {
		ro.Addr = a
	}
}

func WithClientName(n string) func(*RedisOptions) {
	return func(ro *RedisOptions) {
		// Remove empty spaces and new lines
		n = strings.ReplaceAll(n, " ", "")
		n = strings.ReplaceAll(n, "\n", "")

		// Remove special characters using a regular expression
		reg := regexp.MustCompile("[^a-zA-Z0-9]+")
		n = reg.ReplaceAllString(n, "")

		ro.ClientName = n
	}
}

func NewRedisClient(opts ...func(*RedisOptions)) (*RedisClient, error) {
	// Get options from secrets and env vars
	options, err := GetRedisConfig()
	if err != nil {
		return nil, err
	}

	// Apply overrides
	for _, opt := range opts {
		opt(options)
	}

	// Optionally enable TLS
	if options.TLSEnabled {
		options.TLSConfig = &tls.Config{}
	}

	// Initialize a client
	var client *RedisClient

	if options.Mode == RedisModeCluster {
		log.Println("Running Redis in cluster mode.")

		ro := &redis.ClusterOptions{}
		CopyStruct(options, ro)
		ro.Addrs = options.Addrs()

		client = &RedisClient{redis.NewClusterClient(ro)}
	} else {
		log.Println("Running Redis in standalone mode.")

		ro := &redis.Options{}
		CopyStruct(options, ro)

		client = &RedisClient{redis.NewClient(ro)}
	}

	// Connection test
	err = client.Ping(context.TODO()).Err()
	if err != nil {
		return nil, fmt.Errorf("%s: %s", ErrConnectionIssue, err)
	}

	return client, nil
}

func (r *RedisClient) Close() error {
	return r.redisCmdable.Close()
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

		switch client := r.redisCmdable.(type) {
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

func (r *RedisClient) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	client, ok := r.redisCmdable.(*redis.ClusterClient)
	if ok {
		return client.SPublish(ctx, channel, message)
	}

	return r.redisCmdable.Publish(ctx, channel, message)
}

func (r *RedisClient) PSubscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, <-chan error) {
	outCh := make(chan *redis.Message)
	errCh := make(chan error)

	go func() {
		defer close(outCh)
		defer close(errCh)

		switch client := r.redisCmdable.(type) {
		case *redis.Client:
			r.handleChannelSubs(ctx, client.PSubscribe, outCh, errCh, channels...)

		case *redis.ClusterClient:
			// Shared pattern subscribe doesn't exist yet, use ForEachMaster here
			err := client.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
				r.handleChannelSubs(ctx, rdb.PSubscribe, outCh, errCh, channels...)
				return nil
			})

			if err != nil {
				errCh <- err
			}
		}
	}()

	return outCh, errCh
}

func (r *RedisClient) Subscribe(ctx context.Context, channels ...string) (<-chan *redis.Message, <-chan error) {
	outCh := make(chan *redis.Message)
	errCh := make(chan error)

	go func() {
		defer close(outCh)
		defer close(errCh)

		switch client := r.redisCmdable.(type) {
		case *redis.Client:
			r.handleChannelSubs(ctx, client.Subscribe, outCh, errCh, channels...)

		case *redis.ClusterClient:
			r.handleChannelSubs(ctx, client.SSubscribe, outCh, errCh, channels...)
		}
	}()

	return outCh, errCh
}

func (r *RedisClient) handleChannelSubs(
	ctx context.Context,
	subFn func(ctx context.Context, channels ...string) *redis.PubSub,
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

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-ch:
			if !ok {
				errCh <- ErrChannelClosed
				return
			}
			outCh <- message
		}
	}
}

// https://pkg.go.dev/github.com/redis/go-redis/v9#Options
type RedisOptions struct {
	Mode                  string        `env:"REDIS_MODE"`
	Addr                  string        `env:"REDIS_HOSTS"`
	ClientName            string        `env:"REDIS_CLIENT_NAME"`
	ContextTimeoutEnabled bool          `env:"REDIS_CONTEXT_TIMEOUT_ENABLED"`
	MinIdleConns          int           `env:"REDIS_MIN_IDLE_CONNS"`
	MaxIdleConns          int           `env:"REDIS_MAX_IDLE_CONNS"`
	ConnMaxIdleTime       time.Duration `env:"REDIS_CONN_MAX_IDLE_TIME"`
	ConnMaxLifetime       time.Duration `env:"REDIS_CONN_MAX_LIFETIME"`
	DialTimeout           time.Duration `env:"REDIS_DIAL_TIMEOUT"`
	ReadTimeout           time.Duration `env:"REDIS_READ_TIMEOUT"`
	WriteTimeout          time.Duration `env:"REDIS_WRITE_TIMEOUT"`
	MaxRedirects          int           `env:"REDIS_MAX_REDIRECTS"`
	MaxRetries            int           `env:"REDIS_MAX_RETRIES"`
	PoolSize              int           `env:"REDIS_POOL_SIZE"`
	Username              string        `env:"REDIS_USERNAME"`
	Password              string        `env:"REDIS_PASSWORD"`
	TLSEnabled            bool          `env:"REDIS_TLS_ENABLED"`
	TLSConfig             *tls.Config
}

func (ro *RedisOptions) Addrs() []string {
	return strings.Split(ro.Addr, ",")
}

func GetRedisConfig() (*RedisOptions, error) {
	secrets := Secrets()
	options := &RedisOptions{}

	err := secrets.EnvUnmarshal(options)
	if err != nil {
		return nil, err
	}

	return options, nil
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

	if _, ok := l.locks[key]; ok {
		return ErrLockNotReleased
	}

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
		lock.Release(context.TODO())
		delete(l.locks, key)
		return nil
	}

	return redislock.ErrLockNotHeld
}

// Attempts to copy field values of the same name from the src to the dst struct.
func CopyStruct(src, dst interface{}) {
	srcVal := reflect.ValueOf(src).Elem()
	dstVal := reflect.ValueOf(dst).Elem()

	for i := 0; i < srcVal.NumField(); i++ {
		srcField := srcVal.Type().Field(i).Name
		dstField := dstVal.FieldByName(srcField)

		if dstField.IsValid() && dstField.CanSet() {
			dstField.Set(srcVal.Field(i))
		}
	}
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
