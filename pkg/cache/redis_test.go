package cache

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/mitchellh/mapstructure"
	redis "github.com/redis/go-redis/v9"
	"gotest.tools/assert"
)

func Test_CopyConfig(t *testing.T) {

	var config RedisConfig
	faker.FakeData(&config)
	config.Addrs = []string{"localhost:6379"}

	tests := []struct {
		name            string
		sentinel        bool
		inputConfig     RedisConfig
		expectedOptions redis.UniversalOptions
	}{
		{
			name: "simple test",
			inputConfig: RedisConfig{
				Addrs: []string{"localhost:6379"},
			},
			expectedOptions: redis.UniversalOptions{
				Addrs: []string{"localhost:6379"},
			},
		},
		{
			name:        "full config",
			inputConfig: config,
			expectedOptions: redis.UniversalOptions{
				Addrs:           config.Addrs,
				ClientName:      config.ClientName,
				Username:        config.Username,
				Password:        config.Password,
				MinIdleConns:    config.MinIdleConns,
				MaxIdleConns:    config.MaxIdleConns,
				ConnMaxIdleTime: config.ConnMaxIdleTime,
				ConnMaxLifetime: config.ConnMaxLifetime,
				DialTimeout:     config.DialTimeout,
				ReadTimeout:     config.ReadTimeout,
				WriteTimeout:    config.WriteTimeout,
				MaxRedirects:    config.MaxRedirects,
				MaxRetries:      config.MaxRetries,
				PoolSize:        config.PoolSize,
				RouteByLatency:  config.RouteByLatency,
			},
		},
		{
			name:     "sentinel config",
			sentinel: true,
			inputConfig: RedisConfig{
				Addrs:            []string{"localhost:26379"},
				MasterName:       "mymaster",
				RouteByLatency:   true,
				Password:         "password",
				SentinelPassword: "sentinel_password",
			},
			expectedOptions: redis.UniversalOptions{
				Addrs:      []string{"localhost:26379"},
				MasterName: "mymaster",
				Password:   "password",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actualOptions redis.UniversalOptions
			mapstructure.Decode(test.inputConfig, &actualOptions)
			assert.Equal(t, test.expectedOptions.Addrs[0], actualOptions.Addrs[0])
			assert.Equal(t, test.expectedOptions.ClientName, actualOptions.ClientName)
			assert.Equal(t, test.expectedOptions.Username, actualOptions.Username)
			assert.Equal(t, test.expectedOptions.Password, actualOptions.Password)
			assert.Equal(t, test.expectedOptions.MinIdleConns, actualOptions.MinIdleConns)
			assert.Equal(t, test.expectedOptions.MaxIdleConns, actualOptions.MaxIdleConns)
			assert.Equal(t, test.expectedOptions.ConnMaxIdleTime, actualOptions.ConnMaxIdleTime)
			assert.Equal(t, test.expectedOptions.ConnMaxLifetime, actualOptions.ConnMaxLifetime)

			if test.sentinel {
				failoverOpts := actualOptions.Failover()
				assert.Equal(t, test.inputConfig.Addrs[0], failoverOpts.SentinelAddrs[0])
				assert.Equal(t, test.inputConfig.MasterName, failoverOpts.MasterName)
				assert.Equal(t, test.inputConfig.SentinelPassword, failoverOpts.SentinelPassword)
			}
		})
	}

}
