package common

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tj/assert"
)

func TestSecretsEnvUnmarshal(t *testing.T) {
	type TestConfig struct {
		Host      string        `env:"TEST_HOSTNAME"`
		Port      int           `env:"TEST_PORT" default:"3306"`
		Int32     int32         `env:"TEST_INT32"`
		BoolTrue  bool          `env:"TEST_BOOL_TRUE"`
		BoolFalse bool          `env:"TEST_BOOL_FALSE"`
		Duration  time.Duration `env:"TEST_DURATION"`
	}

	os.Setenv("TEST_HOSTNAME", "beam.cloud")
	os.Setenv("TEST_INT32", "77")
	os.Setenv("TEST_BOOL_TRUE", "t")
	os.Setenv("TEST_BOOL_FALSE", "0")
	os.Setenv("TEST_DURATION", fmt.Sprintf("%v", 3600*time.Second))

	expect := &TestConfig{Port: 3306, Host: "beam.cloud", Int32: 77, BoolTrue: true, BoolFalse: false, Duration: 3600 * time.Second}
	actual := &TestConfig{}

	secrets := Secrets()
	err := secrets.EnvUnmarshal(actual)
	assert.NoError(t, err)
	assert.Equal(t, expect, actual)
}
