package cache

import (
	"testing"

	"github.com/tj/assert"
)

func Test_IsTLSEnabled(t *testing.T) {
	assert.True(t, isTLSEnabled("localhost:443"))
	assert.False(t, isTLSEnabled("localhost:2049"))
	assert.True(t, isTLSEnabled("127.0.0.1:443"))
	assert.False(t, isTLSEnabled("127.0.0.1:2049"))
}
