package scheduler

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/tj/assert"
)

func TestRetrySoonIncrementsRetryCount(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Timestamp:   time.Now(),
	}

	newSchedulingAttempt(scheduler, request, nil).retrySoon("test")
	assert.Equal(t, 1, request.RetryCount)

	time.Sleep(requestProcessingInterval + 10*time.Millisecond)
	requeuedRequest, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeuedRequest.ContainerId)
	assert.Equal(t, 1, requeuedRequest.RetryCount)
}
