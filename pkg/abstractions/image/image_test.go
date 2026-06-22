package image

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/stretchr/testify/assert"
)

func TestStreamImageBuildOutputSendsTerminalFailureWhenBuildReturnsWithoutOutput(t *testing.T) {
	outputChan := make(chan common.OutputMsg)
	buildErrChan := make(chan error, 1)
	buildErrChan <- errors.New("build container exited")

	var sent []common.OutputMsg
	lastMessage, err := streamImageBuildOutput(
		context.Background(),
		outputChan,
		buildErrChan,
		func(o common.OutputMsg) error {
			sent = append(sent, o)
			return nil
		},
	)

	assert.Error(t, err)
	assert.Equal(t, "build container exited\n", lastMessage.Msg)
	assert.True(t, lastMessage.Done)
	assert.False(t, lastMessage.Success)
	assert.Equal(t, []common.OutputMsg{lastMessage}, sent)
}

func TestStreamImageBuildOutputReturnsAfterTerminalSuccess(t *testing.T) {
	outputChan := make(chan common.OutputMsg)
	buildErrChan := make(chan error, 1)
	want := common.OutputMsg{
		Msg:           "Build completed successfully",
		Done:          true,
		Success:       true,
		ImageId:       "img-123",
		PythonVersion: "python3.12",
	}
	go func() {
		outputChan <- want
		buildErrChan <- nil
	}()

	var sent []common.OutputMsg
	lastMessage, err := streamImageBuildOutput(
		context.Background(),
		outputChan,
		buildErrChan,
		func(o common.OutputMsg) error {
			sent = append(sent, o)
			return nil
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, want, lastMessage)
	assert.Equal(t, []common.OutputMsg{want}, sent)
}
