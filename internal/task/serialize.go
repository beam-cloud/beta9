package task

import (
	"errors"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

func SerializeHttpPayload(ctx echo.Context) (*types.TaskPayload, error) {
	payload := &types.TaskPayload{}

	if err := ctx.Bind(&payload); err != nil {
		return nil, errors.New("invalid request payload")
	}

	return payload, nil
}
