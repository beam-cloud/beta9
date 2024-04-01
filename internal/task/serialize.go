package task

import (
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

func SerializeHttpPayload(ctx echo.Context) (*types.TaskPayload, error) {
	return &types.TaskPayload{}, nil
}
