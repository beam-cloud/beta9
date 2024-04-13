package task

import (
	"errors"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

// Helper function to check if a payload is purely a map (kwargs)
func isMap(payload map[string]interface{}) bool {
	for _, value := range payload {
		switch value.(type) {
		case []interface{}: // If any value is a slice, this isn't a pure map
			return false
		}
	}
	return true
}

func SerializeHttpPayload(ctx echo.Context) (*types.TaskPayload, error) {
	payload := map[string]interface{}{}
	if err := ctx.Bind(&payload); err != nil {
		return nil, errors.New("invalid request payload")
	}

	taskPayload := &types.TaskPayload{}

	// Check if payload is a list (args)
	if args, ok := payload["args"].([]interface{}); ok {
		taskPayload.Args = args
		delete(payload, "args")
	}

	// Extract and remove 'kwargs' if explicitly present as a submap
	if kwargs, ok := payload["kwargs"].(map[string]interface{}); ok {
		taskPayload.Kwargs = kwargs
		delete(payload, "kwargs")
	} else if len(payload) > 0 && isMap(payload) {
		// Remaining payload is treated as kwargs if it is a map and 'kwargs' key wasn't explicitly provided
		taskPayload.Kwargs = payload
	}

	// If the payload is empty after removing "args", assume all entries are args
	if len(payload) == 0 && len(taskPayload.Args) == 0 {
		if args, ok := payload[""].([]interface{}); ok {
			taskPayload.Args = args
		} else {
			return nil, errors.New("task payload structure not recognized")
		}
	}

	return taskPayload, nil
}
