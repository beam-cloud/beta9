package task

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func SerializeHttpPayload(ctx echo.Context) (*types.TaskPayload, error) {
	defer ctx.Request().Body.Close()

	// Create a JSON decoder
	decoder := json.NewDecoder(ctx.Request().Body)

	// Decode the JSON directly from the reader
	payload := map[string]interface{}{}
	if err := decoder.Decode(&payload); err != nil {
		if err != io.EOF {
			return nil, errors.New("invalid request payload")
		}
	}

	taskPayload := &types.TaskPayload{
		Args:   nil,
		Kwargs: make(map[string]interface{}),
	}

	if len(payload) > 0 {
		parseRequestPayload(payload, taskPayload)
	}

	// Parse query params into kwargs entries
	queryParams := ctx.Request().URL.Query()
	if len(queryParams) > 0 {
		parseRequestArgs(queryParams, taskPayload)
	}

	return taskPayload, nil
}

func parseRequestPayload(payload map[string]interface{}, taskPayload *types.TaskPayload) {
	// Check if payload is a list (args)
	if args, ok := payload["args"].([]interface{}); ok {
		taskPayload.Args = args
		delete(payload, "args")
	}

	// Extract and remove 'kwargs' if explicitly present as a submap
	if kwargs, ok := payload["kwargs"].(map[string]interface{}); ok {
		taskPayload.Kwargs = kwargs
		delete(payload, "kwargs")
	} else if len(payload) > 0 {
		// Remaining payload is treated as kwargs if not empty
		taskPayload.Kwargs = payload
	}
}

func parseRequestArgs(queryParams url.Values, taskPayload *types.TaskPayload) {
	if taskPayload.Kwargs == nil {
		taskPayload.Kwargs = make(map[string]interface{})
	}

	for key, values := range queryParams {
		if len(values) == 1 {
			taskPayload.Kwargs[key] = convertToNumericIfPossible(values[0])
		} else {
			allInt := true
			allFloat := true
			floats := make([]float64, len(values))
			ints := make([]int64, len(values))

			for i, value := range values {
				convertedValue := convertToNumericIfPossible(value)
				switch v := convertedValue.(type) {
				case int64:
					ints[i] = v
					allFloat = false
				case float64:
					floats[i] = v
					allInt = false
				default:
					allInt = false
					allFloat = false
					continue
				}
			}
			if allInt {
				taskPayload.Kwargs[key] = ints
			} else if allFloat {
				taskPayload.Kwargs[key] = floats
			} else {
				taskPayload.Kwargs[key] = values
			}
		}
	}
}

func convertToNumericIfPossible(s string) interface{} {
	if intVal, err := strconv.ParseInt(s, 10, 64); err == nil {
		return intVal
	}
	if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
		return floatVal
	}
	return s
}
