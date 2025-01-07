package task

import (
	"errors"
	"io"
	"net/url"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/types"
	jsoniter "github.com/json-iterator/go"
	"github.com/labstack/echo/v4"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func SerializeHttpPayload(ctx echo.Context) (*types.TaskPayload, error) {
	defer ctx.Request().Body.Close()

	decoder := json.NewDecoder(ctx.Request().Body)

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
		// Check if query param is singular or a list
		if len(values) == 1 {
			taskPayload.Kwargs[key] = tryParseNumeric(values[0])
			continue
		}

		allFloat := true
		floats := make([]float64, len(values))

		for i, value := range values {
			convertedValue := tryParseNumeric(value)
			switch v := convertedValue.(type) {
			case float64:
				floats[i] = v
			default:
				allFloat = false
				continue
			}
		}
		if allFloat {
			taskPayload.Kwargs[key] = floats
		} else {
			taskPayload.Kwargs[key] = values
		}
	}
}

func tryParseNumeric(s string) interface{} {
	if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
		return floatVal
	}
	return s
}
