package common

import "github.com/labstack/echo/v4"

type FunctionPayload struct {
	Args   []interface{}          `json:"args"`
	Kwargs map[string]interface{} `json:"kwargs"`
}

// UnmarshalFunctionPayload unmarshals a task queue function payload,
// treating any non-structured payload as kwargs.
func UnmarshalFunctionPayload(ctx echo.Context) (FunctionPayload, error) {
	var payload FunctionPayload

	// Attempt to unmarshal into the structured payload.
	if err := ctx.Bind(&payload); err != nil {
		return FunctionPayload{}, err
	}

	// If Args is nil and Kwargs is empty, assume the payload is intended as Kwargs.
	if payload.Args == nil && len(payload.Kwargs) == 0 {
		if err := ctx.Bind(&payload.Kwargs); err != nil {
			return FunctionPayload{}, err
		}
	}

	return payload, nil
}
