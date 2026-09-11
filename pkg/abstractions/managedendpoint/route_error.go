package managedendpoint

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// Errors on /v1 use the OpenAI envelope: {"error": {"message", "type", "code",
// "param"}}. The type follows the status; the code names the exact cause.
type routeError struct {
	Status  int
	Code    string
	Message string
}

func (e *routeError) Error() string { return e.Message }

func (e *routeError) write(ctx echo.Context) error {
	h := ctx.Response().Header()
	h.Set("Content-Type", "application/json")
	h.Del("Content-Encoding")
	if e.Status == http.StatusTooManyRequests && h.Get("Retry-After") == "" {
		h.Set("Retry-After", "1")
	}
	body := map[string]any{"message": e.Message, "type": errorType(e.Status), "code": e.Code, "param": nil}
	return ctx.JSON(e.Status, map[string]any{"error": body})
}

const (
	errorTypeInvalidRequest = "invalid_request_error"
	errorTypeServer         = "server_error"
)

var errorTypes = map[int]string{
	http.StatusUnauthorized:    "authentication_error",
	http.StatusForbidden:       "authentication_error",
	http.StatusPaymentRequired: "insufficient_quota",
	http.StatusTooManyRequests: "rate_limit_error",
	http.StatusNotFound:        "not_found_error",
}

func errorType(status int) string {
	if kind, ok := errorTypes[status]; ok {
		return kind
	}
	if status >= 500 {
		return errorTypeServer
	}
	return errorTypeInvalidRequest
}

// Fixed causes. Dynamic messages use the constructors below.
var (
	errUnauthorized          = &routeError{http.StatusUnauthorized, "unauthorized", "a workspace token is required"}
	errEndpointsDisabled     = &routeError{http.StatusNotFound, "not_found", "managed endpoints are not enabled"}
	errUnknownRoute          = &routeError{http.StatusNotFound, "not_found", "unknown route"}
	errRegistry              = &routeError{http.StatusServiceUnavailable, "registry_unavailable", "endpoint registry unavailable"}
	errBodyUnreadable        = &routeError{http.StatusBadRequest, "invalid_body", "failed to read request body"}
	errBodyTooLarge          = &routeError{http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 64MB"}
	errNotJSONObject         = &routeError{http.StatusBadRequest, "invalid_json", "request body must be a JSON object"}
	errMissingModel          = &routeError{http.StatusBadRequest, "missing_model", "the model field is required"}
	errInsufficientCredits   = &routeError{http.StatusPaymentRequired, "insufficient_credits", "insufficient credits: add credits to continue using inference endpoints"}
	errBillingUnavailable    = &routeError{http.StatusServiceUnavailable, "billing_unavailable", "billing check unavailable"}
	errUpstreamUnavailable   = &routeError{http.StatusBadGateway, "upstream_unavailable", "upstream replicas failed"}
	errUpstreamEnded         = &routeError{http.StatusBadGateway, "upstream_failed", "upstream response ended early"}
	errUpstreamTooLarge      = &routeError{http.StatusBadGateway, "upstream_too_large", "upstream response exceeds 64MB"}
	errClientClosed          = &routeError{statusClientClosed, "client_closed", "client closed request"}
	errGatewayDraining       = &routeError{http.StatusServiceUnavailable, "gateway_draining", "gateway is restarting, retry shortly"}
	errAccountingUnavailable = &routeError{http.StatusServiceUnavailable, "accounting_unavailable", "Unable to record request usage"}
	errMissingUsage          = &routeError{http.StatusBadGateway, "missing_usage", "upstream response carried no usage; request not billed"}
	errMissingID             = &routeError{http.StatusBadRequest, "missing_id", "id query parameter is required"}
	errGenerationNotFound    = &routeError{http.StatusNotFound, "generation_not_found", "generation not found"}
	errInvalidCatalog        = &routeError{http.StatusServiceUnavailable, "invalid_catalog", "provider catalog configuration is invalid"}
)

const statusClientClosed = 499 // nginx convention: the client went away first

func badRequest(code, message string) *routeError {
	return &routeError{http.StatusBadRequest, code, message}
}

func notFound(code, message string) *routeError {
	return &routeError{http.StatusNotFound, code, message}
}

func forbidden(code, message string) *routeError {
	return &routeError{http.StatusForbidden, code, message}
}

func capacityError(message string) *routeError {
	return &routeError{http.StatusTooManyRequests, "rate_limit_exceeded", message}
}

func modelNotFound(model string) *routeError {
	return notFound("model_not_found", fmt.Sprintf("model %s not found", model))
}

// openAIErrors rewrites echo's own errors (auth middleware, bad routes) into
// the envelope, unless a response has already started.
func openAIErrors(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx echo.Context) error {
		err := next(ctx)
		if err == nil || ctx.Response().Committed {
			return err
		}
		var httpErr *echo.HTTPError
		if !errors.As(err, &httpErr) {
			return err
		}
		return fromHTTPError(httpErr).write(ctx)
	}
}

func fromHTTPError(httpErr *echo.HTTPError) *routeError {
	code := "invalid_request"
	switch {
	case httpErr.Code == http.StatusUnauthorized || httpErr.Code == http.StatusForbidden:
		code = "invalid_api_key"
	case httpErr.Code >= 500:
		code = errorTypeServer
	}
	return &routeError{httpErr.Code, code, http.StatusText(httpErr.Code)}
}
