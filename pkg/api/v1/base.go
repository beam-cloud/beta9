package apiv1

const (
	HttpServerBaseRoute string = "/api/v1"
	HttpServerRootRoute string = ""
)

type ResponseError struct {
	Message string `json:"message"`
}
