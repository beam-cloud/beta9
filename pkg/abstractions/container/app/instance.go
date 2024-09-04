package container_app

import abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"

type appInstance struct {
	*abstractions.AutoscaledInstance
	// buffer *RequestBuffer
}
