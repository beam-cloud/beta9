package types

type BaseFilter struct {
	Limit  uint32 `schema:"limit"`
	Offset int    `schema:"offset"`
}

type DeploymentFilter struct {
	BaseFilter
	WorkspaceID uint   `schema:"workspace_id"`
	StubType    string `schema:"stub_type"`
	Name        string `schema:"name"`
}

type TaskFilter struct {
	BaseFilter
	WorkspaceID uint   `schema:"workspace_id"`
	StubType    string `schema:"stub_type"`
	Status      string `schema:"status"`
}
