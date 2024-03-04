package types

type BaseFilter struct {
	Limit  uint32 `query:"limit"`
	Offset int    `query:"offset"`
}

type DeploymentFilter struct {
	BaseFilter
	WorkspaceID uint   `query:"workspace_id"`
	StubType    string `query:"stub_type"`
	Name        string `query:"name"`
}

type TaskFilter struct {
	BaseFilter
	WorkspaceID uint   `query:"workspace_id"`
	StubType    string `query:"stub_type"`
	StubId      string `query:"stub_id"`
	Status      string `query:"status"`
}
