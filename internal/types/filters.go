package types

type BaseFilter struct {
	Limit uint32 `schema:"limit"`
}

type DeploymentFilter struct {
	BaseFilter
	WorkspaceID uint   `schema:"workspace_id"`
	StubType    string `schema:"stub_type"`
	Name        string `schema:"name"`
}
