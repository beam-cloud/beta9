package types

type SerializedStub struct {
	Stub
	Id          string `json:"id"`
	ObjectId    string `json:"object_id,omitempty"`
	WorkspaceId string `json:"workspace_id,omitempty"`
}

func (s *SerializedStub) Serialize(stub Stub) *SerializedStub {
	s.Stub = stub
	s.Id = stub.ExternalId
	s.ExternalId = ""
	return s
}

type SerializedStubWithRelated struct {
	SerializedStub
	Object    SerializedObject    `json:"object,omitempty"`
	Workspace SerializedWorkspace `json:"workspace,omitempty"`
}

func (s *SerializedStubWithRelated) Serialize(stub StubWithRelated) *SerializedStubWithRelated {
	s.SerializedStub.Serialize(stub.Stub)
	s.Object.Serialize(stub.Object)
	s.Workspace.Serialize(stub.Workspace)
	return s
}

type SerializedWorkspace struct {
	Workspace
	Id        string `json:"id"`
	StorageId string `json:"storage_id,omitempty"`
}

func (s *SerializedWorkspace) Serialize(workspace Workspace) *SerializedWorkspace {
	s.Workspace = workspace
	s.Id = workspace.ExternalId
	s.ExternalId = ""
	return s
}

type SerializedObject struct {
	Object
	Id          string `json:"id"`
	WorkspaceId string `json:"workspace_id,omitempty"`
}

func (s *SerializedObject) Serialize(object Object) *SerializedObject {
	s.Object = object
	s.Id = object.ExternalId
	s.ExternalId = ""
	return s
}

type SerializedDeployment struct {
	Deployment
	Id          string `json:"id"`
	AppId       string `json:"app_id,omitempty"`
	StubId      string `json:"stub_id,omitempty"`
	WorkspaceId string `json:"workspace_id,omitempty"`
}

func (s *SerializedDeployment) Serialize(deployment Deployment) *SerializedDeployment {
	s.Deployment = deployment
	s.Id = deployment.ExternalId
	s.ExternalId = ""
	return s
}

type SerializedDeploymentWithRelated struct {
	SerializedDeployment
	Stub      SerializedStub      `json:"stub,omitempty"`
	Workspace SerializedWorkspace `json:"workspace,omitempty"`
}

func (s *SerializedDeploymentWithRelated) Serialize(deployment DeploymentWithRelated) *SerializedDeploymentWithRelated {
	s.SerializedDeployment.Serialize(deployment.Deployment)
	s.Stub.Serialize(deployment.Stub)
	s.Workspace.Serialize(deployment.Workspace)
	return s
}

type SerializedApp struct {
	App
	Id          string `json:"id,omitempty"`
	WorkspaceId string `json:"workspace_id,omitempty"`
}

func (s *SerializedApp) Serialize(app App) *SerializedApp {
	s.App = app
	s.Id = app.ExternalId
	s.ExternalId = ""
	return s
}
