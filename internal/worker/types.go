package worker

type Mount struct {
	Name      string `json:"name"`
	LocalPath string `json:"local_path"`
	MountPath string `json:"mount_path"`
	ReadOnly  bool   `json:"read_only"`
}

type ContainerConfigResponse struct {
	IdentityId    string   `json:"identity_id"`
	ImageTag      string   `json:"image_tag"`
	S2SToken      string   `json:"s2s_token"`
	Mounts        []Mount  `json:"mounts"`
	Env           []string `json:"env"`
	WorkspacePath string   `json:"workspace_path"`
}
