package worker

type Mount struct {
	Name      string `json:"name"`
	LocalPath string `json:"local_path"`
	MountPath string `json:"mount_path"`
	ReadOnly  bool   `json:"read_only"`
}
