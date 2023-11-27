package cache

type Map interface {
	Set(string, string) error
	Get(string) (string, error)
	Configure()
	Start()
	OnContainerStart()
	OnContainerQuit()
}
