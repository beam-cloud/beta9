package cache

type Cache interface {
	Set(string, string) error
	Get(string) (string, error)
	Configure()
	Start()
	OnContainerStart()
	OnContainerQuit()
}
