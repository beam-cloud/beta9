package storage

type Storage interface {
	Mount(localPath string) error
	Format(fsName string) error
	Unmount(localPath string) error
}
