package storage

/*

What do we need the storage interface to look like?
 - we need the ability to configure different storage backends
 - we need the ability to mount storage dynamically
 - we probably want the ability to mount subpaths within the storage to a particular directory
 - we should be able to unmount storage
 - we should be able to get statistics / usage data from storage paths?

*/

type Storage interface {
	Mount(path string) error
	Format(fsName string) error
}
