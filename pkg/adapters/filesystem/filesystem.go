package filesystem

type FileSystemOpts struct {
	MountPoint string
	Verbose    bool
}

type FileSystem interface {
	Mount() (func() error, <-chan error, error)
	Unmount() error
}

type FileSystemStorage interface {
	Metadata()
	Get(string)
	ListDirectory(string)
	ReadFile(interface{} /* This could be any sort of FS node type, depending on the implementation */, []byte, int64)
}

type MetadataEngine interface {
}
