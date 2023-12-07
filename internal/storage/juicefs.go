package storage

type JuiceFsStorage struct {
}

func NewJuiceFsStorage() (Storage, error) {
	return &JuiceFsStorage{}, nil
}

func (s *JuiceFsStorage) Mount(path string) error {
	return nil
}
