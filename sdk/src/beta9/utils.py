import os


class TempFile:
    """
    A temporary file that is automatically deleted when closed. This class exists
    because the `tempfile.NamedTemporaryFile` class does not allow for the filename
    to be explicitly set.
    """

    def __init__(self, name: str, mode: str = "wb", dir: str = "."):
        self.name = name
        self._file = open(os.path.join(dir, name), mode)

    def __getattr__(self, attr):
        return getattr(self._file, attr)

    def close(self):
        if not self._file.closed:
            self._file.close()
            os.remove(self.name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
