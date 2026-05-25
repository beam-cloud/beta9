import json
import os
import sys


def main():
    path = sys.argv[1]
    expected = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    if not os.path.exists(path):
        print(
            json.dumps({"ok": False, "path": path, "error": "missing"}, sort_keys=True)
        )
        return
    size = os.stat(path).st_size
    fd = os.open(path, os.O_RDONLY)
    evicted = False
    try:
        advise = getattr(os, "posix_fadvise", None)
        if advise is not None:
            advise(fd, 0, 0, getattr(os, "POSIX_FADV_DONTNEED", 4))
            evicted = True
    finally:
        os.close(fd)
    print(
        json.dumps(
            {
                "ok": evicted,
                "path": path,
                "bytes": size,
                "matchingSize": not expected or size == expected,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
