import json
import os
import sys


def fadvise_dontneed(fd):
    advise = getattr(os, "posix_fadvise", None)
    if advise is None:
        return False
    try:
        advise(fd, 0, 0, getattr(os, "POSIX_FADV_DONTNEED", 4))
        return True
    except OSError:
        return False


def main():
    content_hash = sys.argv[1]
    roots = sys.argv[2:]
    for root in roots:
        if not os.path.isdir(root):
            continue
        total = 0
        chunks = 0
        evicted = False
        index = 0
        while True:
            path = os.path.join(root, f"{content_hash}-{index}")
            if not os.path.exists(path):
                break
            fd = os.open(path, os.O_RDONLY)
            try:
                total += os.stat(path).st_size
                evicted = fadvise_dontneed(fd) or evicted
                chunks += 1
            finally:
                os.close(fd)
            index += 1
        print(
            json.dumps(
                {"evicted": evicted, "root": root, "bytes": total, "chunks": chunks},
                sort_keys=True,
            )
        )
        return
    print(json.dumps({"evicted": False, "bytes": 0, "chunks": 0}, sort_keys=True))


if __name__ == "__main__":
    main()
