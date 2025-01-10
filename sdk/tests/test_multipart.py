from textwrap import dedent

from beta9 import multipart


def test_calculate_chunk_size():
    output = dedent("""
        File:     2.0 MiB → Chunk:   2.0 MiB, Workers: 1
        File:     4.0 MiB → Chunk:   4.0 MiB, Workers: 1
        File:     8.0 MiB → Chunk:   5.0 MiB, Workers: 1
        File:    16.0 MiB → Chunk:   5.0 MiB, Workers: 1
        File:    32.0 MiB → Chunk:   5.0 MiB, Workers: 2
        File:    64.0 MiB → Chunk:   5.0 MiB, Workers: 3
        File:   128.0 MiB → Chunk:   8.0 MiB, Workers: 4
        File:   256.0 MiB → Chunk:  16.0 MiB, Workers: 5
        File:   512.0 MiB → Chunk:  32.0 MiB, Workers: 6
        File:  1024.0 MiB → Chunk:  64.0 MiB, Workers: 7
        File:  2048.0 MiB → Chunk: 128.0 MiB, Workers: 8
        File:  4096.0 MiB → Chunk: 256.0 MiB, Workers: 9
        File:  8192.0 MiB → Chunk: 500.0 MiB, Workers: 10
        File: 16384.0 MiB → Chunk: 500.0 MiB, Workers: 11
        File: 32768.0 MiB → Chunk: 500.0 MiB, Workers: 12
    """)

    mib = 1024 * 1024
    lines = [""]
    sizes = {(2**i) * mib: multipart._calculate_chunk_size((2**i) * mib) for i in range(1, 16)}
    for size, (chunk, workers) in sizes.items():
        line = f"File: {(size/mib):7.1f} MiB → Chunk: {(chunk/mib):5.1f} MiB, Workers: {workers}"
        lines.append(line)
        print(line)

    assert output.splitlines() == lines
