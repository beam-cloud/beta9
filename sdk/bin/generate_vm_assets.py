"""Regenerate the shipped VM bundle after changing guest files."""

import gzip
import io
import tarfile
from pathlib import Path

directory = Path(__file__).resolve().parents[1] / "src/beta9/abstractions/vm"
buffer = io.BytesIO()
with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
    for name in (
        "beam-desktop.service",
        "beam-terminal.service",
        "boot",
        "desktop",
        "browser.desktop",
        "browser-trust",
        "browser-trust-update",
        "browser",
        "terminal.desktop",
        "terminal.svg",
        "install",
        "xstartup",
    ):
        path = directory / name
        contents = path.read_bytes()
        entry = tarfile.TarInfo(name)
        entry.size = len(contents)
        entry.mode = 0o644 if path.suffix in (".service", ".desktop", ".svg") else 0o755
        archive.addfile(entry, io.BytesIO(contents))
with (directory / "assets.tar.gz").open("wb") as output:
    with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0, compresslevel=9) as compressed:
        compressed.write(buffer.getvalue())
