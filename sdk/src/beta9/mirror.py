"""
The mirror utility on this machine: a small program, released separately,
that keeps chosen local files in the workspace so VMs can start with them.
This module only installs it and asks it which mirror this machine pushes to;
choosing files and uploading them belong to the utility.
"""

import hashlib
import json
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

from .config import get_settings

BINARY = "beam-mirror"
RELEASES_URL = "https://github.com/beam-cloud/beam-mirror/releases/latest/download"
CHECKSUMS = "SHA256SUMS"
MAX_DOWNLOAD = 256 << 20
TARGETS = {
    ("Darwin", "arm64"): "aarch64-apple-darwin",
    ("Darwin", "x86_64"): "x86_64-apple-darwin",
    ("Linux", "arm64"): "aarch64-unknown-linux-gnu",
    ("Linux", "x86_64"): "x86_64-unknown-linux-gnu",
}


class MirrorError(RuntimeError):
    pass


def releases_url() -> str:
    """Where releases are downloaded from; a mirror of the release assets can override it."""
    return (os.getenv("BEAM_MIRROR_RELEASES_URL") or RELEASES_URL).rstrip("/")


def asset_name() -> str:
    machine = platform.machine().lower()
    machine = {"amd64": "x86_64", "aarch64": "arm64"}.get(machine, machine)
    target = TARGETS.get((platform.system(), machine))
    if target is None:
        raise MirrorError(f"There is no mirror build for {platform.system()} {machine}.")
    return f"{BINARY}-{target}.tar.gz"


def install_path() -> Path:
    return get_settings().config_path.parent / "bin" / BINARY


def installed() -> Optional[str]:
    """The utility this machine would run: the installed copy, else one on PATH."""
    path = install_path()
    if path.is_file() and os.access(path, os.X_OK):
        return str(path)
    return shutil.which(BINARY)


def _download(url: str, limit: int = MAX_DOWNLOAD) -> bytes:
    try:
        with requests.get(url, stream=True, timeout=60) as response:
            if response.status_code == 404:
                raise MirrorError(f"No mirror release was found at {url}.")
            response.raise_for_status()
            body = bytearray()
            for block in response.iter_content(1 << 20):
                body += block
                if len(body) > limit:
                    raise MirrorError(f"{url} is larger than expected; nothing was installed.")
            return bytes(body)
    except requests.RequestException as exc:
        raise MirrorError(f"Could not download {url}: {exc}") from exc


def _checksums(text: str) -> Dict[str, str]:
    sums = {}
    for line in text.splitlines():
        digest, _, name = line.strip().partition(" ")
        name = name.strip().lstrip("*")
        if len(digest) == 64 and name:
            sums[name] = digest.lower()
    return sums


def _replace(path: Path, data: bytes, mode: int) -> None:
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(data)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def install() -> Tuple[Path, bool]:
    """
    Install the latest release for this platform, or replace an older one.
    Returns the path and whether anything changed. The release's checksum
    file names the archive's SHA-256; an archive that does not match is
    never unpacked, and only the utility itself is taken out of it.
    """
    asset, base = asset_name(), releases_url()
    expected = _checksums(_download(f"{base}/{CHECKSUMS}", 1 << 20).decode("utf-8", "replace")).get(
        asset
    )
    if expected is None:
        raise MirrorError(f"The latest mirror release has no {asset}.")

    path = install_path()
    record = path.with_name(BINARY + ".sha256")
    if installed() == str(path) and record.is_file() and record.read_text().strip() == expected:
        return path, False

    archive = _download(f"{base}/{asset}")
    if hashlib.sha256(archive).hexdigest() != expected:
        raise MirrorError(f"{asset} does not match its checksum; nothing was installed.")
    with tempfile.TemporaryFile() as file:
        file.write(archive)
        file.seek(0)
        try:
            with tarfile.open(fileobj=file, mode="r:gz") as tar:
                member = next(
                    (m for m in tar if m.name in (BINARY, "./" + BINARY) and m.isreg()), None
                )
                if member is None or member.size > MAX_DOWNLOAD:
                    raise MirrorError(f"{asset} does not contain {BINARY}.")
                binary = tar.extractfile(member).read()
        except tarfile.TarError as exc:
            raise MirrorError(f"{asset} is not a valid archive: {exc}") from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    _replace(path, binary, 0o755)
    _replace(record, (expected + "\n").encode(), 0o644)
    return path, True


def _run(*args: str) -> str:
    utility = installed()
    if utility is None:
        raise MirrorError("The mirror utility is not installed on this machine.")
    try:
        result = subprocess.run(
            [utility, *args], capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise MirrorError(f"Could not run {utility}: {exc}") from exc
    if result.returncode != 0:
        reason = (result.stderr or result.stdout).strip().splitlines()
        raise MirrorError(reason[-1] if reason else f"{utility} {args[0]} failed")
    return result.stdout


def start(context: str) -> dict:
    """Point the utility at this CLI's sign-in and start it. Returns its status."""
    _run("init", "--config", str(get_settings().config_path), "--context", context)
    return status()


def status() -> dict:
    try:
        result = json.loads(_run("status", "--json"))
    except ValueError as exc:
        raise MirrorError("The mirror utility returned an unreadable status.") from exc
    if not isinstance(result, dict):
        raise MirrorError("The mirror utility returned an unreadable status.")
    return result
