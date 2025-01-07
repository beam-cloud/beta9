import concurrent.futures
import io
import math
import os
import signal
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from multiprocessing import Manager
from os import PathLike
from pathlib import Path
from queue import Queue
from threading import Thread, local
from typing import (
    Callable,
    ContextManager,
    Final,
    Generator,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

import click
import requests
from requests import Session
from typing_extensions import ParamSpec

from .clients.volume import (
    AbortMultipartUploadRequest,
    CompletedPart,
    CompleteMultipartUploadRequest,
    CreateMultipartUploadRequest,
    CreatePresignedUrlRequest,
    FileUploadPart,
    ListPathRequest,
    ListVolumesRequest,
    PresignedUrlMethod,
    VolumeServiceStub,
)
from .env import try_env
from .exceptions import (
    CompleteMultipartUploadError,
    CreateMultipartUploadError,
    CreatePresignedUrlError,
    DownloadChunkError,
    GetFileSizeError,
    RetryableError,
    UploadPartError,
)

__all__ = ["upload", "download"]

_PROCESS_LOCAL: Final[local] = local()
_MAX_WORKERS: Final[int] = try_env("MULTIPART_MAX_WORKERS", 4)
_REQUEST_TIMEOUT: Final[int] = try_env("MULTIPART_REQUEST_TIMEOUT", 3)

UPLOAD_CHUNK_SIZE: Final[int] = try_env("MULTIPART_UPLOAD_CHUNK_SIZE", 4 * 1024 * 1024)
DOWNLOAD_CHUNK_SIZE: Final[int] = try_env("MULTIPART_DOWNLOAD_CHUNK_SIZE", 32 * 1024 * 1024)


class ProgressCallback(Protocol):
    def __call__(self, total: int, advance: int) -> None: ...


class CompletionCallback(Protocol):
    def __call__(self) -> ContextManager: ...


P = ParamSpec("P")
R = TypeVar("R")


def retry(
    times: int, delay: float = 0.1, max_delay: float = 10.0
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Retry a function multiple times with exponential backoff.

    The exponential backoff starts with the initial delay and
    doubles with each retry.

    Args:
        times: The number of times to retry the function.
        delay: The initial delay between retries. Defaults to 0.1.
        max_delay: The maximum delay between retries. Defaults to 10.0.

    Raises:
        RetryableError: If the function fails after all retries.

    Returns:
        A decorator that wraps the function.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            current_delay = delay
            last_exception = None

            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < times - 1:
                        time.sleep(min(current_delay, max_delay))
                        current_delay *= 2

            raise RetryableError(times, str(last_exception))

        return wrapper

    return decorator


@contextmanager
def _progress_updater(
    file_size: int, queue: Queue, callback: Optional[ProgressCallback] = None, timeout: float = 1.0
) -> Generator[Thread, None, None]:
    """
    Calls a callback with the progress of a multipart upload or download.

    Args:
        file_size: The total size of the file.
        queue: A queue that receives the number of bytes processed.
        callback: A callback that receives the total size and the number of bytes processed.
            Defaults to None.
        timeout: The time to wait for the thread to finish. Defaults to 1.0.

    Yields:
        A thread that updates the progress.
    """

    def target():
        finished = 0
        while finished < file_size:
            try:
                processed = queue.get()
            except Exception:
                time.sleep(0.1)
                continue

            finished += processed
            if callback is not None:
                callback(total=file_size, advance=processed)

    thread = Thread(target=target, daemon=True)
    thread.start()

    yield thread

    thread.join(timeout=timeout)


def _get_session() -> Session:
    """
    Get a requests session from the process's local storage.
    """
    if not hasattr(_PROCESS_LOCAL, "session"):
        _PROCESS_LOCAL.session = requests.Session()
    return _PROCESS_LOCAL.session


def _init():
    """
    Initialize the process by setting a signal handler.
    """
    signal.signal(signal.SIGINT, lambda *_: os.kill(os.getpid(), signal.SIGTERM))


@retry(times=10)
def _upload_part(file_path: Path, file_part: FileUploadPart, queue: Queue) -> CompletedPart:
    """
    Read a chunk of a file and upload it to a URL.

    Args:
        file_path: Path to the file.
        file_part: Information about the part to upload.
        queue: A queue to send the number of bytes processed.

    Raises:
        UploadPartError: If the upload fails.

    Returns:
        Information about the completed part.
    """
    session = _get_session()

    chunk = _get_file_chunk(file_path, file_part.start, file_part.end)
    chunk_size = len(chunk)

    class QueueBuffer(io.BytesIO):
        def read(self, size: Optional[int] = -1) -> bytes:
            b = super().read(size)
            queue.put(len(b), block=False)
            return b

    try:
        response = session.put(
            url=file_part.url,
            data=QueueBuffer(chunk),
            headers={
                "Content-Length": str(chunk_size),
            },
        )
        response.raise_for_status()
        etag = response.headers["ETag"].replace('"', "")
    except Exception as e:
        raise UploadPartError(file_part.number, str(e))

    return CompletedPart(number=file_part.number, etag=etag)


def _get_file_chunk(file_path: Path, start: int, end: int) -> bytes:
    with open(file_path, "rb") as f:
        f.seek(start)
        return f.read(end - start)


def upload(
    service: VolumeServiceStub,
    file_path: Path,
    volume_name: str,
    volume_path: str,
    progress_callback: Optional[ProgressCallback] = None,
    completion_callback: Optional[CompletionCallback] = None,
    chunk_size: int = UPLOAD_CHUNK_SIZE,
):
    """
    Upload a file to a volume using multipart upload.

    Args:
        service: The volume service stub.
        file_path: Path to the file to upload.
        volume_name: Name of the volume.
        volume_path: Path to the file on the volume.
        progress_callback: A callback that receives the total size and the number of
            bytes processed. Defaults to None.
        completion_callback: A context manager that wraps the completion of the upload.
            Defaults to None.
        chunk_size: Size of each chunk in bytes. Defaults to 4 MiB.

    Raises:
        CreateMultipartUploadError: If initializing the upload fails.
        CompleteMultipartUploadError: If completing the upload fails.
        KeyboardInterrupt: If the upload is interrupted by the user.
        Exception: If any other error occurs.
    """
    # Initialize multipart upload
    file_size = file_path.stat().st_size
    initial = retry(times=3, delay=1.0)(service.create_multipart_upload)(
        CreateMultipartUploadRequest(
            volume_name=volume_name,
            volume_path=volume_path,
            chunk_size=chunk_size,
            file_size=file_size,
        )
    )
    if not initial.ok:
        raise CreateMultipartUploadError(initial.err_msg)

    # Start multipart upload
    try:
        with ExitStack() as stack:
            manager = stack.enter_context(Manager())
            executor = stack.enter_context(ProcessPoolExecutor(_MAX_WORKERS, initializer=_init))

            queue = manager.Queue()
            stack.enter_context(_progress_updater(file_size, queue, progress_callback))

            futures = (
                executor.submit(_upload_part, file_path, part, queue)
                for part in initial.file_upload_parts
            )

            parts = [future.result() for future in concurrent.futures.as_completed(futures)]
            parts.sort(key=lambda part: part.number)

        # Complete multipart upload
        def complete_upload():
            completed = retry(times=3, delay=1.0)(service.complete_multipart_upload)(
                CompleteMultipartUploadRequest(
                    upload_id=initial.upload_id,
                    volume_name=volume_name,
                    volume_path=volume_path,
                    completed_parts=parts,
                )
            )
            if not completed.ok:
                raise CompleteMultipartUploadError(completed.err_msg)

        if completion_callback is not None:
            with completion_callback():
                complete_upload()
        else:
            complete_upload()

    except (Exception, KeyboardInterrupt):
        service.abort_multipart_upload(
            AbortMultipartUploadRequest(
                upload_id=initial.upload_id, volume_name=volume_name, volume_path=volume_path
            )
        )
        raise


class FileChunk(NamedTuple):
    number: int
    path: Path


class FileRange(NamedTuple):
    number: int
    start: int
    end: int


@retry(times=3, delay=1.0)
def _get_file_size(url: str) -> int:
    session = _get_session()

    response = session.head(url)
    if response.status_code != 200:
        raise GetFileSizeError(response.status_code, response.text)

    return int(response.headers["Content-Length"])


def _calculate_file_ranges(file_size: int, chunk_size: int) -> List[FileRange]:
    """
    Calculate byte ranges for a file based on the chunk size.

    Args:
        file_size: Size of the file in bytes.
        chunk_size: Size of each chunk in bytes.

    Returns:
        List of byte ranges.
    """
    ranges = math.ceil(file_size / chunk_size)
    return [
        FileRange(
            number=i + 1,
            start=i * chunk_size,
            end=min(file_size - 1, (i + 1) * chunk_size - 1),
        )
        for i in range(ranges)
    ]


@retry(times=10)
def _download_chunk(
    url: str,
    file_range: FileRange,
    output_dir: Path,
    queue: Queue,
) -> FileChunk:
    """
    Download a byte range of a file to a temporary directory.

    Args:
        url: URL of the file.
        file_range: Byte range to download.
        output_dir: Directory to save the file.
        queue: A queue to send the number of bytes processed.

    Raises:
        DownloadChunkError: If the download fails.

    Returns:
        Information about the downloaded chunk.
    """
    session = _get_session()
    headers = {"Range": f"bytes={file_range.start}-{file_range.end}"}

    try:
        response = session.get(url=url, headers=headers, stream=True, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()

        path = output_dir / f"data_{file_range.number}"
        with open(path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                queue.put(file.write(chunk), block=False)
    except Exception as e:
        raise DownloadChunkError(file_range.number, file_range.start, file_range.end, str(e))

    return FileChunk(number=file_range.number, path=path)


def _merge_file_chunks(file_path: PathLike, file_chunks: Sequence[FileChunk]) -> None:
    """
    Merge file chunks into a single file then delete the chunks.
    """
    with open(file_path, "wb") as merged_file:
        for chunk in file_chunks:
            with open(chunk.path, "rb") as chunk_file:
                merged_file.write(chunk_file.read())
            os.remove(chunk.path)


def download(
    service: VolumeServiceStub,
    volume_name: str,
    volume_path: str,
    file_path: Path,
    callback: Optional[ProgressCallback] = None,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
) -> None:
    """
    Download a file from a volume using multipart download.

    Args:
        service: The volume service stub.
        volume_name: Name of the volume.
        volume_path: Path to the file on the volume.
        file_path: Path to save the file.
        callback: A callback that receives the total size and the number of bytes processed.
            Defaults to None.
        chunk_size: Size of each chunk in bytes. Defaults to 32 MiB.

    Raises:
        CreatePresignedUrlError: If a presigned URL cannot be created.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Calculate byte ranges
    presigned = retry(times=3, delay=1.0)(service.create_presigned_url)(
        CreatePresignedUrlRequest(
            volume_name=volume_name,
            volume_path=volume_path,
            expires=30,
            method=PresignedUrlMethod.HeadObject,
        )
    )
    if not presigned.ok:
        raise CreatePresignedUrlError(presigned.err_msg)

    file_size = _get_file_size(presigned.url)
    file_ranges = _calculate_file_ranges(file_size, chunk_size)

    # Download and merge file ranges
    presigned = retry(times=3, delay=1.0)(service.create_presigned_url)(
        CreatePresignedUrlRequest(
            volume_name=volume_name,
            volume_path=volume_path,
            expires=7200,
            method=PresignedUrlMethod.GetObject,
        )
    )
    if not presigned.ok:
        raise CreatePresignedUrlError(presigned.err_msg)

    with ExitStack() as stack:
        manager = stack.enter_context(Manager())
        temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
        executor = stack.enter_context(ProcessPoolExecutor(_MAX_WORKERS, initializer=_init))

        queue = manager.Queue()
        stack.enter_context(_progress_updater(file_size, queue, callback))

        futures = (
            executor.submit(_download_chunk, presigned.url, file_range, Path(temp_dir), queue)
            for file_range in file_ranges
        )

        chunks = [future.result() for future in concurrent.futures.as_completed(futures)]
        chunks.sort(key=lambda chunk: chunk.number)
        _merge_file_chunks(file_path, chunks)


@dataclass
class RemotePath:
    scheme: str
    volume_name: str
    volume_path: str
    is_dir: Optional[bool] = None

    def __str__(self) -> str:
        return f"{self.scheme}://{self.volume_name}/{self.volume_path}"

    def __truediv__(self, other: Union["RemotePath", str]) -> "RemotePath":
        path = ""
        if isinstance(other, str):
            path = other
        elif isinstance(other, RemotePath):
            path = other.volume_path

        return RemotePath(
            self.scheme,
            self.volume_name,
            os.path.join(self.volume_path, path),
            other.is_dir if isinstance(other, RemotePath) else self.is_dir,
        )

    @property
    def name(self) -> str:
        return os.path.basename(self.volume_path)

    @property
    def path(self) -> str:
        return os.path.join(self.volume_name, self.volume_path)


class TransferError(Exception):
    """Base exception for transfer operations"""


class ValidationError(TransferError):
    """Raised when transfer validation fails"""


class SourceError(TransferError):
    """Raised when source path is invalid"""


class DestinationError(TransferError):
    """Raised when destination path is invalid"""


class TransferType(Enum):
    UPLOAD = "upload"
    DOWNLOAD = "download"


class FileTransfer:
    def __init__(
        self,
        service: VolumeServiceStub,
        source: Union[Path, RemotePath],
        destination: Union[Path, RemotePath],
    ):
        self.service = service
        self.source = source
        self.destination = destination
        self._remote_volume_exists: Optional[bool] = None

    def _determine_transfer_type(
        self,
        source: Union[Path, RemotePath],
        destination: Union[Path, RemotePath],
    ) -> TransferType:
        if isinstance(source, Path) and isinstance(destination, RemotePath):
            return TransferType.UPLOAD
        if isinstance(source, RemotePath) and isinstance(destination, Path):
            return TransferType.DOWNLOAD
        raise ValidationError("Invalid source and destination types.")

    def _get_upload_paths(self, source: Path, destination: RemotePath) -> str:
        volume_path = ""

        if isinstance(self.source, Path):
            if self.source.is_dir():
                if "." in destination.name:
                    raise SourceError(
                        "Cannot upload a directory as a file. Remove the file extension on the destination."
                    )

                # TODO: Fix this
                subpath = str(source.relative_to(source.parent))
                try:
                    # if not destination.volume_path:
                    #     subpath = str(source)
                    # else:
                    subpath = str(source.relative_to(destination.volume_path))
                except ValueError:
                    pass
                    # subpath = (
                    #     os.path.join(*source.parts[1:]) if len(source.parts) > 1 else str(source)
                    # )

                volume_path = (destination / str(subpath)).volume_path
            else:
                if "." in destination.name:
                    volume_path = destination.volume_path
                else:
                    volume_path = (destination / source.name).volume_path
        else:
            raise SourceError("Invalid source path.")

        return volume_path

    def _get_download_path(self, source: RemotePath) -> Path:
        local_path: Optional[Path] = None

        if isinstance(self.destination, Path):
            if not self.destination.suffix:
                local_path = self.destination / source.name
                local_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                local_path = self.destination
        else:
            raise DestinationError("Invalid destination path.")

        return local_path

    def get_source_files(self) -> List[Union[Path, RemotePath]]:
        source = self.source
        sources = []

        # Local source
        if isinstance(source, Path):
            if not source.exists():
                raise SourceError(f"Local source path '{source}' does not exist.")

            if source.is_file():
                sources.append(source)
            else:
                sources.extend(p for p in source.rglob("*") if p.is_file())

            return sources

        # Remote source
        res = self.service.list_path(ListPathRequest(path=source.path))
        if not res.ok:
            raise SourceError(f"{source} ({res.err_msg})")

        sources.extend(
            RemotePath(source.scheme, source.volume_name, p.path, is_dir=p.is_dir)
            for p in res.path_infos
            if not p.is_dir
        )

        return sources

    def prepare_destination(self, destination: Union[Path, RemotePath]) -> None:
        # Local destination
        if isinstance(destination, Path):
            if destination.exists() and destination.is_file():
                pass
                # raise DestinationError(f"Destination file '{destination}' exists.")
            if not destination.parent.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
            return

        # Remote destination
        if self._remote_volume_exists is not None:
            return

        res = self.service.list_volumes(ListVolumesRequest())
        if not res.ok:
            raise DestinationError(res.err_msg)
        if not any(v.name == destination.volume_name for v in res.volumes):
            raise DestinationError(f"Volume '{destination.volume_name}' does not exist.")

        self._remote_volume_exists = True

    def copy(
        self,
        source: Union[Path, RemotePath],
        destination: Union[Path, RemotePath],
        progress_callback: Optional[ProgressCallback] = None,
        completion_callback: Optional[CompletionCallback] = None,
    ) -> None:
        """
        Copy files between local and remote volumes.
        """
        self.prepare_destination(destination)

        transfer_type = self._determine_transfer_type(source, destination)

        if transfer_type == TransferType.UPLOAD:
            volume_path = self._get_upload_paths(source, destination)  # type: ignore
            upload(
                self.service,
                file_path=source,  # type: ignore
                volume_name=destination.volume_name,  # type: ignore
                volume_path=volume_path,
                progress_callback=progress_callback,
                completion_callback=completion_callback,
            )

        elif transfer_type == TransferType.DOWNLOAD:
            local_path = self._get_download_path(source)  # type: ignore
            download(
                service=self.service,
                volume_name=source.volume_name,  # type: ignore
                volume_path=source.volume_path,  # type: ignore
                file_path=local_path,
                callback=progress_callback,
            )


class VolumePath(click.ParamType):
    """
    VolumePath Click type converts a string into a Path or RemotePath object.
    """

    name = "volume_path"

    def __init__(self, version_option_name: str = "version") -> None:
        from .config import get_settings

        self.schemes = [
            get_settings().name.lower(),
        ]

        self.version_option_name = version_option_name

    def convert(
        self,
        value: str,
        param: Optional[click.Parameter] = None,
        ctx: Optional[click.Context] = None,
    ) -> Union[str, Path, RemotePath]:
        # maintain backward compatibility
        if version := ctx.params.get(self.version_option_name):
            if version == "v1":
                return value

        if "://" in value:
            return self._parse_remote_path(value)
        return Path(value)

    def _parse_remote_path(self, value: str) -> RemotePath:
        protocol, volume_path = value.split("://", 1)
        if not protocol:
            raise click.BadParameter("Volume protocol is required.")

        if not protocol.startswith(tuple(self.schemes)):
            text = f"Protocol '{protocol}://' is not supported. Supported protocols are {', '.join(self.schemes)}."
            raise click.BadParameter(text)

        try:
            volume_name, volume_key = volume_path.split("/", 1)
        except ValueError:
            volume_name = volume_path
            volume_key = ""

        if not volume_name:
            raise click.BadParameter("Volume name is required.")

        return RemotePath(protocol, volume_name, volume_key)
