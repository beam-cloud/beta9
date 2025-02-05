import abc
import concurrent.futures
import io
import math
import os
import signal
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack, contextmanager
from functools import wraps
from multiprocessing import Manager
from os import PathLike
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import (
    Any,
    Callable,
    ContextManager,
    Final,
    Generator,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Tuple,
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
    PresignedUrlMethod,
    StatPathRequest,
    VolumeServiceStub,
)
from .env import try_env
from .exceptions import (
    CompleteMultipartUploadError,
    CreateMultipartUploadError,
    CreatePresignedUrlError,
    DownloadChunkError,
    GetFileSizeError,
    ListPathError,
    RetryableError,
    StatPathError,
    UploadPartError,
)
from .terminal import CustomProgress

# Value of 0 means the number of workers is calculated based on the file size
_MAX_WORKERS: Final = try_env("MULTIPART_MAX_WORKERS", 0)
_REQUEST_TIMEOUT: Final = try_env("MULTIPART_REQUEST_TIMEOUT", 5)
_DEBUG_RETRY: Final = try_env("MULTIPART_DEBUG_RETRY", False)


class ProgressCallbackType(Protocol):
    def __call__(self, total: int, advance: int) -> None: ...


class CompletionCallbackType(Protocol):
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
                    if _DEBUG_RETRY:
                        print(e)

                    last_exception = e
                    if attempt < times - 1:
                        time.sleep(min(current_delay, max_delay))
                        current_delay *= 2

            raise RetryableError(times, str(last_exception))

        return wrapper

    return decorator


@contextmanager
def _progress_updater(
    file_size: int,
    queue: Queue,
    callback: Optional[ProgressCallbackType] = None,
    timeout: float = 1.0,
) -> Generator[Thread, None, None]:
    """
    Calls a callback with the progress of a multipart upload or download.

    Args:
        file_size: The total size of the file.
        queue: A queue that receives the number of bytes processed.
        callback: A callback that receives the total size and the number of bytes processed. Defaults to None.
        timeout: The time to wait for the thread to finish. Defaults to 1.0.

    Yields:
        A thread that updates the progress.
    """

    def target():
        def cb(total: int, advance: int):
            if callback is not None:
                callback(total=total, advance=advance)

        finished = 0
        while finished < file_size:
            try:
                processed = queue.get(timeout=1)
            except Empty:
                continue
            except Exception:
                break

            if processed:
                finished += processed
                cb(total=file_size, advance=processed)

        cb(total=file_size, advance=0)

    thread = Thread(target=target, daemon=True)
    thread.start()

    yield thread

    thread.join(timeout=timeout)


# Global session for making HTTP requests
_session = None


def _get_session() -> Session:
    """
    Get a session for making HTTP requests.

    This is not thread safe, but should be process safe.
    """
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def _init():
    """
    Initialize the process by setting a signal handler.
    """
    _get_session()

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

    class QueueBuffer(io.BytesIO):
        def read(self, size: Optional[int] = -1) -> bytes:
            b = super().read(size)
            queue.put_nowait(len(b))
            return b

    try:
        response = session.put(
            url=file_part.url,
            data=QueueBuffer(chunk) if chunk else None,
            headers={
                "Content-Length": str(len(chunk)),
            },
            timeout=_REQUEST_TIMEOUT,
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


def beta9_upload(
    service: VolumeServiceStub,
    file_path: Path,
    remote_path: "RemotePath",
    progress_callback: Optional[ProgressCallbackType] = None,
    completion_callback: Optional[CompletionCallbackType] = None,
):
    """
    Upload a file to a volume using multipart upload.

    Args:
        service: The volume service stub.
        file_path: Path to the file to upload.
        remote_path: Path to save the file on the volume.
        progress_callback: A callback that receives the total size and the number of
            bytes processed. Defaults to None.
        completion_callback: A context manager that wraps the completion of the upload.
            Defaults to None.

    Raises:
        CreateMultipartUploadError: If initializing the upload fails.
        CompleteMultipartUploadError: If completing the upload fails.
        KeyboardInterrupt: If the upload is interrupted by the user.
        Exception: If any other error occurs.
    """
    # Initialize multipart upload
    file_size = file_path.stat().st_size
    chunk_size, max_workers = _calculate_chunk_size(file_size)
    initial = retry(times=3, delay=1.0)(service.create_multipart_upload)(
        CreateMultipartUploadRequest(
            volume_name=remote_path.volume_name,
            volume_path=remote_path.volume_path,
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
            workers = max(max_workers, _MAX_WORKERS)
            executor = stack.enter_context(ProcessPoolExecutor(workers, initializer=_init))

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
                    volume_name=remote_path.volume_name,
                    volume_path=remote_path.volume_path,
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
                upload_id=initial.upload_id,
                volume_name=remote_path.volume_name,
                volume_path=remote_path.volume_path,
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
    ranges = math.ceil(file_size / (chunk_size or 1))
    return [
        FileRange(
            number=i + 1,
            start=i * chunk_size,
            end=min(file_size - 1, (i + 1) * chunk_size - 1),
        )
        for i in range(ranges)
    ]


def _calculate_chunk_size(
    file_size: int,
    min_chunk_size: int = 5 * 1024 * 1024,
    max_chunk_size: int = 500 * 1024 * 1024,
    chunk_size_divisor: int = 10,
) -> Tuple[int, int]:
    """
    Calculate chunk size using log2 scaling.
    """
    if file_size <= min_chunk_size:
        return file_size, 1

    # Use log2 directly for chunk size
    chunk_size = 2 ** math.floor(math.log2(file_size / chunk_size_divisor))
    chunk_size = min(max(chunk_size, min_chunk_size), max_chunk_size, file_size)

    # Workers scale with log2
    workers = min(max(int(math.log2(file_size / min_chunk_size)), 1), 16)

    return chunk_size, workers


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
                queue.put_nowait(file.write(chunk))
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


def beta9_download(
    service: VolumeServiceStub,
    remote_path: "RemotePath",
    file_path: Path,
    callback: Optional[ProgressCallbackType] = None,
) -> None:
    """
    Download a file from a volume using multipart download.

    Args:
        service: The volume service stub.
        remote_path: Path to the file on the volume.
        file_path: Path to save the file.
        callback: A callback that receives the total size and the number of bytes processed. Defaults to None.

    Raises:
        CreatePresignedUrlError: If a presigned URL cannot be created.
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Calculate byte ranges
    presigned = retry(times=3, delay=1.0)(service.create_presigned_url)(
        CreatePresignedUrlRequest(
            volume_name=remote_path.volume_name,
            volume_path=remote_path.volume_path,
            expires=30,
            method=PresignedUrlMethod.HeadObject,
        )
    )
    if not presigned.ok:
        raise CreatePresignedUrlError(presigned.err_msg)

    file_size = _get_file_size(presigned.url)
    chunk_size, max_workers = _calculate_chunk_size(file_size)
    file_ranges = _calculate_file_ranges(file_size, chunk_size)

    # Download and merge file ranges
    presigned = retry(times=3, delay=1.0)(service.create_presigned_url)(
        CreatePresignedUrlRequest(
            volume_name=remote_path.volume_name,
            volume_path=remote_path.volume_path,
            expires=7200,
            method=PresignedUrlMethod.GetObject,
        )
    )
    if not presigned.ok:
        raise CreatePresignedUrlError(presigned.err_msg)

    with ExitStack() as stack:
        manager = stack.enter_context(Manager())
        temp_dir = stack.enter_context(tempfile.TemporaryDirectory())
        workers = max(max_workers, _MAX_WORKERS)
        executor = stack.enter_context(ProcessPoolExecutor(workers, initializer=_init))

        queue = manager.Queue()
        stack.enter_context(_progress_updater(file_size, queue, callback))

        futures = (
            executor.submit(_download_chunk, presigned.url, file_range, Path(temp_dir), queue)
            for file_range in file_ranges
        )

        chunks = [future.result() for future in concurrent.futures.as_completed(futures)]
        chunks.sort(key=lambda chunk: chunk.number)
        _merge_file_chunks(file_path, chunks)


class RemotePath:
    def __init__(
        self, scheme: str, volume_name: str, volume_path: str, is_dir: Optional[bool] = None
    ):
        self.scheme = scheme
        self.volume_name = volume_name
        self.volume_path = volume_path
        self.is_dir = is_dir

    @property
    def volume_path(self) -> str:
        return self._volume_path

    @volume_path.setter
    def volume_path(self, value: str) -> None:
        self._volume_path = value.replace("//", "/")

    @classmethod
    def parse(cls, value: str) -> "RemotePath":
        scheme, volume_path = value.split("://", 1)
        try:
            volume_name, volume_path = volume_path.split("/", 1)
            if not volume_path:
                volume_path = "/"
        except ValueError:
            volume_name = volume_path
            volume_path = ""
        return cls(scheme, volume_name, volume_path)

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
        if self.volume_path == "":
            return self.volume_name + "/"
        return os.path.join(self.volume_name, self.volume_path)


class PathTypeConverter(click.ParamType):
    """
    VolumePath Click type converts a string into a str, Path, or RemotePath object.
    """

    def __init__(self, version_option_name: str = "version") -> None:
        self.version_option_name = version_option_name

    def convert(
        self,
        value: str,
        param: Optional[click.Parameter] = None,
        ctx: Optional[click.Context] = None,
    ) -> Union[str, Path, RemotePath]:
        if "://" in value:
            return RemotePath.parse(value)
        return Path(value)


class RemoteHandler(abc.ABC):
    def __init__(self, **kwargs) -> None:
        pass

    @abc.abstractmethod
    def upload(self, local_path: Path, remote_path: RemotePath) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def download(self, remote_path: RemotePath, local_path: Path) -> None:
        raise NotImplementedError


class Beta9Handler(RemoteHandler):
    def __init__(
        self, service: VolumeServiceStub, progress: Optional[CustomProgress] = None, **kwargs
    ) -> None:
        self.service = service
        self.progress = progress

    def list_dir(self, remote_path: RemotePath, recursive: bool = False) -> List[RemotePath]:
        path = remote_path.path
        if recursive:
            path = f"{path}/**".replace("//", "/")

        res = self.service.list_path(ListPathRequest(path=path))
        if not res.ok:
            raise ListPathError(remote_path.path, res.err_msg)

        return [
            RemotePath(remote_path.scheme, remote_path.volume_name, p.path, is_dir=p.is_dir)
            for p in res.path_infos
        ]

    def is_dir(self, remote_path: RemotePath) -> bool:
        if remote_path.volume_path == "":
            return True

        res = self.service.stat_path(StatPathRequest(path=remote_path.path))
        if not res.ok:
            raise StatPathError(remote_path.path, res.err_msg)

        return res.path_info.is_dir if not res.err_msg else False

    def upload(self, local_path: Path, remote_path: RemotePath) -> None:
        # Recursively upload directories
        if local_path.is_dir():
            for item in local_path.iterdir():
                if remote_path.volume_path.endswith("/"):
                    volume_path = f"{remote_path.volume_path}{local_path.name}/{item.name}"
                else:
                    volume_path = f"{remote_path.volume_path}/{item.name}"

                volume_path = volume_path.lstrip("/")
                remote_subpath = RemotePath(
                    scheme=remote_path.scheme,
                    volume_name=remote_path.volume_name,
                    volume_path=volume_path,
                )
                self.upload(item, remote_subpath)
        else:
            # Adjust remote_path for single-file upload
            if remote_path.volume_path.endswith("/"):
                volume_path = f"{remote_path.volume_path}{local_path.name}".lstrip("/")
                remote_path.volume_path = volume_path
            elif self.is_dir(remote_path):
                volume_path = f"{remote_path.volume_path}/{local_path.name}".lstrip("/")
                remote_path.volume_path = volume_path
            elif local_path.suffix and "." not in remote_path.volume_path:
                ext = ".".join(local_path.suffixes)
                volume_path = f"{remote_path.volume_path}{ext}".lstrip("/")
                remote_path.volume_path = volume_path

            progress_callback, completion_callback = self._setup_callbacks(remote_path)
            beta9_upload(
                service=self.service,
                file_path=local_path,
                remote_path=remote_path,
                progress_callback=progress_callback,
                completion_callback=completion_callback,
            )

    def download(self, remote_path: RemotePath, local_path: Path) -> None:
        # Recursively download directories
        if self.is_dir(remote_path):
            local_path.mkdir(parents=True, exist_ok=True)

            for rpath in self.list_dir(remote_path, recursive=True):
                lpath = local_path / rpath.volume_path
                if not remote_path.volume_path.endswith("/"):
                    lpath = local_path / rpath.name

                self.download(rpath, lpath)
        else:
            # Adjust local_path for single-file download
            if local_path.is_dir():
                local_path = local_path / Path(remote_path.volume_path).name
            local_path.parent.mkdir(parents=True, exist_ok=True)

            callback, _ = self._setup_callbacks(local_path.relative_to(Path.cwd()))
            beta9_download(
                service=self.service,
                remote_path=remote_path,
                file_path=local_path,
                callback=callback,
            )

    def _setup_callbacks(
        self,
        task_name: Any,
    ) -> Tuple[Optional[ProgressCallbackType], Optional[CompletionCallbackType]]:
        if self.progress is None:
            return None, None

        p = self.progress
        task_id = p.add_task(task_name)

        def progress_callback(total: int, advance: int):
            """
            Updates the progress bar with the current status of the upload.
            """
            p.update(task_id=task_id, total=total, advance=advance)

        @contextmanager
        def completion_callback():
            """
            Shows status while the upload is being processed server-side.
            """
            p.stop()

            from . import terminal

            with terminal.progress("Working...") as s:
                yield s

            # Move cursor up 2x, clear line, and redraw the progress bar
            terminal.print("\033[A\033[A\r", highlight=False)
            p.start()

        return progress_callback, completion_callback


class S3Handler(RemoteHandler):
    def upload(self, local_path: Path, remote_path: RemotePath) -> None:
        raise NotImplementedError

    def download(self, remote_path: RemotePath, local_path: Path) -> None:
        raise NotImplementedError


class HuggingFaceHandler(RemoteHandler):
    def upload(self, local_path: Path, remote_path: RemotePath) -> None:
        raise NotImplementedError

    def download(self, remote_path: RemotePath, local_path: Path) -> None:
        raise NotImplementedError


def get_remote_handler(scheme: str, **kwargs: Any) -> RemoteHandler:
    from .config import get_settings

    this = get_settings().name.lower()
    handlers = {
        this: Beta9Handler,
        "hf": HuggingFaceHandler,
        "s3": S3Handler,
    }

    if scheme not in handlers:
        raise ValueError(
            f"Protocol '{scheme}://' is not supported. Supported protocols are {', '.join(handlers)}."
        )

    return handlers[scheme](**kwargs)


def copy(src, dst, **kwargs: Any):
    # Resolve local paths
    dst_path = Path(dst).resolve() if not isinstance(dst, RemotePath) else dst
    if isinstance(dst_path, Path) and str(dst) == ".":
        dst_path = Path.cwd()

    # Determine handler based on source or destination type
    if isinstance(src, RemotePath) and isinstance(dst_path, Path):
        handler = get_remote_handler(src.scheme, **kwargs)
        handler.download(src, dst_path)
    elif isinstance(src, Path) and isinstance(dst_path, RemotePath):
        handler = get_remote_handler(dst.scheme, **kwargs)
        handler.upload(src, dst_path)
    else:
        raise ValueError("Invalid source and destination types.")
