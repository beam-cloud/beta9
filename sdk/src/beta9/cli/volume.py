import functools
import glob
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Union, cast

import click
from rich.table import Column, Table, box

from .. import multipart, terminal
from ..channel import ServiceClient
from ..clients.volume import (
    CopyPathRequest,
    CopyPathResponse,
    DeletePathRequest,
    DeleteVolumeRequest,
    DeleteVolumeResponse,
    GetOrCreateVolumeRequest,
    GetOrCreateVolumeResponse,
    ListPathRequest,
    ListPathResponse,
    ListVolumesRequest,
    ListVolumesResponse,
    MovePathRequest,
)
from ..multipart import FileTransfer, ProgressCallback, RemotePath, VolumePath
from ..terminal import StyledProgress, pluralize
from . import extraclick
from .extraclick import ClickCommonGroup, ClickManagementGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(
    help="List contents in a volume.",
    epilog="""
    Match Syntax:
      This command provides support for Unix shell-style wildcards, which are not the same as regular expressions.

      *       matches everything
      ?       matches any single character
      [seq]   matches any character in `seq`
      [!seq]  matches any character not in `seq`

      For a literal match, wrap the meta-characters in brackets. For example, '[?]' matches the character '?'.

    Examples:

      # List files in the volume named myvol, in the directory named subdir/
      {cli_name} ls myvol/subdir

      # List files ending in .txt in the directory named subdir
      {cli_name} ls myvol/subdir/\\*.txt

      # Same as above, but with single quotes to avoid escaping
      {cli_name} ls 'myvol/subdir/*.txt'
      \b
    """,
)
@click.argument(
    "remote_path",
    required=True,
)
@extraclick.pass_service_client
def ls(service: ServiceClient, remote_path: str):
    res: ListPathResponse
    res = service.volume.list_path(ListPathRequest(path=remote_path))

    if not res.ok:
        terminal.error(f"{remote_path} ({res.err_msg})")

    table = Table(
        Column("Name"),
        Column("Size", justify="right"),
        Column("Modified Time"),
        Column("IsDir"),
        box=box.SIMPLE,
    )

    total_size = 0
    for p in res.path_infos:
        total_size += p.size
        table.add_row(
            p.path,
            terminal.humanize_memory(p.size),
            terminal.humanize_date(p.mod_time),
            "Yes" if p.is_dir else "No",
        )

    table.add_section()
    table.add_row(
        f"[bold]{len(res.path_infos)} items | {terminal.humanize_memory(total_size)} used"
    )

    terminal.print(table)


@common.command(
    help="Copy contents to a volume.",
    epilog="""
    Match Syntax:
      This command provides support for Unix shell-style wildcards, which are not the same as regular expressions.

      *       matches everything
      ?       matches any single character
      [seq]   matches any character in `seq`
      [!seq]  matches any character not in `seq`

      For a literal match, wrap the meta-characters in brackets. For example, '[?]' matches the character '?'.

    Examples:

      # Copy contents to a remote volume
      {cli_name} cp mydir myvol/subdir
      {cli_name} cp myfile.txt myvol/subdir

      # Use a question mark to match a single character in a path
      {cli_name} cp 'mydir/?/data?.json' myvol/sub/path

      # Use an asterisk to match all characters in a path
      {cli_name} cp 'mydir/*/*.json' myvol/data

      # Use a sequence to match a specific set of characters in a path
      {cli_name} cp 'mydir/[a-c]/data[0-1].json' myvol/data

      # Escape special characters if you don't want to single quote your local path
      {cli_name} cp mydir/\\[a-c\\]/data[0-1].json' myvol/data
      {cli_name} cp mydir/\\?/data\\?.json myvol/sub/path
      \b
    """,
)
@click.argument(
    "local_path",
    type=click.STRING,
    required=True,
)
@click.argument(
    "remote_path",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def cp(service: ServiceClient, local_path: str, remote_path: str):
    local_path = str(Path(local_path).resolve())
    files_to_upload: List[Path] = []

    for match in glob.glob(local_path, recursive=True):
        mpath = Path(match)
        if mpath.is_dir():
            files_to_upload.extend([p for p in mpath.rglob("*") if p.is_file()])
        else:
            files_to_upload.append(mpath)

    if not files_to_upload:
        terminal.error("Could not find files to upload.")

    num_upload, suffix = pluralize(files_to_upload)
    terminal.header(f"{remote_path} (copying {num_upload} object{suffix})")

    desc_width = max((len(f.name) for f in files_to_upload))
    for file in files_to_upload:
        dst = (remote_path / file.relative_to(Path.cwd())).as_posix()
        req = (
            CopyPathRequest(path=dst, content=chunk)
            for chunk in read_with_progress(file, max_desc_width=desc_width)
        )
        res: CopyPathResponse = service.volume.copy_path_stream(req)

        if not res.ok:
            terminal.error(f"{dst} ({res.err_msg})")


def read_with_progress(
    path: Union[Path, str],
    chunk_size: int = 1024 * 256,
    max_desc_width: int = 30,
) -> Iterable[bytes]:
    path = Path(path)
    name = "/".join(path.relative_to(Path.cwd()).parts[-(len(path.parts)) :])

    if len(name) > max_desc_width:
        desc = f"...{name[-(max_desc_width - 3):]}"
    else:
        desc = name.ljust(max_desc_width)

    with terminal.progress_open(path, "rb", description=desc) as file:
        while chunk := file.read(chunk_size):
            yield chunk
    yield b""


@common.command(
    help="Remove content from a volume.",
    epilog="""
    Match Syntax:
      This command provides support for Unix shell-style wildcards, which are not the same as regular expressions.

      *       matches everything
      ?       matches any single character
      [seq]   matches any character in `seq`
      [!seq]  matches any character not in `seq`

      For a literal match, wrap the meta-characters in brackets. For example, '[?]' matches the character '?'.

    Examples:

      # Remove the directory
      {cli_name} rm myvol/subdir

      # Remove files ending in .json
      {cli_name} rm myvol/\\*.json

      # Remove files with letters a - c in their names
      {cli_name} rm myvol/\\[a-c\\].json

      # Remove files, use single quotes to avoid escaping
      {cli_name} rm 'myvol/?/[i-j][e-g]/*.txt'
      \b
    """,
)
@click.argument(
    "remote_path",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def rm(service: ServiceClient, remote_path: str):
    req = DeletePathRequest(path=remote_path)
    res = service.volume.delete_path(req)

    if not res.ok:
        terminal.error(f"{remote_path} ({res.err_msg})")

    num_del, suffix = pluralize(res.deleted)
    terminal.header(f"{remote_path} ({num_del} object{suffix} deleted)")
    for deleted in res.deleted:
        terminal.print(deleted, highlight=False, markup=False)


@common.command(
    help="Move a file or directory to a new location within the same volume.",
    epilog="""
    Examples:

      # Move a directory within the same volume
      {cli_name} mv myvol/subdir1 myvol/subdir2

      # Move a file to another directory within the same volume
      {cli_name} mv myvol/subdir/file.txt myvol/anotherdir/file.txt
      \b
    """,
)
@click.argument(
    "original_path",
    type=click.STRING,
    required=True,
)
@click.argument(
    "new_path",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def mv(service: ServiceClient, original_path: str, new_path: str):
    req = MovePathRequest(original_path=original_path, new_path=new_path)
    res = service.volume.move_path(req)

    if not res.ok:
        terminal.error(f"Failed to move {original_path} to {new_path} ({res.err_msg})")
    else:
        terminal.success(f"Moved {original_path} to {res.new_path}")


# class VolumePathType(click.ParamType):
#     """
#     The ClickPath type converts a string into a Path or RemotePath object.
#     """

#     name = "path"

#     schemes: ClassVar[List[str]] = [
#         get_settings().name.lower(),
#     ]

#     def convert(
#         self,
#         value: str,
#         param: Optional[click.Parameter] = None,
#         ctx: Optional[click.Context] = None,
#     ) -> Union[Path, RemotePath]:
#         if "://" in value:
#             return self._parse_remote_path(value)
#         return Path(value)

#     def _parse_remote_path(self, value: str) -> RemotePath:
#         protocol, volume_path = value.split("://", 1)
#         if not protocol:
#             raise click.BadParameter("Volume protocol is required.")

#         if not protocol.startswith(tuple(self.schemes)):
#             text = f"Protocol '{protocol}://' is not supported. Supported protocols are {', '.join(self.schemes)}."
#             raise click.BadParameter(text)

#         try:
#             volume_name, volume_key = volume_path.split("/", 1)
#         except ValueError:
#             volume_name = volume_path
#             volume_key = ""

#         if not volume_name:
#             raise click.BadParameter("Volume name is required.")

#         return RemotePath(protocol, volume_name, volume_key)


@common.command(
    help="[Experimental] Copy contents to and from a volume.",
)
@click.argument(
    "source",
    # nargs=-1,
    # type=click.Path(path_type=Path, exists=True),
    type=VolumePath(),
    required=True,
)
@click.argument(
    "destination",
    type=VolumePath(),
    required=True,
)
@extraclick.pass_service_client
def cp2(
    service: ServiceClient, source: Union[Path, RemotePath], destination: Union[Path, RemotePath]
):
    # if isinstance(source, Path) and isinstance(destination, Path):
    #     terminal.error("Source and destination paths cannot both be local paths.")

    # sources = []
    # if isinstance(source, Path):
    #     if not source.exists():
    #         return terminal.error(f"Local source path '{source}' does not exist.")

    #     if source.is_file():
    #         sources.append(source)
    #     else:
    #         sources.extend([p for p in source.rglob("*") if p.is_file()])
    # else:
    #     res = service.volume.list_path(ListPathRequest(path=source.path))
    #     if not res.ok:
    #         return terminal.error(f"{source} ({res.err_msg})")

    #     sources.extend(
    #         [RemotePath(source.scheme, source.volume_name, p.path) for p in res.path_infos]
    #     )
    # if not sources:
    #     return terminal.error("No files to copy.")

    # if isinstance(destination, Path):
    #     # Check if the destination path exists
    #     if destination.exists() and destination.is_file():
    #         return terminal.error(f"Destination file '{destination}' exists.")

    #     if not destination.parent.exists():
    #         destination.parent.mkdir(parents=True, exist_ok=True)

    # else:
    #     # Check if the destination volume exists
    #     res = service.volume.list_volumes(ListVolumesRequest())
    #     if not res.ok:
    #         return terminal.error(res.err_msg)

    #     if not any(v.name == destination.volume_name for v in res.volumes):
    #         return terminal.error(f"Volume '{destination.volume_name}' does not exist.")
    try:
        transfer = FileTransfer(service.volume)
        transfer.validate_paths(source, destination)
        sources = transfer.get_source_files(source)
        transfer.prepare_destination(destination)

        with StyledProgress() as p:
            for source in sources:
                task_id = p.add_task(source)

                progress_callback = cast(
                    ProgressCallback, functools.partial(p.update, task_id=task_id)
                )

                @contextmanager
                def completion_callback():
                    """
                    Shows progress status while the upload is being completed.
                    """
                    p.stop()

                    with terminal.progress("Completing...") as s:
                        yield s

                    # Move cursor up 2x, clear line, and redraw the progress bar
                    terminal.print("\033[A\033[A\r", highlight=False)
                    p.start()

                transfer.copy(source, destination, progress_callback, completion_callback)

    except KeyboardInterrupt:
        terminal.warn("\rUpload cancelled")

    except Exception as e:
        terminal.error(f"\rUpload failed: {e}")

    # try:
    #     with StyledProgress() as p:
    #         for source_item in sources:
    #             task_id = p.add_task(source_item)
    #             progress_callback = cast(
    #                 ProgressCallback, functools.partial(p.update, task_id=task_id)
    #             )

    #             @contextmanager
    #             def completion_callback():
    #                 """
    #                 Shows progress status while the upload is being completed.
    #                 """
    #                 p.stop()

    #                 with terminal.progress("Completing...") as s:
    #                     yield s

    #                 # Move cursor up 2x, clear line, and redraw the progress bar
    #                 terminal.print("\033[A\033[A\r", highlight=False)
    #                 p.start()

    #             if isinstance(source_item, Path) and isinstance(destination, RemotePath):
    #                 volume_name = destination.volume_name
    #                 volume_path = (destination / source_item.name).volume_key

    #                 multipart.upload(
    #                     service.volume,
    #                     source_item,
    #                     volume_name,
    #                     volume_path,
    #                     progress_callback,
    #                     completion_callback,
    #                 )

    #             # download
    #             elif isinstance(source_item, RemotePath) and isinstance(destination, Path):
    #                 volume_name = source_item.volume_name
    #                 volume_path = source_item.volume_key
    #                 local_path = destination / volume_path
    #                 local_path.parent.mkdir(parents=True, exist_ok=True)

    #                 multipart.download(
    #                     service.volume,
    #                     volume_name,
    #                     volume_path,
    #                     local_path,
    #                     progress_callback,
    #                 )
    #             else:
    #                 terminal.error("Invalid source and destination paths.")

    # except KeyboardInterrupt:
    #     terminal.warn("\rUpload cancelled")

    # except Exception as e:
    #     terminal.error(f"\rUpload failed: {e}")


@common.command(
    help="[Experimental] Download contents from a volume.",
)
@click.argument(
    "volume_name",
    type=click.STRING,
    required=True,
)
@click.argument(
    "remote_path",
    type=click.STRING,
    required=True,
)
@click.argument(
    "local_path",
    type=click.Path(path_type=Path),
    required=True,
)
@extraclick.pass_service_client
def download(service: ServiceClient, volume_name: str, remote_path: str, local_path: Path):
    try:
        with StyledProgress() as p:
            task_id = p.add_task(local_path)
            callback = cast(ProgressCallback, functools.partial(p.update, task_id=task_id))

            multipart.download(service.volume, volume_name, remote_path, local_path, callback)

    except KeyboardInterrupt:
        terminal.warn("\rDownload cancelled")

    except Exception as e:
        terminal.error(f"\rDownload failed: {e}")


@click.group(
    name="volume",
    help="Manage volumes.",
    cls=ClickManagementGroup,
)
def management():
    pass


@management.command(
    name="list",
    help="List available volumes.",
)
@extraclick.pass_service_client
def list_volumes(service: ServiceClient):
    res: ListVolumesResponse
    res = service.volume.list_volumes(ListVolumesRequest())

    if not res.ok:
        terminal.error(res.err_msg)

    table = Table(
        Column("Name"),
        Column("Size", justify="right"),
        Column("Created At"),
        Column("Updated At"),
        Column("Workspace Name"),
        box=box.SIMPLE,
    )

    total_size = 0
    for volume in res.volumes:
        table.add_row(
            volume.name,
            terminal.humanize_memory(volume.size),
            terminal.humanize_date(volume.created_at),
            terminal.humanize_date(volume.updated_at),
            volume.workspace_name,
        )
        total_size += volume.size

    table.add_section()
    table.add_row(f"[bold]{len(res.volumes)} volumes | {terminal.humanize_memory(total_size)} used")
    terminal.print(table)


@management.command(
    name="create",
    help="Create a new volume.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def create_volume(service: ServiceClient, name: str):
    res: GetOrCreateVolumeResponse
    res = service.volume.get_or_create_volume(GetOrCreateVolumeRequest(name=name))

    if not res.ok:
        terminal.print(res.volume)
        terminal.error(res.err_msg)

    table = Table(
        Column("Name"),
        Column("Created At"),
        Column("Updated At"),
        Column("Workspace Name"),
        box=box.SIMPLE,
    )

    table.add_row(
        res.volume.name,
        terminal.humanize_date(res.volume.created_at),
        terminal.humanize_date(res.volume.updated_at),
        res.volume.workspace_name,
    )
    terminal.print(table)


@management.command(
    name="delete",
    help="Delete a volume.",
)
@click.argument(
    "name",
    type=click.STRING,
    required=True,
)
@extraclick.pass_service_client
def delete_volume(service: ServiceClient, name: str):
    terminal.warn(
        "Any apps (functions, endpoints, taskqueue, etc) that\n"
        "refer to this volume should be updated before it is deleted."
    )

    if terminal.prompt(text="Are you sure? (y/n)", default="n") not in ["y", "yes"]:
        return

    res: DeleteVolumeResponse
    res = service.volume.delete_volume(DeleteVolumeRequest(name=name))

    if not res.ok:
        terminal.error(res.err_msg, exit=True)
    terminal.success(f"Deleted volume {name}")
