import glob
from pathlib import Path
from typing import Iterable, Union

import click
from rich.table import Column, Table, box

from .. import terminal
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
from ..terminal import pluralize
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
    files_to_upload = []

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
        dst = str(remote_path / file.relative_to(Path.cwd()))
        req = (
            CopyPathRequest(path=dst, content=chunk)
            for chunk in read_with_progress(file, desc_width=desc_width)
        )
        res: CopyPathResponse = service.volume.copy_path_stream(req)

        if not res.ok:
            terminal.error(f"{dst} ({res.err_msg})")


def read_with_progress(
    path: Union[Path, str],
    chunk_size: int = 1024 * 256,
    desc_width: int = 20,
) -> Iterable[bytes]:
    path = Path(path)
    desc = path.name[: min(len(path.name), desc_width)].ljust(desc_width)

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
