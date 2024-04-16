import glob
from pathlib import Path
from typing import Iterable, Union

import click

from beta9 import aio, terminal
from beta9.cli.contexts import ServiceClient
from beta9.cli.extraclick import ClickCommonGroup, ClickManagementGroup
from beta9.clients.volume import (
    CopyPathRequest,
    CopyPathResponse,
    DeletePathRequest,
    ListPathRequest,
    ListPathResponse,
)
from beta9.terminal import pluralize


@click.group(cls=ClickCommonGroup)
@click.pass_context
def common(ctx: click.Context):
    ctx.obj = ctx.with_resource(ServiceClient())


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
      \b
    """,
)
@click.argument(
    "remote_path",
    required=True,
)
@click.option(
    "--long-format",
    "-l",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show mode, modified date, size, and name of file.",
)
@click.pass_obj
def ls(service: ServiceClient, remote_path: str, long_format: bool) -> None:
    with terminal.progress("Working..."):
        req = ListPathRequest(path=remote_path, long_format=long_format)
        res: ListPathResponse = aio.run_sync(service.volume.list_path(req))

        if not res.ok:
            terminal.error(f"{remote_path} ({res.err_msg})")

        num_list, suffix = pluralize(res.paths)
        terminal.header(f"{remote_path} (found {num_list} object{suffix})")
        for p in res.paths:
            terminal.print(p, highlight=False, markup=False)


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
      beam cp mydir myvol/subdir
      beam cp myfile.txt myvol/subdir

      # Use a question mark to match a single character in a path
      beam cp 'mydir/?/data?.json' myvol/sub/path

      # Use an asterisk to match all characters in a path
      beam cp 'mydir/*/*.json' myvol/data

      # Use a sequence to match a specific set of characters in a path
      beam cp 'mydir/[a-c]/data[0-1].json' myvol/data

      # Escape special characters if you don't want to single quote your local path
      beam cp mydir/\\[a-c\\]/data[0-1].json' myvol/data
      beam cp mydir/\\?/data\\?.json myvol/sub/path
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
@click.pass_obj
def cp(service: ServiceClient, local_path: str, remote_path: str) -> None:
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
        res: CopyPathResponse = aio.run_sync(service.volume.copy_path_stream(req))

        if not res.ok:
            terminal.error(f"{dst} ({res.error_msg})")


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
      \b
    """,
)
@click.argument(
    "remote_path",
    type=click.STRING,
    required=True,
)
@click.pass_obj
def rm(service: ServiceClient, remote_path: str) -> None:
    req = DeletePathRequest(path=remote_path)
    res = aio.run_sync(service.volume.delete_path(req))

    if not res.ok:
        terminal.error(f"{remote_path} ({res.err_msg})")

    num_del, suffix = pluralize(res.deleted)
    terminal.header(f"{remote_path} ({num_del} object{suffix} deleted)")
    for deleted in res.deleted:
        terminal.print(deleted, highlight=False, markup=False)


@click.group(
    name="volume",
    help="Manage volumes.",
    cls=ClickManagementGroup,
)
@click.pass_context
def management(ctx: click.Context):
    ctx.obj = ctx.with_resource(ServiceClient())


@management.command(name="list")
@click.pass_obj
def list_volumes(service: ServiceClient):
    pass


@management.command(name="create")
def create_volume():
    pass


@management.command(name="delete")
def delete_volume():
    pass
