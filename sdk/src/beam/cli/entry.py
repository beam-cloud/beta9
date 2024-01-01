import click

from . import configure


@click.group()
def entrypoint():
    pass


entrypoint.add_command(configure.configure)
