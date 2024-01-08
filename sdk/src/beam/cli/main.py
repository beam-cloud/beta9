import shutil

import click

from beam.cli import configure, tasks

click.formatting.FORCED_WIDTH = shutil.get_terminal_size().columns

cli = click.Group()
cli.add_command(configure.configure)
cli.add_command(tasks.cli)
