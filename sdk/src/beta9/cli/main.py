import shutil

import click
from beta9.cli import configure, deploy, tasks

click.formatting.FORCED_WIDTH = shutil.get_terminal_size().columns

cli = click.Group()
cli.add_command(configure.configure)
cli.add_command(tasks.cli)
cli.add_command(deploy.cli)
