import click

from beam.cli import configure, tasks

cli = click.Group()
cli.add_command(configure.configure)
cli.add_command(tasks.cli)
