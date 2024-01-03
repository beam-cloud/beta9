import click

from beam.cli import configure, tasks


@click.group()
def cli():
    pass


cli.add_command(configure.configure)
cli.add_command(tasks.cli)
