import click

from beam import terminal
from beam.config import configure_gateway_credentials, load_config_from_file, save_config_to_file


@click.command()
@click.option("--name", default=None)
@click.option("--token", default=None)
@click.option("--gateway_host", default=None)
@click.option("--gateway_port", default=None)
def configure(name: str, token: str, gateway_host: str, gateway_port: str):
    config = load_config_from_file()

    config = configure_gateway_credentials(
        config, name=name, gateway_host=gateway_host, gateway_port=gateway_port, token=token
    )

    save_config_to_file(
        config=config,
        name=name,
    )

    terminal.success("Successfully configured Beam!")
