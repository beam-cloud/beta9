import configparser
import os

import click
from rich import prompt

from beam import terminal


@click.command()
@click.option("--name", default=None)
@click.option("--token", default=None)
@click.option("--gateway_url", default=None)
@click.option("--gateway_port", default=None)
def configure(name: str, token: str, gateway_url: str, gateway_port: str):
    homedir = os.getenv("HOME")
    config_path = os.path.join(homedir, ".beam", "creds")

    if not os.path.exists(config_path):
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        open(config_path, "w+").close()

    config = configparser.ConfigParser()
    config.read(config_path)

    name = name or prompt.Prompt.ask("Name")
    token = token or prompt.Prompt.ask("Token")
    gateway_url = gateway_url or prompt.Prompt.ask("Gateway URL")
    gateway_port = gateway_port or prompt.Prompt.ask("Gateway Port")

    try:
        gateway_port = int(gateway_port)
    except ValueError:
        terminal.error("Gateway port must be an integer.")
        return

    if config.has_section(name):
        if not prompt.Confirm.ask(f"Configuration for {name} already exists. Overwrite?"):
            return
    else:
        config.add_section(name)

    config.set(name, "token", token)
    config.set(name, "gateway_url", gateway_url)
    config.set(name, "gateway_port", str(gateway_port))

    if not config.has_section("default"):
        config.add_section("default")
        config.set("default", "token", token)

    with open(config_path, "w") as f:
        config.write(f)

    terminal.success("Successfully configured Beam!")
