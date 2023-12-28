import configparser
import os

import click
from rich import prompt

from beam import terminal


@click.command()
@click.option("--name", default=None)
@click.option("--token", default=None)
def configure(name: str, token: str):
    # First check if .beam/config exists
    homedir = os.getenv("HOME")
    config_path = os.path.join(homedir, ".beam", "creds")

    if not os.path.exists(config_path):
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        open(config_path, "w+").close()

    config = configparser.ConfigParser()
    config.read(config_path)

    name = name or prompt.Prompt.ask("Name")
    token = token or prompt.Prompt.ask("Token")

    if config.has_section(name):
        if not prompt.Confirm.ask(f"Configuration for {name} already exists. Overwrite?"):
            return
    else:
        config.add_section(name)

    config.set(name, "token", token)

    if not config.has_section("default"):
        config.add_section("default")
        config.set("default", "token", token)

    with open(config_path, "w") as f:
        config.write(f)

    terminal.success("Successfully configured Beam!")
