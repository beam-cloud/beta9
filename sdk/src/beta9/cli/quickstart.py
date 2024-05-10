import click
import requests

from beta9 import terminal

from .extraclick import ClickCommonGroup


@click.group(cls=ClickCommonGroup)
def common(**_):
    pass


@common.command(name="quickstart", help="Get started fast with the quickstart example.")
def quickstart():
    quickstart_raw_url = "https://raw.githubusercontent.com/beam-cloud/examples/main/01_getting_started/quickstart.py"

    terminal.success("Downloading quickstart example...")

    response = requests.get(quickstart_raw_url)

    with open("quickstart.py", "w+") as f:
        f.write(response.text)

    terminal.success("Quickstart example downloaded to quickstart.py.")
