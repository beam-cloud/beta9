import argparse
import os
import pathlib
import random
import sys

import jinja2

from .build import AppBuilder

templates_path = os.path.join(pathlib.Path(__file__).parent.parent.resolve(), "templates")


def create_template(name: str, **kwargs):
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_path))
    template = environment.get_template("base.jinja")
    rendered_template = template.render(name=name, **kwargs)

    app_path = os.path.join(os.getcwd(), "app.py")

    if os.path.exists(app_path):
        app_path = f"{name}"

        while os.path.exists(f"{app_path}.py"):
            random_int = random.randint(0, 9)
            app_path = f"{app_path}{random_int}"

        app_path = os.path.join(os.getcwd(), f"{app_path}.py")

    with open(app_path, "w") as f:
        f.write(rendered_template)

    try:
        os.environ["SKIP_VALIDATION"] = "1"

        # Override stdout
        stdout = sys.stdout
        sys.stdout = open(os.devnull, "w")
        AppBuilder.build(module_path=app_path, func_or_app_name=None)
        sys.stdout = stdout
    except BaseException as e:
        os.remove(app_path)
        raise e

    sys.__stdout__.write(f"{app_path.replace(os.getcwd(), '.')}")


def parse_args():
    parser = argparse.ArgumentParser(description="Create a new app")
    parser.add_argument("--name", type=str, help="Name of the app")
    parser.add_argument("--cpu", type=int, help="CPU", default=1)
    parser.add_argument("--memory", type=str, help="Memory", default="2Gi")
    parser.add_argument("--trigger", type=str, help="Trigger")

    return parser.parse_args()


if __name__ == "__main__":
    parsed_args = parse_args()
    create_template(**vars(parsed_args))
