#!/usr/bin/python3

import subprocess
import os
import json
import sys

TEMPLATE_FILE_LOCATION = ".secrets.template"
OUTPUT_FILE_LOCATION = ".secrets"


def _generate_secrets_file():
    okteto_docker_username = None
    okteto_docker_password = None

    # retrieve okteto namespace from environment
    okteto_namespace = os.environ.get("OKTETO_NAMESPACE")
    if okteto_namespace is None:
        print(
            "ERROR: unable to load okteto namespace, do you have the OKTETO_NAMESPACE value set in your shell?"
        )
        sys.exit(1)

    # load template secrets
    template_contents = None
    with open(TEMPLATE_FILE_LOCATION, "r") as f:
        template_contents = f.read()

    # load okteto context for docker registry credentials
    try:
        okteto_context = subprocess.check_output(
            args=["okteto", "context", "show"], env=os.environ
        )
        okteto_context = json.loads(okteto_context.decode("utf-8"))
        okteto_docker_username = okteto_context["username"]
        okteto_docker_password = okteto_context["token"]
    except BaseException:
        okteto_docker_username = okteto_namespace
        okteto_docker_password = os.environ["OKTETO_DOCKER_PASSWORD"]

    # populate template values
    populated_template = (
        template_contents.replace("<OKTETO_NAMESPACE>", okteto_namespace, -1)
        .replace("<OKTETO_DOCKER_USERNAME>", okteto_docker_username, -1)
        .replace("<OKTETO_DOCKER_PASSWORD>", okteto_docker_password, -1)
    )

    print("\n***** Generated secrets file *****\n")
    print(populated_template)
    with open(OUTPUT_FILE_LOCATION, "w") as f:
        f.write(populated_template)
    print("\n**********************************\n")


if __name__ == "__main__":
    _generate_secrets_file()
