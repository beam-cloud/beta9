#!/usr/bin/env python3
"""Copy the verified staging config into a private GitOps-disabled bootstrap file."""

import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile

import yaml


def main():
    # Run only after AWS -> ExternalSecret synchronization has been verified.
    # Capture secret contents in memory; never print them or put them in this repo.
    result = subprocess.run(
        [
            "kubectl", "--context",
            "arn:aws:eks:us-east-1:683656326989:cluster/eks-stage-01",
            "-n", "beta9", "get", "secret", "beta9-config", "-o", "json",
        ],
        check=True, capture_output=True, text=True,
    )
    secret = json.loads(result.stdout)
    config = yaml.safe_load(base64.b64decode(secret["data"]["config.yaml"], validate=True))
    config["managedEndpoints"]["repo"]["url"] = ""
    output = Path("/private/tmp/beta9-hosted-bootstrap.yaml")
    fd, temporary = tempfile.mkstemp(prefix=".beta9-hosted-bootstrap-", dir=output.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            os.fchmod(handle.fileno(), 0o600)
            yaml.safe_dump(config, handle, sort_keys=False)
        os.replace(temporary, output)
    finally:
        Path(temporary).unlink(missing_ok=True)
    print(f"Prepared {output} with GitOps disabled and mode 0600.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # YAML parser exceptions can contain snippets of the input secret.
        raise SystemExit(f"Could not prepare bootstrap config ({type(error).__name__}); secret contents are not logged.") from None
