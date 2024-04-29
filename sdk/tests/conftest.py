import os
import tempfile
import textwrap

import pytest

CUSTOMER_APP_BASE_DIR = "tests/fixtures/customer_apps"


def get_app_dirs():
    return [
        f.path
        for f in os.scandir(CUSTOMER_APP_BASE_DIR)
        if f.is_dir() and os.path.exists(os.path.join(f.path, "app.py"))
    ]


@pytest.fixture(scope="package", autouse=True)
def write_test_config():
    with tempfile.NamedTemporaryFile("w") as file:
        os.environ["CONFIG_PATH"] = file.name

        file.write(
            textwrap.dedent(
                """
                [default]
                token = test-token
                gateway_host = 0.0.0.0
                gateway_port = 1993
                gateway_http_port = 1994
                """
            )
        )

        file.flush()

        yield
