import os

import pytest

CUSTOMER_APP_BASE_DIR = "tests/fixtures/customer_apps"


def get_app_dirs():
    return [
        f.path
        for f in os.scandir(CUSTOMER_APP_BASE_DIR)
        if f.is_dir() and os.path.exists(os.path.join(f.path, "app.py"))
    ]


@pytest.fixture(scope="session", autouse=True)
def write_test_config():
    os.environ["BETA9_GATEWAY_HOST"] = "0.0.0.0"
    os.environ["BETA9_GATEWAY_PORT"] = "443"
    os.environ["BETA9_TOKEN"] = "test-token"
