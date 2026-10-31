import os

import pytest

CUSTOMER_APP_BASE_DIR = "tests/fixtures/customer_apps"


def get_app_dirs():
    return [
        f.path
        for f in os.scandir(CUSTOMER_APP_BASE_DIR)
        if f.is_dir() and os.path.exists(os.path.join(f.path, "app.py"))
    ]


@pytest.fixture(autouse=True)
def isolate_last_error(tmp_path, monkeypatch):
    """CLI runs record their last error beside the config; keep tests off the real one."""
    from beta9 import feedback

    monkeypatch.setattr(feedback, "last_error_path", lambda: tmp_path / "last_error.json")
    yield
    feedback.record_errors(False)


@pytest.fixture(scope="session", autouse=True)
def write_test_config():
    os.environ["BETA9_GATEWAY_HOST"] = "0.0.0.0"
    os.environ["BETA9_GATEWAY_PORT"] = "443"
    os.environ["BETA9_TOKEN"] = "test-token"
