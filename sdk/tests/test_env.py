import os
from contextlib import contextmanager
from typing import Any

import pytest

from beta9 import config, env


@contextmanager
def temp_env(name: str, value: Any):
    env_prefix = config.get_settings().name
    name = f"{env_prefix}_{str(name)}".upper()
    if value is not None:
        os.environ[name] = str(value)
    yield
    if name in os.environ:
        del os.environ[name]


@pytest.mark.parametrize(
    ("env_name", "env_value", "default", "expected"),
    [
        ("VAR1", None, "a-string-id", "a-string-id"),
        ("VAR2", None, 123, 123),
        ("VAR3", None, 0.5, 0.5),
        ("VAR4", None, False, False),
        ("VAR5", "my-env-value", "", "my-env-value"),
        ("VAR6", "100", 0, 100),
        ("VAR7", "on", False, True),
        ("VAR8", "no", True, False),
        ("VAR9", "33.3", 0.1, 33.3),
    ],
)
def test_try_env(env_name: str, env_value: Any, default: Any, expected: Any):
    with temp_env(env_name, env_value):
        value = env.try_env(env_name, default)
        assert value == expected
