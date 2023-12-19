import os
import sys

import pytest
from tests.conftest import get_app_dirs

from beam.utils.build import AppBuilder


@pytest.fixture(scope="function")
def change_cwd_to_app_dir(request):
    original_cwd = os.getcwd()

    # Add app directory to sys path and chdir
    app_dir_absolute_path = os.path.abspath(request.param)

    # Change directory to the app directory
    os.chdir(app_dir_absolute_path)
    sys.path.insert(0, app_dir_absolute_path)

    yield

    # Revert back to normal
    sys.path.remove(app_dir_absolute_path)

    os.chdir(original_cwd)


@pytest.mark.parametrize("change_cwd_to_app_dir", get_app_dirs(), indirect=True)
def test_app_build(change_cwd_to_app_dir):
    AppBuilder.build(module_path="app.py", func_or_app_name=None)
