import os

CUSTOMER_APP_BASE_DIR = "tests/fixtures/customer_apps"


def get_app_dirs():
    return [
        f.path
        for f in os.scandir(CUSTOMER_APP_BASE_DIR)
        if f.is_dir() and os.path.exists(os.path.join(f.path, "app.py"))
    ]
