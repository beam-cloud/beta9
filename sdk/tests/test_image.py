import os
from contextlib import contextmanager
from unittest import TestCase

from beta9.abstractions.image import Image, ImageCredentialValueNotFound


class TestImage(TestCase):
    def test_image_container_commands(self):
        image = Image()
        image.add_commands(["apt-get install curl -y"])
        image.add_python_packages(["requests"])
        image.add_commands(["apt-get install wget -y"])
        image.add_commands(["numpy", "pytorch"])

        assert len(image.container_commands) == 5
        assert image.container_commands[0].command == "apt-get install curl -y"
        assert image.container_commands[1].command == "requests"
        assert image.container_commands[2].command == "apt-get install wget -y"
        assert image.container_commands[3].command == "numpy"
        assert image.container_commands[4].command == "pytorch"

    def test_image_credentials(self):
        env = {
            "Key1": "1234",
            "Key2": "5678",
        }
        with temp_env_vars(env):
            image = Image(base_image_creds=env.keys())
            creds = image.get_credentials_from_env()
            self.assertTrue(creds == env)

    def test_image_credentials_value_error(self):
        env = {
            "Key1": "1234",
            "Key2": "",
        }
        with temp_env_vars(env):
            image = Image(base_image_creds=list(env.keys()))

            with self.assertRaises(ImageCredentialValueNotFound) as context:
                image.get_credentials_from_env()

            self.assertTrue("Did not find the environment variable Key2." in str(context.exception))


@contextmanager
def temp_env_vars(d: dict):
    for key, value in d.items():
        os.environ[key] = value
    yield
    for key in d.keys():
        os.unsetenv(key)
