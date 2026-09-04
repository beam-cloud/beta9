import os
from contextlib import contextmanager
from unittest import TestCase

from beta9.abstractions.image import Image, ImageCredentialValueNotFound


class TestImage(TestCase):
    def test_image_build_steps(self):
        image = Image()
        image.add_commands(["apt-get install curl -y"])
        image.add_python_packages(["requests"])
        image.add_commands(["apt-get install wget -y"])
        image.add_commands(["numpy", "pytorch"])

        assert len(image.build_steps) == 5
        assert image.build_steps[0].command == "apt-get install curl -y"
        assert image.build_steps[1].command == "requests"
        assert image.build_steps[2].command == "apt-get install wget -y"
        assert image.build_steps[3].command == "numpy"
        assert image.build_steps[4].command == "pytorch"

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


class TestImageLocalFiles(TestCase):
    def setUp(self):
        import tempfile

        self.cwd = os.getcwd()
        self.tmp = tempfile.mkdtemp()
        os.chdir(self.tmp)
        os.makedirs("assets/sub")
        with open("assets/sub/a.txt", "w") as f:
            f.write("a")
        with open("config.yaml", "w") as f:
            f.write("x: 1")

    def tearDown(self):
        os.chdir(self.cwd)

    def test_add_local_dir_mounted(self):
        image = Image().add_local_dir("assets")
        self.assertEqual(image.include_files_patterns, ["assets/**"])
        self.assertEqual(image.build_steps, [])

    def test_add_local_dir_symlinked(self):
        image = Image().add_local_dir("./assets", "/app/assets")
        self.assertEqual(image.include_files_patterns, ["assets/**"])
        self.assertEqual(len(image.build_steps), 1)
        self.assertEqual(
            image.build_steps[0].command, "mkdir -p /app && ln -sfn /mnt/code/assets /app/assets"
        )

    def test_add_local_dir_copied(self):
        image = Image().add_local_dir("assets", "/app", copy=True)
        self.assertEqual(
            image.build_steps[0].command, "mkdir -p /app && cp -a /mnt/code/assets/. /app/"
        )

    def test_add_local_file_copied(self):
        image = Image().add_local_file("config.yaml", "/etc/app/config.yaml", copy=True)
        self.assertEqual(image.include_files_patterns, ["config.yaml"])
        self.assertEqual(
            image.build_steps[0].command,
            "mkdir -p /etc/app && cp -a /mnt/code/config.yaml /etc/app/config.yaml",
        )

    def test_add_local_rejects_outside_and_missing(self):
        with self.assertRaises(ValueError):
            Image().add_local_dir("..")
        with self.assertRaises(ValueError):
            Image().add_local_dir("missing")
        with self.assertRaises(ValueError):
            Image().add_local_dir("assets", copy=True)

    def test_modal_style_aliases(self):
        image = (
            Image()
            .apt_install("git", "curl")
            .pip_install("numpy")
            .run_commands("echo hi")
            .env({"A": "1"})
        )
        self.assertIn(
            "apt-get install -y -qq --no-install-recommends git curl", image.build_steps[0].command
        )
        self.assertEqual(
            (image.build_steps[1].command, image.build_steps[1].type), ("numpy", "pip")
        )
        self.assertEqual(
            (image.build_steps[2].command, image.build_steps[2].type), ("echo hi", "shell")
        )
        self.assertEqual(image.env_vars, ["A=1"])
