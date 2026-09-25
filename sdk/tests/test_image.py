import os
from unittest import TestCase

from beta9.abstractions.image import Image


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

    def test_image_credentials_are_workspace_secret_names(self):
        # Only names travel; the gateway reads the values from workspace secrets.
        image = Image(base_image_creds=["Key1", "Key2"])
        self.assertEqual(image.base_image_creds, ["Key1", "Key2"])
        self.assertEqual(Image().base_image_creds, [])


class TestImageLocalFiles(TestCase):
    def setUp(self):
        import shutil
        import tempfile

        self.cwd = os.getcwd()
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
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

    def test_add_local_dir_of_working_directory_includes_everything(self):
        # "./**" would match nothing: synced paths carry no "./" prefix.
        image = Image().add_local_dir(".")
        self.assertEqual(image.include_files_patterns, ["*"])
        image = Image().add_local_dir(".", "/app")
        self.assertIn("ln -sfn /mnt/code /app", image.build_steps[0].command)

    def test_add_local_dir_symlinked(self):
        image = Image().add_local_dir("./assets", "/app/assets")
        self.assertEqual(image.include_files_patterns, ["assets/**"])
        self.assertEqual(len(image.build_steps), 1)
        self.assertEqual(
            image.build_steps[0].command,
            "mkdir -p /app && "
            "if [ -d /app/assets ] && [ ! -L /app/assets ]; then rmdir /app/assets; fi && "
            "ln -sfn /mnt/code/assets /app/assets",
        )

    def test_add_local_dir_copied(self):
        image = Image().add_local_dir("assets", "/app", copy=True)
        self.assertEqual(
            image.build_steps[0].command,
            "mkdir -p /app && if [ -d /mnt/code/assets ]; then cp -a /mnt/code/assets/. /app/; fi",
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
        image = Image()
        with self.assertRaises(ValueError):
            image.add_local_dir("assets", copy=True)
        self.assertEqual(image.include_files_patterns, [], "a rejected call leaves no trace")

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


class TestImageLookupDedup(TestCase):
    def test_concurrent_lookups_of_one_image_check_existence_once(self):
        # A burst of sandboxes sharing an Image must not each ask the gateway
        # whether it exists; the first lookup fills the per-process cache and
        # the rest wait for it instead of stampeding.
        import threading
        import uuid
        from unittest.mock import patch

        from beta9.abstractions.image import ImageBuildResult
        from beta9.clients.image import VerifyImageBuildResponse

        calls = []
        release = threading.Event()

        class _Stub:
            def verify_image_build(self, req):
                calls.append(req)
                release.wait(timeout=5)
                return VerifyImageBuildResponse(exists=True, image_id="img-1", valid=True)

        class _Channel:
            cache_key = uuid.uuid4().hex

        images = [Image(base_image="docker.io/library/python:3.12-slim") for _ in range(8)]
        for img in images:
            img._stub = _Stub()
            img._channel = _Channel()

        results = []
        with (
            patch.object(Image, "stub", property(lambda self: self._stub)),
            patch.object(Image, "channel", property(lambda self: self._channel)),
            patch.object(Image, "_prepare_context", lambda self: None),
        ):
            threads = [
                threading.Thread(target=lambda i=img: results.append(i.build())) for img in images
            ]
            for t in threads:
                t.start()
            release.set()
            for t in threads:
                t.join(timeout=10)

        self.assertEqual(len(calls), 1, "one existence check must serve the whole burst")
        self.assertEqual(len(results), 8)
        self.assertTrue(
            all(r == ImageBuildResult(True, "img-1", results[0].python_version) for r in results)
        )
