import os
from unittest import TestCase
from unittest.mock import MagicMock

from beta9.sync import FileSyncer


class TestFileSyncer(TestCase):
    def test_init_ignore_file(self):
        syncer = FileSyncer(gateway_stub=MagicMock())

        syncer._init_ignore_file()
        self.assertTrue(syncer.ignore_file_path.exists())

        # Clean up
        os.remove(syncer.ignore_file_path)

    def test_ignore_file_contents(self):
        syncer = FileSyncer(gateway_stub=MagicMock())

        syncer._init_ignore_file()
        self.assertTrue(syncer.ignore_file_path.exists())

        # Add some additional ignore patterns to file
        with open(syncer.ignore_file_path, "a") as f:
            f.write("*.pyc\n")
            f.write("**/node_modules\n")
            f.write("logs/*.log\n")
            f.write("docs/*/temp\n")

        syncer.ignore_patterns = syncer._read_ignore_file()

        self.assertTrue(syncer._should_ignore(".venv"))
        self.assertFalse(syncer._should_ignore("test.py"))
        self.assertTrue(syncer._should_ignore(".venv/"))
        self.assertTrue(syncer._should_ignore(".venv/lib/python3.8/site-packages/"))
        self.assertTrue(syncer._should_ignore(".git/"))
        self.assertTrue(syncer._should_ignore(".DS_Store"))
        self.assertTrue(syncer._should_ignore("logs/test.log"))
        self.assertTrue(syncer._should_ignore("docs/123/temp"))
        self.assertTrue(syncer._should_ignore("docs/123/temp/test.txt"))
        self.assertFalse(syncer._should_ignore("docs/123/test.txt"))
        self.assertTrue(syncer._should_ignore("blah.pyc"))
        self.assertTrue(syncer._should_ignore("oawdi/oawidj/oiw/node_modules"))
        self.assertTrue(syncer._should_ignore("node_modules"))

        # Clean up
        os.remove(syncer.ignore_file_path)
