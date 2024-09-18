import os
from unittest import TestCase
from unittest.mock import MagicMock

from beta9.sync import IGNORE_FILE_CONTENTS, FileSyncer


class TestFileSyncer(TestCase):
    def test_init_ignore_file(self):
        syncer = FileSyncer(gateway_stub=MagicMock())
        syncer._init_ignore_file()
        self.assertTrue(syncer.ignore_file_path.exists())
        os.remove(syncer.ignore_file_path)

    def test_read_ignore_file(self):
        syncer = FileSyncer(gateway_stub=MagicMock())
        syncer._init_ignore_file()
        self.assertTrue(syncer.ignore_file_path.exists())
        patterns = syncer._read_ignore_file()
        ignore_contents = IGNORE_FILE_CONTENTS.split("\n")
        # remove empty line at the end
        ignore_contents = ignore_contents[: len(ignore_contents) - 1]
        self.assertEqual(patterns, ignore_contents)
        os.remove(syncer.ignore_file_path)

    def test_ignore_file_contents(self):
        syncer = FileSyncer(gateway_stub=MagicMock())
        syncer._init_ignore_file()
        self.assertTrue(syncer.ignore_file_path.exists())
        syncer.ignore_patterns = syncer._read_ignore_file()
        self.assertTrue(syncer._should_ignore(".venv"))
        self.assertFalse(syncer._should_ignore("test.py"))
        self.assertTrue(syncer._should_ignore(".venv/"))
        self.assertTrue(syncer._should_ignore(".venv/lib/python3.8/site-packages/"))
        self.assertTrue(syncer._should_ignore(".git/"))
        self.assertTrue(syncer._should_ignore(".DS_Store"))
        os.remove(syncer.ignore_file_path)
