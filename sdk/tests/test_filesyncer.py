import os
import tempfile
import zipfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from beta9.clients.gateway import (
    CommitObjectDeltaResponse,
    CompleteObjectUploadResponse,
    CreateObjectDeltaResponse,
    CreateObjectResponse,
    HeadObjectResponse,
    ObjectUploadPart,
)
from beta9.sync import FileSyncer, _Manifest, _ManifestEntry, _SyncCache


class _FakeGateway:
    """Records what the syncer uploads; every archive PUT lands in self.puts."""

    def __init__(self, known_hashes=(), base_missing=False, commit_ok=True, part_size=0):
        self.known_hashes = set(known_hashes)
        self.base_missing = base_missing
        self.commit_ok = commit_ok
        self.part_size = part_size
        self.created = []
        self.deltas = []
        self.commits = []
        self.completed = []
        self.objects = 0

    def head_object(self, req):
        if req.hash in self.known_hashes:
            return HeadObjectResponse(
                ok=True, exists=True, object_id="obj-known", use_workspace_storage=True
            )
        return HeadObjectResponse(ok=True, exists=False, use_workspace_storage=True)

    def create_object(self, req):
        self.objects += 1
        self.created.append(req)
        if self.part_size and req.multipart_total_size > self.part_size:
            parts = []
            start = 0
            while start < req.multipart_total_size:
                end = min(start + self.part_size, req.multipart_total_size)
                parts.append(
                    ObjectUploadPart(
                        number=len(parts) + 1,
                        start=start,
                        end=end,
                        url=f"https://put/part/{len(parts) + 1}",
                    )
                )
                start = end
            return CreateObjectResponse(
                ok=True, object_id=f"obj-{self.objects}", upload_id="mpu-1", upload_parts=parts
            )
        return CreateObjectResponse(
            ok=True, object_id=f"obj-{self.objects}", presigned_url="https://put/full"
        )

    def complete_object_upload(self, req):
        self.completed.append(req)
        return CompleteObjectUploadResponse(ok=True)

    def create_object_delta(self, req):
        self.deltas.append(req)
        if self.base_missing:
            return CreateObjectDeltaResponse(ok=False, base_missing=True)
        self.objects += 1
        return CreateObjectDeltaResponse(
            ok=True, object_id=f"obj-{self.objects}", presigned_url="https://put/delta"
        )

    def commit_object_delta(self, req):
        self.commits.append(req)
        return CommitObjectDeltaResponse(
            ok=self.commit_ok, object_id=req.object_id, error_msg="" if self.commit_ok else "boom"
        )


class TestIncrementalSync(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "proj"
        self.root.mkdir()
        (self.root / "app").mkdir()
        for i in range(5):
            (self.root / "app" / f"f{i}.bin").write_bytes(bytes([i]) * 10000)
        (self.root / "main.py").write_text("print(1)\n")
        self.cache_dir = Path(self.tmp.name) / "cache"
        self.puts = []

        self.part_bodies = {}

        def fake_put(url, data=None, headers=None):
            resp = MagicMock()
            resp.status_code = 200
            if isinstance(data, bytes):  # a multipart part
                self.part_bodies[url] = data
                resp.headers = {"ETag": f'"etag-{len(self.part_bodies)}"'}
                return resp
            names = sorted(zipfile.ZipFile(data.name).namelist())
            self.puts.append((url, names))
            return resp

        self.patches = [
            patch("beta9.sync._SyncCache.cache_dir", classmethod(lambda cls: self.cache_dir)),
            patch("beta9.sync.requests.put", side_effect=fake_put),
            patch(
                "beta9.sync.terminal.progress_open", side_effect=lambda p, mode, **k: open(p, mode)
            ),
            patch("beta9.sync.is_local", return_value=False),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def _sync(self, gw):
        syncer = FileSyncer(gateway_stub=gw, root_dir=str(self.root))
        return syncer.sync(cache_object_id=False)

    def test_manifest_hash_depends_on_content_path_and_mode(self):
        a = _Manifest({"x": _ManifestEntry(1, 1, "h1", 0o644)})
        self.assertEqual(
            a.hash(),
            _Manifest({"x": _ManifestEntry(1, 999, "h1", 0o644)}).hash(),
            "mtime is not identity",
        )
        self.assertNotEqual(a.hash(), _Manifest({"x": _ManifestEntry(1, 1, "h2", 0o644)}).hash())
        self.assertNotEqual(a.hash(), _Manifest({"y": _ManifestEntry(1, 1, "h1", 0o644)}).hash())
        self.assertNotEqual(a.hash(), _Manifest({"x": _ManifestEntry(1, 1, "h1", 0o755)}).hash())

    def test_first_sync_uploads_everything_and_seeds_cache(self):
        gw = _FakeGateway()
        result = self._sync(gw)
        self.assertTrue(result.success)
        self.assertEqual(result.object_id, "obj-1")
        self.assertEqual(len(gw.created), 1)
        self.assertEqual(gw.deltas, [])
        self.assertEqual(self.puts[0][0], "https://put/full")
        self.assertEqual(
            self.puts[0][1],
            sorted(
                ["app/f0.bin", "app/f1.bin", "app/f2.bin", "app/f3.bin", "app/f4.bin", "main.py"]
            ),
        )
        cache = _SyncCache.load(self.root, [])
        self.assertEqual(cache.object_id, "obj-1")
        self.assertEqual(len(cache.manifest.entries), 6)

    def test_large_full_upload_goes_as_concurrent_parts(self):
        gw = _FakeGateway(part_size=20000)
        result = self._sync(gw)
        self.assertTrue(result.success)
        self.assertEqual(self.puts, [], "no single-stream PUT for a multipart upload")
        req = gw.created[0]
        self.assertGreater(req.multipart_part_size, 0)
        self.assertGreater(req.multipart_total_size, 20000)
        self.assertEqual(len(gw.completed), 1)
        completed = gw.completed[0]
        self.assertEqual(completed.upload_id, "mpu-1")
        self.assertEqual(
            [p.number for p in completed.parts], list(range(1, len(self.part_bodies) + 1))
        )
        self.assertTrue(all(p.etag.startswith('"etag-') for p in completed.parts))
        # The parts, in order, are the archive.
        archive = b"".join(
            self.part_bodies[f"https://put/part/{n}"] for n in range(1, len(self.part_bodies) + 1)
        )
        self.assertEqual(len(archive), req.multipart_total_size)
        import io

        names = sorted(zipfile.ZipFile(io.BytesIO(archive)).namelist())
        self.assertEqual(
            names,
            sorted(
                ["app/f0.bin", "app/f1.bin", "app/f2.bin", "app/f3.bin", "app/f4.bin", "main.py"]
            ),
        )

    def test_unchanged_directory_is_a_head_hit(self):
        gw = _FakeGateway()
        first = self._sync(gw)
        gw.known_hashes.add(gw.created[0].hash)
        again = self._sync(gw)
        self.assertEqual(again.object_id, "obj-known")
        self.assertEqual(len(self.puts), 1, "nothing uploaded the second time")
        self.assertEqual(first.object_id, "obj-1")

    def test_one_changed_file_uploads_only_the_delta(self):
        gw = _FakeGateway()
        self._sync(gw)
        (self.root / "app" / "f2.bin").write_bytes(b"changed" * 100)
        (self.root / "app" / "f4.bin").unlink()
        (self.root / "app" / "new.txt").write_text("new")

        result = self._sync(gw)
        self.assertTrue(result.success)
        self.assertEqual(result.object_id, "obj-2")
        self.assertEqual(len(gw.deltas), 1)
        self.assertEqual(gw.deltas[0].base_object_id, "obj-1")
        self.assertEqual(self.puts[-1][0], "https://put/delta")
        self.assertEqual(self.puts[-1][1], ["app/f2.bin", "app/new.txt"])
        self.assertEqual(gw.commits[0].removed_paths, ["app/f4.bin"])
        self.assertEqual(gw.commits[0].base_object_id, "obj-1")
        self.assertEqual(len(gw.created), 1, "no full upload")
        self.assertEqual(_SyncCache.load(self.root, []).object_id, "obj-2")

    def test_same_root_with_other_include_patterns_is_a_delta_base(self):
        """A build context synced with add_local_dir (include patterns) and the
        same directory mounted with sync_local_dir (no patterns) are different
        objects, but the second sync should only upload the files the first
        one did not cover."""
        gw = _FakeGateway()
        syncer = FileSyncer(gateway_stub=gw, root_dir=str(self.root))
        first = syncer.sync(include_patterns=["app/**"], cache_object_id=False)
        self.assertTrue(first.success)
        self.assertEqual(len(gw.created), 1)
        self.assertEqual(self.puts[-1][1], [f"app/f{i}.bin" for i in range(5)])

        result = self._sync(gw)
        self.assertTrue(result.success)
        self.assertEqual(result.object_id, "obj-2")
        self.assertEqual(len(gw.created), 1, "no second full upload")
        self.assertEqual(gw.deltas[0].base_object_id, "obj-1")
        self.assertEqual(self.puts[-1][1], ["main.py"])
        self.assertEqual(gw.commits[0].removed_paths, [])
        self.assertEqual(_SyncCache.load(self.root, []).object_id, "obj-2")
        self.assertEqual(_SyncCache.load(self.root, ["app/**"]).object_id, "obj-1")

    def test_missing_base_falls_back_to_full_upload(self):
        gw = _FakeGateway()
        self._sync(gw)
        gw.base_missing = True
        (self.root / "main.py").write_text("print(2)\n")
        result = self._sync(gw)
        self.assertTrue(result.success)
        self.assertEqual(len(gw.deltas), 1)
        self.assertEqual(len(gw.created), 2)
        self.assertEqual(self.puts[-1][0], "https://put/full")
        self.assertEqual(len(self.puts[-1][1]), 6)

    def test_failed_commit_falls_back_to_full_upload(self):
        gw = _FakeGateway(commit_ok=False)
        self._sync(gw)
        (self.root / "main.py").write_text("print(3)\n")
        result = self._sync(gw)
        self.assertTrue(result.success)
        self.assertEqual(len(gw.commits), 1)
        self.assertEqual(len(gw.created), 2)
        self.assertEqual(self.puts[-1][0], "https://put/full")

    def test_large_delta_uses_full_upload(self):
        gw = _FakeGateway()
        self._sync(gw)
        for i in range(4):
            (self.root / "app" / f"f{i}.bin").write_bytes(bytes([9]) * 10000)
        self._sync(gw)
        self.assertEqual(gw.deltas, [])
        self.assertEqual(len(gw.created), 2)

    def test_unchanged_files_are_not_rehashed(self):
        gw = _FakeGateway()
        self._sync(gw)
        with patch.object(
            FileSyncer, "_calculate_sha256", wraps=FileSyncer._calculate_sha256
        ) as calc:
            (self.root / "main.py").write_text("print(4)\n")
            self._sync(gw)
            self.assertEqual(calc.call_count, 1)


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
