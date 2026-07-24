from __future__ import annotations

import contextlib
import io
import json
import runpy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
MODULE = runpy.run_path(str(ROOT / "scripts/download_release.py"))


class DownloadReleaseTests(unittest.TestCase):
    def test_frozen_manifest_totals(self) -> None:
        files = MODULE["FILES"]
        self.assertEqual(len(files), 24)
        self.assertEqual(sum(item.bytes for item in files), 83_253_466_397)
        self.assertEqual(len({item.filename for item in files}), 24)
        self.assertTrue(all(len(item.sha256) == 64 for item in files))

    def test_profile_totals(self) -> None:
        expected = {
            "control": (8, 22_725),
            "core": (9, 499_616_679),
            "smoke": (10, 903_269_834),
            "s4": (17, 66_637_955_922),
            "all": (24, 83_253_466_397),
        }
        for profile, (count, total) in expected.items():
            with self.subTest(profile=profile):
                specs = MODULE["specs_for_profile"](profile)
                self.assertEqual(len(specs), count)
                self.assertEqual(sum(item.bytes for item in specs), total)

    def test_plan_is_json_and_does_not_disclose_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = MODULE["command_plan"](
                    SimpleNamespace(profile="control", output=Path(temporary) / "data")
                )
            payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["schema"], "coverfish.download.v1")
        self.assertEqual(payload["status"], "PASS")
        self.assertNotIn(temporary, stdout.getvalue())

    def test_large_download_requires_explicit_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = MODULE["command_download"](
                    SimpleNamespace(
                        profile="all",
                        output=Path(temporary) / "data",
                        accept_large_download=False,
                    )
                )
            payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 4)
        self.assertEqual(payload["status"], "PENDING")
        self.assertEqual(payload["error"], "LARGE_DOWNLOAD_CONFIRMATION_REQUIRED")

    def test_sha_verification_has_stable_states(self) -> None:
        spec_type = MODULE["FileSpec"]
        digest = MODULE["hashlib"].sha256(b"fixture").hexdigest()
        spec = spec_type("fixture.bin", "TEST", 7, digest)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / spec.filename).write_bytes(b"fixture")
            rows, failures = MODULE["verify_specs"](root, (spec,), progress=False)
            self.assertEqual(rows[0]["state"], "verified")
            self.assertEqual(failures, [])
            (root / spec.filename).write_bytes(b"changed")
            rows, failures = MODULE["verify_specs"](root, (spec,), progress=False)
            self.assertEqual(rows[0]["state"], "sha256_mismatch")
            self.assertEqual(failures[0]["reason"], "sha256_mismatch")


if __name__ == "__main__":
    unittest.main()
