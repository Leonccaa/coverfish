# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures/minimal-pointer-receipt-v04"
VERIFIER = ROOT / "software/verify_pointer_audit_minimal_fixture_v04.py"


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def run_fixture(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VERIFIER), "--root", str(root)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
        check=False,
    )


class PointerFixtureTests(unittest.TestCase):
    def test_fixture_is_deterministic_and_read_only(self) -> None:
        before = tree_digest(FIXTURE)
        first = run_fixture(FIXTURE)
        middle = tree_digest(FIXTURE)
        second = run_fixture(FIXTURE)
        after = tree_digest(FIXTURE)

        self.assertEqual((first.returncode, second.returncode), (0, 0))
        self.assertEqual(first.stderr, "")
        self.assertEqual(second.stderr, "")
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual((before, middle, after), (before, before, before))

        result = json.loads(first.stdout)
        expected = json.loads((FIXTURE / "EXPECTED-RESULT.json").read_text())
        self.assertEqual(result, expected)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["scientific_status"], "PENDING")
        self.assertEqual(result["aggregate"]["exact_rows"], 1)
        self.assertEqual(result["aggregate"]["diagnostic_near_rows"], 1)
        self.assertFalse(result["bytes_retained"])
        self.assertFalse(result["network_accessed"])
        self.assertFalse(result["gpu_used"])

    def test_fixture_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            copy = Path(temporary) / "fixture"
            shutil.copytree(FIXTURE, copy)
            target = copy / "shard-0/pointer-health.tsv"
            target.write_bytes(target.read_bytes() + b"\n")

            result = run_fixture(copy)
            payload = json.loads(result.stdout)

        self.assertEqual(result.returncode, 6)
        self.assertEqual(result.stderr, "")
        self.assertEqual(payload["status"], "FAIL")
        self.assertFalse(payload["bytes_retained"])
        self.assertFalse(payload["network_accessed"])
        self.assertFalse(payload["gpu_used"])


if __name__ == "__main__":
    unittest.main()
