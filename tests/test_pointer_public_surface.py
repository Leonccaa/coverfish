# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import unittest
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/pointer-audit-public-surface-v04.json"


class PointerPublicSurfaceTests(unittest.TestCase):
    def test_public_surface_manifest_is_exact(self) -> None:
        value = json.loads(MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(value["schema"], "coverfish.pointer-audit-public-surface.v1")
        self.assertEqual(
            value["dataset"]["revision"],
            "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8",
        )
        rows = value["files"]
        paths = [row["path"] for row in rows]
        self.assertEqual(paths, sorted(paths))
        self.assertEqual(len(paths), len(set(paths)))
        self.assertEqual(len(paths), value["totals"]["files"])
        self.assertEqual(sum(row["bytes"] for row in rows), value["totals"]["bytes"])

        for row in rows:
            relative = PurePosixPath(row["path"])
            self.assertFalse(relative.is_absolute())
            self.assertNotIn("..", relative.parts)
            path = ROOT.joinpath(*relative.parts)
            payload = path.read_bytes()
            self.assertEqual(len(payload), row["bytes"], row["path"])
            self.assertEqual(hashlib.sha256(payload).hexdigest(), row["sha256"], row["path"])

    def test_excluded_release_engineering_surfaces_are_absent(self) -> None:
        forbidden = (
            ROOT / "outputs",
            ROOT / "receipts",
            ROOT / "runtime-formal-v04-shard0",
            ROOT / "inputs/pilot-manifest.tsv",
            ROOT / "software/reconstruct_pointers_parallel_v03.py",
            ROOT / "software/verify_scientific_nonmutation.py",
            ROOT / "software/build_pointer_audit_final_package_v04.py",
            ROOT / "software/verify_pointer_audit_final_package_v04.py",
        )
        self.assertTrue(all(not path.exists() for path in forbidden))

    def test_public_checker_accepts_pointer_surface(self) -> None:
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/check_public_docs.py")],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("pointer-audit public surface hashes are fixed", result.stdout)


if __name__ == "__main__":
    unittest.main()
