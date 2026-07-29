from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "verify_analysis_evidence", ROOT / "scripts/verify_analysis_evidence.py"
)
assert SPEC is not None and SPEC.loader is not None
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)


class AnalysisEvidenceVerifierTests(unittest.TestCase):
    def test_release_constants(self) -> None:
        self.assertEqual(VERIFY.TOOL_VERSION, "1.0.0")
        self.assertEqual(VERIFY.EXPECTED_ALIGNMENT, "REV035")
        self.assertEqual(VERIFY.EXPECTED_MODULES, 13)
        self.assertEqual(VERIFY.EXPECTED_ARTIFACTS, 169)
        self.assertEqual(VERIFY.EXPECTED_CLAIMS, 14)

    def test_safe_archive_members(self) -> None:
        self.assertTrue(VERIFY.safe_archive_member("analysis-evidence/RELEASE.json"))
        self.assertTrue(VERIFY.safe_archive_member("analysis-evidence/modules/a/file.tsv"))
        self.assertFalse(VERIFY.safe_archive_member("other/file.tsv"))
        self.assertFalse(VERIFY.safe_archive_member("analysis-evidence/../escape"))
        self.assertFalse(VERIFY.safe_archive_member("/analysis-evidence/file.tsv"))


if __name__ == "__main__":
    unittest.main()
