# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from PIL import Image
import imagehash


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


AUDIT = load_module(
    "coverfish_public_pointer_producer",
    ROOT / "software/reconstruct_pointers.py",
)
RUNNER = load_module(
    "coverfish_public_pointer_runner_v04",
    ROOT / "software/reconstruct_pointers_parallel_v04.py",
)
WRAPPER = load_module(
    "coverfish_public_pointer_wrapper_v04",
    ROOT / "software/verify_pointer_receipt_parallel_v04.py",
)
PAIR = load_module(
    "coverfish_public_pointer_pair_v04",
    ROOT / "software/verify_pointer_receipt_pair_v04.py",
)


def jpeg_bytes() -> bytes:
    image = Image.new("RGB", (64, 48), (20, 100, 180))
    for x in range(8, 56):
        image.putpixel((x, 24), (220, 40, 30))
    target = io.BytesIO()
    image.save(target, format="JPEG", quality=90)
    return target.getvalue()


class PointerToolTests(unittest.TestCase):
    def test_bound_tool_chain_and_public_controls(self) -> None:
        self.assertEqual(AUDIT.TOOL_VERSION, "0.2.0")
        self.assertEqual(RUNNER.RUNNER_VERSION, "0.4.0")
        self.assertEqual(WRAPPER.VERSION, "0.4.2")
        self.assertEqual(PAIR.VERSION, "0.1.2")
        self.assertEqual(
            RUNNER.file_sha256(RUNNER.PRODUCER_PATH),
            RUNNER.EXPECTED_PRODUCER_SHA256,
        )
        self.assertEqual(
            RUNNER.file_sha256(RUNNER.PARALLEL_REQUIREMENTS_PATH),
            RUNNER.EXPECTED_PARALLEL_REQUIREMENTS_SHA256,
        )
        self.assertEqual(
            RUNNER.file_sha256(ROOT / "config/host-policy-fishbase-2s-v2.json"),
            RUNNER.APPROVED_POLICY_SHA256,
        )
        self.assertEqual(PAIR.EXPECTED_BINDINGS_SHA256, hashlib.sha256(
            (ROOT / "inputs/input-bindings.json").read_bytes()
        ).hexdigest())

    def test_v04_defaults_and_verifier_parsers(self) -> None:
        args = RUNNER.build_parser().parse_args(
            [
                "--source-root", "source", "--scope", "archive",
                "--output-dir", "receipt", "--window-id", "window-1",
            ]
        )
        self.assertEqual((args.shard_count, args.shard_index), (1, 0))
        self.assertEqual(
            args.policy,
            ROOT / "config/host-policy-fishbase-2s-v2.json",
        )
        pair_args = PAIR.build_parser().parse_args(
            [
                "--source-root", "source",
                "--bindings", "inputs/input-bindings.json",
                "--policy", "config/host-policy-fishbase-2s-v2.json",
                "--shard-0-dir", "receipt-0",
                "--shard-1-dir", "receipt-1",
            ]
        )
        self.assertEqual((pair_args.audit_dir_a.name, pair_args.audit_dir_b.name),
                         ("receipt-0", "receipt-1"))

    def test_network_confirmation_precedes_output_or_input_access(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "must-not-exist"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = RUNNER.main(
                    [
                        "--source-root", "missing-source",
                        "--scope", "archive",
                        "--output-dir", str(output),
                        "--window-id", "window-1",
                        "--policy", str(ROOT / "config/host-policy-fishbase-2s-v2.json"),
                    ]
                )
            payload = json.loads(stdout.getvalue())
        self.assertEqual(code, AUDIT.EXIT_SAFETY)
        self.assertEqual(payload["status"], "PENDING")
        self.assertEqual(payload["error"], "NETWORK_CONFIRMATION_REQUIRED")
        self.assertFalse(output.exists())

    def test_fixed_two_shards_are_disjoint_and_complete(self) -> None:
        hosts = ("a.example", "b.example", "c.example")
        rows = [
            {"record_id": f"row-{index:02d}", "source_host": hosts[index % 3]}
            for index in range(30)
        ]
        policy = {
            "hosts": {
                "a.example": {"rate_group": "shared-a"},
                "b.example": {"rate_group": "shared-a"},
                "c.example": {},
            }
        }
        source_ordinals = {row["record_id"]: index for index, row in enumerate(rows)}
        full = RUNNER._schedule_rows(rows, policy, source_ordinals)
        shards = [RUNNER._select_shard(full, 2, index) for index in (0, 1)]
        ids = [{row["record_id"] for row in shard} for shard in shards]
        self.assertTrue(ids[0].isdisjoint(ids[1]))
        self.assertEqual(ids[0] | ids[1], {row["record_id"] for row in rows})
        self.assertEqual(sum(map(len, shards)), len(rows))
        self.assertTrue(all(
            row["lane_ordinal"] % 2 == index
            for index, shard in enumerate(shards)
            for row in shard
        ))

    def test_phash_classes_never_promote_to_byte_exact(self) -> None:
        payload = jpeg_bytes()
        with Image.open(io.BytesIO(payload)) as image:
            actual = int(str(imagehash.phash(
                image.convert("RGB"), hash_size=8, highfreq_factor=4
            )), 16)
        expected = {
            0: "visual_near_candidate_d0_2",
            2: "visual_near_candidate_d0_2",
            3: "visual_related_candidate_d3_6",
            6: "visual_related_candidate_d3_6",
            7: "content_changed_candidate_d_gt6",
        }
        for distance, expected_class in expected.items():
            with self.subTest(distance=distance):
                frozen_phash = f"{actual ^ ((1 << distance) - 1):016x}"
                result = AUDIT.image_diagnostics(payload, "0" * 64, frozen_phash)
                self.assertEqual(result["phash_distance"], str(distance))
                self.assertEqual(result["identity_class"], expected_class)
                self.assertEqual(result["sha256_match"], "false")

    def test_private_address_and_sensitive_query_fail_closed(self) -> None:
        policy = json.loads(
            (ROOT / "config/host-policy-fishbase-2s-v2.json").read_text()
        )
        policy["hosts"]["localhost"] = {
            "policy_state": "allow_with_limits",
            "min_interval_seconds": 1,
            "roles": ["image"],
        }
        with tempfile.TemporaryDirectory() as temporary:
            client = AUDIT.NetworkClient(
                policy,
                "COVER-Fish-pointer-audit/1.0 (+https://github.com/Leonccaa/coverfish)",
                "test",
                rate_state_path=Path(temporary) / "rate.json",
            )
            with mock.patch.object(
                AUDIT.socket,
                "getaddrinfo",
                return_value=[(2, 1, 6, "", ("127.0.0.1", 443))],
            ), self.assertRaises(AUDIT.AuditError) as context:
                client.validate_url("https://localhost/example.jpg")
        self.assertEqual(context.exception.code, "NON_PUBLIC_ADDRESS_FORBIDDEN")
        self.assertEqual(
            AUDIT.sensitive_query_keys("https://example.org/a?access_token=synthetic"),
            {"access_token"},
        )

    @unittest.skipUnless(
        os.environ.get("COVERFISH_RC2_ROOT"),
        "set COVERFISH_RC2_ROOT to run the frozen-input integration",
    )
    def test_optional_frozen_archive_partition(self) -> None:
        source = Path(os.environ["COVERFISH_RC2_ROOT"])
        bindings, rows = AUDIT.load_bound_rows(
            source, ROOT / "inputs/input-bindings.json"
        )
        policy = json.loads(
            (ROOT / "config/host-policy-fishbase-2s-v2.json").read_text()
        )
        ordinals = {row["record_id"]: index for index, row in enumerate(rows)}
        full = RUNNER._schedule_rows(rows, policy, ordinals)
        shards = [RUNNER._select_shard(full, 2, index) for index in (0, 1)]
        ids = [{row["record_id"] for row in shard} for shard in shards]

        self.assertEqual(len(rows), bindings["expected"]["archive_pointer_rows"])
        self.assertEqual(len(rows), 42_387)
        self.assertEqual(sum(row["active"] == "true" for row in rows), 41_945)
        self.assertEqual(tuple(map(len, shards)), PAIR.EXPECTED_SHARD_ROWS)
        self.assertTrue(ids[0].isdisjoint(ids[1]))
        self.assertEqual(len(ids[0] | ids[1]), 42_387)


if __name__ == "__main__":
    unittest.main()
