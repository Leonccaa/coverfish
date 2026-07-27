#!/usr/bin/env python3
"""Offline verifier adapter for the sharded COVER-Fish v0.4 executor.

The adapter verifies the v0.4 execution profile and every per-invocation
execution summary, then maps the additive v2 contract to the frozen v1
scientific contract in a private temporary directory and runs the independent
receipt verifier there.  No network access is performed.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("MODULE_IMPORT_FAILED")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


RUNNER = load_module(
    "coverfish_parallel_runner_v04",
    ROOT / "software/reconstruct_pointers_parallel_v04.py",
)
EXPECTED_BASE_VERIFIER_SHA256 = (
    "714f4eab6513e112095bf3c93121349284b30dba7429c163abd63e29e898527d"
)
BASE_VERIFIER_PATH = ROOT / "software/verify_pointer_receipt.py"
if RUNNER.file_sha256(BASE_VERIFIER_PATH) != EXPECTED_BASE_VERIFIER_SHA256:
    raise RuntimeError("FROZEN_BASE_VERIFIER_SHA256_MISMATCH")
BASE = load_module(
    "coverfish_pointer_verifier_v02",
    BASE_VERIFIER_PATH,
)

SCHEMA = "coverfish.pointer-parallel-verifier.v2"
VERSION = "0.4.2"

BASE_CHECK_PREFIX = (
    "receipt_paths_preflight",
    "bindings_hardcoded_contract",
    "expected_count_closures",
    "frozen_s0_sha256",
    "frozen_s0_rows",
    "frozen_s0_id_field",
    "frozen_s1_sha256",
    "frozen_s1_rows",
    "frozen_s1_id_field",
    "frozen_s2_sha256",
    "frozen_s2_rows",
    "frozen_s2_id_field",
    "frozen_s3_sha256",
    "frozen_s3_rows",
    "frozen_s3_id_field",
    "frozen_s4_sha256",
    "frozen_s4_rows",
    "frozen_s4_id_field",
    "frozen_d0_sha256",
    "frozen_d0_rows",
    "frozen_d0_id_field",
    "frozen_rows_valid",
    "frozen_global_ids_unique",
    "frozen_archive_rows",
    "frozen_component_counts",
    "frozen_r0_pointer_rows",
    "frozen_active_rows",
    "frozen_retired_rows",
    "frozen_container_files_tsv_sha256",
    "frozen_container_sha256sums_sha256",
    "frozen_e0_bytes_sha256",
    "frozen_e0_bytes_rows",
    "frozen_e0_pointers_sha256",
    "frozen_e0_pointers_rows",
)
BASE_CHECK_SUFFIX = (
    "attempt_schema",
    "completion_schema",
    "disposition_schema",
    "health_schema",
    "policy_top_level_schema",
    "policy_schema",
    "policy_hosts_exact",
    "policy_host_safety_contract",
    "policy_exact_profile",
    "policy_file_sha256_allowlist",
    "policy_common_safety_contract",
    "attempt_ids_unique_nonempty",
    "attempt_full_semantics",
    "transaction_full_binding",
    "tail_recoveries_optional",
    "atomic_temp_recoveries_schema",
    "atomic_temp_recoveries_full_semantics",
    "producer_hardcoded_sha256",
    "requirements_hardcoded_sha256",
    "audit_contract_full_binding",
    "run_metadata_full_binding",
    "robots_schema",
    "robots_full_semantics",
    "health_full_rebuild",
    "summary_full_rebuild",
    "output_tree_safe",
)
BASE_CHECK_ROSTERS = {
    "pilot": BASE_CHECK_PREFIX
    + (
        "pilot_manifest_hardcoded_sha256",
        "pilot_manifest_schema",
        "pilot_manifest_rows",
        "pilot_ids_unique",
        "pilot_rows_bound_to_frozen",
        "pilot_component_counts",
        "pilot_sample_rank",
    )
    + BASE_CHECK_SUFFIX,
    "archive": BASE_CHECK_PREFIX
    + ("archive_manifest_omitted",)
    + BASE_CHECK_SUFFIX,
}
BASE_CHECK_ROSTERS_WITH_TAIL = {
    scope: roster[: roster.index("tail_recoveries_optional")]
    + ("tail_recoveries_schema", "tail_recoveries_full_semantics")
    + roster[roster.index("tail_recoveries_optional") + 1 :]
    for scope, roster in BASE_CHECK_ROSTERS.items()
}
BASE_CHECK_FIELDS = {"check_id", "status", "observed", "expected"}
ROBOTS_CONTINUITY_FIELDS = (
    "host", "robots_url", "http_status", "fetch_status", "robots_state",
    "error_code", "redirect_count", "redirect_chain_json", "sha256",
)
ROBOTS_COMPATIBLE_STATES = {
    "allowed": {"parsed"},
    "allowed_no_robots": {"not_present_allow"},
    "disallowed": {"parsed"},
    "unavailable_disallow": {"unavailable_disallow", "parse_error"},
}


def add_check(
    rows: list[dict[str, str]], check_id: str, passed: bool, observed: object, expected: object
) -> None:
    rows.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "observed": json.dumps(observed, ensure_ascii=False, sort_keys=True),
            "expected": json.dumps(expected, ensure_ascii=False, sort_keys=True),
        }
    )


def copy_base_view(
    audit_dir: Path, target: Path, base_record_order: Sequence[str]
) -> dict[str, object]:
    base_names = {
        "pointer-health-attempts.tsv",
        "pointer-health-summary.json",
        "record-completions.tsv",
        "attempt-dispositions.tsv",
        "robots.tsv",
        "tail-recoveries.tsv",
        "atomic-temp-recoveries.tsv",
    }
    for name in base_names:
        source = audit_dir / name
        if source.is_file():
            shutil.copyfile(source, target / name)
    health_fields, health_rows = RUNNER.AUDIT.read_tsv(
        audit_dir / "pointer-health.tsv"
    )
    health_by_id = {row.get("record_id", ""): row for row in health_rows}
    if (
        len(health_by_id) != len(health_rows)
        or set(health_by_id) != set(base_record_order)
        or len(set(base_record_order)) != len(base_record_order)
    ):
        raise ValueError("base view health order binding failed")
    RUNNER.AUDIT.atomic_write_tsv(
        target / "pointer-health.tsv",
        health_fields,
        [health_by_id[record_id] for record_id in base_record_order],
    )
    copied_fields, copied_health = RUNNER.AUDIT.read_tsv(
        target / "pointer-health.tsv"
    )
    copied_by_id = {row.get("record_id", ""): row for row in copied_health}
    if copied_fields != health_fields or copied_by_id != health_by_id:
        raise ValueError("base view health content changed")
    source_content_sha256 = RUNNER.canonical_json_sha256(
        [health_by_id[record_id] for record_id in sorted(health_by_id)]
    )
    copied_content_sha256 = RUNNER.canonical_json_sha256(
        [copied_by_id[record_id] for record_id in sorted(copied_by_id)]
    )
    contract_v2 = json.loads((audit_dir / "audit-contract.json").read_text(encoding="utf-8"))
    contract_v1 = dict(contract_v2)
    contract_v1["schema"] = "coverfish.pointer-audit-contract.v1"
    contract_v1.pop("execution_profile_sha256", None)
    contract_v1.pop("execution_profile_schema", None)
    contract_v1.pop("parallel_scheduler", None)
    contract_v1.pop("parallel_sharding", None)
    contract_v1.pop("parallel_requirements_sha256", None)
    contract_v1.pop("parallel_dependencies", None)
    contract_v1["scope_ids_sha256"] = RUNNER.hashlib.sha256(
        "".join(f"{record_id}\n" for record_id in base_record_order).encode()
    ).hexdigest()
    (target / "audit-contract.json").write_text(
        json.dumps(contract_v1, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    contract_sha = BASE.file_sha256(target / "audit-contract.json")
    for source in sorted(audit_dir.glob("run-metadata-*.json")):
        metadata = json.loads(source.read_text(encoding="utf-8"))
        # These are the v0.4-only run-metadata fields.  They are verified
        # against the execution summary before the private v0.2 view is made.
        metadata.pop("parallel_max_inflight", None)
        metadata.pop("parallel_worker_epoch", None)
        metadata.pop("parallel_scheduler", None)
        metadata.pop("parallel_sharding", None)
        metadata.pop("parallel_frozen_candidate_rows", None)
        metadata.pop("parallel_frozen_shard_rows", None)
        metadata.pop("parallel_eligible_rows_before_limit", None)
        metadata.pop("parallel_shard_eligible_rows_before_limit", None)
        metadata.pop("parallel_full_schedule_sha256", None)
        metadata.pop("parallel_frozen_shard_schedule_sha256", None)
        metadata.pop("parallel_shard_schedule_sha256", None)
        metadata.pop("parallel_selected_schedule_sha256", None)
        metadata["contract_sha256"] = contract_sha
        (target / source.name).write_text(
            json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    summary_path = target / "pointer-health-summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["receipt"]["contract_sha256"] = contract_sha
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return {
        "rows": len(health_rows),
        "source_content_sha256": source_content_sha256,
        "private_view_content_sha256": copied_content_sha256,
        "content_unchanged": source_content_sha256 == copied_content_sha256,
        "source_order_sha256": RUNNER.hashlib.sha256(
            "".join(f"{row['record_id']}\n" for row in health_rows).encode()
        ).hexdigest(),
        "private_view_order_sha256": RUNNER.hashlib.sha256(
            "".join(f"{record_id}\n" for record_id in base_record_order).encode()
        ).hexdigest(),
    }


def base_view_record_order(
    args: argparse.Namespace, rows: list[dict[str, str]]
) -> list[str] | None:
    """Bind the private v0.2 view to both frozen producer/verifier row orders."""
    try:
        bindings = json.loads(args.bindings.read_text(encoding="utf-8"))
        producer_bindings, archive_rows = RUNNER.AUDIT.load_bound_rows(
            args.source_root, args.bindings
        )
        if producer_bindings != bindings:
            raise ValueError("producer bindings mismatch")
        if args.scope == "archive":
            producer_scope = archive_rows
        else:
            if args.manifest is None:
                raise ValueError("pilot manifest missing")
            sample = RUNNER.AUDIT.load_sample_manifest(args.manifest)
            archive_by_id = {row["record_id"]: row for row in archive_rows}
            producer_scope = [archive_by_id[row["record_id"]] for row in sample]
        base_checks = BASE.Checks()
        frozen_order, frozen_by_id = BASE.load_frozen(
            args.source_root, bindings, base_checks
        )
        base_scope = BASE.load_scope(
            args.scope,
            args.manifest,
            frozen_order,
            frozen_by_id,
            base_checks,
        )
        producer_ids = [row["record_id"] for row in producer_scope]
        base_ids = [row["record_id"] for row in base_scope]
        health_fields, health_rows = RUNNER.AUDIT.read_tsv(
            args.audit_dir / "pointer-health.tsv"
        )
        health_ids = [row.get("record_id", "") for row in health_rows]
        attempts = RUNNER.AUDIT.read_attempts(
            args.audit_dir / "pointer-health-attempts.tsv"
        )
        completions = RUNNER.AUDIT.read_completions(
            args.audit_dir / "record-completions.tsv"
        )
        dispositions = RUNNER.AUDIT.read_dispositions(
            args.audit_dir / "attempt-dispositions.tsv"
        )
        robots = RUNNER.AUDIT.read_robots(args.audit_dir / "robots.tsv")
        metadata = RUNNER.AUDIT.load_and_validate_invocation_metadata(
            args.audit_dir,
            attempts,
            completions,
            dispositions,
            robots,
        )
        final_window_ids = {
            str(item["window_id"])
            for item in metadata
            if item.get("status") == "COMPLETE"
            and item.get("final_window") is True
        }
        rebuilt_health = RUNNER.AUDIT.build_health_rows(
            producer_scope,
            RUNNER.AUDIT.select_committed_attempts(attempts, completions),
            completions,
            final_window_ids,
        )
        contract = json.loads(
            (args.audit_dir / "audit-contract.json").read_text(encoding="utf-8")
        )
        policy = RUNNER.AUDIT.load_json(args.policy)
        RUNNER.AUDIT.validate_host_policy(policy)
        expected_user_agent = (
            f"{policy.get('user_agent_product')} "
            f"(+{policy.get('public_contact_url')})"
        )
        profile_path = args.audit_dir / RUNNER.EXECUTION_PROFILE_NAME
        expected_contract = RUNNER._parallel_contract(
            args,
            producer_bindings,
            producer_scope,
            expected_user_agent,
            RUNNER.file_sha256(profile_path),
        )
        producer_sha256 = RUNNER.AUDIT.scope_ids_sha256(producer_scope)
        base_sha256 = RUNNER.hashlib.sha256(
            "".join(f"{record_id}\n" for record_id in base_ids).encode()
        ).hexdigest()
        passed = (
            base_checks.passed
            and tuple(health_fields) == RUNNER.AUDIT.HEALTH_FIELDS
            and health_ids == producer_ids
            and health_rows == rebuilt_health
            and len(producer_ids) == len(set(producer_ids))
            and len(base_ids) == len(set(base_ids))
            and set(producer_ids) == set(base_ids)
            and contract == expected_contract
        )
        observed = {
            "scope": args.scope,
            "rows": len(producer_ids),
            "same_record_set": set(producer_ids) == set(base_ids),
            "original_health_in_producer_order": health_ids == producer_ids,
            "original_health_content_exact": health_rows == rebuilt_health,
            "original_contract_v2_exact": contract == expected_contract,
            "producer_order_sha256": producer_sha256,
            "base_verifier_order_sha256": base_sha256,
            "health_content_sha256": RUNNER.canonical_json_sha256(
                sorted(health_rows, key=lambda row: row["record_id"])
            ),
            "private_order_adapter_required": producer_ids != base_ids,
        }
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        RUNNER.AUDIT.AuditError,
    ):
        passed = False
        observed = {"scope": args.scope, "error": "order_binding_failed"}
        base_ids = []
    add_check(
        rows,
        "parallel_private_base_order_adapter_bound",
        passed,
        observed,
        {
            "same_record_set": True,
            "original_health_in_producer_order": True,
            "original_health_content_exact": True,
            "original_contract_v2_exact": True,
        },
    )
    return base_ids if passed else None


def verify_spool_recoveries(
    audit_dir: Path, rows: list[dict[str, str]]
) -> bool:
    path = audit_dir / RUNNER.SPOOL_RECOVERY_FILE
    try:
        fields, recoveries = RUNNER.AUDIT.read_tsv(path)
    except (OSError, UnicodeDecodeError, ValueError, RUNNER.AUDIT.AuditError):
        fields, recoveries = [], []
    failures: dict[str, int] = {}

    def fail(name: str) -> None:
        failures[name] = failures.get(name, 0) + 1

    seen: set[str] = set()
    if tuple(fields) != RUNNER.SPOOL_RECOVERY_FIELDS:
        fail("schema")
    for row in recoveries:
        try:
            original = BASE.as_int(row["original_size_bytes"])
            retained = BASE.as_int(row["retained_size_bytes"])
            discarded = BASE.as_int(row["discarded_fragment_bytes"])
            BASE.parse_utc(row["detected_at_utc"])
        except (KeyError, ValueError):
            fail("types")
            continue
        safety = row.get("requires_safety_review")
        identity_values = (
            row.get("window_id", ""),
            row.get("invocation_id", ""),
            row.get("record_id", ""),
            row.get("spool_name", ""),
            row.get("artifact_name", ""),
            row.get("action", ""),
            str(original),
            str(retained),
            str(discarded),
            row.get("discarded_fragment_sha256", ""),
            safety or "",
        )
        expected_id = RUNNER.hashlib.sha256(
            "\0".join(identity_values).encode()
        ).hexdigest()[:24]
        if (
            row.get("recovery_id") != expected_id
            or expected_id in seen
            or not BASE.WINDOW_RE.fullmatch(row.get("window_id", ""))
            or not RUNNER.re.fullmatch(
                r"[0-9a-f]{16}", row.get("invocation_id", "")
            )
            or safety not in {"true", "false"}
            or not BASE.is_hex(row.get("discarded_fragment_sha256", ""), 64)
            or bool(row.get("record_id")) != bool(row.get("spool_name"))
            or row.get("spool_name", "")
            and not RUNNER.re.fullmatch(
                r"[0-9]{8}-[0-9a-f]{16}", row["spool_name"]
            )
        ):
            fail("identity")
        seen.add(expected_id)
        action = row.get("action")
        artifact = row.get("artifact_name")
        artifact_action_ok = RUNNER._spool_recovery_artifact_action_valid(
            artifact,
            action,
            row.get("record_id", ""),
            row.get("spool_name", ""),
        )
        if not artifact_action_ok:
            fail("artifact_action_allowlist")
        if action == "truncate_incomplete_tsv_tail":
            shape = original == retained + discarded and discarded > 0
            expected_safety = artifact in {
                "pointer-health-attempts.tsv",
                "robots.tsv",
            }
        elif action in {
            "promote_complete_json_temp",
            "discard_duplicate_complete_json_temp",
        }:
            shape = original == retained and discarded == 0
            expected_safety = False
        elif action == "quarantine_incomplete_json_temp":
            shape = original == discarded and retained == 0
            expected_safety = False
        elif action == "discard_prestart_missing_manifest":
            shape = original == retained == discarded == 0
            expected_safety = False
        elif action == "discard_empty_tsv_prewrite":
            shape = original == retained == discarded == 0
            expected_safety = False
        else:
            shape = False
            expected_safety = False
        if (
            not artifact_action_ok
            or not shape
            or (safety == "true") != expected_safety
        ):
            fail("action")
    semantic_ok = not failures
    add_check(
        rows,
        "parallel_spool_recoveries_semantics",
        semantic_ok,
        failures,
        {},
    )
    safety_rows = sum(
        row.get("requires_safety_review") == "true" for row in recoveries
    )
    add_check(
        rows,
        "parallel_spool_recovery_safety_closed",
        safety_rows == 0,
        safety_rows,
        0,
    )
    return semantic_ok and safety_rows == 0


def _receipt_string_has_local_identity(value: str) -> bool:
    """Reject portable receipt fields that disclose a local filesystem identity."""
    candidate = value.strip()
    if not candidate:
        return False
    lowered = candidate.lower()
    if (
        candidate.startswith(("/", "~/", "\\\\"))
        or lowered.startswith("file://")
        or RUNNER.re.match(r"^[A-Za-z]:[\\/]", candidate)
    ):
        return True
    # Also catch a path embedded in a diagnostic sentence.  A URL beginning
    # with http(s) is a public pointer, not a local path receipt.
    if lowered.startswith(("http://", "https://")):
        return False
    return bool(
        RUNNER.re.search(
            r"(?:^|[\s=:'\"(])(?:/home/[^/\s]+|/Users/[^/\s]+|"
            r"/mnt/[^/\s]+|/srv/[^/\s]+|/tmp(?:/|\b)|/var/tmp(?:/|\b))",
            candidate,
        )
    )


def _json_scalar_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [
            scalar
            for item in value
            for scalar in _json_scalar_strings(item)
        ]
    if isinstance(value, dict):
        return [
            scalar
            for key, item in value.items()
            for scalar in (str(key), *_json_scalar_strings(item))
        ]
    return []


def verify_receipt_field_privacy(
    audit_dir: Path, rows: list[dict[str, str]]
) -> bool:
    """Scan structured receipt fields for portable-path/privacy violations."""
    failures: dict[str, int] = {}
    files_scanned = 0
    for path in sorted(audit_dir.iterdir(), key=lambda item: item.name):
        if path.name == "independent-checks.tsv" or path.suffix not in {
            ".json",
            ".tsv",
        }:
            continue
        try:
            if path.suffix == ".json":
                values = _json_scalar_strings(
                    json.loads(path.read_text(encoding="utf-8"))
                )
                violations = sum(
                    _receipt_string_has_local_identity(value)
                    for value in values
                )
            else:
                with path.open(newline="", encoding="utf-8") as source:
                    reader = csv.reader(source, delimiter="\t")
                    violations = sum(
                        _receipt_string_has_local_identity(cell)
                        for record in reader
                        for cell in record
                    )
            files_scanned += 1
            if violations:
                failures[path.name] = violations
        except (OSError, UnicodeDecodeError, csv.Error, json.JSONDecodeError):
            failures[path.name] = -1
    passed = not failures
    add_check(
        rows,
        "parallel_receipt_fields_no_local_identity",
        passed,
        {"files_scanned": files_scanned, "failures": failures},
        {"failures": {}},
    )
    return passed


def verify_recovery_ledger_tail_receipts(
    audit_dir: Path, rows: list[dict[str, str]]
) -> tuple[bool, set[str]]:
    failures: dict[str, int] = {}

    def fail(name: str) -> None:
        failures[name] = failures.get(name, 0) + 1

    names: set[str] = set()
    seen: set[str] = set()
    pattern = RUNNER.re.compile(
        rf"{RUNNER.RECOVERY_LEDGER_TAIL_PREFIX}([0-9a-f]{{24}})\.json\Z"
    )
    for path in sorted(
        audit_dir.glob(f"{RUNNER.RECOVERY_LEDGER_TAIL_PREFIX}*.json")
    ):
        names.add(path.name)
        match = pattern.fullmatch(path.name)
        if match is None or not RUNNER._safe_private_file(path):
            fail("path")
            continue
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            fail("json")
            continue
        expected_keys = {
            "schema",
            "recovery_id",
            "ledger",
            "detected_at_utc",
            "original_size_bytes",
            "retained_size_bytes",
            "discarded_fragment_bytes",
            "discarded_fragment_sha256",
        }
        if not isinstance(receipt, dict) or set(receipt) != expected_keys:
            fail("schema")
            continue
        original = receipt.get("original_size_bytes")
        retained = receipt.get("retained_size_bytes")
        discarded = receipt.get("discarded_fragment_bytes")
        typed_sizes = all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in (original, retained, discarded)
        )
        try:
            BASE.parse_utc(str(receipt.get("detected_at_utc", "")))
            timestamp_ok = True
        except ValueError:
            timestamp_ok = False
        fragment_sha = str(receipt.get("discarded_fragment_sha256", ""))
        if typed_sizes:
            expected_id = RUNNER.hashlib.sha256(
                (
                    f"{RUNNER.SPOOL_RECOVERY_FILE}\0{original}\0{retained}\0"
                    f"{discarded}\0{fragment_sha}"
                ).encode()
            ).hexdigest()[:24]
        else:
            expected_id = ""
        header_size = len(
            ("\t".join(RUNNER.SPOOL_RECOVERY_FIELDS) + "\n").encode()
        )
        recovery_id = str(receipt.get("recovery_id", ""))
        identity_ok = (
            typed_sizes
            and original == retained + discarded
            and retained >= header_size
            and discarded > 0
            and receipt.get("schema") == RUNNER.RECOVERY_LEDGER_TAIL_SCHEMA
            and receipt.get("ledger") == RUNNER.SPOOL_RECOVERY_FILE
            and BASE.is_hex(fragment_sha, 64)
            and recovery_id == match.group(1) == expected_id
            and recovery_id not in seen
            and timestamp_ok
        )
        if not identity_ok:
            fail("semantics")
        seen.add(recovery_id)
    valid = not failures
    add_check(
        rows,
        "parallel_recovery_ledger_tail_receipts",
        valid,
        {"files": len(names), "failures": failures},
        {"failures": {}},
    )
    return valid, names


def verify_parallel_surface(audit_dir: Path, rows: list[dict[str, str]]) -> bool:
    ok = True
    try:
        directory = audit_dir.lstat()
        preflight_ok = (
            stat.S_ISDIR(directory.st_mode)
            and not audit_dir.is_symlink()
            and directory.st_uid == os.getuid()
            and not directory.st_mode & 0o022
        )
        for path in audit_dir.iterdir():
            metadata = path.lstat()
            if (
                path.is_symlink()
                or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_nlink != 1
                or metadata.st_mode & 0o022
            ):
                preflight_ok = False
    except OSError:
        preflight_ok = False
    add_check(rows, "parallel_paths_preflight", preflight_ok, preflight_ok, True)
    if not preflight_ok:
        return False
    privacy_ok = verify_receipt_field_privacy(audit_dir, rows)
    ok &= privacy_ok
    recoveries_ok = verify_spool_recoveries(audit_dir, rows)
    ok &= recoveries_ok
    atomic_ledger_present = (audit_dir / "atomic-temp-recoveries.tsv").is_file()
    add_check(
        rows,
        "parallel_atomic_recovery_ledger_present",
        atomic_ledger_present,
        atomic_ledger_present,
        True,
    )
    ok &= atomic_ledger_present
    recovery_tail_ok, recovery_tail_names = verify_recovery_ledger_tail_receipts(
        audit_dir, rows
    )
    ok &= recovery_tail_ok
    profile_path = audit_dir / RUNNER.EXECUTION_PROFILE_NAME
    contract_path = audit_dir / "audit-contract.json"
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        sharding = contract.get("parallel_sharding")
        if not isinstance(sharding, dict):
            raise ValueError("sharding missing")
        shard_count = sharding.get("shard_count")
        shard_index = sharding.get("shard_index")
        RUNNER._validate_shard_values(shard_count, shard_index)
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        expected_profile = RUNNER._execution_profile(shard_count, shard_index)
        profile_ok = profile == expected_profile
        contract_ok = (
            contract.get("schema") == "coverfish.pointer-audit-contract.v3"
            and contract.get("execution_profile_schema")
            == "coverfish.pointer-parallel-execution-profile.v2"
            and contract.get("execution_profile_sha256")
            == RUNNER.file_sha256(profile_path)
            and contract.get("parallel_scheduler")
            == RUNNER._scheduler_contract()
            and sharding
            == RUNNER._sharding_contract(shard_count, shard_index)
            and contract.get("parallel_requirements_sha256")
            == RUNNER.EXPECTED_PARALLEL_REQUIREMENTS_SHA256
            and contract.get("parallel_dependencies")
            == RUNNER._parallel_dependency_report()
            and contract.get("tool_sha256")
            == RUNNER.EXPECTED_PRODUCER_SHA256
            and contract.get("bytes_retained") is False
            and contract.get("gpu_used") is False
        )
    except (OSError, ValueError, TypeError, RUNNER.AUDIT.AuditError):
        profile = {}
        expected_profile = RUNNER._execution_profile()
        profile_ok = False
        contract = {}
        sharding = {}
        contract_ok = False
    add_check(rows, "parallel_execution_profile_exact", profile_ok, profile, expected_profile)
    ok &= profile_ok
    add_check(
        rows,
        "parallel_contract_binding",
        contract_ok,
        contract,
        "bound v0.4 profile and sharding",
    )
    ok &= contract_ok
    work_absent = not (audit_dir / RUNNER.WORK_ROOT_NAME).exists()
    add_check(rows, "parallel_work_spool_closed", work_absent, work_absent, True)
    ok &= work_absent
    metadata_paths = sorted(audit_dir.glob("run-metadata-*.json"))
    metadata_pairs: set[tuple[str, str]] = set()
    contract_sha = RUNNER.file_sha256(contract_path) if contract_path.is_file() else ""
    metadata_contract_ok = bool(metadata_paths)
    metadata_parallel_ok = bool(metadata_paths)
    expected_execution_names: set[str] = set()
    summaries_ok = True
    for metadata_path in metadata_paths:
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata_pairs.add(
                (str(metadata.get("window_id", "")), str(metadata.get("invocation_id", "")))
            )
            metadata_contract_ok &= metadata.get("contract_sha256") == contract_sha
            max_inflight = metadata.get("parallel_max_inflight")
            worker_epoch = metadata.get("parallel_worker_epoch")
            metadata_parallel_ok &= (
                isinstance(max_inflight, int)
                and not isinstance(max_inflight, bool)
                and 1 <= max_inflight <= RUNNER.MAX_INFLIGHT
                and worker_epoch
                == RUNNER._worker_epoch(
                    audit_dir,
                    str(metadata.get("invocation_id", "")),
                    str(metadata.get("window_id", "")),
                    str(metadata.get("started_at_utc", "")),
                )
                and RUNNER._schedule_metadata_valid(metadata)
                and metadata.get("parallel_sharding") == sharding
            )
            if not metadata_parallel_ok:
                raise ValueError("parallel_max_inflight")
            name = (
                f"parallel-execution-{metadata['window_id']}-"
                f"{metadata['invocation_id']}.json"
            )
            expected_execution_names.add(name)
            summary_path = audit_dir / name
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            expected_summary = RUNNER._execution_summary_payload(
                audit_dir, metadata, max_inflight
            )
            if summary != expected_summary:
                raise ValueError("execution summary mismatch")
        except (OSError, ValueError, TypeError, KeyError, RUNNER.AUDIT.AuditError):
            summaries_ok = False
    try:
        _, recovery_rows = RUNNER.AUDIT.read_tsv(
            audit_dir / RUNNER.SPOOL_RECOVERY_FILE
        )
        recovery_binding_ok = all(
            (row.get("window_id", ""), row.get("invocation_id", ""))
            in metadata_pairs
            for row in recovery_rows
        )
    except (OSError, UnicodeDecodeError, ValueError, RUNNER.AUDIT.AuditError):
        recovery_binding_ok = False
    add_check(
        rows,
        "parallel_spool_recoveries_metadata_binding",
        recovery_binding_ok,
        recovery_binding_ok,
        True,
    )
    ok &= recovery_binding_ok
    try:
        health_summary = json.loads(
            (audit_dir / "pointer-health-summary.json").read_text(encoding="utf-8")
        )
        health_contract_ok = (
            health_summary.get("receipt", {}).get("contract_sha256") == contract_sha
        )
    except (OSError, ValueError, TypeError):
        health_contract_ok = False
    add_check(
        rows,
        "parallel_original_contract_sha_bindings",
        metadata_contract_ok and health_contract_ok,
        {"metadata": metadata_contract_ok, "summary": health_contract_ok},
        {"metadata": True, "summary": True},
    )
    ok &= metadata_contract_ok and health_contract_ok
    add_check(
        rows,
        "parallel_metadata_max_inflight_binding",
        metadata_parallel_ok,
        metadata_parallel_ok,
        True,
    )
    ok &= metadata_parallel_ok
    actual_execution_names = {
        path.name
        for path in audit_dir.glob("parallel-execution-*.json")
        if path.name != RUNNER.EXECUTION_PROFILE_NAME
    }
    summaries_ok &= actual_execution_names == expected_execution_names
    add_check(
        rows,
        "parallel_execution_summaries_full_binding",
        summaries_ok,
        sorted(actual_execution_names),
        sorted(expected_execution_names),
    )
    ok &= summaries_ok
    allowed = {
        "audit-contract.json",
        RUNNER.EXECUTION_PROFILE_NAME,
        "pointer-health-attempts.tsv",
        "pointer-health.tsv",
        "pointer-health-summary.json",
        "record-completions.tsv",
        "attempt-dispositions.tsv",
        "robots.tsv",
        "tail-recoveries.tsv",
        "atomic-temp-recoveries.tsv",
        RUNNER.SPOOL_RECOVERY_FILE,
        "independent-checks.tsv",
        *expected_execution_names,
        *(path.name for path in metadata_paths),
        *recovery_tail_names,
    }
    tree_ok = True
    for path in audit_dir.iterdir():
        if path.is_symlink() or not path.is_file() or path.name not in allowed:
            tree_ok = False
            break
    add_check(rows, "parallel_output_tree_closed", tree_ok, tree_ok, True)
    return ok and tree_ok


def _independent_schedule(
    candidate_rows: list[dict[str, str]],
    source_rows: list[dict[str, str]],
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    source_ordinals = {
        row["record_id"]: ordinal for ordinal, row in enumerate(source_rows)
    }
    ordered = sorted(
        candidate_rows, key=lambda row: source_ordinals[row["record_id"]]
    )
    lanes: dict[str, deque[tuple[int, dict[str, str]]]] = {}
    for row in ordered:
        host = str(row.get("source_host", "")).lower().rstrip(".")
        config = policy.get("hosts", {}).get(host)
        if not isinstance(config, dict):
            raise ValueError("unbound schedule host")
        rate_group = str(config.get("rate_group", "")).strip()
        lane_key = f"rate_group:{rate_group}" if rate_group else f"host:{host}"
        lane = lanes.setdefault(lane_key, deque())
        lane.append((len(lane), row))
    active = list(lanes)
    schedule: list[dict[str, Any]] = []
    while active:
        remaining: list[str] = []
        for lane_key in active:
            lane_ordinal, row = lanes[lane_key].popleft()
            schedule.append(
                {
                    "ordinal": len(schedule),
                    "source_ordinal": source_ordinals[row["record_id"]],
                    "record_id": row["record_id"],
                    "lane_key": lane_key,
                    "lane_ordinal": lane_ordinal,
                    "row": row,
                }
            )
            if lanes[lane_key]:
                remaining.append(lane_key)
        active = remaining
    return schedule


def _independent_digest(
    entries: list[dict[str, Any]], fields: Sequence[str]
) -> str:
    payload = [[entry.get(field) for field in fields] for entry in entries]
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return RUNNER.hashlib.sha256(encoded).hexdigest()


def verify_shard_selection(
    args: argparse.Namespace, rows: list[dict[str, str]]
) -> bool:
    failures: dict[str, int] = defaultdict(int)
    observations: list[dict[str, Any]] = []
    try:
        _, source_rows = RUNNER.AUDIT.load_bound_rows(
            args.source_root, args.bindings
        )
        if args.scope == "pilot":
            if args.manifest is None:
                raise ValueError("pilot manifest missing")
            sample = RUNNER.AUDIT.load_sample_manifest(args.manifest)
            by_id = {row["record_id"]: row for row in source_rows}
            source_rows = [by_id[row["record_id"]] for row in sample]
        policy = RUNNER.AUDIT.load_json(args.policy)
        RUNNER.AUDIT.validate_host_policy(policy)
        contract = json.loads(
            (args.audit_dir / "audit-contract.json").read_text(encoding="utf-8")
        )
        sharding = contract["parallel_sharding"]
        shard_count = sharding["shard_count"]
        shard_index = sharding["shard_index"]
        RUNNER._validate_shard_values(shard_count, shard_index)
        attempts = RUNNER.AUDIT.read_attempts(
            args.audit_dir / "pointer-health-attempts.tsv"
        )
        completions = RUNNER.AUDIT.read_completions(
            args.audit_dir / "record-completions.tsv"
        )
        dispositions = RUNNER.AUDIT.read_dispositions(
            args.audit_dir / "attempt-dispositions.tsv"
        )
        metadata_paths = RUNNER.AUDIT.invocation_metadata_files(args.audit_dir)
        for metadata_path in metadata_paths:
            metadata = RUNNER.AUDIT.load_json(metadata_path)
            components = metadata.get("components")
            if not isinstance(components, list) or any(
                component not in RUNNER.AUDIT.COMPONENT_ORDER
                for component in components
            ):
                raise ValueError("component selection invalid")
            component_set = set(components)
            candidates = (
                [
                    row
                    for row in source_rows
                    if row["component"] in component_set
                ]
                if components
                else source_rows
            )
            full_schedule = _independent_schedule(
                candidates, source_rows, policy
            )
            all_shards: list[list[dict[str, Any]]] = []
            for index in range(shard_count):
                shard_entries = []
                for entry in full_schedule:
                    if entry["lane_ordinal"] % shard_count == index:
                        shard_entries.append(
                            {
                                **entry,
                                "global_ordinal": entry["ordinal"],
                                "shard_ordinal": len(shard_entries),
                            }
                        )
                all_shards.append(shard_entries)
            shard_sets = [
                {entry["record_id"] for entry in shard}
                for shard in all_shards
            ]
            union = set().union(*shard_sets) if shard_sets else set()
            coverage_ok = (
                union == {entry["record_id"] for entry in full_schedule}
                and sum(len(values) for values in shard_sets) == len(union)
            )
            if not coverage_ok:
                failures["partition_coverage"] += 1
            attempts_before = metadata.get("attempts_before")
            completions_before = metadata.get("completions_before")
            if (
                not isinstance(attempts_before, int)
                or isinstance(attempts_before, bool)
                or not 0 <= attempts_before <= len(attempts)
                or not isinstance(completions_before, int)
                or isinstance(completions_before, bool)
                or not 0 <= completions_before <= len(completions)
            ):
                raise ValueError("ledger prefix invalid")
            prior_attempts = attempts[:attempts_before]
            prior_completions = completions[:completions_before]
            prior_attempt_ids = {
                attempt["attempt_id"] for attempt in prior_attempts
            }
            prior_dispositions = [
                disposition
                for disposition in dispositions
                if disposition.get("attempt_id") in prior_attempt_ids
            ]
            committed = RUNNER.AUDIT.select_committed_attempts(
                prior_attempts, prior_completions
            )
            by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
            for attempt in committed:
                by_record[attempt.get("record_id", "")].append(attempt)
            abandoned = RUNNER.AUDIT.unrecovered_abandoned_record_ids(
                prior_attempts, prior_completions, prior_dispositions
            )
            completed_in_window = {
                row.get("record_id", "")
                for row in prior_completions
                if row.get("window_id") == metadata.get("window_id")
            }
            retry_mode = metadata.get("retry_mode")
            eligible_ids = {
                row["record_id"]
                for row in candidates
                if row["record_id"] not in completed_in_window
                and (
                    row["record_id"] in abandoned
                    or RUNNER.AUDIT.should_process(
                        by_record.get(row["record_id"], []), retry_mode
                    )
                )
            }
            frozen_shard = all_shards[shard_index]
            shard_eligible = []
            for entry in frozen_shard:
                if entry["record_id"] in eligible_ids:
                    shard_eligible.append(
                        {**entry, "ordinal": len(shard_eligible)}
                    )
            max_rows = metadata.get("max_rows")
            selected = (
                shard_eligible[:max_rows]
                if isinstance(max_rows, int) and not isinstance(max_rows, bool)
                else shard_eligible
            )
            expected = {
                "parallel_sharding": RUNNER._sharding_contract(
                    shard_count, shard_index
                ),
                "parallel_frozen_candidate_rows": len(full_schedule),
                "parallel_frozen_shard_rows": len(frozen_shard),
                "parallel_eligible_rows_before_limit": len(eligible_ids),
                "parallel_shard_eligible_rows_before_limit": len(
                    shard_eligible
                ),
                "rows_selected": len(selected),
                "parallel_full_schedule_sha256": _independent_digest(
                    full_schedule, RUNNER.SCHEDULE_DIGEST_FIELDS
                ),
                "parallel_frozen_shard_schedule_sha256": (
                    _independent_digest(
                        frozen_shard, RUNNER.FROZEN_SHARD_DIGEST_FIELDS
                    )
                ),
                "parallel_shard_schedule_sha256": _independent_digest(
                    shard_eligible, RUNNER.SHARD_SCHEDULE_DIGEST_FIELDS
                ),
                "parallel_selected_schedule_sha256": _independent_digest(
                    selected, RUNNER.SHARD_SCHEDULE_DIGEST_FIELDS
                ),
            }
            mismatched = [
                key for key, value in expected.items() if metadata.get(key) != value
            ]
            if mismatched:
                failures["metadata_binding"] += 1
            observations.append(
                {
                    "window_id": metadata.get("window_id"),
                    "candidate_rows": len(full_schedule),
                    "partition_rows": [len(shard) for shard in all_shards],
                    "partition_union_rows": len(union),
                    "partition_disjoint": coverage_ok,
                    "selected_shard_rows": len(frozen_shard),
                    "eligible_shard_rows": len(shard_eligible),
                    "rows_selected": len(selected),
                    "mismatched_fields": mismatched,
                }
            )
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        RUNNER.AUDIT.AuditError,
    ):
        failures["runtime"] += 1
    passed = not failures and bool(observations)
    add_check(
        rows,
        "parallel_shard_selection_and_coverage",
        passed,
        {"failures": dict(failures), "invocations": observations},
        {"failures": {}},
    )
    return passed


def base_check_roster_exact(source_rows: list[dict[str, str]], scope: str) -> bool:
    expected = BASE_CHECK_ROSTERS.get(scope)
    expected_with_tail = BASE_CHECK_ROSTERS_WITH_TAIL.get(scope)
    observed = tuple(row.get("check_id", "") for row in source_rows)
    return (
        expected is not None
        and bool(source_rows)
        and all(set(row) == BASE_CHECK_FIELDS for row in source_rows)
        and observed in {expected, expected_with_tail}
    )


def verify_robots_cache_boundary_continuity(
    audit_dir: Path, policy: dict[str, Any]
) -> dict[str, Any]:
    """Verify stale-at-finish attempts against an unchanged adjacent refresh.

    A worker obtains its robots decision before the image request, while the
    attempt timestamp is written after that request finishes. The timestamp
    can therefore cross cache expiry even though the decision itself did not.
    This adapter accepts only a request-timeout-sized boundary bracketed by two
    otherwise identical robots receipts from the same invocation and host.
    """
    failures: Counter[str] = Counter()
    missing_or_stale = 0
    continuous = 0
    grace_seconds = 0
    try:
        attempt_fields, attempts = RUNNER.AUDIT.read_tsv(
            audit_dir / "pointer-health-attempts.tsv"
        )
        robots_fields, robots = RUNNER.AUDIT.read_tsv(audit_dir / "robots.tsv")
        if tuple(attempt_fields) != BASE.ATTEMPT_FIELDS:
            failures["attempt_schema"] += 1
        if tuple(robots_fields) != BASE.ROBOTS_FIELDS:
            failures["robots_schema"] += 1
        cache_seconds = int(policy.get("robots_cache_seconds", 0) or 0)
        grace_seconds = int(policy.get("request_timeout_seconds", 0) or 0)
        if cache_seconds <= 0 or grace_seconds <= 0:
            failures["policy_bounds"] += 1
        observations: dict[
            tuple[str, str, str], list[tuple[Any, dict[str, str]]]
        ] = defaultdict(list)
        for row in robots:
            key = (row["window_id"], row["invocation_id"], row["host"])
            observations[key].append((BASE.parse_utc(row["checked_at_utc"]), row))
        for items in observations.values():
            if [item[0] for item in items] != sorted(item[0] for item in items):
                failures["observation_order"] += 1
        if failures:
            raise ValueError("preflight")
        for attempt in attempts:
            robots_status = attempt["robots_status"]
            if not robots_status:
                continue
            attempt_time = BASE.parse_utc(attempt["checked_at_utc"])
            key = (
                attempt["window_id"], attempt["invocation_id"], attempt["host"]
            )
            items = observations.get(key, [])
            eligible = [
                item for item in items
                if item[0] <= attempt_time
                and (attempt_time - item[0]).total_seconds() <= cache_seconds
            ]
            if eligible:
                continue
            missing_or_stale += 1
            preceding = [item for item in items if item[0] <= attempt_time]
            following = [item for item in items if item[0] > attempt_time]
            if not preceding or not following:
                failures["adjacent_receipt_missing"] += 1
                continue
            previous_time, previous = max(preceding, key=lambda item: item[0])
            next_time, refreshed = min(following, key=lambda item: item[0])
            previous_age = (attempt_time - previous_time).total_seconds()
            refresh_lag = (next_time - attempt_time).total_seconds()
            if not (
                cache_seconds < previous_age <= cache_seconds + grace_seconds
                and 0 < refresh_lag <= grace_seconds
            ):
                failures["outside_request_timeout_boundary"] += 1
                continue
            if any(
                previous.get(field, "") != refreshed.get(field, "")
                for field in ROBOTS_CONTINUITY_FIELDS
            ):
                failures["refresh_changed"] += 1
                continue
            if refreshed["robots_state"] not in ROBOTS_COMPATIBLE_STATES.get(
                robots_status, set()
            ):
                failures["attempt_state_binding"] += 1
                continue
            continuous += 1
    except (OSError, KeyError, TypeError, ValueError):
        if not failures:
            failures["parse_or_runtime"] += 1
    return {
        "passed": not failures and continuous == missing_or_stale,
        "missing_or_stale_attempts": missing_or_stale,
        "continuous_boundary_attempts": continuous,
        "grace_seconds": grace_seconds,
        "failures": dict(failures),
    }


def adapt_base_checks(
    source_rows: list[dict[str, str]],
    returncode: int,
    scope: str,
    robots_continuity: dict[str, Any],
) -> tuple[list[dict[str, str]], bool, dict[str, int]]:
    """Allow only documented parallel timestamp differences of v0.4.

    Parallel record transactions remain contiguous and internally ordered, but
    their wall-clock intervals may overlap.  The base verifier consequently
    reports only ``global_time_order`` while still checking per-record order,
    transaction coverage, metadata time bounds, and every other semantic. A
    post-request timestamp may also cross an otherwise identical robots cache
    refresh within one request timeout. No other base failure is rewritten.
    """
    adapted: list[dict[str, str]] = []
    tolerated = {"global_time_order": 0, "robots_cache_boundary": 0}
    for source in source_rows:
        row = dict(source)
        allow = False
        if row.get("check_id") == "attempt_full_semantics" and row.get("status") == "FAIL":
            try:
                observed = json.loads(row.get("observed", ""))
                expected = json.loads(row.get("expected", ""))
                allow = (
                    isinstance(observed, dict)
                    and set(observed) == {"global_time_order"}
                    and isinstance(observed["global_time_order"], int)
                    and not isinstance(observed["global_time_order"], bool)
                    and observed["global_time_order"] > 0
                    and expected == {}
                )
            except (json.JSONDecodeError, TypeError):
                allow = False
        if allow:
            tolerated["global_time_order"] += 1
            row["status"] = "PASS"
            row["expected"] = json.dumps(
                {"only_parallel_divergence": "global_time_order"},
                sort_keys=True,
                separators=(",", ":"),
            )
        elif row.get("check_id") == "robots_full_semantics" and row.get("status") == "FAIL":
            try:
                observed = json.loads(row.get("observed", ""))
                expected = json.loads(row.get("expected", ""))
                boundary_count = robots_continuity.get(
                    "continuous_boundary_attempts", 0
                )
                allow = (
                    robots_continuity.get("passed") is True
                    and isinstance(boundary_count, int)
                    and not isinstance(boundary_count, bool)
                    and boundary_count > 0
                    and observed
                    == {"attempt_receipt_missing_or_stale": boundary_count}
                    and expected == {}
                )
            except (json.JSONDecodeError, TypeError):
                allow = False
            if allow:
                tolerated["robots_cache_boundary"] += 1
                row["status"] = "PASS"
                row["expected"] = json.dumps(
                    {
                        "only_parallel_divergence": (
                            "post_request_timestamp_crosses_identical_robots_refresh"
                        ),
                        "attempts": boundary_count,
                        "grace_seconds": robots_continuity.get("grace_seconds"),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
        row["check_id"] = f"base:{row.get('check_id', '')}"
        adapted.append(row)
    expected_exit = (
        BASE.EXIT_INTEGRITY if sum(tolerated.values()) else BASE.EXIT_OK
    )
    passed = (
        base_check_roster_exact(source_rows, scope)
        and
        returncode == expected_exit
        and all(row.get("status") == "PASS" for row in adapted)
    )
    return adapted, passed, tolerated


def write_checks(path: Path, rows: list[dict[str, str]]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".parallel-checks-", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(
                target,
                fieldnames=("check_id", "status", "observed", "expected"),
                delimiter="\t",
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(rows)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        RUNNER.AUDIT.fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--bindings", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--scope", choices=("pilot", "archive"), required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    checks: list[dict[str, str]] = []
    try:
        output_ok = (
            args.output.name == "independent-checks.tsv"
            and args.output.parent.resolve() == args.audit_dir.resolve()
        )
    except OSError:
        output_ok = False
    add_check(checks, "parallel_verifier_output_location", output_ok, output_ok, True)
    parallel_ok = output_ok and verify_parallel_surface(args.audit_dir, checks)
    shard_ok = False
    base_ok = False
    try:
        if not parallel_ok:
            raise ValueError("parallel preflight failed")
        # Only propagate shard arguments after verify_parallel_surface has
        # authenticated the contract and exact execution profile.  The base
        # adapter must never derive its expected contract from unverified
        # receipt fields.
        verified_contract = json.loads(
            (args.audit_dir / "audit-contract.json").read_text(encoding="utf-8")
        )
        verified_sharding = verified_contract["parallel_sharding"]
        RUNNER._validate_shard_values(
            verified_sharding["shard_count"], verified_sharding["shard_index"]
        )
        args.shard_count = verified_sharding["shard_count"]
        args.shard_index = verified_sharding["shard_index"]
        shard_ok = verify_shard_selection(args, checks)
        if not shard_ok:
            raise ValueError("independent shard reconstruction failed")
        base_record_order = base_view_record_order(args, checks)
        if base_record_order is None:
            raise ValueError("private base order adapter binding failed")
        with tempfile.TemporaryDirectory(prefix="coverfish-pointer-v04-verify-") as temporary:
            base_dir = Path(temporary)
            adapter_evidence = copy_base_view(
                args.audit_dir, base_dir, base_record_order
            )
            adapter_content_ok = adapter_evidence.get("content_unchanged") is True
            add_check(
                checks,
                "parallel_private_base_view_content_preserved",
                adapter_content_ok,
                adapter_evidence,
                {"content_unchanged": True},
            )
            if not adapter_content_ok:
                raise ValueError("private base view content changed")
            command = [
                sys.executable,
                str(ROOT / "software/verify_pointer_receipt.py"),
                "--source-root",
                str(args.source_root),
                "--bindings",
                str(args.bindings),
                "--policy",
                str(args.policy),
                "--scope",
                args.scope,
                "--audit-dir",
                str(base_dir),
                "--output",
                str(base_dir / "independent-checks.tsv"),
            ]
            if args.manifest is not None:
                command.extend(["--manifest", str(args.manifest)])
            result = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=600,
                check=False,
            )
            base_rows: list[dict[str, str]] = []
            if (base_dir / "independent-checks.tsv").is_file():
                with (base_dir / "independent-checks.tsv").open(
                    newline="", encoding="utf-8"
                ) as source:
                    reader = csv.DictReader(source, delimiter="\t")
                    if tuple(reader.fieldnames or ()) == (
                        "check_id",
                        "status",
                        "observed",
                        "expected",
                    ):
                        base_rows = list(reader)
            roster_ok = base_check_roster_exact(base_rows, args.scope)
            add_check(
                checks,
                "parallel_base_check_roster_exact",
                roster_ok,
                [row.get("check_id", "") for row in base_rows],
                [
                    list(BASE_CHECK_ROSTERS[args.scope]),
                    list(BASE_CHECK_ROSTERS_WITH_TAIL[args.scope]),
                ],
            )
            robots_continuity = verify_robots_cache_boundary_continuity(
                base_dir, RUNNER.AUDIT.load_json(args.policy)
            )
            adapted, base_ok, tolerated = adapt_base_checks(
                base_rows,
                result.returncode,
                args.scope,
                robots_continuity,
            )
            expected_robots_adaptations = (
                1 if robots_continuity["missing_or_stale_attempts"] else 0
            )
            continuity_bound = (
                robots_continuity["passed"] is True
                and tolerated["robots_cache_boundary"]
                == expected_robots_adaptations
            )
            base_ok = base_ok and continuity_bound
            checks.extend(adapted)
            add_check(
                checks,
                "parallel_base_semantic_adaptations",
                base_ok,
                {
                    "tolerated_base_checks": tolerated,
                    "robots_cache_boundary": robots_continuity,
                    "base_exit": result.returncode,
                },
                {
                    "only_failures_allowed": [
                        "attempt_full_semantics.global_time_order",
                        (
                            "robots_full_semantics."
                            "post_request_timestamp_crosses_identical_refresh"
                        ),
                    ],
                    "robots_boundary_evidence_bound": True,
                },
            )
    except (OSError, ValueError, subprocess.SubprocessError):
        base_ok = False
    add_check(checks, "base_verifier_exit", base_ok, base_ok, True)
    write_checks(args.output, checks)
    passed = (
        parallel_ok
        and shard_ok
        and base_ok
        and all(row["status"] == "PASS" for row in checks)
    )
    print(
        json.dumps(
            {
                "schema": SCHEMA,
                "tool_version": VERSION,
                "status": "PASS" if passed else "FAIL",
                "checks": len(checks),
                "failed": sum(row["status"] == "FAIL" for row in checks),
                "network_accessed": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if passed else 6


if __name__ == "__main__":
    raise SystemExit(main())
