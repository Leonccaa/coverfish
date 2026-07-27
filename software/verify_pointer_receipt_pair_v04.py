#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Offline, read-only verifier for a two-shard COVER-Fish v0.4 receipt pair.

The verifier independently rebuilds the frozen archive schedule, authenticates
the two complementary shard contracts, and runs the pinned single-receipt v0.4
wrapper against private temporary copies.  It never writes into either source
receipt and never opens the network.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "software/reconstruct_pointers_parallel_v04.py"
WRAPPER_PATH = ROOT / "software/verify_pointer_receipt_parallel_v04.py"
PRODUCER_PATH = ROOT / "software/reconstruct_pointers.py"
BASE_VERIFIER_PATH = ROOT / "software/verify_pointer_receipt.py"
PARALLEL_REQUIREMENTS_PATH = ROOT / "requirements-pointer-audit-parallel-v04.txt"

SCHEMA = "coverfish.pointer-pair-verifier.v1"
VERSION = "0.1.2"
EXPECTED_RUNNER_SHA256 = (
    "e957c795a34d17755e8954439ee3ec2e9ba3c459534dedb41189b1ec3912635f"
)
EXPECTED_WRAPPER_SHA256 = (
    "164aabe179e111df9a361d45e47d79f39bb1bc349164ca17de8f5acc7ff9d4d5"
)
EXPECTED_PRODUCER_SHA256 = (
    "14a0b11025256f22a79883d7337687cfd7b02af8646fb38e6351209cc7bca522"
)
EXPECTED_BASE_VERIFIER_SHA256 = (
    "714f4eab6513e112095bf3c93121349284b30dba7429c163abd63e29e898527d"
)
EXPECTED_PARALLEL_REQUIREMENTS_SHA256 = (
    "b813c6b1e86fbcbb3b55ba05e9662adb44c6488e18343adc2ce40c784b53e60c"
)
EXPECTED_POLICY_SHA256 = (
    "2987dffde63b8a0fa1e4a795142267d964d6644bd23d22c35b76b337112687a9"
)
EXPECTED_BINDINGS_SHA256 = (
    "917bd5603b33aadb5e1b23522592109cbafc2108ba6eaa67b18e4574148a00d7"
)
EXPECTED_REVISION = "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8"
EXPECTED_ARCHIVE_ROWS = 42_387
EXPECTED_ACTIVE_ROWS = 41_945
EXPECTED_RETIRED_ROWS = 442
EXPECTED_SHARD_ROWS = (21_195, 21_192)
EXPECTED_SHARD_ACTIVE_ROWS = (20_973, 20_972)
EXPECTED_SHARD_RETIRED_ROWS = (222, 220)
EXPECTED_S1_ROWS = (17_044, 17_044)
EXPECTED_FULL_SCHEDULE_SHA256 = (
    "7ace10d7a558485a3bc26f6a0a2dd5295c84d58cca3fead03312317995d3ebee"
)
EXPECTED_FROZEN_SHARD_SHA256 = (
    "bad013f343deae2580c2b054a4f4bbfa07ebd555f95acb9316c640d9260cff4c",
    "fd11cc2fa779fb06696acee476ba0e55f15ed1c44cea4284ee4c9a61fe5eb8b4",
)
SCHEDULE_DIGEST_FIELDS = (
    "ordinal",
    "source_ordinal",
    "record_id",
    "lane_key",
    "lane_ordinal",
)
FROZEN_SHARD_DIGEST_FIELDS = (
    "shard_ordinal",
    "global_ordinal",
    "source_ordinal",
    "record_id",
    "lane_key",
    "lane_ordinal",
)
EXPECTED_SCHEDULER = {
    "schema": "coverfish.pointer-parallel-scheduler.v2",
    "algorithm": "stable_round_robin_primary_source_lane_sharded_v2",
    "lane_key": "host_policy.rate_group_or_primary_source_host",
    "lane_order": "first_appearance_in_frozen_candidate_order",
    "lane_internal_order": "frozen_candidate_order",
    "shard_selection": (
        "frozen_candidate_schedule_lane_ordinal_modulo_shard_count"
    ),
    "eligibility_order": "after_fixed_shard_membership",
    "max_rows_semantics": "after_shard_membership_and_current_eligibility",
    "multi_shard_component_selection": "forbidden",
}


class PairVerificationError(Exception):
    """Fail-closed verifier error with a stable, non-sensitive code."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PairVerificationError("PAIR_JSON_INVALID") from exc
    if not isinstance(value, dict):
        raise PairVerificationError("PAIR_JSON_INVALID")
    return value


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise PairVerificationError("PAIR_MODULE_IMPORT_FAILED")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def expected_sharding(index: int) -> dict[str, Any]:
    return {
        "schema": "coverfish.pointer-parallel-sharding.v1",
        "shard_count": 2,
        "shard_index": index,
        "selection": "lane_ordinal_modulo_shard_count_equals_shard_index",
        "selected_order": "ascending_global_schedule_ordinal",
        "max_rows_order": "after_fixed_membership_and_current_eligibility",
    }


def add_check(
    checks: list[dict[str, Any]],
    check_id: str,
    status: str,
    observed: object,
    expected: object,
) -> None:
    if status not in {"PASS", "FAIL", "PENDING"}:
        raise ValueError("invalid check status")
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "observed": observed,
            "expected": expected,
        }
    )


def top_status(checks: Sequence[dict[str, Any]]) -> str:
    statuses = {str(check.get("status", "FAIL")) for check in checks}
    if "FAIL" in statuses:
        return "FAIL"
    if "PENDING" in statuses:
        return "PENDING"
    return "PASS"


def safe_receipt_directory(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(metadata.st_mode)
        and not path.is_symlink()
        and metadata.st_uid == os.getuid()
        and not metadata.st_mode & 0o022
    )


def receipt_tree_digest(path: Path) -> str:
    rows: list[list[object]] = []
    for child in sorted(path.iterdir(), key=lambda item: item.name):
        metadata = child.lstat()
        if (
            child.is_symlink()
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            raise PairVerificationError("PAIR_RECEIPT_TREE_UNSAFE")
        if stat.S_ISREG(metadata.st_mode):
            rows.append([child.name, metadata.st_size, file_sha256(child)])
        elif stat.S_ISDIR(metadata.st_mode):
            rows.append([child.name, "directory"])
        else:
            raise PairVerificationError("PAIR_RECEIPT_TREE_UNSAFE")
    return canonical_sha256(rows)


def read_tsv(path: Path) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    try:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source, delimiter="\t")
            fields = tuple(reader.fieldnames or ())
            rows = list(reader)
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise PairVerificationError("PAIR_TSV_INVALID") from exc
    if not fields or any(set(row) != set(fields) for row in rows):
        raise PairVerificationError("PAIR_TSV_INVALID")
    return fields, rows


def independent_schedule(
    source_rows: Sequence[dict[str, str]], policy: dict[str, Any]
) -> list[dict[str, Any]]:
    hosts = policy.get("hosts")
    if not isinstance(hosts, dict):
        raise PairVerificationError("PAIR_POLICY_INVALID")
    lanes: dict[str, deque[tuple[int, int, dict[str, str]]]] = {}
    seen: set[str] = set()
    for source_ordinal, row in enumerate(source_rows):
        record_id = row.get("record_id", "")
        host = row.get("source_host", "").lower().rstrip(".")
        config = hosts.get(host)
        if not record_id or record_id in seen or not isinstance(config, dict):
            raise PairVerificationError("PAIR_SOURCE_SCHEDULE_INVALID")
        seen.add(record_id)
        rate_group = str(config.get("rate_group", "")).strip()
        lane_key = f"rate_group:{rate_group}" if rate_group else f"host:{host}"
        lane = lanes.setdefault(lane_key, deque())
        lane.append((len(lane), source_ordinal, row))
    active = list(lanes)
    schedule: list[dict[str, Any]] = []
    while active:
        remaining: list[str] = []
        for lane_key in active:
            lane_ordinal, source_ordinal, row = lanes[lane_key].popleft()
            schedule.append(
                {
                    "ordinal": len(schedule),
                    "source_ordinal": source_ordinal,
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


def schedule_digest(
    entries: Sequence[dict[str, Any]], fields: Sequence[str]
) -> str:
    return canonical_sha256(
        [[entry.get(field) for field in fields] for entry in entries]
    )


def split_schedule(
    full_schedule: Sequence[dict[str, Any]], shard_index: int
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for entry in full_schedule:
        if int(entry["lane_ordinal"]) % 2 != shard_index:
            continue
        selected.append(
            {
                **entry,
                "shard_ordinal": len(selected),
                "global_ordinal": int(entry["ordinal"]),
            }
        )
    return selected


def load_frozen_partition(
    runner: Any, source_root: Path, bindings_path: Path, policy_path: Path
) -> dict[str, Any]:
    if file_sha256(bindings_path) != EXPECTED_BINDINGS_SHA256:
        raise PairVerificationError("PAIR_BINDINGS_SHA256_MISMATCH")
    if file_sha256(policy_path) != EXPECTED_POLICY_SHA256:
        raise PairVerificationError("PAIR_POLICY_SHA256_MISMATCH")
    try:
        bindings, source_rows = runner.AUDIT.load_bound_rows(
            source_root, bindings_path
        )
        policy = runner.AUDIT.load_json(policy_path)
        runner.AUDIT.validate_host_policy(policy)
    except Exception as exc:
        raise PairVerificationError("PAIR_FROZEN_INPUT_BINDING_FAILED") from exc
    if (
        len(source_rows) != EXPECTED_ARCHIVE_ROWS
        or bindings.get("dataset", {}).get("revision") != EXPECTED_REVISION
    ):
        raise PairVerificationError("PAIR_FROZEN_ARCHIVE_CLOSURE_FAILED")
    full = independent_schedule(source_rows, policy)
    shards = [split_schedule(full, index) for index in range(2)]
    member_sets = [
        {entry["record_id"] for entry in shard} for shard in shards
    ]
    source_by_id = {row["record_id"]: row for row in source_rows}
    full_digest = schedule_digest(full, SCHEDULE_DIGEST_FIELDS)
    shard_digests = [
        schedule_digest(shard, FROZEN_SHARD_DIGEST_FIELDS)
        for shard in shards
    ]
    active = sum(row.get("active") == "true" for row in source_rows)
    retired = len(source_rows) - active
    shard_s1 = [
        sum(entry["row"].get("component") == "S1" for entry in shard)
        for shard in shards
    ]
    shard_active = [
        sum(entry["row"].get("active") == "true" for entry in shard)
        for shard in shards
    ]
    shard_retired = [len(shard) - count for shard, count in zip(shards, shard_active, strict=True)]
    partition_ok = (
        len(full) == EXPECTED_ARCHIVE_ROWS
        and [len(shard) for shard in shards] == list(EXPECTED_SHARD_ROWS)
        and shard_active == list(EXPECTED_SHARD_ACTIVE_ROWS)
        and shard_retired == list(EXPECTED_SHARD_RETIRED_ROWS)
        and shard_s1 == list(EXPECTED_S1_ROWS)
        and member_sets[0].isdisjoint(member_sets[1])
        and member_sets[0] | member_sets[1] == set(source_by_id)
        and active == EXPECTED_ACTIVE_ROWS
        and retired == EXPECTED_RETIRED_ROWS
        and full_digest == EXPECTED_FULL_SCHEDULE_SHA256
        and tuple(shard_digests) == EXPECTED_FROZEN_SHARD_SHA256
    )
    if not partition_ok:
        raise PairVerificationError("PAIR_FIXED_PARTITION_MISMATCH")
    return {
        "bindings": bindings,
        "source_rows": source_rows,
        "source_by_id": source_by_id,
        "full_schedule": full,
        "shards": shards,
        "member_sets": member_sets,
        "full_digest": full_digest,
        "scope_ids_sha256": runner.AUDIT.scope_ids_sha256(source_rows),
        "shard_digests": shard_digests,
        "active": active,
        "retired": retired,
        "shard_s1": shard_s1,
        "shard_active": shard_active,
        "shard_retired": shard_retired,
    }


def inspect_receipt_static(
    audit_dir: Path,
    expected_index: int,
    partition: dict[str, Any],
    bindings_path: Path,
    policy_path: Path,
) -> dict[str, Any]:
    if not safe_receipt_directory(audit_dir):
        raise PairVerificationError("PAIR_RECEIPT_DIRECTORY_UNSAFE")
    contract = load_json(audit_dir / "audit-contract.json")
    profile_path = audit_dir / "parallel-execution-profile-v04.json"
    profile = load_json(profile_path)
    sharding = expected_sharding(expected_index)
    contract_ok = (
        contract.get("schema") == "coverfish.pointer-audit-contract.v3"
        and contract.get("scope") == "archive"
        and contract.get("scope_rows") == EXPECTED_ARCHIVE_ROWS
        and contract.get("scope_ids_sha256") == partition["scope_ids_sha256"]
        and contract.get("dataset") == partition["bindings"].get("dataset")
        and contract.get("bindings_sha256") == file_sha256(bindings_path)
        and contract.get("policy_sha256") == file_sha256(policy_path)
        and contract.get("tool_sha256") == EXPECTED_PRODUCER_SHA256
        and contract.get("parallel_requirements_sha256")
        == EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        and contract.get("parallel_scheduler") == EXPECTED_SCHEDULER
        and contract.get("parallel_sharding") == sharding
        and contract.get("execution_profile_schema")
        == "coverfish.pointer-parallel-execution-profile.v2"
        and contract.get("execution_profile_sha256") == file_sha256(profile_path)
        and contract.get("bytes_retained") is False
        and contract.get("gpu_used") is False
    )
    profile_ok = (
        profile.get("schema") == "coverfish.pointer-parallel-execution-profile.v2"
        and profile.get("runner_version") == "0.4.0"
        and profile.get("runner_sha256") == EXPECTED_RUNNER_SHA256
        and profile.get("producer_version") == "0.2.0"
        and profile.get("producer_sha256") == EXPECTED_PRODUCER_SHA256
        and profile.get("scheduler") == EXPECTED_SCHEDULER
        and profile.get("sharding") == sharding
        and profile.get("parallel_requirements_sha256")
        == EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        and profile.get("parallel_dependencies", {}).get("expected_lock_sha256")
        == EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        and profile.get("parallel_dependencies", {}).get("lock_sha256")
        == EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        and profile.get("parallel_dependencies", {}).get("ready") is True
        and profile.get("bytes_retained") is False
        and profile.get("gpu_used") is False
    )
    if not contract_ok or not profile_ok:
        raise PairVerificationError("PAIR_RECEIPT_STATIC_BINDING_FAILED")
    metadata_paths = sorted(audit_dir.glob("run-metadata-*.json"))
    metadata = [load_json(path) for path in metadata_paths]
    metadata_ok = bool(metadata)
    seen_windows: set[str] = set()
    seen_invocations: set[str] = set()
    for metadata_path, item in zip(metadata_paths, metadata, strict=True):
        window_id = str(item.get("window_id", ""))
        invocation_id = str(item.get("invocation_id", ""))
        identity_ok = (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", window_id)
            is not None
            and re.fullmatch(r"[0-9a-f]{16}", invocation_id) is not None
            and window_id not in seen_windows
            and invocation_id not in seen_invocations
            and metadata_path.name
            == f"run-metadata-{window_id}-{invocation_id}.json"
        )
        seen_windows.add(window_id)
        seen_invocations.add(invocation_id)
        metadata_ok &= (
            identity_ok
            and item.get("schema") == "coverfish.pointer-audit-run.v1"
            and item.get("scope") == "archive"
            and item.get("components") == []
            and item.get("parallel_scheduler") == EXPECTED_SCHEDULER
            and item.get("parallel_sharding") == sharding
            and item.get("parallel_frozen_candidate_rows")
            == EXPECTED_ARCHIVE_ROWS
            and item.get("parallel_frozen_shard_rows")
            == EXPECTED_SHARD_ROWS[expected_index]
            and item.get("parallel_full_schedule_sha256")
            == EXPECTED_FULL_SCHEDULE_SHA256
            and item.get("parallel_frozen_shard_schedule_sha256")
            == EXPECTED_FROZEN_SHARD_SHA256[expected_index]
            and item.get("policy_sha256") == EXPECTED_POLICY_SHA256
            and item.get("bindings_sha256") == EXPECTED_BINDINGS_SHA256
            and item.get("bytes_retained") is False
            and item.get("gpu_used") is False
        )
    if not metadata_ok:
        raise PairVerificationError("PAIR_RECEIPT_METADATA_BINDING_FAILED")
    statuses = [str(item.get("status", "")) for item in metadata]
    accepted_statuses = {
        "RUNNING",
        "COMPLETE",
        "ERROR",
        "INTERRUPTED",
        "ABANDONED_BY_RESUME",
    }
    if any(status not in accepted_statuses for status in statuses):
        raise PairVerificationError("PAIR_RECEIPT_METADATA_STATUS_INVALID")
    running = any(status == "RUNNING" for status in statuses)
    work_present = (audit_dir / ".parallel-work-v04").exists()
    expected_execution_names = {
        f"parallel-execution-{item['window_id']}-{item['invocation_id']}.json"
        for item in metadata
    }
    execution_summaries_present = all(
        (audit_dir / name).is_file() for name in expected_execution_names
    )
    terminal_derived_present = all(
        (audit_dir / name).is_file()
        for name in ("pointer-health.tsv", "pointer-health-summary.json")
    )
    terminal = (
        not running
        and not work_present
        and execution_summaries_present
        and terminal_derived_present
    )
    profile_common = dict(profile)
    profile_common.pop("sharding", None)
    contract_common = dict(contract)
    contract_common.pop("parallel_sharding", None)
    contract_common.pop("execution_profile_sha256", None)
    return {
        "audit_dir": audit_dir,
        "shard_index": expected_index,
        "contract": contract,
        "contract_common": contract_common,
        "profile": profile,
        "profile_common": profile_common,
        "metadata": metadata,
        "terminal": terminal,
        "running": running,
        "work_present": work_present,
        "execution_summaries_present": execution_summaries_present,
        "terminal_derived_present": terminal_derived_present,
        "tree_digest_before": receipt_tree_digest(audit_dir) if terminal else None,
    }


def _write_network_guard(directory: Path) -> None:
    guard = directory / "sitecustomize.py"
    guard.write_text(
        "import socket\n"
        "def _blocked(*args, **kwargs):\n"
        "    raise RuntimeError('OFFLINE_VERIFIER_NETWORK_BLOCKED')\n"
        "socket.create_connection = _blocked\n"
        "_original_socket = socket.socket\n"
        "class _OfflineSocket(_original_socket):\n"
        "    def connect(self, *args, **kwargs):\n"
        "        return _blocked(*args, **kwargs)\n"
        "    def connect_ex(self, *args, **kwargs):\n"
        "        return _blocked(*args, **kwargs)\n"
        "socket.socket = _OfflineSocket\n",
        encoding="utf-8",
    )


def copy_receipt_surface(source: Path, target: Path) -> None:
    target.mkdir(mode=0o700)
    for child in sorted(source.iterdir(), key=lambda item: item.name):
        metadata = child.lstat()
        if (
            child.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            raise PairVerificationError("PAIR_TERMINAL_RECEIPT_TREE_INVALID")
        destination = target / child.name
        shutil.copyfile(child, destination)
        os.chmod(destination, metadata.st_mode & 0o777)


def run_single_wrapper(
    receipt: dict[str, Any],
    source_root: Path,
    bindings_path: Path,
    policy_path: Path,
    member_ids: set[str],
    source_by_id: dict[str, dict[str, str]],
) -> dict[str, Any]:
    if not receipt["terminal"]:
        return {"status": "PENDING", "checks": None, "failed": None}
    with tempfile.TemporaryDirectory(prefix="coverfish-pair-wrapper-") as temporary:
        root = Path(temporary)
        os.chmod(root, 0o700)
        guard_dir = root / "offline-guard"
        guard_dir.mkdir(mode=0o700)
        _write_network_guard(guard_dir)
        clone = root / "receipt"
        copy_receipt_surface(receipt["audit_dir"], clone)
        snapshot_sha256 = receipt_tree_digest(clone)
        if snapshot_sha256 != receipt.get("tree_digest_before"):
            raise PairVerificationError("PAIR_RECEIPT_SNAPSHOT_CHANGED")
        command = [
            sys.executable,
            str(WRAPPER_PATH),
            "--source-root",
            str(source_root),
            "--bindings",
            str(bindings_path),
            "--policy",
            str(policy_path),
            "--scope",
            "archive",
            "--audit-dir",
            str(clone),
            "--output",
            str(clone / "independent-checks.tsv"),
        ]
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(guard_dir)
        environment["CUDA_VISIBLE_DEVICES"] = ""
        environment.pop("HTTP_PROXY", None)
        environment.pop("HTTPS_PROXY", None)
        environment.pop("ALL_PROXY", None)
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=900,
            check=False,
            env=environment,
        )
        try:
            payload = json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            raise PairVerificationError("PAIR_WRAPPER_OUTPUT_INVALID") from exc
        if not isinstance(payload, dict):
            raise PairVerificationError("PAIR_WRAPPER_OUTPUT_INVALID")
        _, check_rows = read_tsv(clone / "independent-checks.tsv")
        passed = (
            result.returncode == 0
            and payload.get("schema") == "coverfish.pointer-parallel-verifier.v2"
            and payload.get("tool_version") == "0.4.2"
            and payload.get("status") == "PASS"
            and payload.get("failed") == 0
            and payload.get("network_accessed") is False
            and payload.get("checks") == len(check_rows)
            and check_rows
            and all(row.get("status") == "PASS" for row in check_rows)
        )
        wrapper_result = {
            "status": "PASS" if passed else "FAIL",
            "checks": payload.get("checks"),
            "failed": payload.get("failed"),
            "network_accessed": payload.get("network_accessed"),
            "snapshot_sha256": snapshot_sha256,
        }
        if passed:
            wrapper_result["_aggregate"] = aggregate_terminal_receipt(
                clone, member_ids, source_by_id
            )
        return wrapper_result


def integer_or_zero(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0


def aggregate_terminal_receipt(
    audit_dir: Path,
    member_ids: set[str],
    source_by_id: dict[str, dict[str, str]],
) -> dict[str, Any]:
    _, attempts = read_tsv(audit_dir / "pointer-health-attempts.tsv")
    _, completions = read_tsv(audit_dir / "record-completions.tsv")
    _, dispositions = read_tsv(audit_dir / "attempt-dispositions.tsv")
    _, robots = read_tsv(audit_dir / "robots.tsv")
    _, health = read_tsv(audit_dir / "pointer-health.tsv")
    health_by_id = {row.get("record_id", ""): row for row in health}
    if len(health_by_id) != len(health) or not member_ids <= set(health_by_id):
        raise PairVerificationError("PAIR_HEALTH_MEMBERSHIP_INVALID")
    if any(row.get("record_id", "") not in member_ids for row in attempts):
        raise PairVerificationError("PAIR_ATTEMPT_OUTSIDE_SHARD")
    if any(row.get("record_id", "") not in member_ids for row in completions):
        raise PairVerificationError("PAIR_COMPLETION_OUTSIDE_SHARD")
    completion_ranges: dict[tuple[str, str, str], list[tuple[int, int]]] = defaultdict(list)
    for row in completions:
        first = integer_or_zero(row.get("first_attempt_index", ""))
        last = integer_or_zero(row.get("last_attempt_index", ""))
        completion_ranges[
            (
                row.get("record_id", ""),
                row.get("window_id", ""),
                row.get("invocation_id", ""),
            )
        ].append((first, last))
    committed_attempts = []
    for row in attempts:
        key = (
            row.get("record_id", ""),
            row.get("window_id", ""),
            row.get("invocation_id", ""),
        )
        index = integer_or_zero(row.get("attempt_index", ""))
        if any(first <= index <= last for first, last in completion_ranges.get(key, [])):
            committed_attempts.append(row)
    selected_health = [health_by_id[record_id] for record_id in sorted(member_ids)]
    outcomes = Counter(row.get("final_class", "") for row in selected_health)
    active_outcomes = Counter(
        row.get("final_class", "")
        for row in selected_health
        if source_by_id[row["record_id"]].get("active") == "true"
    )
    completed_ids = {row.get("record_id", "") for row in completions}
    active_completed = sum(
        source_by_id[record_id].get("active") == "true"
        for record_id in completed_ids
    )
    physical_bytes = sum(integer_or_zero(row.get("actual_bytes", "")) for row in attempts)
    committed_bytes = sum(
        integer_or_zero(row.get("actual_bytes", ""))
        for row in committed_attempts
    )
    attempt_safety_rows = sum(
        row.get("transport_status") == "safety_block"
        or row.get("identity_class") == "safety_review_required"
        for row in attempts
    )
    robots_safety_rows = sum(
        row.get("fetch_status") == "safety_block" for row in robots
    )
    return {
        "attempt_ids": [row.get("attempt_id", "") for row in attempts],
        "completion_ids": [row.get("completion_id", "") for row in completions],
        "completed_record_ids": completed_ids,
        "outcomes": outcomes,
        "active_outcomes": active_outcomes,
        "finality_statuses": Counter(
            row.get("finality_status", "") for row in selected_health
        ),
        "physical_attempt_rows": len(attempts),
        "physical_attempt_bytes": physical_bytes,
        "physical_attempt_by_transport_status": Counter(
            row.get("transport_status", "") for row in attempts
        ),
        "physical_attempt_by_identity_class": Counter(
            row.get("identity_class", "") for row in attempts
        ),
        "committed_attempt_rows": len(committed_attempts),
        "committed_attempt_bytes": committed_bytes,
        "completion_rows": len(completions),
        "abandoned_attempt_rows": len(dispositions),
        "active_completed_rows": active_completed,
        "retired_completed_rows": len(completed_ids) - active_completed,
        "robots_rows": len(robots),
        "attempt_safety_review_rows": attempt_safety_rows,
        "robots_safety_review_rows": robots_safety_rows,
    }


def counters_sum(items: Iterable[Counter[str]]) -> dict[str, int]:
    total: Counter[str] = Counter()
    for item in items:
        total.update(item)
    return dict(sorted(total.items()))


def scientific_status(
    outcome_counts: dict[str, int],
    finality_counts: dict[str, int],
    coverage_complete: bool,
    physical_safety_review_rows: int = 0,
) -> str:
    if (
        physical_safety_review_rows
        or outcome_counts.get("safety_review_required", 0)
    ):
        return "FAIL"
    if not coverage_complete:
        return "PENDING"
    satisfied = {"not_applicable_exact", "satisfied"}
    if any(key not in satisfied and count for key, count in finality_counts.items()):
        return "PENDING"
    return "PASS"


def _common_contracts_equal(receipts: Sequence[dict[str, Any]]) -> bool:
    return (
        len(receipts) == 2
        and receipts[0]["contract_common"] == receipts[1]["contract_common"]
        and receipts[0]["profile_common"] == receipts[1]["profile_common"]
    )


def build_pair_report(args: argparse.Namespace) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    pin_observed = {
        "runner_sha256": file_sha256(RUNNER_PATH),
        "wrapper_sha256": file_sha256(WRAPPER_PATH),
        "producer_sha256": file_sha256(PRODUCER_PATH),
        "base_verifier_sha256": file_sha256(BASE_VERIFIER_PATH),
        "parallel_requirements_sha256": file_sha256(PARALLEL_REQUIREMENTS_PATH),
    }
    pin_expected = {
        "runner_sha256": EXPECTED_RUNNER_SHA256,
        "wrapper_sha256": EXPECTED_WRAPPER_SHA256,
        "producer_sha256": EXPECTED_PRODUCER_SHA256,
        "base_verifier_sha256": EXPECTED_BASE_VERIFIER_SHA256,
        "parallel_requirements_sha256": EXPECTED_PARALLEL_REQUIREMENTS_SHA256,
    }
    pins_ok = pin_observed == pin_expected
    add_check(checks, "pair_tool_bindings_exact", "PASS" if pins_ok else "FAIL", pin_observed, pin_expected)
    if not pins_ok:
        return minimal_report(checks, "PAIR_TOOL_BINDING_MISMATCH")
    runner = load_module("coverfish_pair_v04_runner", RUNNER_PATH)
    partition = load_frozen_partition(
        runner, args.source_root, args.bindings, args.policy
    )
    partition_observed = {
        "archive_rows": len(partition["source_rows"]),
        "active_rows": partition["active"],
        "retired_rows": partition["retired"],
        "full_schedule_sha256": partition["full_digest"],
        "shard_rows": [len(shard) for shard in partition["shards"]],
        "shard_active_rows": partition["shard_active"],
        "shard_retired_rows": partition["shard_retired"],
        "shard_schedule_sha256": partition["shard_digests"],
        "s1_rows": partition["shard_s1"],
        "intersection_rows": len(
            partition["member_sets"][0] & partition["member_sets"][1]
        ),
        "union_rows": len(
            partition["member_sets"][0] | partition["member_sets"][1]
        ),
    }
    partition_expected = {
        "archive_rows": EXPECTED_ARCHIVE_ROWS,
        "active_rows": EXPECTED_ACTIVE_ROWS,
        "retired_rows": EXPECTED_RETIRED_ROWS,
        "full_schedule_sha256": EXPECTED_FULL_SCHEDULE_SHA256,
        "shard_rows": list(EXPECTED_SHARD_ROWS),
        "shard_active_rows": list(EXPECTED_SHARD_ACTIVE_ROWS),
        "shard_retired_rows": list(EXPECTED_SHARD_RETIRED_ROWS),
        "shard_schedule_sha256": list(EXPECTED_FROZEN_SHARD_SHA256),
        "s1_rows": list(EXPECTED_S1_ROWS),
        "intersection_rows": 0,
        "union_rows": EXPECTED_ARCHIVE_ROWS,
    }
    add_check(
        checks,
        "pair_fixed_membership_closure",
        "PASS",
        partition_observed,
        partition_expected,
    )
    raw_receipts = [
        inspect_receipt_static(
            args.audit_dir_a, 0, partition, args.bindings, args.policy
        ),
        inspect_receipt_static(
            args.audit_dir_b, 1, partition, args.bindings, args.policy
        ),
    ]
    common_ok = _common_contracts_equal(raw_receipts)
    add_check(
        checks,
        "pair_common_contract_and_profile",
        "PASS" if common_ok else "FAIL",
        common_ok,
        True,
    )
    terminal_states = [bool(receipt["terminal"]) for receipt in raw_receipts]
    add_check(
        checks,
        "pair_receipts_terminal",
        "PASS" if all(terminal_states) else "PENDING",
        terminal_states,
        [True, True],
    )
    wrappers: list[dict[str, Any]] = []
    terminal_aggregates: list[dict[str, Any] | None] = []
    for receipt in raw_receipts:
        wrapper_with_private = run_single_wrapper(
            receipt,
            args.source_root,
            args.bindings,
            args.policy,
            partition["member_sets"][receipt["shard_index"]],
            partition["source_by_id"],
        )
        aggregate = wrapper_with_private.pop("_aggregate", None)
        wrapper = wrapper_with_private
        wrappers.append(wrapper)
        terminal_aggregates.append(aggregate)
        add_check(
            checks,
            f"pair_shard_{receipt['shard_index']}_wrapper",
            wrapper["status"],
            wrapper,
            {"status": "PASS", "failed": 0, "network_accessed": False},
        )
    dynamic_ready = all(item is not None for item in terminal_aggregates)
    if dynamic_ready:
        aggregates = [item for item in terminal_aggregates if item is not None]
        attempt_ids = [
            attempt_id
            for item in aggregates
            for attempt_id in item["attempt_ids"]
        ]
        completion_ids = [
            completion_id
            for item in aggregates
            for completion_id in item["completion_ids"]
        ]
        completed_sets = [item["completed_record_ids"] for item in aggregates]
        attempt_unique = len(attempt_ids) == len(set(attempt_ids))
        completion_unique = len(completion_ids) == len(set(completion_ids))
        completed_disjoint = completed_sets[0].isdisjoint(completed_sets[1])
        completion_union = completed_sets[0] | completed_sets[1]
        coverage_complete = (
            completed_disjoint
            and completion_union
            == partition["member_sets"][0] | partition["member_sets"][1]
            and completed_sets[0] == partition["member_sets"][0]
            and completed_sets[1] == partition["member_sets"][1]
        )
        add_check(
            checks,
            "pair_attempt_and_completion_ids_unique",
            "PASS" if attempt_unique and completion_unique else "FAIL",
            {
                "attempt_rows": len(attempt_ids),
                "attempt_unique_rows": len(set(attempt_ids)),
                "completion_rows": len(completion_ids),
                "completion_unique_rows": len(set(completion_ids)),
            },
            {"attempt_ids_unique": True, "completion_ids_unique": True},
        )
        coverage_observed = {
            "shard_completed_record_rows": [len(values) for values in completed_sets],
            "intersection_rows": len(completed_sets[0] & completed_sets[1]),
            "union_rows": len(completion_union),
            "active_completed_rows": sum(item["active_completed_rows"] for item in aggregates),
            "retired_completed_rows": sum(item["retired_completed_rows"] for item in aggregates),
        }
        add_check(
            checks,
            "pair_terminal_completion_membership",
            "PASS" if coverage_complete else "PENDING",
            coverage_observed,
            {
                "shard_completed_record_rows": list(EXPECTED_SHARD_ROWS),
                "intersection_rows": 0,
                "union_rows": EXPECTED_ARCHIVE_ROWS,
                "active_completed_rows": EXPECTED_ACTIVE_ROWS,
                "retired_completed_rows": EXPECTED_RETIRED_ROWS,
            },
        )
        outcomes = counters_sum(item["outcomes"] for item in aggregates)
        active_outcomes = counters_sum(item["active_outcomes"] for item in aggregates)
        finality = counters_sum(item["finality_statuses"] for item in aggregates)
        aggregate = {
            "outcomes": outcomes,
            "active_outcomes": active_outcomes,
            "finality_statuses": finality,
            "attempts": {
                "physical_rows": sum(item["physical_attempt_rows"] for item in aggregates),
                "physical_bytes": sum(item["physical_attempt_bytes"] for item in aggregates),
                "committed_rows": sum(item["committed_attempt_rows"] for item in aggregates),
                "committed_bytes": sum(item["committed_attempt_bytes"] for item in aggregates),
                "abandoned_rows": sum(item["abandoned_attempt_rows"] for item in aggregates),
                "by_transport_status": counters_sum(
                    item["physical_attempt_by_transport_status"]
                    for item in aggregates
                ),
                "by_identity_class": counters_sum(
                    item["physical_attempt_by_identity_class"]
                    for item in aggregates
                ),
            },
            "robots_rows": sum(item["robots_rows"] for item in aggregates),
            "safety_review": {
                "attempt_rows": sum(
                    item["attempt_safety_review_rows"] for item in aggregates
                ),
                "robots_rows": sum(
                    item["robots_safety_review_rows"] for item in aggregates
                ),
            },
            "completion_rows": sum(item["completion_rows"] for item in aggregates),
            "completed_record_rows": len(completion_union),
            "active_completed_rows": sum(item["active_completed_rows"] for item in aggregates),
            "retired_completed_rows": sum(item["retired_completed_rows"] for item in aggregates),
        }
        aggregate_closure = (
            sum(outcomes.values()) == EXPECTED_ARCHIVE_ROWS
            and sum(active_outcomes.values()) == EXPECTED_ACTIVE_ROWS
            and aggregate["attempts"]["committed_rows"]
            <= aggregate["attempts"]["physical_rows"]
            and aggregate["active_completed_rows"]
            + aggregate["retired_completed_rows"]
            == aggregate["completed_record_rows"]
            and aggregate["completion_rows"]
            >= aggregate["completed_record_rows"]
        )
        add_check(
            checks,
            "pair_aggregate_count_closure",
            "PASS" if aggregate_closure else "FAIL",
            {
                "outcome_rows": sum(outcomes.values()),
                "active_outcome_rows": sum(active_outcomes.values()),
                "completed_record_rows": aggregate["completed_record_rows"],
                "active_completed_rows": aggregate["active_completed_rows"],
                "retired_completed_rows": aggregate["retired_completed_rows"],
                "physical_attempt_rows": aggregate["attempts"]["physical_rows"],
                "committed_attempt_rows": aggregate["attempts"]["committed_rows"],
            },
            {
                "outcome_rows": EXPECTED_ARCHIVE_ROWS,
                "active_outcome_rows": EXPECTED_ACTIVE_ROWS,
                "active_plus_retired_equals_completed": True,
                "committed_attempts_lte_physical_attempts": True,
            },
        )
        physical_safety = (
            aggregate["safety_review"]["attempt_rows"]
            + aggregate["safety_review"]["robots_rows"]
        )
        science = scientific_status(
            outcomes, finality, coverage_complete, physical_safety
        )
    else:
        coverage_complete = False
        aggregate = {
            "outcomes": {},
            "active_outcomes": {},
            "finality_statuses": {},
            "attempts": None,
            "robots_rows": None,
            "safety_review": None,
            "completion_rows": None,
            "completed_record_rows": None,
            "active_completed_rows": None,
            "retired_completed_rows": None,
        }
        science = "PENDING"
        add_check(
            checks,
            "pair_terminal_dynamic_aggregation",
            "PENDING",
            {"terminal_receipts": sum(terminal_states)},
            {"terminal_receipts": 2},
        )
    unchanged: list[bool | None] = []
    for receipt in raw_receipts:
        if receipt["terminal"]:
            unchanged.append(
                receipt_tree_digest(receipt["audit_dir"])
                == receipt["tree_digest_before"]
            )
        else:
            unchanged.append(None)
    unchanged_status = (
        "FAIL" if any(value is False for value in unchanged)
        else "PASS" if all(value is True for value in unchanged)
        else "PENDING"
    )
    add_check(
        checks,
        "pair_original_receipts_unchanged",
        unchanged_status,
        unchanged,
        [True, True],
    )
    receipt_reports = []
    for receipt, wrapper, item in zip(raw_receipts, wrappers, terminal_aggregates, strict=True):
        receipt_reports.append(
            {
                "shard_index": receipt["shard_index"],
                "terminal": receipt["terminal"],
                "wrapper_status": wrapper["status"],
                "wrapper_checks": wrapper["checks"],
                "invocations": len(receipt["metadata"]),
                "invocation_statuses": dict(
                    sorted(Counter(str(row.get("status", "")) for row in receipt["metadata"]).items())
                ),
                "frozen_rows": EXPECTED_SHARD_ROWS[receipt["shard_index"]],
                "s1_rows": EXPECTED_S1_ROWS[receipt["shard_index"]],
                "completed_record_rows": (
                    len(item["completed_record_ids"]) if item is not None else None
                ),
            }
        )
    return {
        "schema": SCHEMA,
        "tool_version": VERSION,
        "status": top_status(checks),
        "scientific_status": science,
        "checks": checks,
        "tool_bindings": {
            "pair_verifier_sha256": file_sha256(Path(__file__)),
            **pin_observed,
        },
        "input_bindings": {
            "dataset_revision": EXPECTED_REVISION,
            "bindings_sha256": EXPECTED_BINDINGS_SHA256,
            "policy_sha256": EXPECTED_POLICY_SHA256,
        },
        "partition": partition_observed,
        "receipts": receipt_reports,
        "aggregate": aggregate,
        "coverage_complete": coverage_complete,
        "network_accessed": False,
        "bytes_retained": False,
        "gpu_used": False,
        "frozen_tier_mutated": False,
        "original_receipt_write_operations": 0,
        "exit_policy": {"PASS": 0, "PENDING": 0, "FAIL": 6},
    }


def minimal_report(
    checks: list[dict[str, Any]], error: str
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "tool_version": VERSION,
        "status": top_status(checks),
        "scientific_status": "PENDING",
        "error": error,
        "checks": checks,
        "tool_bindings": {
            "pair_verifier_sha256": file_sha256(Path(__file__)),
        },
        "network_accessed": False,
        "bytes_retained": False,
        "gpu_used": False,
        "frozen_tier_mutated": False,
        "original_receipt_write_operations": 0,
        "exit_policy": {"PASS": 0, "PENDING": 0, "FAIL": 6},
    }


def output_location_valid(output: Path, audit_dirs: Sequence[Path]) -> bool:
    try:
        resolved = output.resolve()
        for audit_dir in audit_dirs:
            receipt = audit_dir.resolve(strict=True)
            if resolved == receipt or receipt in resolved.parents:
                return False
        return True
    except OSError:
        return False


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as target:
            json.dump(payload, target, ensure_ascii=False, sort_keys=True, indent=2)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify two complementary COVER-Fish v0.4 receipts offline.",
        epilog=(
            "Exit 0 means integrity has not failed: inspect status for PASS or "
            "PENDING. Exit 6 means FAIL."
        ),
    )
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--bindings", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument(
        "--shard-0-dir", "--audit-dir-a", dest="audit_dir_a", type=Path,
        required=True,
    )
    parser.add_argument(
        "--shard-1-dir", "--audit-dir-b", dest="audit_dir_b", type=Path,
        required=True,
    )
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_approved = args.output is None
    try:
        if args.output is not None:
            output_approved = output_location_valid(
                args.output, (args.audit_dir_a, args.audit_dir_b)
            )
            if not output_approved:
                raise PairVerificationError("PAIR_OUTPUT_LOCATION_INVALID")
        report = build_pair_report(args)
    except PairVerificationError as exc:
        checks: list[dict[str, Any]] = []
        add_check(checks, "pair_verifier_runtime", "FAIL", exc.code, "no error")
        report = minimal_report(checks, exc.code)
    except Exception:
        checks = []
        add_check(
            checks,
            "pair_verifier_runtime",
            "FAIL",
            "PAIR_UNEXPECTED_RUNTIME_ERROR",
            "no error",
        )
        report = minimal_report(checks, "PAIR_UNEXPECTED_RUNTIME_ERROR")
    if args.output is not None and output_approved:
        try:
            atomic_write_json(args.output, report)
        except Exception:
            checks = []
            add_check(
                checks,
                "pair_output_write",
                "FAIL",
                "PAIR_OUTPUT_WRITE_FAILED",
                "write completed",
            )
            report = minimal_report(checks, "PAIR_OUTPUT_WRITE_FAILED")
    print(
        json.dumps(
            report, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    )
    return 6 if report["status"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
