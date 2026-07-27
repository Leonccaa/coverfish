#!/usr/bin/env python3
"""Deterministically sharded process executor for the frozen pointer producer.

This module does not fork or modify the scientific producer.  Worker processes
import the byte-pinned v0.2 producer, use its URL, transport, identity, and
route logic, and discard every response body after the producer has derived an
attempt receipt.  A coordinator is the only writer to the canonical receipt
directory.

The executor deliberately uses processes rather than threads because the
producer's request and image-decode deadlines use SIGALRM.  Request starts and
byte reservations are serialized through a separate, flock-protected v0.4 rate
state shared by every worker.  The default and maximum concurrency is five.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import email.utils
import fcntl
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import multiprocessing
import os
import re
import shutil
import stat
import sys
import time
import urllib.robotparser
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NoReturn


ROOT = Path(__file__).resolve().parents[1]
PRODUCER_PATH = ROOT / "software/reconstruct_pointers.py"
EXPECTED_PRODUCER_SHA256 = (
    "14a0b11025256f22a79883d7337687cfd7b02af8646fb38e6351209cc7bca522"
)
APPROVED_POLICY_SHA256 = (
    "2987dffde63b8a0fa1e4a795142267d964d6644bd23d22c35b76b337112687a9"
)
PARALLEL_REQUIREMENTS_PATH = (
    ROOT / "requirements-pointer-audit-parallel-v04.txt"
)
EXPECTED_PARALLEL_REQUIREMENTS_SHA256 = (
    "b813c6b1e86fbcbb3b55ba05e9662adb44c6488e18343adc2ce40c784b53e60c"
)
RUNNER_SCHEMA = "coverfish.pointer-parallel-executor.v2"
RUNNER_VERSION = "0.4.0"
MAX_INFLIGHT = 5
MAX_SHARD_COUNT = 1024
EXECUTION_PROFILE_NAME = "parallel-execution-profile-v04.json"
WORK_ROOT_NAME = ".parallel-work-v04"
SPOOL_RECOVERY_FILE = "parallel-spool-recoveries.tsv"
SPOOL_RECOVERY_FIELDS = (
    "recovery_id",
    "window_id",
    "invocation_id",
    "record_id",
    "spool_name",
    "artifact_name",
    "action",
    "detected_at_utc",
    "original_size_bytes",
    "retained_size_bytes",
    "discarded_fragment_bytes",
    "discarded_fragment_sha256",
    "requires_safety_review",
)
SPOOL_TSV_ARTIFACTS = frozenset(
    {
        "pointer-health-attempts.tsv",
        "record-completions.tsv",
        "robots.tsv",
    }
)
SPOOL_ROOT_JSON_TEMP_TARGETS = frozenset(
    {"invocation-work.json", "robots-cache.json"}
)
SPOOL_ROW_JSON_TEMP_TARGETS = frozenset(
    {"worker-result.json", "disposition-stamp.json"}
)
SPOOL_JSON_TEMP_ACTIONS = frozenset(
    {
        "promote_complete_json_temp",
        "discard_duplicate_complete_json_temp",
        "quarantine_incomplete_json_temp",
    }
)
RECOVERY_LEDGER_TAIL_SCHEMA = (
    "coverfish.pointer-parallel-recovery-ledger-tail.v1"
)
RECOVERY_LEDGER_TAIL_PREFIX = "parallel-recovery-ledger-tail-"
WORKER_EPOCH_NAME = "worker-epoch.json"
WORKER_FENCE_NAME = "worker-fenced.json"
WORKER_EPOCH_SCHEMA = "coverfish.pointer-parallel-worker-epoch.v1"
WORKER_FENCE_SCHEMA = "coverfish.pointer-parallel-worker-fence.v1"
WORKER_QUIESCENCE_TIMEOUT_SECONDS = 900.0
WORKER_LOCK_POLL_SECONDS = 0.05
SCHEDULER_SCHEMA = "coverfish.pointer-parallel-scheduler.v2"
SCHEDULER_ALGORITHM = "stable_round_robin_primary_source_lane_sharded_v2"
SHARDING_SCHEMA = "coverfish.pointer-parallel-sharding.v1"
WORK_MANIFEST_SCHEMA = "coverfish.pointer-parallel-work.v3"
SCHEDULE_DIGEST_FIELDS = (
    "ordinal",
    "source_ordinal",
    "record_id",
    "lane_key",
    "lane_ordinal",
)
SHARD_SCHEDULE_DIGEST_FIELDS = (
    "ordinal",
    "shard_ordinal",
    "global_ordinal",
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


def _scheduler_contract() -> dict[str, str]:
    return {
        "schema": SCHEDULER_SCHEMA,
        "algorithm": SCHEDULER_ALGORITHM,
        "lane_key": "host_policy.rate_group_or_primary_source_host",
        "lane_order": "first_appearance_in_frozen_candidate_order",
        "lane_internal_order": "frozen_candidate_order",
        "shard_selection": "frozen_candidate_schedule_lane_ordinal_modulo_shard_count",
        "eligibility_order": "after_fixed_shard_membership",
        "max_rows_semantics": "after_shard_membership_and_current_eligibility",
        "multi_shard_component_selection": "forbidden",
    }


def _sharding_contract(shard_count: int, shard_index: int) -> dict[str, Any]:
    return {
        "schema": SHARDING_SCHEMA,
        "shard_count": shard_count,
        "shard_index": shard_index,
        "selection": "lane_ordinal_modulo_shard_count_equals_shard_index",
        "selected_order": "ascending_global_schedule_ordinal",
        "max_rows_order": "after_fixed_membership_and_current_eligibility",
    }


def _scheduler_lane_key(row: dict[str, str], policy: dict[str, Any]) -> str:
    host = AUDIT.clean_text(row.get("source_host")).lower().rstrip(".")
    hosts = policy.get("hosts", {})
    config = hosts.get(host) if isinstance(hosts, dict) else None
    if not host or not isinstance(config, dict):
        raise AUDIT.AuditError(
            "PARALLEL_SCHEDULER_HOST_UNBOUND", AUDIT.EXIT_SAFETY
        )
    rate_group = AUDIT.clean_text(config.get("rate_group"))
    return f"rate_group:{rate_group}" if rate_group else f"host:{host}"


def _schedule_rows(
    rows: Sequence[dict[str, str]],
    policy: dict[str, Any],
    source_ordinals: dict[str, int],
) -> list[dict[str, Any]]:
    """Return a deterministic fair schedule without changing row identity."""
    if len(source_ordinals) != len(set(source_ordinals.values())):
        raise AUDIT.AuditError(
            "PARALLEL_SCHEDULER_SOURCE_ORDER_INVALID", AUDIT.EXIT_SAFETY
        )
    try:
        ordered = sorted(rows, key=lambda row: source_ordinals[row["record_id"]])
    except (KeyError, TypeError) as exc:
        raise AUDIT.AuditError(
            "PARALLEL_SCHEDULER_SOURCE_ORDER_INVALID", AUDIT.EXIT_SAFETY
        ) from exc
    if len({row["record_id"] for row in ordered}) != len(ordered):
        raise AUDIT.AuditError(
            "PARALLEL_SCHEDULER_DUPLICATE_RECORD", AUDIT.EXIT_SAFETY
        )
    lanes: dict[str, deque[tuple[int, dict[str, str]]]] = {}
    for row in ordered:
        lane_key = _scheduler_lane_key(row, policy)
        queue = lanes.setdefault(lane_key, deque())
        queue.append((len(queue), row))
    active_lanes = list(lanes)
    scheduled: list[dict[str, Any]] = []
    while active_lanes:
        remaining: list[str] = []
        for lane_key in active_lanes:
            queue = lanes[lane_key]
            lane_ordinal, row = queue.popleft()
            scheduled.append(
                {
                    "ordinal": len(scheduled),
                    "source_ordinal": source_ordinals[row["record_id"]],
                    "record_id": row["record_id"],
                    "lane_key": lane_key,
                    "lane_ordinal": lane_ordinal,
                    "row": row,
                }
            )
            if queue:
                remaining.append(lane_key)
        active_lanes = remaining
    return scheduled


def _schedule_sha256(entries: Sequence[dict[str, Any]]) -> str:
    return canonical_json_sha256(
        [
            [entry.get(field) for field in SCHEDULE_DIGEST_FIELDS]
            for entry in entries
        ]
    )


def _select_shard(
    full_schedule: Sequence[dict[str, Any]],
    shard_count: int,
    shard_index: int,
) -> list[dict[str, Any]]:
    _validate_shard_values(shard_count, shard_index)
    selected: list[dict[str, Any]] = []
    for entry in full_schedule:
        global_ordinal = entry.get("ordinal")
        lane_ordinal = entry.get("lane_ordinal")
        if (
            not isinstance(global_ordinal, int)
            or isinstance(global_ordinal, bool)
            or not isinstance(lane_ordinal, int)
            or isinstance(lane_ordinal, bool)
            or lane_ordinal < 0
        ):
            raise AUDIT.AuditError(
                "PARALLEL_SHARD_GLOBAL_ORDINAL_INVALID", AUDIT.EXIT_SAFETY
            )
        if lane_ordinal % shard_count != shard_index:
            continue
        selected.append(
            {
                **entry,
                "ordinal": len(selected),
                "shard_ordinal": len(selected),
                "global_ordinal": global_ordinal,
            }
        )
    return selected


def _eligible_shard_schedule(
    frozen_shard_schedule: Sequence[dict[str, Any]],
    eligible_record_ids: set[str],
) -> list[dict[str, Any]]:
    return [
        {**entry, "ordinal": ordinal}
        for ordinal, entry in enumerate(
            entry
            for entry in frozen_shard_schedule
            if entry.get("record_id") in eligible_record_ids
        )
    ]


def _shard_schedule_sha256(entries: Sequence[dict[str, Any]]) -> str:
    return canonical_json_sha256(
        [
            [entry.get(field) for field in SHARD_SCHEDULE_DIGEST_FIELDS]
            for entry in entries
        ]
    )


def _frozen_shard_schedule_sha256(
    entries: Sequence[dict[str, Any]],
) -> str:
    return canonical_json_sha256(
        [
            [entry.get(field) for field in FROZEN_SHARD_DIGEST_FIELDS]
            for entry in entries
        ]
    )


def _validate_shard_values(shard_count: object, shard_index: object) -> None:
    if (
        not isinstance(shard_count, int)
        or isinstance(shard_count, bool)
        or not 1 <= shard_count <= MAX_SHARD_COUNT
        or not isinstance(shard_index, int)
        or isinstance(shard_index, bool)
        or not 0 <= shard_index < shard_count
    ):
        raise AUDIT.AuditError("PARALLEL_SHARD_ARGUMENTS_INVALID", AUDIT.EXIT_USAGE)


def _schedule_metadata_valid(metadata: dict[str, Any]) -> bool:
    frozen_candidate_rows = metadata.get("parallel_frozen_candidate_rows")
    frozen_shard_rows = metadata.get("parallel_frozen_shard_rows")
    eligible_rows = metadata.get("parallel_eligible_rows_before_limit")
    shard_eligible_rows = metadata.get(
        "parallel_shard_eligible_rows_before_limit"
    )
    rows_selected = metadata.get("rows_selected")
    max_rows = metadata.get("max_rows")
    sharding = metadata.get("parallel_sharding")
    if (
        not isinstance(frozen_candidate_rows, int)
        or isinstance(frozen_candidate_rows, bool)
        or frozen_candidate_rows < 0
        or not isinstance(frozen_shard_rows, int)
        or isinstance(frozen_shard_rows, bool)
        or not 0 <= frozen_shard_rows <= frozen_candidate_rows
        or not isinstance(eligible_rows, int)
        or isinstance(eligible_rows, bool)
        or not 0 <= eligible_rows <= frozen_candidate_rows
        or not isinstance(shard_eligible_rows, int)
        or isinstance(shard_eligible_rows, bool)
        or not 0 <= shard_eligible_rows <= frozen_shard_rows
        or not isinstance(rows_selected, int)
        or isinstance(rows_selected, bool)
        or rows_selected < 0
        or metadata.get("parallel_scheduler") != _scheduler_contract()
        or not isinstance(sharding, dict)
        or not AUDIT.is_hex(
            AUDIT.clean_text(metadata.get("parallel_full_schedule_sha256")),
            64,
        )
        or not AUDIT.is_hex(
            AUDIT.clean_text(
                metadata.get("parallel_frozen_shard_schedule_sha256")
            ),
            64,
        )
        or not AUDIT.is_hex(
            AUDIT.clean_text(metadata.get("parallel_shard_schedule_sha256")),
            64,
        )
        or not AUDIT.is_hex(
            AUDIT.clean_text(metadata.get("parallel_selected_schedule_sha256")),
            64,
        )
    ):
        return False
    try:
        _validate_shard_values(
            sharding.get("shard_count"), sharding.get("shard_index")
        )
    except AUDIT.AuditError:
        return False
    if sharding != _sharding_contract(
        sharding["shard_count"], sharding["shard_index"]
    ):
        return False
    components = metadata.get("components")
    if (
        not isinstance(components, list)
        or any(
            not isinstance(component, str)
            or component not in AUDIT.COMPONENT_ORDER
            for component in components
        )
        or sharding["shard_count"] > 1
        and bool(components)
    ):
        return False
    if max_rows is None:
        return rows_selected == shard_eligible_rows
    return (
        isinstance(max_rows, int)
        and not isinstance(max_rows, bool)
        and max_rows >= 0
        and rows_selected == min(shard_eligible_rows, max_rows)
    )


def _work_manifest_schedule_valid(
    manifest: dict[str, Any],
    metadata: dict[str, Any],
    source_rows: Sequence[dict[str, str]],
    policy: dict[str, Any],
    eligible_record_ids: set[str],
) -> bool:
    rows = manifest.get("rows")
    if not isinstance(rows, list):
        return False
    frozen_candidate_rows = manifest.get("frozen_candidate_rows")
    frozen_shard_rows = manifest.get("frozen_shard_rows")
    eligible_rows = manifest.get("eligible_rows_before_limit")
    shard_eligible_rows = manifest.get("shard_eligible_rows_before_limit")
    max_rows = manifest.get("max_rows")
    if (
        not isinstance(frozen_candidate_rows, int)
        or isinstance(frozen_candidate_rows, bool)
        or frozen_candidate_rows < 0
        or not isinstance(frozen_shard_rows, int)
        or isinstance(frozen_shard_rows, bool)
        or not 0 <= frozen_shard_rows <= frozen_candidate_rows
        or not isinstance(eligible_rows, int)
        or isinstance(eligible_rows, bool)
        or not 0 <= eligible_rows <= frozen_candidate_rows
        or not isinstance(shard_eligible_rows, int)
        or isinstance(shard_eligible_rows, bool)
        or not 0 <= shard_eligible_rows <= frozen_shard_rows
    ):
        return False
    if max_rows is None:
        expected_selected = shard_eligible_rows
    elif (
        isinstance(max_rows, int)
        and not isinstance(max_rows, bool)
        and max_rows >= 0
    ):
        expected_selected = min(shard_eligible_rows, max_rows)
    else:
        return False
    sharding = manifest.get("sharding")
    if (
        manifest.get("scheduler") != _scheduler_contract()
        or not isinstance(sharding, dict)
        or expected_selected != len(rows)
        or manifest.get("scheduler") != metadata.get("parallel_scheduler")
        or sharding != metadata.get("parallel_sharding")
        or frozen_candidate_rows
        != metadata.get("parallel_frozen_candidate_rows")
        or frozen_shard_rows != metadata.get("parallel_frozen_shard_rows")
        or eligible_rows
        != metadata.get("parallel_eligible_rows_before_limit")
        or shard_eligible_rows
        != metadata.get("parallel_shard_eligible_rows_before_limit")
        or max_rows != metadata.get("max_rows")
        or manifest.get("full_schedule_sha256")
        != metadata.get("parallel_full_schedule_sha256")
        or manifest.get("frozen_shard_schedule_sha256")
        != metadata.get("parallel_frozen_shard_schedule_sha256")
        or manifest.get("shard_schedule_sha256")
        != metadata.get("parallel_shard_schedule_sha256")
        or manifest.get("selected_schedule_sha256")
        != metadata.get("parallel_selected_schedule_sha256")
        or not _schedule_metadata_valid(metadata)
    ):
        return False
    try:
        shard_count = sharding["shard_count"]
        shard_index = sharding["shard_index"]
        _validate_shard_values(shard_count, shard_index)
    except (KeyError, AUDIT.AuditError):
        return False
    components = metadata.get("components")
    if (
        not isinstance(components, list)
        or any(
            not isinstance(component, str)
            or component not in AUDIT.COMPONENT_ORDER
            for component in components
        )
    ):
        return False
    component_set = set(components)
    candidate_rows = (
        [row for row in source_rows if row.get("component") in component_set]
        if components
        else list(source_rows)
    )
    candidate_ids = {row.get("record_id", "") for row in candidate_rows}
    if (
        len(candidate_ids) != len(candidate_rows)
        or not isinstance(eligible_record_ids, set)
        or not eligible_record_ids <= candidate_ids
        or len(eligible_record_ids) != eligible_rows
    ):
        return False
    source_ordinals = {
        row.get("record_id", ""): ordinal
        for ordinal, row in enumerate(source_rows)
    }
    try:
        full_schedule = _schedule_rows(candidate_rows, policy, source_ordinals)
        expected_frozen_shard = _select_shard(
            full_schedule, shard_count, shard_index
        )
        expected_shard_eligible = _eligible_shard_schedule(
            expected_frozen_shard, eligible_record_ids
        )
        expected_selected = (
            expected_shard_eligible[:max_rows]
            if max_rows is not None
            else expected_shard_eligible
        )
    except (KeyError, TypeError, AUDIT.AuditError):
        return False
    if (
        len(full_schedule) != frozen_candidate_rows
        or len(expected_frozen_shard) != frozen_shard_rows
        or len(expected_shard_eligible) != shard_eligible_rows
        or _schedule_sha256(full_schedule)
        != manifest.get("full_schedule_sha256")
        or _frozen_shard_schedule_sha256(expected_frozen_shard)
        != manifest.get("frozen_shard_schedule_sha256")
        or _shard_schedule_sha256(expected_shard_eligible)
        != manifest.get("shard_schedule_sha256")
        or _shard_schedule_sha256(expected_selected)
        != manifest.get("selected_schedule_sha256")
    ):
        return False
    for actual, expected in zip(rows, expected_selected, strict=True):
        if any(
            actual.get(field) != expected.get(field)
            for field in SHARD_SCHEDULE_DIGEST_FIELDS
        ):
            return False
    return True


def _eligible_record_ids_at_invocation_start(
    output_dir: Path,
    metadata: dict[str, Any],
    candidate_rows: Sequence[dict[str, str]],
) -> set[str]:
    """Rebuild the invocation's eligibility from its immutable ledger prefixes."""
    attempts = AUDIT.read_attempts(output_dir / "pointer-health-attempts.tsv")
    completions = AUDIT.read_completions(output_dir / "record-completions.tsv")
    dispositions = AUDIT.read_dispositions(
        output_dir / "attempt-dispositions.tsv"
    )
    attempts_before = metadata.get("attempts_before")
    completions_before = metadata.get("completions_before")
    retry_mode = metadata.get("retry_mode")
    if (
        not isinstance(attempts_before, int)
        or isinstance(attempts_before, bool)
        or not 0 <= attempts_before <= len(attempts)
        or not isinstance(completions_before, int)
        or isinstance(completions_before, bool)
        or not 0 <= completions_before <= len(completions)
        or retry_mode not in {"none", "transient", "nonexact", "all"}
    ):
        raise AUDIT.AuditError(
            "PARALLEL_RECOVERED_LEDGER_PREFIX_INVALID", AUDIT.EXIT_SAFETY
        )
    prior_attempts = attempts[:attempts_before]
    prior_completions = completions[:completions_before]
    prior_attempt_ids = {row.get("attempt_id", "") for row in prior_attempts}
    prior_dispositions = [
        row
        for row in dispositions
        if row.get("attempt_id", "") in prior_attempt_ids
    ]
    committed = AUDIT.select_committed_attempts(
        prior_attempts, prior_completions
    )
    by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for attempt in committed:
        by_record[attempt.get("record_id", "")].append(attempt)
    abandoned = AUDIT.unrecovered_abandoned_record_ids(
        prior_attempts, prior_completions, prior_dispositions
    )
    completed_in_window = {
        row.get("record_id", "")
        for row in prior_completions
        if row.get("window_id") == metadata.get("window_id")
    }
    return {
        row["record_id"]
        for row in candidate_rows
        if row["record_id"] not in completed_in_window
        and (
            row["record_id"] in abandoned
            or AUDIT.should_process(by_record.get(row["record_id"], []), retry_mode)
        )
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parallel_dependency_report() -> dict[str, Any]:
    expected = {
        "ImageHash": "4.3.2",
        "Pillow": "12.1.1",
        "PyWavelets": "1.8.0",
        "numpy": "2.2.6",
        "scipy": "1.15.3",
    }
    installed: dict[str, str | None] = {}
    for distribution in expected:
        try:
            installed[distribution] = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            installed[distribution] = None
    lock_sha256 = (
        file_sha256(PARALLEL_REQUIREMENTS_PATH)
        if PARALLEL_REQUIREMENTS_PATH.is_file()
        else None
    )
    return {
        "installed": installed,
        "expected": expected,
        "lock_sha256": lock_sha256,
        "expected_lock_sha256": EXPECTED_PARALLEL_REQUIREMENTS_SHA256,
        "ready": (
            installed == expected
            and lock_sha256 == EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        ),
    }


if file_sha256(PRODUCER_PATH) != EXPECTED_PRODUCER_SHA256:
    raise RuntimeError("FROZEN_PRODUCER_SHA256_MISMATCH")

SPEC = importlib.util.spec_from_file_location(
    "coverfish_pointer_producer_v02", PRODUCER_PATH
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("FROZEN_PRODUCER_IMPORT_FAILED")
AUDIT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = AUDIT
SPEC.loader.exec_module(AUDIT)
SERIAL_RATE_LIMITER = AUDIT.RateLimiter


class RunnerArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        emit({"schema": RUNNER_SCHEMA, "status": "ERROR", "error": "INVALID_ARGUMENTS"})
        raise SystemExit(AUDIT.EXIT_USAGE)


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def canonical_json_sha256(payload: object) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def positive_worker_count(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("invalid worker count") from exc
    if not 1 <= parsed <= MAX_INFLIGHT:
        raise argparse.ArgumentTypeError("worker count outside safe range")
    return parsed


def positive_shard_count(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("invalid shard count") from exc
    if not 1 <= parsed <= MAX_SHARD_COUNT:
        raise argparse.ArgumentTypeError("shard count outside safe range")
    return parsed


def nonnegative_shard_index(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("invalid shard index") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("negative shard index")
    return parsed


def _safe_private_file(path: Path) -> bool:
    try:
        metadata = os.lstat(path)
    except OSError:
        return False
    return (
        stat.S_ISREG(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and metadata.st_nlink == 1
        and not metadata.st_mode & 0o077
    )


def _safe_private_directory(path: Path) -> bool:
    try:
        metadata = os.lstat(path)
    except OSError:
        return False
    return (
        stat.S_ISDIR(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and not metadata.st_mode & 0o077
    )


def _canonical_output_identity(output_dir: Path) -> str:
    try:
        metadata = os.lstat(output_dir)
        resolved = output_dir.resolve(strict=True)
    except OSError as exc:
        raise AUDIT.AuditError(
            "PARALLEL_OUTPUT_IDENTITY_FAILED", AUDIT.EXIT_SAFETY
        ) from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or output_dir.is_symlink()
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o022
    ):
        raise AUDIT.AuditError(
            "PARALLEL_OUTPUT_IDENTITY_INVALID", AUDIT.EXIT_SAFETY
        )
    return f"{resolved}\0{metadata.st_dev}\0{metadata.st_ino}"


def _output_identity_sha256(output_dir: Path) -> str:
    return hashlib.sha256(_canonical_output_identity(output_dir).encode()).hexdigest()


def _worker_lease_path(output_dir: Path) -> Path:
    return AUDIT.secure_runtime_root() / (
        f"parallel-worker-lease-{_output_identity_sha256(output_dir)[:24]}.lock"
    )


def _worker_epoch(
    output_dir: Path, invocation_id: str, window_id: str, started_at_utc: str
) -> str:
    contract_path = output_dir / "audit-contract.json"
    if not contract_path.is_file():
        raise AUDIT.AuditError(
            "PARALLEL_WORKER_EPOCH_CONTRACT_MISSING", AUDIT.EXIT_SAFETY
        )
    return hashlib.sha256(
        (
            f"{file_sha256(contract_path)}\0{invocation_id}\0"
            f"{window_id}\0{started_at_utc}\0worker-epoch-v1"
        ).encode()
    ).hexdigest()


def _open_safe_worker_lease(path: Path) -> int:
    flags = (
        os.O_CREAT
        | os.O_RDWR
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise AUDIT.AuditError(
            "PARALLEL_WORKER_LEASE_OPEN_FAILED", AUDIT.EXIT_SAFETY
        ) from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o077
    ):
        os.close(descriptor)
        raise AUDIT.AuditError(
            "PARALLEL_WORKER_LEASE_INVALID", AUDIT.EXIT_SAFETY
        )
    return descriptor


def _acquire_worker_lock(
    path: Path, operation: int, timeout_seconds: float
) -> int:
    descriptor = _open_safe_worker_lease(path)
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    try:
        while True:
            try:
                fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
                return descriptor
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise AUDIT.AuditError(
                        "PARALLEL_WORKER_QUIESCENCE_TIMEOUT", AUDIT.EXIT_SAFETY
                    )
                time.sleep(WORKER_LOCK_POLL_SECONDS)
    except BaseException:
        os.close(descriptor)
        raise


def _worker_epoch_payload(
    output_identity_sha256: str,
    invocation_id: str,
    window_id: str,
    worker_epoch: str,
) -> dict[str, str]:
    return {
        "schema": WORKER_EPOCH_SCHEMA,
        "output_identity_sha256": output_identity_sha256,
        "invocation_id": invocation_id,
        "window_id": window_id,
        "worker_epoch": worker_epoch,
    }


def _acquire_worker_shared_lease(
    lease_path: Path,
    epoch_path: Path,
    fence_path: Path,
    expected_epoch_payload: dict[str, str],
    timeout_seconds: float = WORKER_QUIESCENCE_TIMEOUT_SECONDS,
) -> int:
    descriptor = _acquire_worker_lock(lease_path, fcntl.LOCK_SH, timeout_seconds)
    try:
        if os.path.lexists(fence_path):
            raise AUDIT.AuditError(
                "PARALLEL_WORKER_INVOCATION_FENCED", AUDIT.EXIT_SAFETY
            )
        payload = json.loads(_read_private_file(epoch_path).decode("utf-8"))
        if payload != expected_epoch_payload:
            raise AUDIT.AuditError(
                "PARALLEL_WORKER_EPOCH_MISMATCH", AUDIT.EXIT_SAFETY
            )
        return descriptor
    except BaseException:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
        raise


@contextlib.contextmanager
def _exclusive_worker_recovery_lease(
    output_dir: Path,
    timeout_seconds: float = WORKER_QUIESCENCE_TIMEOUT_SECONDS,
) -> Iterable[None]:
    descriptor = _acquire_worker_lock(
        _worker_lease_path(output_dir), fcntl.LOCK_EX, timeout_seconds
    )
    try:
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class ProcessSafeRateLimiter:
    """Cross-process start scheduler and byte-budget ledger.

    Every read-modify-write operation uses the same stable lock inode.  The
    request timestamp is recorded immediately before a worker is released to
    start the request; response completion never moves it.  This is the
    intentional v0.4 difference from the serial executor.
    """

    SCHEMA = "coverfish.pointer-rate-state.v3"
    STATE_KEYS = {
        "schema",
        "policy_sha256",
        "last_request_epoch",
        "last_group_request_epoch",
        "retry_until_epoch",
        "transient_failures",
        "group_bytes",
        "inflight_group_bytes",
        "dynamic_interval",
    }

    def __init__(self, host_policy: dict[str, Any], state_path: Path):
        self.host_policy = {str(key): dict(value) for key, value in host_policy.items()}
        self.state_path = Path(state_path)
        self.lock_path = self.state_path.with_name(self.state_path.name + ".lock")
        self.policy_sha256 = canonical_json_sha256(self.host_policy)
        self.group_interval: dict[str, float] = {}
        for config in self.host_policy.values():
            group = AUDIT.clean_text(config.get("rate_group"))
            if group:
                self.group_interval[group] = max(
                    self.group_interval.get(group, 0.0),
                    float(config.get("min_interval_seconds", 1.0)),
                )
        self._ensure_initialized()

    @classmethod
    def empty_state(
        cls, host_policy: dict[str, Any], legacy: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        groups = {
            AUDIT.clean_text(config.get("rate_group"))
            for config in host_policy.values()
            if AUDIT.clean_text(config.get("rate_group"))
        }
        legacy = legacy or {}
        return {
            "schema": cls.SCHEMA,
            "policy_sha256": canonical_json_sha256(host_policy),
            "last_request_epoch": dict(legacy.get("last_request_epoch", {})),
            "last_group_request_epoch": dict(
                legacy.get("last_group_request_epoch", {})
            ),
            "retry_until_epoch": dict(legacy.get("retry_until_epoch", {})),
            "transient_failures": dict(legacy.get("transient_failures", {})),
            "group_bytes": {
                group: list(legacy.get("group_bytes", {}).get(group, []))
                for group in groups
                if legacy.get("group_bytes", {}).get(group)
            },
            "inflight_group_bytes": {
                group: dict(
                    legacy.get("inflight_group_bytes", {}).get(group, {})
                )
                for group in groups
                if legacy.get("inflight_group_bytes", {}).get(group)
            },
            "dynamic_interval": {},
        }

    @classmethod
    def initialize_from_legacy(
        cls,
        host_policy: dict[str, Any],
        state_path: Path,
        legacy_state: dict[str, Any] | None,
    ) -> None:
        instance = cls.__new__(cls)
        instance.host_policy = {
            str(key): dict(value) for key, value in host_policy.items()
        }
        instance.state_path = Path(state_path)
        instance.lock_path = instance.state_path.with_name(
            instance.state_path.name + ".lock"
        )
        instance.policy_sha256 = canonical_json_sha256(instance.host_policy)
        instance.group_interval = {}
        for config in instance.host_policy.values():
            group = AUDIT.clean_text(config.get("rate_group"))
            if group:
                instance.group_interval[group] = max(
                    instance.group_interval.get(group, 0.0),
                    float(config.get("min_interval_seconds", 1.0)),
                )
        with instance._locked_state(create=False) as current:
            if current is None:
                state = cls.empty_state(instance.host_policy, legacy_state)
                instance._validate_state(state)
                instance._write_state_unlocked(state)

    def _open_lock_file(self) -> int:
        parent = self.state_path.parent
        if not _safe_private_directory(parent):
            raise AUDIT.AuditError("PARALLEL_RATE_DIRECTORY_INVALID", AUDIT.EXIT_SAFETY)
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        try:
            descriptor = os.open(self.lock_path, flags, 0o600)
        except OSError as exc:
            raise AUDIT.AuditError(
                "PARALLEL_RATE_LOCK_OPEN_FAILED", AUDIT.EXIT_SAFETY
            ) from exc
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o077
        ):
            os.close(descriptor)
            raise AUDIT.AuditError(
                "PARALLEL_RATE_LOCK_INVALID", AUDIT.EXIT_SAFETY
            )
        return descriptor

    @contextlib.contextmanager
    def _locked_state(
        self, *, create: bool = True
    ) -> Iterable[dict[str, Any] | None]:
        descriptor = self._open_lock_file()
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            state: dict[str, Any] | None
            if self.state_path.exists():
                if not _safe_private_file(self.state_path):
                    raise AUDIT.AuditError(
                        "PARALLEL_RATE_STATE_INVALID", AUDIT.EXIT_SAFETY
                    )
                try:
                    loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    raise AUDIT.AuditError(
                        "PARALLEL_RATE_STATE_INVALID", AUDIT.EXIT_SAFETY
                    ) from exc
                if not isinstance(loaded, dict):
                    raise AUDIT.AuditError(
                        "PARALLEL_RATE_STATE_INVALID", AUDIT.EXIT_SAFETY
                    )
                state = loaded
                self._validate_state(state)
            elif create:
                state = self.empty_state(self.host_policy)
                self._write_state_unlocked(state)
            else:
                state = None
            yield state
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _ensure_initialized(self) -> None:
        with self._locked_state() as state:
            if state is None:
                raise AUDIT.AuditError(
                    "PARALLEL_RATE_STATE_UNAVAILABLE", AUDIT.EXIT_SAFETY
                )

    def _write_state_unlocked(self, state: dict[str, Any]) -> None:
        self._validate_state(state)
        AUDIT.atomic_write_json(self.state_path, state)
        os.chmod(self.state_path, 0o600)

    def _validate_state(self, state: dict[str, Any]) -> None:
        if (
            set(state) != self.STATE_KEYS
            or state.get("schema") != self.SCHEMA
            or state.get("policy_sha256") != self.policy_sha256
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RATE_STATE_SCHEMA_INVALID", AUDIT.EXIT_SAFETY
            )
        allowed_hosts = set(self.host_policy)
        allowed_groups = set(self.group_interval)

        def finite_nonnegative(value: object) -> bool:
            return (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(float(value))
                and float(value) >= 0
            )

        for key in ("last_request_epoch", "retry_until_epoch", "dynamic_interval"):
            mapping = state.get(key)
            if (
                not isinstance(mapping, dict)
                or any(host not in allowed_hosts for host in mapping)
                or any(not finite_nonnegative(value) for value in mapping.values())
            ):
                raise AUDIT.AuditError(
                    "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
                )
        requests = state.get("last_group_request_epoch")
        if (
            not isinstance(requests, dict)
            or any(group not in allowed_groups for group in requests)
            or any(not finite_nonnegative(value) for value in requests.values())
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
            )
        transient = state.get("transient_failures")
        transient_outcomes = {
            "rate_limited",
            "server_error",
            "timeout",
            "dns_error",
            "tls_error",
            "network_error",
        }
        if not isinstance(transient, dict) or any(
            host not in allowed_hosts for host in transient
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
            )
        for value in transient.values():
            if (
                not isinstance(value, dict)
                or set(value)
                != {"consecutive", "defer_until_epoch", "last_outcome"}
                or not isinstance(value["consecutive"], int)
                or isinstance(value["consecutive"], bool)
                or value["consecutive"] < 0
                or not finite_nonnegative(value["defer_until_epoch"])
                or value["last_outcome"] not in transient_outcomes
            ):
                raise AUDIT.AuditError(
                    "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
                )
        usage = state.get("group_bytes")
        inflight = state.get("inflight_group_bytes")
        if not isinstance(usage, dict) or not isinstance(inflight, dict):
            raise AUDIT.AuditError(
                "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
            )
        if any(group not in allowed_groups for group in usage) or any(
            group not in allowed_groups for group in inflight
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
            )
        for entries in usage.values():
            if not isinstance(entries, list):
                raise AUDIT.AuditError(
                    "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
                )
            for value in entries:
                if (
                    not isinstance(value, list)
                    or len(value) != 2
                    or not finite_nonnegative(value[0])
                    or not isinstance(value[1], int)
                    or isinstance(value[1], bool)
                    or value[1] < 0
                ):
                    raise AUDIT.AuditError(
                        "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
                    )
        for reservations in inflight.values():
            if not isinstance(reservations, dict):
                raise AUDIT.AuditError(
                    "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
                )
            for reservation_id, value in reservations.items():
                if (
                    not re.fullmatch(r"[0-9a-f]{24}", str(reservation_id))
                    or not isinstance(value, list)
                    or len(value) != 2
                    or not finite_nonnegative(value[0])
                    or not isinstance(value[1], int)
                    or isinstance(value[1], bool)
                    or value[1] <= 0
                ):
                    raise AUDIT.AuditError(
                        "PARALLEL_RATE_STATE_VALUE_INVALID", AUDIT.EXIT_SAFETY
                    )

    def _prune(
        self, state: dict[str, Any], group: str, now: float
    ) -> tuple[list[list[float | int]], dict[str, list[float | int]]]:
        entries = [
            [float(stamp), int(value)]
            for stamp, value in state.setdefault("group_bytes", {}).setdefault(
                group, []
            )
            if now - float(stamp) <= 86400
        ]
        inflight = {
            str(reservation_id): [float(value[0]), int(value[1])]
            for reservation_id, value in state.setdefault(
                "inflight_group_bytes", {}
            ).setdefault(group, {}).items()
            if now - float(value[0]) <= 86400
        }
        state["group_bytes"][group] = entries
        state["inflight_group_bytes"][group] = inflight
        return entries, inflight

    def _group_caps(self, group: str) -> tuple[int, int]:
        configs = [
            config
            for config in self.host_policy.values()
            if AUDIT.clean_text(config.get("rate_group")) == group
        ]
        hour = min(int(config["max_bytes_per_hour"]) for config in configs)
        day = min(int(config["max_bytes_per_day"]) for config in configs)
        return hour, day

    def _budget_error(
        self, state: dict[str, Any], host: str, additional: int, now: float
    ) -> str:
        group = AUDIT.clean_text(self.host_policy.get(host, {}).get("rate_group"))
        if not group:
            return ""
        entries, inflight = self._prune(state, group, now)
        inflight_day = sum(int(value[1]) for value in inflight.values())
        inflight_hour = sum(
            int(value[1])
            for value in inflight.values()
            if now - float(value[0]) <= 3600
        )
        day_total = sum(int(value) for _, value in entries) + inflight_day
        hour_total = (
            sum(int(value) for stamp, value in entries if now - float(stamp) <= 3600)
            + inflight_hour
        )
        max_hour, max_day = self._group_caps(group)
        if hour_total + additional > max_hour or day_total + additional > max_day:
            return "BANDWIDTH_BUDGET_REACHED"
        return ""

    def before(self, host: str) -> str:
        while True:
            wait_for = 0.0
            with self._locked_state() as state:
                assert state is not None
                policy = self.host_policy.get(host, {})
                interval = max(
                    float(policy.get("min_interval_seconds", 1.0)),
                    float(state["dynamic_interval"].get(host, 0.0)),
                )
                group = AUDIT.clean_text(policy.get("rate_group"))
                if group:
                    interval = max(interval, self.group_interval.get(group, 0.0))
                now = time.time()
                previous = float(state["last_request_epoch"].get(host, 0.0) or 0.0)
                group_previous = (
                    float(state["last_group_request_epoch"].get(group, 0.0) or 0.0)
                    if group
                    else 0.0
                )
                retry_until = float(state["retry_until_epoch"].get(host, 0.0) or 0.0)
                transient_until = float(
                    state["transient_failures"]
                    .get(host, {})
                    .get("defer_until_epoch", 0.0)
                    or 0.0
                )
                wait_for = max(
                    previous + interval - now,
                    group_previous + interval - now,
                    retry_until - now,
                    transient_until - now,
                )
                if wait_for <= 0:
                    budget_error = self._budget_error(state, host, 0, now)
                    if budget_error:
                        self._write_state_unlocked(state)
                        return budget_error
                    started = time.time()
                    state["last_request_epoch"][host] = started
                    if group:
                        state["last_group_request_epoch"][group] = started
                    self._write_state_unlocked(state)
                    return ""
                if wait_for > 60:
                    if retry_until - now > 60:
                        return "RETRY_AFTER_ACTIVE"
                    if transient_until - now > 60:
                        return "TRANSIENT_CIRCUIT_OPEN"
                    return "RATE_INTERVAL_ACTIVE"
            time.sleep(wait_for)

    def after(self, host: str, byte_count: int) -> None:
        if byte_count <= 0:
            return
        with self._locked_state() as state:
            assert state is not None
            group = AUDIT.clean_text(
                self.host_policy.get(host, {}).get("rate_group")
            )
            if group:
                state["group_bytes"].setdefault(group, []).append(
                    [time.time(), int(byte_count)]
                )
            self._write_state_unlocked(state)

    def reserve_bytes(self, host: str, amount: int) -> tuple[str, str]:
        group = AUDIT.clean_text(self.host_policy.get(host, {}).get("rate_group"))
        if not group or amount <= 0:
            return "", ""
        with self._locked_state() as state:
            assert state is not None
            now = time.time()
            error = self._budget_error(state, host, amount, now)
            if error:
                self._write_state_unlocked(state)
                return "", error
            reservation_id = hashlib.sha256(
                f"{host}\0{os.getpid()}\0{time.time_ns()}\0{amount}".encode()
            ).hexdigest()[:24]
            state["inflight_group_bytes"].setdefault(group, {})[reservation_id] = [
                now,
                int(amount),
            ]
            self._write_state_unlocked(state)
            return reservation_id, ""

    def settle_byte_reservation(
        self, host: str, reservation_id: str, actual_bytes: int
    ) -> None:
        group = AUDIT.clean_text(self.host_policy.get(host, {}).get("rate_group"))
        if not group or not reservation_id:
            return
        with self._locked_state() as state:
            assert state is not None
            reservations = state["inflight_group_bytes"].setdefault(group, {})
            raw = reservations.get(reservation_id)
            if (
                not isinstance(raw, list)
                or len(raw) != 2
                or actual_bytes < 0
                or actual_bytes > int(raw[1])
            ):
                raise AUDIT.AuditError(
                    "PARALLEL_RATE_BYTE_RESERVATION_INVALID", AUDIT.EXIT_SAFETY
                )
            del reservations[reservation_id]
            if actual_bytes:
                state["group_bytes"].setdefault(group, []).append(
                    [time.time(), int(actual_bytes)]
                )
            self._write_state_unlocked(state)

    def set_crawl_delay(self, host: str, seconds: float) -> None:
        if not math.isfinite(seconds) or seconds <= 0:
            return
        with self._locked_state() as state:
            assert state is not None
            state["dynamic_interval"][host] = max(
                float(state["dynamic_interval"].get(host, 0.0)), float(seconds)
            )
            self._write_state_unlocked(state)

    def defer(self, host: str, raw_retry_after: str) -> None:
        value = AUDIT.clean_text(raw_retry_after)
        if not value:
            return
        now = time.time()
        try:
            deadline = now + max(0, int(value))
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                deadline = parsed.timestamp()
            except (TypeError, ValueError, OverflowError):
                return
        with self._locked_state() as state:
            assert state is not None
            current = float(state["retry_until_epoch"].get(host, 0.0) or 0.0)
            state["retry_until_epoch"][host] = max(current, deadline)
            self._write_state_unlocked(state)

    def record_outcome(self, host: str, outcome: str) -> None:
        transient = {
            "rate_limited",
            "server_error",
            "timeout",
            "dns_error",
            "tls_error",
            "network_error",
        }
        if outcome == "redirect":
            return
        with self._locked_state() as state:
            assert state is not None
            states = state["transient_failures"]
            if outcome not in transient:
                if host in states:
                    del states[host]
                    self._write_state_unlocked(state)
                return
            previous = states.get(host, {})
            consecutive = int(previous.get("consecutive", 0) or 0) + 1
            base = max(
                1.0,
                float(
                    self.host_policy.get(host, {}).get(
                        "min_interval_seconds", 1.0
                    )
                ),
            )
            delay = min(
                float(AUDIT.TRANSIENT_BACKOFF_MAX_SECONDS),
                base * (2 ** min(consecutive - 1, 6)),
            )
            if consecutive >= AUDIT.TRANSIENT_CIRCUIT_THRESHOLD:
                delay = max(delay, float(AUDIT.TRANSIENT_CIRCUIT_MIN_SECONDS))
            states[host] = {
                "consecutive": consecutive,
                "defer_until_epoch": time.time() + delay,
                "last_outcome": outcome,
            }
            self._write_state_unlocked(state)


def _serialize_robot_entry(entry: Any) -> dict[str, Any]:
    return {
        "useragents": [str(value) for value in entry.useragents],
        "rulelines": [
            {"path": str(line.path), "allowance": bool(line.allowance)}
            for line in entry.rulelines
        ],
        "delay": entry.delay,
    }


def _serialize_robot_parser(
    parser: urllib.robotparser.RobotFileParser,
) -> dict[str, Any]:
    return {
        "allow_all": bool(parser.allow_all),
        "disallow_all": bool(parser.disallow_all),
        "entries": [_serialize_robot_entry(entry) for entry in parser.entries],
        "default_entry": (
            _serialize_robot_entry(parser.default_entry)
            if parser.default_entry is not None
            else None
        ),
    }


def _restore_robot_entry(payload: dict[str, Any]) -> urllib.robotparser.Entry:
    if set(payload) != {"useragents", "rulelines", "delay"}:
        raise AUDIT.AuditError("PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY)
    useragents = payload["useragents"]
    rulelines = payload["rulelines"]
    delay = payload["delay"]
    if (
        not isinstance(useragents, list)
        or not all(isinstance(value, str) for value in useragents)
        or not isinstance(rulelines, list)
        or delay is not None
        and (
            not isinstance(delay, (int, float))
            or isinstance(delay, bool)
            or not math.isfinite(float(delay))
            or float(delay) < 0
        )
    ):
        raise AUDIT.AuditError("PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY)
    entry = urllib.robotparser.Entry()
    entry.useragents = list(useragents)
    entry.rulelines = []
    entry.delay = delay
    for item in rulelines:
        if (
            not isinstance(item, dict)
            or set(item) != {"path", "allowance"}
            or not isinstance(item["path"], str)
            or not isinstance(item["allowance"], bool)
        ):
            raise AUDIT.AuditError(
                "PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY
            )
        line = urllib.robotparser.RuleLine("/", item["allowance"])
        # ``path`` is already the canonical quoted value produced by the
        # stdlib parser.  Re-running RuleLine.__init__ would quote '%' again.
        line.path = item["path"]
        line.allowance = item["allowance"]
        entry.rulelines.append(line)
    return entry


def _restore_robot_parser(
    payload: dict[str, Any], robots_url: str
) -> urllib.robotparser.RobotFileParser:
    if set(payload) != {"allow_all", "disallow_all", "entries", "default_entry"}:
        raise AUDIT.AuditError("PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY)
    if (
        not isinstance(payload["allow_all"], bool)
        or not isinstance(payload["disallow_all"], bool)
        or not isinstance(payload["entries"], list)
        or payload["default_entry"] is not None
        and not isinstance(payload["default_entry"], dict)
    ):
        raise AUDIT.AuditError("PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY)
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url(robots_url)
    parser.allow_all = payload["allow_all"]
    parser.disallow_all = payload["disallow_all"]
    parser.entries = [
        _restore_robot_entry(item)
        for item in payload["entries"]
        if isinstance(item, dict)
    ]
    if len(parser.entries) != len(payload["entries"]):
        raise AUDIT.AuditError("PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY)
    parser.default_entry = (
        _restore_robot_entry(payload["default_entry"])
        if payload["default_entry"] is not None
        else None
    )
    parser.last_checked = time.time()
    return parser


class ParallelNetworkClient(AUDIT.NetworkClient):
    """Network client with a per-invocation cross-process robots singleflight."""

    ROBOTS_SCHEMA = "coverfish.pointer-robots-cache.v1"

    def __init__(self, *args: Any, robots_state_path: Path, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.robots_state_path = Path(robots_state_path)
        self.robots_lock_path = self.robots_state_path.with_name(
            self.robots_state_path.name + ".lock"
        )

    def _robots_lock_descriptor(self) -> int:
        if not _safe_private_directory(self.robots_state_path.parent):
            raise AUDIT.AuditError(
                "PARALLEL_ROBOTS_DIRECTORY_INVALID", AUDIT.EXIT_SAFETY
            )
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        descriptor = os.open(self.robots_lock_path, flags, 0o600)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_mode & 0o077
        ):
            os.close(descriptor)
            raise AUDIT.AuditError(
                "PARALLEL_ROBOTS_LOCK_INVALID", AUDIT.EXIT_SAFETY
            )
        return descriptor

    def _load_robots_state(self) -> dict[str, Any]:
        if not self.robots_state_path.exists():
            return {
                "schema": self.ROBOTS_SCHEMA,
                "window_id": self.window_id,
                "invocation_id": self.invocation_id,
                "hosts": {},
            }
        if not _safe_private_file(self.robots_state_path):
            raise AUDIT.AuditError(
                "PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY
            )
        state = AUDIT.load_json(self.robots_state_path)
        if (
            set(state) != {"schema", "window_id", "invocation_id", "hosts"}
            or state["schema"] != self.ROBOTS_SCHEMA
            or state["window_id"] != self.window_id
            or state["invocation_id"] != self.invocation_id
            or not isinstance(state["hosts"], dict)
        ):
            raise AUDIT.AuditError(
                "PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY
            )
        return state

    def _write_robots_state(self, state: dict[str, Any]) -> None:
        AUDIT.atomic_write_json(self.robots_state_path, state)
        os.chmod(self.robots_state_path, 0o600)

    def robots_decision(
        self, url: str, allowed_hosts: frozenset[str] | None = None
    ) -> str:
        try:
            scheme, host, _ = self.validate_url(url)
        except AUDIT.AuditError as exc:
            return f"safety_block:{exc.code}"
        robots_url = f"{scheme}://{host}/robots.txt"
        descriptor = self._robots_lock_descriptor()
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            state = self._load_robots_state()
            cached = state["hosts"].get(host)
            if cached is not None:
                if (
                    not isinstance(cached, dict)
                    or set(cached)
                    != {"checked_epoch", "robots_state", "parser"}
                    or not isinstance(cached["checked_epoch"], (int, float))
                    or isinstance(cached["checked_epoch"], bool)
                    or not math.isfinite(float(cached["checked_epoch"]))
                    or cached["robots_state"]
                    not in {"parsed", "parse_error", "not_present_allow", "unavailable_disallow"}
                    or cached["parser"] is not None
                    and not isinstance(cached["parser"], dict)
                ):
                    raise AUDIT.AuditError(
                        "PARALLEL_ROBOTS_CACHE_INVALID", AUDIT.EXIT_SAFETY
                    )
            if (
                cached is None
                or time.time() - float(cached["checked_epoch"])
                >= self.robots_cache_seconds
            ):
                result = self._fetch_without_robots(
                    robots_url,
                    "robots",
                    1024 * 1024,
                    "text/plain,*/*;q=0.1",
                    allowed_hosts=allowed_hosts,
                )
                parser: urllib.robotparser.RobotFileParser | None = None
                if (
                    result.status == "ok"
                    and result.http_status
                    and 200 <= result.http_status < 300
                ):
                    try:
                        text = result.data.decode("utf-8", errors="replace")
                        parser = urllib.robotparser.RobotFileParser()
                        parser.set_url(robots_url)
                        parser.parse(text.splitlines())
                        crawl_delay = parser.crawl_delay(
                            self.policy.get("user_agent_product", "")
                        )
                        if crawl_delay is None:
                            crawl_delay = parser.crawl_delay("*")
                        if crawl_delay is not None:
                            self.rate.set_crawl_delay(host, float(crawl_delay))
                        robots_state = "parsed"
                    except (ValueError, TypeError):
                        robots_state = "parse_error"
                elif result.http_status in {404, 410}:
                    robots_state = "not_present_allow"
                else:
                    robots_state = "unavailable_disallow"
                receipt = {
                    "window_id": self.window_id,
                    "invocation_id": self.invocation_id,
                    "checked_at_utc": AUDIT.utc_now(),
                    "host": host,
                    "robots_url": robots_url,
                    "http_status": result.http_status or "",
                    "fetch_status": result.status,
                    "robots_state": robots_state,
                    "error_code": result.error_code,
                    "redirect_count": str(len(result.redirects)),
                    "redirect_chain_json": json.dumps(
                        result.redirects,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "sha256": (
                        AUDIT.bytes_sha256(result.data)
                        if result.status == "ok"
                        else ""
                    ),
                }
                # Receipt fsync precedes cache publication.  A worker can never
                # rely on a shared decision whose source receipt was not first
                # made durable in that worker's private spool.
                self.record_robots_observation(receipt)
                cached = {
                    "checked_epoch": time.time(),
                    "robots_state": robots_state,
                    "parser": (
                        _serialize_robot_parser(parser) if parser is not None else None
                    ),
                }
                state["hosts"][host] = cached
                self._write_robots_state(state)
            robots_state = cached["robots_state"]
            parser_payload = cached["parser"]
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
        if robots_state == "not_present_allow":
            return "allowed_no_robots"
        if robots_state != "parsed" or parser_payload is None:
            return "unavailable_disallow"
        parser = _restore_robot_parser(parser_payload, robots_url)
        return "allowed" if parser.can_fetch(self.user_agent, url) else "disallowed"


_WORKER_CLIENT: Any = None
_WORKER_LEASE_DESCRIPTOR: int | None = None
_WORKER_FENCE_ERROR = "WORKER_NOT_INITIALIZED"


def _worker_initialize(
    policy: dict[str, Any],
    user_agent: str,
    window_id: str,
    invocation_id: str,
    rate_state_path: str,
    robots_state_path: str,
    worker_lease_path: str,
    worker_epoch_path: str,
    worker_fence_path: str,
    output_identity_sha256: str,
    worker_epoch: str,
) -> None:
    global _WORKER_CLIENT, _WORKER_LEASE_DESCRIPTOR, _WORKER_FENCE_ERROR
    os.umask(0o077)
    _WORKER_CLIENT = None
    _WORKER_LEASE_DESCRIPTOR = None
    _WORKER_FENCE_ERROR = "WORKER_INITIALIZATION_FAILED"
    try:
        expected_epoch = _worker_epoch_payload(
            output_identity_sha256,
            invocation_id,
            window_id,
            worker_epoch,
        )
        # The shared lease and epoch/fence check happen before constructing the
        # client, which is the earliest point DNS, robots, or image I/O could
        # otherwise become reachable.
        _WORKER_LEASE_DESCRIPTOR = _acquire_worker_shared_lease(
            Path(worker_lease_path),
            Path(worker_epoch_path),
            Path(worker_fence_path),
            expected_epoch,
        )
        AUDIT.RateLimiter = ProcessSafeRateLimiter
        _WORKER_CLIENT = ParallelNetworkClient(
            policy,
            user_agent,
            window_id,
            invocation_id,
            rate_state_path=Path(rate_state_path),
            robots_state_path=Path(robots_state_path),
        )
        _WORKER_FENCE_ERROR = ""
    except BaseException as exc:
        _WORKER_FENCE_ERROR = (
            exc.code if isinstance(exc, AUDIT.AuditError) else type(exc).__name__
        )


def _worker_audit_row(task: dict[str, Any]) -> dict[str, Any]:
    if _WORKER_CLIENT is None or _WORKER_LEASE_DESCRIPTOR is None:
        return {"status": "ERROR", "error": _WORKER_FENCE_ERROR}
    ordinal = int(task["ordinal"])
    row = dict(task["row"])
    window_id = str(task["window_id"])
    invocation_id = str(task["invocation_id"])
    starting_index = int(task["starting_index"])
    spool = Path(task["spool"])
    attempts_path = spool / "pointer-health-attempts.tsv"
    completion_path = spool / "record-completions.tsv"
    robots_path = spool / "robots.tsv"
    try:
        if not _safe_private_directory(spool) or any(spool.iterdir()):
            raise AUDIT.AuditError("PARALLEL_SPOOL_NOT_EMPTY", AUDIT.EXIT_SAFETY)
        _WORKER_CLIENT.robots_receipt_path = robots_path
        audit_result = AUDIT.audit_one(
            row,
            _WORKER_CLIENT,
            window_id,
            starting_index,
            on_attempt=lambda attempt: AUDIT.append_attempts(
                attempts_path, [attempt]
            ),
        )
        AUDIT.append_completion(
            completion_path, row, window_id, invocation_id, audit_result
        )
        result = {
            "schema": "coverfish.pointer-parallel-worker-result.v1",
            "status": "COMPLETE",
            "ordinal": ordinal,
            "record_id": row["record_id"],
            "attempt_rows": len(audit_result.attempts),
            "bytes_retained": False,
            "gpu_used": False,
        }
        AUDIT.atomic_write_json(spool / "worker-result.json", result)
        return result
    except BaseException as exc:
        error = exc.code if isinstance(exc, AUDIT.AuditError) else type(exc).__name__
        result = {
            "schema": "coverfish.pointer-parallel-worker-result.v1",
            "status": "ERROR",
            "ordinal": ordinal,
            "record_id": row.get("record_id", ""),
            "error": error,
            "bytes_retained": False,
            "gpu_used": False,
        }
        try:
            AUDIT.atomic_write_json(spool / "worker-result.json", result)
        except Exception:
            pass
        return result
    finally:
        _WORKER_CLIENT.robots_receipt_path = None


def _append_spool_recovery(
    writer: "CanonicalLedgerWriter",
    *,
    window_id: str,
    invocation_id: str,
    record_id: str,
    spool_name: str,
    artifact_name: str,
    action: str,
    original_size: int,
    retained_size: int,
    discarded_size: int,
    artifact_sha256: str,
    requires_safety_review: bool,
) -> dict[str, str]:
    if not _spool_recovery_artifact_action_valid(
        artifact_name, action, record_id, spool_name
    ):
        raise AUDIT.AuditError(
            "PARALLEL_SPOOL_RECOVERY_CLASS_INVALID", AUDIT.EXIT_SAFETY
        )
    identity_values = (
        window_id,
        invocation_id,
        record_id,
        spool_name,
        artifact_name,
        action,
        str(original_size),
        str(retained_size),
        str(discarded_size),
        artifact_sha256,
        "true" if requires_safety_review else "false",
    )
    recovery_id = hashlib.sha256("\0".join(identity_values).encode()).hexdigest()[:24]
    prior = writer.indexes["spool_recoveries"].get(recovery_id)
    detected_at = (
        prior["detected_at_utc"] if prior is not None else utc_now()
    )
    row = {
        "recovery_id": recovery_id,
        "window_id": window_id,
        "invocation_id": invocation_id,
        "record_id": record_id,
        "spool_name": spool_name,
        "artifact_name": artifact_name,
        "action": action,
        "detected_at_utc": detected_at,
        "original_size_bytes": str(original_size),
        "retained_size_bytes": str(retained_size),
        "discarded_fragment_bytes": str(discarded_size),
        "discarded_fragment_sha256": artifact_sha256,
        "requires_safety_review": "true" if requires_safety_review else "false",
    }
    writer.append("spool_recoveries", [row])
    return row


def _spool_recovery_artifact_action_valid(
    artifact_name: object,
    action: object,
    record_id: object,
    spool_name: object,
) -> bool:
    """Bind every recovery action to one exact, basename-only artifact class."""
    if not all(
        isinstance(value, str)
        for value in (artifact_name, action, record_id, spool_name)
    ):
        return False
    has_row_identity = bool(record_id) and bool(spool_name)
    has_no_row_identity = not record_id and not spool_name
    if action in {
        "truncate_incomplete_tsv_tail",
        "discard_empty_tsv_prewrite",
    }:
        return artifact_name in SPOOL_TSV_ARTIFACTS and has_row_identity
    if action == "discard_prestart_missing_manifest":
        return artifact_name == "invocation-work.json" and has_no_row_identity
    if action not in SPOOL_JSON_TEMP_ACTIONS:
        return False
    match = re.fullmatch(
        r"\.(invocation-work\.json|robots-cache\.json|"
        r"worker-result\.json|disposition-stamp\.json)\."
        r"[A-Za-z0-9_-]+\.tmp",
        artifact_name,
    )
    if match is None:
        return False
    target = match.group(1)
    if target in SPOOL_ROOT_JSON_TEMP_TARGETS:
        return has_no_row_identity
    return target in SPOOL_ROW_JSON_TEMP_TARGETS and has_row_identity


def _read_private_file(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AUDIT.AuditError(
            "PARALLEL_PRIVATE_ARTIFACT_OPEN_FAILED", AUDIT.EXIT_SAFETY
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o077
        ):
            raise AUDIT.AuditError(
                "PARALLEL_PRIVATE_ARTIFACT_INVALID", AUDIT.EXIT_SAFETY
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _recover_spool_tsv_tail(
    writer: "CanonicalLedgerWriter",
    spool: Path,
    row: dict[str, str],
    window_id: str,
    invocation_id: str,
    artifact_name: str,
    fields: Sequence[str],
) -> None:
    path = spool / artifact_name
    if not path.exists():
        return
    payload = _read_private_file(path)
    if not payload:
        _append_spool_recovery(
            writer,
            window_id=window_id,
            invocation_id=invocation_id,
            record_id=row["record_id"],
            spool_name=spool.name,
            artifact_name=artifact_name,
            action="discard_empty_tsv_prewrite",
            original_size=0,
            retained_size=0,
            discarded_size=0,
            artifact_sha256=hashlib.sha256(b"").hexdigest(),
            requires_safety_review=False,
        )
        path.unlink()
        AUDIT.fsync_directory(spool)
        return
    if payload.endswith(b"\n"):
        return
    header = "\t".join(fields).encode("utf-8") + b"\n"
    retained = 0
    if payload.startswith(header):
        newline = payload.rfind(b"\n")
        retained = newline + 1 if newline >= len(header) - 1 else 0
    fragment = payload[retained:]
    _append_spool_recovery(
        writer,
        window_id=window_id,
        invocation_id=invocation_id,
        record_id=row["record_id"],
        spool_name=spool.name,
        artifact_name=artifact_name,
        action="truncate_incomplete_tsv_tail",
        original_size=len(payload),
        retained_size=retained,
        discarded_size=len(fragment),
        artifact_sha256=hashlib.sha256(fragment).hexdigest(),
        requires_safety_review=artifact_name
        in {"pointer-health-attempts.tsv", "robots.tsv"},
    )
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AUDIT.AuditError(
            "PARALLEL_SPOOL_TAIL_OPEN_FAILED", AUDIT.EXIT_SAFETY
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o077
            or metadata.st_size != len(payload)
        ):
            raise AUDIT.AuditError(
                "PARALLEL_SPOOL_TAIL_CHANGED", AUDIT.EXIT_SAFETY
            )
        os.ftruncate(descriptor, retained)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if retained == 0:
        path.unlink()
    AUDIT.fsync_directory(spool)


def _recover_json_atomic_temps(
    writer: "CanonicalLedgerWriter",
    directory: Path,
    *,
    targets: frozenset[str],
    window_id: str,
    invocation_id: str,
    record_id: str = "",
    spool_name: str = "",
) -> None:
    for temporary in sorted(directory.iterdir(), key=lambda item: item.name):
        match = re.fullmatch(r"\.(.+)\.[A-Za-z0-9_-]+\.tmp", temporary.name)
        if match is None or match.group(1) not in targets:
            continue
        payload = _read_private_file(temporary)
        target_name = match.group(1)
        target = directory / target_name
        try:
            parsed = json.loads(payload.decode("utf-8"))
            complete = isinstance(parsed, dict)
        except (UnicodeDecodeError, json.JSONDecodeError):
            complete = False
        if complete and target.exists():
            if not _safe_private_file(target) or _read_private_file(target) != payload:
                raise AUDIT.AuditError(
                    "PARALLEL_ATOMIC_TEMP_TARGET_MISMATCH", AUDIT.EXIT_SAFETY
                )
            action = "discard_duplicate_complete_json_temp"
            retained = len(payload)
            discarded = 0
        elif complete:
            action = "promote_complete_json_temp"
            retained = len(payload)
            discarded = 0
        else:
            action = "quarantine_incomplete_json_temp"
            retained = 0
            discarded = len(payload)
        _append_spool_recovery(
            writer,
            window_id=window_id,
            invocation_id=invocation_id,
            record_id=record_id,
            spool_name=spool_name,
            artifact_name=temporary.name,
            action=action,
            original_size=len(payload),
            retained_size=retained,
            discarded_size=discarded,
            artifact_sha256=hashlib.sha256(payload).hexdigest(),
            requires_safety_review=False,
        )
        if action == "promote_complete_json_temp":
            os.replace(temporary, target)
        else:
            temporary.unlink()
        AUDIT.fsync_directory(directory)


def _load_spool(
    spool: Path,
    row: dict[str, str],
    window_id: str,
    invocation_id: str,
    starting_index: int,
) -> tuple[list[dict[str, str]], dict[str, str], list[dict[str, str]]]:
    allowed = {
        "pointer-health-attempts.tsv",
        "record-completions.tsv",
        "robots.tsv",
        "worker-result.json",
        "disposition-stamp.json",
    }
    if not _safe_private_directory(spool) or any(
        not _safe_private_file(path) or path.name not in allowed
        for path in spool.iterdir()
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_TREE_INVALID", AUDIT.EXIT_SAFETY)
    result = AUDIT.load_json(spool / "worker-result.json")
    if (
        result
        != {
            "schema": "coverfish.pointer-parallel-worker-result.v1",
            "status": "COMPLETE",
            "ordinal": result.get("ordinal"),
            "record_id": row["record_id"],
            "attempt_rows": result.get("attempt_rows"),
            "bytes_retained": False,
            "gpu_used": False,
        }
        or not isinstance(result.get("ordinal"), int)
        or not isinstance(result.get("attempt_rows"), int)
        or result["attempt_rows"] <= 0
    ):
        raise AUDIT.AuditError("PARALLEL_WORKER_RESULT_INVALID", AUDIT.EXIT_SAFETY)
    attempt_fields, attempts = AUDIT.read_tsv(
        spool / "pointer-health-attempts.tsv"
    )
    completion_fields, completions = AUDIT.read_tsv(
        spool / "record-completions.tsv"
    )
    robots: list[dict[str, str]] = []
    if (spool / "robots.tsv").exists():
        robots_fields, robots = AUDIT.read_tsv(spool / "robots.tsv")
        if tuple(robots_fields) != AUDIT.ROBOTS_FIELDS:
            raise AUDIT.AuditError("PARALLEL_SPOOL_ROBOTS_INVALID")
    if (
        tuple(attempt_fields) != AUDIT.ATTEMPT_FIELDS
        or tuple(completion_fields) != AUDIT.COMPLETION_FIELDS
        or len(attempts) != result["attempt_rows"]
        or len(completions) != 1
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_SCHEMA_INVALID")
    expected_indexes = list(range(starting_index, starting_index + len(attempts)))
    actual_indexes = [int(item.get("attempt_index", "0") or 0) for item in attempts]
    if actual_indexes != expected_indexes:
        raise AUDIT.AuditError("PARALLEL_SPOOL_ATTEMPT_ORDER_INVALID")
    if any(
        attempt.get("record_id") != row["record_id"]
        or attempt.get("window_id") != window_id
        or attempt.get("invocation_id") != invocation_id
        or attempt.get("bytes_retained") != "false"
        or attempt.get("attempt_id")
        != hashlib.sha256(
            f"{row['record_id']}\0{window_id}\0{attempt['attempt_index']}".encode()
        ).hexdigest()[:24]
        for attempt in attempts
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_ATTEMPT_BINDING_FAILED")
    completion = completions[0]
    if (
        completion.get("record_id") != row["record_id"]
        or completion.get("component") != row["component"]
        or completion.get("window_id") != window_id
        or completion.get("invocation_id") != invocation_id
        or completion.get("first_attempt_index") != str(expected_indexes[0])
        or completion.get("last_attempt_index") != str(expected_indexes[-1])
        or completion.get("attempt_count") != str(len(attempts))
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_COMPLETION_BINDING_FAILED")
    if any(
        robot.get("window_id") != window_id
        or robot.get("invocation_id") != invocation_id
        for robot in robots
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_ROBOTS_BINDING_FAILED")
    return attempts, completion, robots


def _commit_spool(
    output_dir: Path,
    writer: "CanonicalLedgerWriter",
    spool: Path,
    row: dict[str, str],
    window_id: str,
    invocation_id: str,
    starting_index: int,
) -> int:
    attempts, completion, robots = _load_spool(
        spool, row, window_id, invocation_id, starting_index
    )
    # A completion is the transaction commit.  All evidence it references is
    # fsynced first, so an interruption can only leave uncommitted evidence.
    writer.append("robots", robots)
    writer.append("attempts", attempts)
    writer.append("completions", [completion])
    shutil.rmtree(spool)
    AUDIT.fsync_directory(spool.parent)
    return len(attempts)


def _row_digest(row: dict[str, str], fields: Sequence[str]) -> str:
    return canonical_json_sha256([row.get(field, "") for field in fields])


class CanonicalLedgerWriter:
    """Load canonical IDs once, then update indexes after each fsynced append."""

    LEDGERS = {
        "robots": ("robots.tsv", AUDIT.ROBOTS_FIELDS, None),
        "attempts": ("pointer-health-attempts.tsv", AUDIT.ATTEMPT_FIELDS, "attempt_id"),
        "completions": ("record-completions.tsv", AUDIT.COMPLETION_FIELDS, "completion_id"),
        "dispositions": ("attempt-dispositions.tsv", AUDIT.DISPOSITION_FIELDS, "attempt_id"),
        "spool_recoveries": (
            SPOOL_RECOVERY_FILE,
            SPOOL_RECOVERY_FIELDS,
            "recovery_id",
        ),
    }

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.indexes: dict[str, dict[str, dict[str, str]]] = {}
        for name, (filename, fields, key_field) in self.LEDGERS.items():
            rows: list[dict[str, str]] = []
            path = output_dir / filename
            if path.exists():
                current_fields, rows = AUDIT.read_tsv(path)
                if tuple(current_fields) != tuple(fields):
                    raise AUDIT.AuditError("PARALLEL_CANONICAL_SCHEMA_INVALID")
            index: dict[str, dict[str, str]] = {}
            for row in rows:
                key = row[str(key_field)] if key_field else _row_digest(row, fields)
                if key in index and index[key] != row:
                    raise AUDIT.AuditError("PARALLEL_CANONICAL_DUPLICATE_MISMATCH")
                index[key] = row
            self.indexes[name] = index

    def append(self, name: str, rows: list[dict[str, str]]) -> None:
        if not rows:
            return
        filename, fields, key_field = self.LEDGERS[name]
        index = self.indexes[name]
        additions: list[dict[str, str]] = []
        for row in rows:
            key = row[str(key_field)] if key_field else _row_digest(row, fields)
            prior = index.get(key)
            if prior is not None and prior != row:
                raise AUDIT.AuditError("PARALLEL_CANONICAL_REPLAY_MISMATCH")
            if prior is None:
                additions.append(row)
                index[key] = row
        if additions:
            AUDIT.append_tsv_rows(self.output_dir / filename, fields, additions)


def _load_partial_spool(
    spool: Path,
    row: dict[str, str],
    window_id: str,
    invocation_id: str,
    starting_index: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    allowed = {
        "pointer-health-attempts.tsv",
        "record-completions.tsv",
        "robots.tsv",
        "worker-result.json",
        "disposition-stamp.json",
    }
    if not _safe_private_directory(spool) or any(
        not _safe_private_file(path) or path.name not in allowed
        for path in spool.iterdir()
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_TREE_INVALID", AUDIT.EXIT_SAFETY)
    attempts: list[dict[str, str]] = []
    if (spool / "pointer-health-attempts.tsv").exists():
        fields, attempts = AUDIT.read_tsv(spool / "pointer-health-attempts.tsv")
        if tuple(fields) != AUDIT.ATTEMPT_FIELDS:
            raise AUDIT.AuditError("PARALLEL_SPOOL_SCHEMA_INVALID")
    robots: list[dict[str, str]] = []
    if (spool / "robots.tsv").exists():
        fields, robots = AUDIT.read_tsv(spool / "robots.tsv")
        if tuple(fields) != AUDIT.ROBOTS_FIELDS:
            raise AUDIT.AuditError("PARALLEL_SPOOL_ROBOTS_INVALID")
    expected_indexes = list(range(starting_index, starting_index + len(attempts)))
    if [int(item.get("attempt_index", "0") or 0) for item in attempts] != expected_indexes:
        raise AUDIT.AuditError("PARALLEL_SPOOL_ATTEMPT_ORDER_INVALID")
    if any(
        attempt.get("record_id") != row["record_id"]
        or attempt.get("window_id") != window_id
        or attempt.get("invocation_id") != invocation_id
        or attempt.get("bytes_retained") != "false"
        or attempt.get("attempt_id")
        != hashlib.sha256(
            f"{row['record_id']}\0{window_id}\0{attempt['attempt_index']}".encode()
        ).hexdigest()[:24]
        for attempt in attempts
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_ATTEMPT_BINDING_FAILED")
    if any(
        robot.get("window_id") != window_id
        or robot.get("invocation_id") != invocation_id
        for robot in robots
    ):
        raise AUDIT.AuditError("PARALLEL_SPOOL_ROBOTS_BINDING_FAILED")
    return attempts, robots


def _abandon_spool(
    output_dir: Path,
    writer: CanonicalLedgerWriter,
    spool: Path,
    row: dict[str, str],
    window_id: str,
    invocation_id: str,
    starting_index: int,
) -> int:
    attempts, robots = _load_partial_spool(
        spool, row, window_id, invocation_id, starting_index
    )
    writer.append("robots", robots)
    writer.append("attempts", attempts)
    stamp_path = spool / "disposition-stamp.json"
    if stamp_path.exists():
        stamp = AUDIT.load_json(stamp_path)
        if (
            set(stamp) != {"schema", "recorded_at_utc"}
            or stamp["schema"] != "coverfish.pointer-parallel-disposition-stamp.v1"
            or not AUDIT.is_utc_timestamp(str(stamp["recorded_at_utc"]))
        ):
            raise AUDIT.AuditError("PARALLEL_DISPOSITION_STAMP_INVALID")
    else:
        stamp = {
            "schema": "coverfish.pointer-parallel-disposition-stamp.v1",
            "recorded_at_utc": utc_now(),
        }
        AUDIT.atomic_write_json(stamp_path, stamp)
    dispositions = [
        {
            "attempt_id": attempt["attempt_id"],
            "record_id": attempt["record_id"],
            "window_id": attempt["window_id"],
            "invocation_id": attempt["invocation_id"],
            "disposition": "abandoned_incomplete_transaction",
            "recorded_at_utc": str(stamp["recorded_at_utc"]),
        }
        for attempt in attempts
    ]
    writer.append("dispositions", dispositions)
    shutil.rmtree(spool)
    AUDIT.fsync_directory(spool.parent)
    return len(attempts)


def _execution_profile(
    shard_count: int = 1, shard_index: int = 0
) -> dict[str, Any]:
    _validate_shard_values(shard_count, shard_index)
    parallel_dependencies = _parallel_dependency_report()
    return {
        "schema": "coverfish.pointer-parallel-execution-profile.v2",
        "runner_version": RUNNER_VERSION,
        "runner_sha256": file_sha256(Path(__file__)),
        "producer_version": AUDIT.TOOL_VERSION,
        "producer_sha256": EXPECTED_PRODUCER_SHA256,
        "process_start_method": "spawn",
        "max_inflight_cap": MAX_INFLIGHT,
        "default_max_inflight": 5,
        "request_spacing_basis": "shared_rate_group_request_start",
        "rate_state_schema": ProcessSafeRateLimiter.SCHEMA,
        "rate_state_update": "flock_reload_modify_atomic_write",
        "robots_coordination": "per_invocation_flock_singleflight",
        "worker_receipt_model": "private_fsynced_spool",
        "canonical_writer": "coordinator_only",
        "worker_reuse_gate": "canonical_fsync_ack",
        "physical_commit_order": "worker_completion_event_order",
        "logical_identity_order": "frozen_record_attempt_index",
        "spool_recovery": "mandatory_canonicalize_or_disposition_before_resume",
        "spool_tail_recovery": "hash_bound_truncate_then_disposition",
        "nested_atomic_temp_recovery": "hash_bound_promote_or_quarantine",
        "recovery_ledger_tail_recovery": "immutable_json_then_truncate",
        "recovery_ledger_initial_temp": "pre_network_promote_or_discard_prefix",
        "worker_quiescence": "stable_flock_shared_worker_exclusive_recovery",
        "worker_fence": "immutable_epoch_plus_durable_fence_before_spool_read",
        "worker_quiescence_timeout_seconds": int(
            WORKER_QUIESCENCE_TIMEOUT_SECONDS
        ),
        "worker_network_gate": "shared_lease_epoch_and_absent_fence_before_client",
        "scheduler": _scheduler_contract(),
        "sharding": _sharding_contract(shard_count, shard_index),
        "parallel_requirements_file": PARALLEL_REQUIREMENTS_PATH.name,
        "parallel_requirements_sha256": (
            EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        ),
        "parallel_dependencies": parallel_dependencies,
        "bytes_retained": False,
        "gpu_used": False,
    }


def _write_new_json(path: Path, payload: object, mode: int = 0o600) -> None:
    flags = (
        os.O_CREAT
        | os.O_EXCL
        | os.O_WRONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    try:
        descriptor = os.open(path, flags, mode)
    except OSError as exc:
        raise AUDIT.AuditError("PARALLEL_JSON_CREATE_FAILED", AUDIT.EXIT_SAFETY) from exc
    try:
        offset = 0
        while offset < len(encoded):
            offset += os.write(descriptor, encoded[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    AUDIT.fsync_directory(path.parent)


def _parallel_contract(
    args: argparse.Namespace,
    bindings: dict[str, Any],
    source_rows: list[dict[str, str]],
    user_agent: str,
    profile_sha256: str,
) -> dict[str, Any]:
    policy = AUDIT.load_json(args.policy)
    host_rate_policy = {
        host: {
            key: config[key]
            for key in (
                "policy_state",
                "roles",
                "min_interval_seconds",
                "rate_group",
                "max_bytes_per_hour",
                "max_bytes_per_day",
                "full_run_contact_recommended",
            )
            if key in config
        }
        for host, config in sorted(policy.get("hosts", {}).items())
    }
    return {
        "schema": "coverfish.pointer-audit-contract.v3",
        "tool_version": AUDIT.TOOL_VERSION,
        "tool_sha256": EXPECTED_PRODUCER_SHA256,
        "requirements_sha256": file_sha256(ROOT / "requirements-pointer-audit.txt"),
        "dependencies": AUDIT.dependency_report(),
        "parallel_requirements_sha256": (
            EXPECTED_PARALLEL_REQUIREMENTS_SHA256
        ),
        "parallel_dependencies": _parallel_dependency_report(),
        "runtime": AUDIT.runtime_report(),
        "phash_algorithm": AUDIT.PHASH_ALGORITHM,
        "scope": args.scope,
        "scope_rows": len(source_rows),
        "scope_ids_sha256": AUDIT.scope_ids_sha256(source_rows),
        "dataset": bindings.get("dataset", {}),
        "bindings_sha256": file_sha256(args.bindings),
        "manifest_sha256": file_sha256(args.manifest) if args.manifest else None,
        "policy_sha256": file_sha256(args.policy),
        "host_rate_policy": host_rate_policy,
        "transient_backoff": {
            "strategy": "exponential_per_host_persisted",
            "base": "configured_host_min_interval_seconds",
            "circuit_threshold": AUDIT.TRANSIENT_CIRCUIT_THRESHOLD,
            "circuit_min_seconds": AUDIT.TRANSIENT_CIRCUIT_MIN_SECONDS,
            "maximum_seconds": AUDIT.TRANSIENT_BACKOFF_MAX_SECONDS,
            "reset_on_nontransient_response": True,
        },
        "execution_profile_sha256": profile_sha256,
        "execution_profile_schema": "coverfish.pointer-parallel-execution-profile.v2",
        "parallel_scheduler": _scheduler_contract(),
        "parallel_sharding": _sharding_contract(
            args.shard_count, args.shard_index
        ),
        "user_agent": user_agent,
        "bytes_retained": False,
        "gpu_used": False,
    }


def ensure_parallel_output_contract(
    output_dir: Path,
    args: argparse.Namespace,
    bindings: dict[str, Any],
    source_rows: list[dict[str, str]],
    user_agent: str,
) -> dict[str, Any]:
    profile_path = output_dir / EXECUTION_PROFILE_NAME
    contract_path = output_dir / "audit-contract.json"
    profile = _execution_profile(args.shard_count, args.shard_index)
    occupied = any(output_dir.iterdir())
    if profile_path.exists():
        existing_profile = AUDIT.load_json(profile_path)
        if existing_profile != profile:
            raise AUDIT.AuditError("PARALLEL_EXECUTION_PROFILE_MISMATCH", AUDIT.EXIT_SAFETY)
    else:
        if occupied:
            raise AUDIT.AuditError("PARALLEL_V04_NEW_OUTPUT_REQUIRED", AUDIT.EXIT_SAFETY)
        _write_new_json(profile_path, profile)
    profile_sha = file_sha256(profile_path)
    contract = _parallel_contract(
        args, bindings, source_rows, user_agent, profile_sha
    )
    if contract_path.exists():
        if AUDIT.load_json(contract_path) != contract:
            raise AUDIT.AuditError("OUTPUT_CONTRACT_MISMATCH", AUDIT.EXIT_SAFETY)
    else:
        unexpected = [
            path.name
            for path in output_dir.iterdir()
            if path.name != EXECUTION_PROFILE_NAME
        ]
        if unexpected:
            raise AUDIT.AuditError("OUTPUT_CONTRACT_MISSING", AUDIT.EXIT_SAFETY)
        AUDIT.atomic_write_json(contract_path, contract)
    return contract


def _scope_rows(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, str]]]:
    bindings, archive_rows = AUDIT.load_bound_rows(args.source_root, args.bindings)
    if args.scope == "pilot":
        if args.manifest is None:
            raise AUDIT.AuditError("PILOT_MANIFEST_REQUIRED", AUDIT.EXIT_USAGE)
        pilot_binding = bindings.get("pilot", {})
        if (
            not isinstance(pilot_binding, dict)
            or file_sha256(args.manifest) != AUDIT.clean_text(pilot_binding.get("sha256"))
            or int(pilot_binding.get("rows", -1)) != 800
        ):
            raise AUDIT.AuditError("PILOT_MANIFEST_BINDING_FAILED")
        sample = AUDIT.load_sample_manifest(args.manifest)
        archive_by_id = {row["record_id"]: row for row in archive_rows}
        if any(
            row["record_id"] not in archive_by_id
            or row["component"] != archive_by_id[row["record_id"]]["component"]
            for row in sample
        ):
            raise AUDIT.AuditError("PILOT_ROW_NOT_IN_FROZEN_INPUT")
        source_rows = [
            archive_by_id[row["record_id"]]
            | {
                "sample_reason": row.get("sample_reason", ""),
                "sample_rank": row.get("sample_rank", ""),
            }
            for row in sample
        ]
    else:
        source_rows = archive_rows
    return bindings, source_rows


def _recover_parallel_recovery_ledger_tail(output_dir: Path) -> dict[str, Any] | None:
    """Recover the recovery ledger without recursively appending to itself."""
    path = output_dir / SPOOL_RECOVERY_FILE
    if not path.exists():
        return None
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AUDIT.AuditError(
            "PARALLEL_RECOVERY_LEDGER_OPEN_FAILED", AUDIT.EXIT_SAFETY
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        original_size = int(metadata.st_size)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERY_LEDGER_INVALID", AUDIT.EXIT_SAFETY
            )
        if original_size == 0:
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERY_LEDGER_HEADER_MISSING", AUDIT.EXIT_SAFETY
            )
        if os.pread(descriptor, 1, original_size - 1) == b"\n":
            return None
        expected_header = "\t".join(SPOOL_RECOVERY_FIELDS).encode() + b"\n"
        if os.pread(descriptor, len(expected_header), 0) != expected_header:
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERY_LEDGER_HEADER_INVALID", AUDIT.EXIT_SAFETY
            )
        position = original_size
        last_newline = -1
        while position > 0 and last_newline < 0:
            start = max(0, position - 64 * 1024)
            block = os.pread(descriptor, position - start, start)
            index = block.rfind(b"\n")
            if index >= 0:
                last_newline = start + index
            position = start
        if last_newline < len(expected_header) - 1:
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERY_LEDGER_HEADER_TRUNCATED", AUDIT.EXIT_SAFETY
            )
        retained_size = last_newline + 1
        fragment = bytearray()
        offset = retained_size
        while offset < original_size:
            block = os.pread(
                descriptor, min(1024 * 1024, original_size - offset), offset
            )
            if not block:
                raise AUDIT.AuditError("PARALLEL_RECOVERY_LEDGER_READ_FAILED")
            fragment.extend(block)
            offset += len(block)
        fragment_sha = hashlib.sha256(fragment).hexdigest()
        discarded_size = len(fragment)
        recovery_id = hashlib.sha256(
            (
                f"{SPOOL_RECOVERY_FILE}\0{original_size}\0{retained_size}\0"
                f"{discarded_size}\0{fragment_sha}"
            ).encode()
        ).hexdigest()[:24]
        receipt_path = output_dir / (
            f"{RECOVERY_LEDGER_TAIL_PREFIX}{recovery_id}.json"
        )
        expected_without_time = {
            "schema": RECOVERY_LEDGER_TAIL_SCHEMA,
            "recovery_id": recovery_id,
            "ledger": SPOOL_RECOVERY_FILE,
            "original_size_bytes": original_size,
            "retained_size_bytes": retained_size,
            "discarded_fragment_bytes": discarded_size,
            "discarded_fragment_sha256": fragment_sha,
        }
        for temporary in sorted(
            output_dir.glob(f".{receipt_path.name}.*.tmp")
        ):
            if not _safe_private_file(temporary):
                raise AUDIT.AuditError(
                    "PARALLEL_RECOVERY_RECEIPT_TEMP_INVALID", AUDIT.EXIT_SAFETY
                )
            # The damaged source fragment is still intact and authoritative;
            # an interrupted redundant JSON write is therefore safe to retry.
            temporary.unlink()
            AUDIT.fsync_directory(output_dir)
        if receipt_path.exists():
            if not _safe_private_file(receipt_path):
                raise AUDIT.AuditError(
                    "PARALLEL_RECOVERY_RECEIPT_INVALID", AUDIT.EXIT_SAFETY
                )
            receipt = AUDIT.load_json(receipt_path)
            if (
                {key: receipt.get(key) for key in expected_without_time}
                != expected_without_time
                or set(receipt) != {*expected_without_time, "detected_at_utc"}
                or not AUDIT.is_utc_timestamp(
                    AUDIT.clean_text(receipt.get("detected_at_utc"))
                )
            ):
                raise AUDIT.AuditError(
                    "PARALLEL_RECOVERY_RECEIPT_INVALID", AUDIT.EXIT_SAFETY
                )
        else:
            receipt = expected_without_time | {"detected_at_utc": utc_now()}
            AUDIT.atomic_write_json(receipt_path, receipt)
            os.chmod(receipt_path, 0o600)
        current = os.fstat(descriptor)
        if (
            current.st_dev != metadata.st_dev
            or current.st_ino != metadata.st_ino
            or current.st_size != original_size
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERY_LEDGER_CHANGED", AUDIT.EXIT_SAFETY
            )
        os.ftruncate(descriptor, retained_size)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    AUDIT.fsync_directory(output_dir)
    return receipt


def _recover_initial_spool_recovery_ledger_temps(output_dir: Path) -> None:
    """Close the pre-network atomic-create crash window for this ledger."""
    target = output_dir / SPOOL_RECOVERY_FILE
    pattern = re.compile(
        rf"\.{re.escape(SPOOL_RECOVERY_FILE)}\.[A-Za-z0-9_-]+\.tmp\Z"
    )
    temporaries = [
        path
        for path in sorted(output_dir.iterdir(), key=lambda item: item.name)
        if pattern.fullmatch(path.name)
    ]
    if not temporaries:
        return
    # This atomic write occurs before invocation metadata, work manifests, or
    # any worker can exist.  Outside that provable pre-network state, do not
    # guess why the canonical ledger is absent or why a temp exists.
    if (
        target.exists()
        or any(output_dir.glob("run-metadata-*.json"))
        or (output_dir / WORK_ROOT_NAME).exists()
    ):
        raise AUDIT.AuditError(
            "PARALLEL_SPOOL_CREATION_TEMP_UNEXPECTED", AUDIT.EXIT_SAFETY
        )
    expected = ("\t".join(SPOOL_RECOVERY_FIELDS) + "\n").encode("utf-8")
    complete: list[Path] = []
    for temporary in temporaries:
        payload = _read_private_file(temporary)
        if payload == expected:
            complete.append(temporary)
        elif not expected.startswith(payload):
            raise AUDIT.AuditError(
                "PARALLEL_SPOOL_CREATION_TEMP_INVALID", AUDIT.EXIT_SAFETY
            )
    if complete:
        promoted = complete[0]
        os.replace(promoted, target)
        os.chmod(target, 0o600)
    for temporary in temporaries:
        if temporary.exists():
            temporary.unlink()
    AUDIT.fsync_directory(output_dir)


def _fence_parallel_workers(output_dir: Path) -> None:
    """Durably fence every old invocation while holding the EX lease."""
    work_root = output_dir / WORK_ROOT_NAME
    if not work_root.exists():
        return
    if not _safe_private_directory(work_root):
        raise AUDIT.AuditError("PARALLEL_WORK_ROOT_INVALID", AUDIT.EXIT_SAFETY)
    for invocation_root in sorted(work_root.iterdir(), key=lambda path: path.name):
        if (
            not _safe_private_directory(invocation_root)
            or not re.fullmatch(r"[0-9a-f]{16}", invocation_root.name)
        ):
            raise AUDIT.AuditError(
                "PARALLEL_WORK_INVOCATION_INVALID", AUDIT.EXIT_SAFETY
            )
        fence_path = invocation_root / WORKER_FENCE_NAME
        if os.path.lexists(fence_path):
            if not _safe_private_file(fence_path):
                raise AUDIT.AuditError(
                    "PARALLEL_WORKER_FENCE_INVALID", AUDIT.EXIT_SAFETY
                )
        else:
            _write_new_json(
                fence_path,
                {
                    "schema": WORKER_FENCE_SCHEMA,
                    "invocation_id": invocation_root.name,
                    "fenced_at_utc": utc_now(),
                },
            )
        # A final file fsync plus directory fsync is repeated here so a marker
        # left by an interrupted prior fencer is durable before any spool read.
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        descriptor = os.open(fence_path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        AUDIT.fsync_directory(invocation_root)


def _recover_parallel_work_quiesced(
    output_dir: Path,
    source_rows: list[dict[str, str]],
    *,
    timeout_seconds: float = WORKER_QUIESCENCE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    # No spool byte is inspected or changed until every registered worker has
    # released SH.  A late, not-yet-registered worker blocks behind EX and sees
    # the durable marker (or the removed invocation root) before network I/O.
    with _exclusive_worker_recovery_lease(output_dir, timeout_seconds):
        _fence_parallel_workers(output_dir)
        return _recover_parallel_work(output_dir, source_rows)


def _recover_parallel_work(
    output_dir: Path, source_rows: list[dict[str, str]]
) -> dict[str, Any]:
    """Canonicalize every durable prior spool before reading resume state."""
    work_root = output_dir / WORK_ROOT_NAME
    counts: dict[str, Any] = {
        "completed": 0,
        "abandoned": 0,
        "attempts": 0,
        "invocations": [],
    }
    if not work_root.exists():
        return counts
    if not _safe_private_directory(work_root):
        raise AUDIT.AuditError("PARALLEL_WORK_ROOT_INVALID", AUDIT.EXIT_SAFETY)
    by_id = {row["record_id"]: row for row in source_rows}
    contract = AUDIT.load_json(output_dir / "audit-contract.json")
    host_rate_policy = contract.get("host_rate_policy")
    recovery_policy = {"hosts": host_rate_policy}
    contract_sharding = contract.get("parallel_sharding")
    if (
        contract.get("parallel_scheduler") != _scheduler_contract()
        or not isinstance(contract_sharding, dict)
        or not isinstance(host_rate_policy, dict)
    ):
        raise AUDIT.AuditError(
            "PARALLEL_RECOVERED_SCHEDULER_CONTRACT_INVALID",
            AUDIT.EXIT_SAFETY,
        )
    writer = CanonicalLedgerWriter(output_dir)
    for invocation_root in sorted(work_root.iterdir(), key=lambda path: path.name):
        if (
            invocation_root.is_symlink()
            or not _safe_private_directory(invocation_root)
            or not re.fullmatch(r"[0-9a-f]{16}", invocation_root.name)
        ):
            raise AUDIT.AuditError("PARALLEL_WORK_INVOCATION_INVALID", AUDIT.EXIT_SAFETY)
        metadata_paths = list(
            output_dir.glob(f"run-metadata-*-{invocation_root.name}.json")
        )
        if len(metadata_paths) != 1:
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERED_METADATA_MISSING", AUDIT.EXIT_SAFETY
            )
        metadata_hint = AUDIT.load_json(metadata_paths[0])
        hinted_window = AUDIT.clean_text(metadata_hint.get("window_id"))
        hinted_inflight = metadata_hint.get("parallel_max_inflight")
        hinted_started = AUDIT.clean_text(metadata_hint.get("started_at_utc"))
        hinted_epoch = AUDIT.clean_text(
            metadata_hint.get("parallel_worker_epoch")
        )
        if (
            not AUDIT.is_window_id(hinted_window)
            or not isinstance(hinted_inflight, int)
            or isinstance(hinted_inflight, bool)
            or not 1 <= hinted_inflight <= MAX_INFLIGHT
            or not AUDIT.is_utc_timestamp(hinted_started)
            or hinted_epoch
            != _worker_epoch(
                output_dir,
                invocation_root.name,
                hinted_window,
                hinted_started,
            )
            or not _schedule_metadata_valid(metadata_hint)
            or metadata_hint.get("parallel_sharding") != contract_sharding
        ):
            raise AUDIT.AuditError(
                "PARALLEL_RECOVERED_METADATA_INVALID", AUDIT.EXIT_SAFETY
            )
        fence_path = invocation_root / WORKER_FENCE_NAME
        if not _safe_private_file(fence_path):
            raise AUDIT.AuditError(
                "PARALLEL_WORKER_FENCE_REQUIRED", AUDIT.EXIT_SAFETY
            )
        _recover_json_atomic_temps(
            writer,
            invocation_root,
            targets=frozenset({"invocation-work.json", "robots-cache.json"}),
            window_id=hinted_window,
            invocation_id=invocation_root.name,
        )
        manifest_path = invocation_root / "invocation-work.json"
        if not manifest_path.exists():
            if any(path.is_dir() for path in invocation_root.iterdir()):
                raise AUDIT.AuditError(
                    "PARALLEL_WORK_MANIFEST_MISSING_WITH_SPOOL",
                    AUDIT.EXIT_SAFETY,
                )
            _append_spool_recovery(
                writer,
                window_id=hinted_window,
                invocation_id=invocation_root.name,
                record_id="",
                spool_name="",
                artifact_name="invocation-work.json",
                action="discard_prestart_missing_manifest",
                original_size=0,
                retained_size=0,
                discarded_size=0,
                artifact_sha256=hashlib.sha256(b"").hexdigest(),
                requires_safety_review=False,
            )
            for path in list(invocation_root.iterdir()):
                if path.name not in {
                    "robots-cache.json",
                    "robots-cache.json.lock",
                    WORKER_EPOCH_NAME,
                    WORKER_FENCE_NAME,
                }:
                    raise AUDIT.AuditError(
                        "PARALLEL_WORK_TREE_INVALID", AUDIT.EXIT_SAFETY
                    )
                if not _safe_private_file(path):
                    raise AUDIT.AuditError(
                        "PARALLEL_WORK_TREE_INVALID", AUDIT.EXIT_SAFETY
                    )
                path.unlink()
            counts["invocations"].append(
                {
                    "invocation_id": invocation_root.name,
                    "max_inflight": hinted_inflight,
                }
            )
            invocation_root.rmdir()
            AUDIT.fsync_directory(work_root)
            continue
        if not _safe_private_file(manifest_path):
            raise AUDIT.AuditError(
                "PARALLEL_WORK_MANIFEST_INVALID", AUDIT.EXIT_SAFETY
            )
        manifest = AUDIT.load_json(manifest_path)
        if (
            set(manifest)
            != {
                "schema",
                "window_id",
                "invocation_id",
                "max_inflight",
                "worker_epoch",
                "scheduler",
                "sharding",
                "frozen_candidate_rows",
                "frozen_shard_rows",
                "eligible_rows_before_limit",
                "shard_eligible_rows_before_limit",
                "max_rows",
                "full_schedule_sha256",
                "frozen_shard_schedule_sha256",
                "shard_schedule_sha256",
                "selected_schedule_sha256",
                "rows",
            }
            or manifest["schema"] != WORK_MANIFEST_SCHEMA
            or manifest["invocation_id"] != invocation_root.name
            or manifest["window_id"] != hinted_window
            or not AUDIT.is_window_id(str(manifest["window_id"]))
            or not isinstance(manifest["max_inflight"], int)
            or isinstance(manifest["max_inflight"], bool)
            or not 1 <= manifest["max_inflight"] <= MAX_INFLIGHT
            or manifest["max_inflight"] != hinted_inflight
            or manifest["worker_epoch"] != hinted_epoch
            or not isinstance(manifest["rows"], list)
        ):
            raise AUDIT.AuditError("PARALLEL_WORK_MANIFEST_INVALID", AUDIT.EXIT_SAFETY)
        epoch_path = invocation_root / WORKER_EPOCH_NAME
        try:
            epoch_payload = json.loads(_read_private_file(epoch_path).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AUDIT.AuditError(
                "PARALLEL_WORKER_EPOCH_INVALID", AUDIT.EXIT_SAFETY
            ) from exc
        expected_epoch_payload = _worker_epoch_payload(
            _output_identity_sha256(output_dir),
            invocation_root.name,
            hinted_window,
            hinted_epoch,
        )
        if epoch_payload != expected_epoch_payload:
            raise AUDIT.AuditError(
                "PARALLEL_WORKER_EPOCH_INVALID", AUDIT.EXIT_SAFETY
            )
        allowed_root_files = {
            "invocation-work.json",
            "robots-cache.json",
            "robots-cache.json.lock",
            WORKER_EPOCH_NAME,
            WORKER_FENCE_NAME,
        }
        declared_spools: set[str] = set()
        declared_ordinals: set[int] = set()
        declared_records: set[str] = set()
        for item in manifest["rows"]:
            if (
                not isinstance(item, dict)
                or set(item)
                != {
                    "ordinal",
                    "shard_ordinal",
                    "global_ordinal",
                    "source_ordinal",
                    "record_id",
                    "lane_key",
                    "lane_ordinal",
                    "starting_index",
                    "spool_name",
                }
                or not isinstance(item["ordinal"], int)
                or item["ordinal"] < 0
                or not isinstance(item["shard_ordinal"], int)
                or isinstance(item["shard_ordinal"], bool)
                or item["shard_ordinal"] < 0
                or not isinstance(item["global_ordinal"], int)
                or isinstance(item["global_ordinal"], bool)
                or item["global_ordinal"] < 0
                or not isinstance(item["source_ordinal"], int)
                or isinstance(item["source_ordinal"], bool)
                or item["source_ordinal"] < 0
                or not isinstance(item["lane_key"], str)
                or not re.fullmatch(
                    r"(?:rate_group|host):[^/\\\x00]+", item["lane_key"]
                )
                or not isinstance(item["lane_ordinal"], int)
                or isinstance(item["lane_ordinal"], bool)
                or item["lane_ordinal"] < 0
                or not isinstance(item["starting_index"], int)
                or item["starting_index"] <= 0
                or not isinstance(item["record_id"], str)
                or item["record_id"] not in by_id
                or not isinstance(item["spool_name"], str)
                or not re.fullmatch(r"[0-9]{8}-[0-9a-f]{16}", item["spool_name"])
            ):
                raise AUDIT.AuditError("PARALLEL_WORK_MANIFEST_INVALID", AUDIT.EXIT_SAFETY)
            if (
                item["ordinal"] in declared_ordinals
                or item["record_id"] in declared_records
                or item["spool_name"] in declared_spools
            ):
                raise AUDIT.AuditError(
                    "PARALLEL_WORK_MANIFEST_DUPLICATE", AUDIT.EXIT_SAFETY
                )
            declared_ordinals.add(item["ordinal"])
            declared_records.add(item["record_id"])
            declared_spools.add(item["spool_name"])
        if declared_ordinals != set(range(len(manifest["rows"]))):
            raise AUDIT.AuditError(
                "PARALLEL_WORK_MANIFEST_ORDINAL_GAP", AUDIT.EXIT_SAFETY
            )
        components = metadata_hint.get("components")
        if (
            not isinstance(components, list)
            or any(
                not isinstance(component, str)
                or component not in AUDIT.COMPONENT_ORDER
                for component in components
            )
        ):
            raise AUDIT.AuditError(
                "PARALLEL_WORK_MANIFEST_SCHEDULE_INVALID", AUDIT.EXIT_SAFETY
            )
        component_set = set(components)
        candidate_rows = (
            [
                row
                for row in source_rows
                if row.get("component") in component_set
            ]
            if components
            else source_rows
        )
        eligible_record_ids = _eligible_record_ids_at_invocation_start(
            output_dir, metadata_hint, candidate_rows
        )
        if not _work_manifest_schedule_valid(
            manifest,
            metadata_hint,
            source_rows,
            recovery_policy,
            eligible_record_ids,
        ):
            raise AUDIT.AuditError(
                "PARALLEL_WORK_MANIFEST_SCHEDULE_INVALID", AUDIT.EXIT_SAFETY
            )
        children = {path.name for path in invocation_root.iterdir()}
        if children - allowed_root_files - declared_spools:
            raise AUDIT.AuditError("PARALLEL_WORK_TREE_INVALID", AUDIT.EXIT_SAFETY)
        for item in sorted(manifest["rows"], key=lambda value: value["ordinal"]):
            spool = invocation_root / item["spool_name"]
            if not spool.exists():
                continue
            row = by_id[item["record_id"]]
            _recover_json_atomic_temps(
                writer,
                spool,
                targets=frozenset({"worker-result.json", "disposition-stamp.json"}),
                window_id=str(manifest["window_id"]),
                invocation_id=str(manifest["invocation_id"]),
                record_id=row["record_id"],
                spool_name=spool.name,
            )
            for artifact_name, fields in (
                ("pointer-health-attempts.tsv", AUDIT.ATTEMPT_FIELDS),
                ("record-completions.tsv", AUDIT.COMPLETION_FIELDS),
                ("robots.tsv", AUDIT.ROBOTS_FIELDS),
            ):
                _recover_spool_tsv_tail(
                    writer,
                    spool,
                    row,
                    str(manifest["window_id"]),
                    str(manifest["invocation_id"]),
                    artifact_name,
                    fields,
                )
            result_path = spool / "worker-result.json"
            complete = False
            if result_path.exists():
                result = AUDIT.load_json(result_path)
                complete = (
                    result.get("status") == "COMPLETE"
                    and (spool / "record-completions.tsv").exists()
                )
            if complete:
                written = _commit_spool(
                    output_dir,
                    writer,
                    spool,
                    row,
                    str(manifest["window_id"]),
                    str(manifest["invocation_id"]),
                    int(item["starting_index"]),
                )
                counts["completed"] += 1
            else:
                written = _abandon_spool(
                    output_dir,
                    writer,
                    spool,
                    row,
                    str(manifest["window_id"]),
                    str(manifest["invocation_id"]),
                    int(item["starting_index"]),
                )
                if written:
                    counts["abandoned"] += 1
            counts["attempts"] += written
        for name in (
            "robots-cache.json",
            "robots-cache.json.lock",
            "invocation-work.json",
            WORKER_EPOCH_NAME,
            WORKER_FENCE_NAME,
        ):
            path = invocation_root / name
            if path.exists():
                if path.is_symlink() or not path.is_file():
                    raise AUDIT.AuditError("PARALLEL_WORK_TREE_INVALID", AUDIT.EXIT_SAFETY)
                if not _safe_private_file(path):
                    raise AUDIT.AuditError("PARALLEL_WORK_TREE_INVALID", AUDIT.EXIT_SAFETY)
                path.unlink()
        if any(invocation_root.iterdir()):
            raise AUDIT.AuditError("PARALLEL_WORK_RECONCILIATION_INCOMPLETE")
        counts["invocations"].append(
            {
                "invocation_id": str(manifest["invocation_id"]),
                "max_inflight": int(manifest["max_inflight"]),
            }
        )
        invocation_root.rmdir()
        AUDIT.fsync_directory(work_root)
    if not any(work_root.iterdir()):
        work_root.rmdir()
        AUDIT.fsync_directory(output_dir)
    return counts


def _segment_sha256(rows: list[dict[str, str]], fields: Sequence[str]) -> str:
    payload = [[row.get(field, "") for field in fields] for row in rows]
    return canonical_json_sha256(payload)


def _execution_summary_payload(
    output_dir: Path, metadata: dict[str, Any], max_inflight: int
) -> dict[str, Any]:
    invocation_id = str(metadata.get("invocation_id", ""))
    window_id = str(metadata.get("window_id", ""))
    eligible_rows = metadata.get("parallel_eligible_rows_before_limit")
    if (
        metadata.get("schema") != "coverfish.pointer-audit-run.v1"
        or metadata.get("status")
        not in {"COMPLETE", "ERROR", "INTERRUPTED", "ABANDONED_BY_RESUME"}
        or not re.fullmatch(r"[0-9a-f]{16}", invocation_id)
        or not AUDIT.is_window_id(window_id)
        or not 1 <= max_inflight <= MAX_INFLIGHT
        or metadata.get("parallel_max_inflight") != max_inflight
        or metadata.get("parallel_worker_epoch")
        != _worker_epoch(
            output_dir,
            invocation_id,
            window_id,
            str(metadata.get("started_at_utc", "")),
        )
        or not _schedule_metadata_valid(metadata)
    ):
        raise AUDIT.AuditError("PARALLEL_EXECUTION_SUMMARY_INPUT_INVALID")
    attempts = [
        row
        for row in AUDIT.read_attempts(output_dir / "pointer-health-attempts.tsv")
        if row.get("invocation_id") == invocation_id
    ]
    completions = [
        row
        for row in AUDIT.read_completions(output_dir / "record-completions.tsv")
        if row.get("invocation_id") == invocation_id
    ]
    dispositions = [
        row
        for row in AUDIT.read_dispositions(output_dir / "attempt-dispositions.tsv")
        if row.get("invocation_id") == invocation_id
    ]
    robots = [
        row
        for row in AUDIT.read_robots(output_dir / "robots.tsv")
        if row.get("invocation_id") == invocation_id
    ]
    spool_recoveries: list[dict[str, str]] = []
    spool_recovery_path = output_dir / SPOOL_RECOVERY_FILE
    if spool_recovery_path.exists():
        fields, spool_recoveries = AUDIT.read_tsv(spool_recovery_path)
        if tuple(fields) != SPOOL_RECOVERY_FIELDS:
            raise AUDIT.AuditError("PARALLEL_SPOOL_RECOVERY_SCHEMA_INVALID")
        spool_recoveries = [
            row
            for row in spool_recoveries
            if row.get("invocation_id") == invocation_id
        ]
    metadata_paths = list(output_dir.glob(f"run-metadata-*-{invocation_id}.json"))
    if len(metadata_paths) != 1:
        raise AUDIT.AuditError("PARALLEL_EXECUTION_METADATA_BINDING_FAILED")
    work_root = output_dir / WORK_ROOT_NAME
    if work_root.exists() and (work_root / invocation_id).exists():
        raise AUDIT.AuditError("PARALLEL_EXECUTION_SPOOL_NOT_RECONCILED")
    payload: dict[str, Any] = {
        "schema": "coverfish.pointer-parallel-execution-summary.v2",
        "status": metadata["status"],
        "window_id": window_id,
        "invocation_id": invocation_id,
        "runner_version": RUNNER_VERSION,
        "runner_sha256": file_sha256(Path(__file__)),
        "execution_profile_sha256": file_sha256(
            output_dir / EXECUTION_PROFILE_NAME
        ),
        "audit_contract_sha256": file_sha256(output_dir / "audit-contract.json"),
        "run_metadata_sha256": file_sha256(metadata_paths[0]),
        "rows_selected": metadata["rows_selected"],
        "rows_committed": len(completions),
        "attempt_rows_canonicalized": len(attempts),
        "attempt_rows_abandoned": len(dispositions),
        "robots_rows_canonicalized": len(robots),
        "spool_recovery_rows": len(spool_recoveries),
        "max_inflight": max_inflight,
        "worker_epoch": metadata["parallel_worker_epoch"],
        "scheduler": metadata["parallel_scheduler"],
        "sharding": metadata["parallel_sharding"],
        "frozen_candidate_rows": metadata["parallel_frozen_candidate_rows"],
        "frozen_shard_rows": metadata["parallel_frozen_shard_rows"],
        "eligible_rows_before_limit": eligible_rows,
        "shard_eligible_rows_before_limit": metadata[
            "parallel_shard_eligible_rows_before_limit"
        ],
        "full_schedule_sha256": metadata["parallel_full_schedule_sha256"],
        "frozen_shard_schedule_sha256": metadata[
            "parallel_frozen_shard_schedule_sha256"
        ],
        "shard_schedule_sha256": metadata[
            "parallel_shard_schedule_sha256"
        ],
        "selected_schedule_sha256": metadata[
            "parallel_selected_schedule_sha256"
        ],
        "attempt_segment_sha256": _segment_sha256(attempts, AUDIT.ATTEMPT_FIELDS),
        "completion_segment_sha256": _segment_sha256(
            completions, AUDIT.COMPLETION_FIELDS
        ),
        "disposition_segment_sha256": _segment_sha256(
            dispositions, AUDIT.DISPOSITION_FIELDS
        ),
        "robots_segment_sha256": _segment_sha256(robots, AUDIT.ROBOTS_FIELDS),
        "spool_recovery_segment_sha256": _segment_sha256(
            spool_recoveries, SPOOL_RECOVERY_FIELDS
        ),
        "spool_reconciliation_complete": True,
        "bytes_retained": False,
        "gpu_used": False,
    }
    if metadata["status"] in {"ERROR", "INTERRUPTED"}:
        payload["error_code"] = metadata.get("error_code", "")
    return payload


def _write_execution_summary(
    output_dir: Path, metadata: dict[str, Any], max_inflight: int
) -> dict[str, Any]:
    payload = _execution_summary_payload(output_dir, metadata, max_inflight)
    path = output_dir / (
        f"parallel-execution-{metadata['window_id']}-{metadata['invocation_id']}.json"
    )
    if path.exists():
        if AUDIT.load_json(path) != payload:
            raise AUDIT.AuditError("PARALLEL_EXECUTION_SUMMARY_MISMATCH")
    else:
        _write_new_json(path, payload)
    return payload


def _close_stale_parallel_invocations(
    output_dir: Path, attempts_count: int, completions_count: int
) -> None:
    """Close every pre-existing v0.4 invocation, including zero-work starts."""
    AUDIT.close_stale_invocations(
        output_dir, attempts_count, completions_count
    )


def _write_existing_parallel_execution_summaries(output_dir: Path) -> None:
    """Bind summaries only after dispositions have updated stale metadata."""
    for metadata_path in AUDIT.invocation_metadata_files(output_dir):
        metadata = AUDIT.load_json(metadata_path)
        max_inflight = metadata.get("parallel_max_inflight")
        if (
            metadata.get("status") == "RUNNING"
            or not isinstance(max_inflight, int)
            or isinstance(max_inflight, bool)
            or not 1 <= max_inflight <= MAX_INFLIGHT
        ):
            raise AUDIT.AuditError(
                "PARALLEL_STALE_METADATA_INVALID", AUDIT.EXIT_SAFETY
            )
        _write_execution_summary(output_dir, metadata, max_inflight)


def _spool_recovery_safety_rows(output_dir: Path) -> int:
    path = output_dir / SPOOL_RECOVERY_FILE
    fields, rows = AUDIT.read_tsv(path)
    if tuple(fields) != SPOOL_RECOVERY_FIELDS:
        raise AUDIT.AuditError(
            "PARALLEL_SPOOL_RECOVERY_SCHEMA_INVALID", AUDIT.EXIT_SAFETY
        )
    return sum(row.get("requires_safety_review") == "true" for row in rows)


def _require_spool_recovery_safety_clear(output_dir: Path) -> None:
    if _spool_recovery_safety_rows(output_dir):
        raise AUDIT.AuditError(
            "PARALLEL_SPOOL_RECOVERY_REQUIRES_SAFETY_REVIEW",
            AUDIT.EXIT_SAFETY,
        )


def _initialize_rate_state(policy: dict[str, Any]) -> Path:
    runtime_root = AUDIT.secure_runtime_root()
    state_path = runtime_root / "parallel-rate-state-v4.json"
    legacy_path = runtime_root / "rate-state.json"
    legacy_state: dict[str, Any] | None = None
    if not state_path.exists() and legacy_path.exists():
        legacy = SERIAL_RATE_LIMITER(policy["hosts"], legacy_path)
        legacy_state = dict(legacy.state)
    ProcessSafeRateLimiter.initialize_from_legacy(
        policy["hosts"], state_path, legacy_state
    )
    return state_path


def command_audit(args: argparse.Namespace) -> int:
    _validate_shard_values(args.shard_count, args.shard_index)
    if args.shard_count > 1 and args.component:
        raise AUDIT.AuditError(
            "PARALLEL_SHARDED_COMPONENT_SELECTION_FORBIDDEN",
            AUDIT.EXIT_USAGE,
        )
    AUDIT.validate_audit_mode(args)
    if not args.accept_network:
        emit(
            {
                "command": "audit",
                "schema": RUNNER_SCHEMA,
                "status": "PENDING",
                "error": "NETWORK_CONFIRMATION_REQUIRED",
                "runner_version": RUNNER_VERSION,
            }
        )
        return AUDIT.EXIT_SAFETY
    if file_sha256(args.policy) != APPROVED_POLICY_SHA256:
        raise AUDIT.AuditError("PARALLEL_POLICY_NOT_APPROVED", AUDIT.EXIT_SAFETY)
    if not _parallel_dependency_report()["ready"]:
        raise AUDIT.AuditError(
            "PARALLEL_DEPENDENCIES_NOT_PINNED", AUDIT.EXIT_DEPENDENCY
        )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    AUDIT.validate_output_directory(args.output_dir)
    with AUDIT.exclusive_audit_lock():
        return command_audit_locked(args)


def command_audit_locked(args: argparse.Namespace) -> int:
    bindings, source_rows = _scope_rows(args)
    if not AUDIT.dependency_report()["ready"]:
        raise AUDIT.AuditError(
            "POINTER_AUDIT_DEPENDENCIES_NOT_PINNED", AUDIT.EXIT_DEPENDENCY
        )
    if not AUDIT.runtime_report()["ready"]:
        raise AUDIT.AuditError(
            "POINTER_AUDIT_RUNTIME_NOT_SUPPORTED", AUDIT.EXIT_DEPENDENCY
        )
    candidate_rows = (
        [row for row in source_rows if row["component"] in set(args.component)]
        if args.component
        else source_rows
    )
    policy = AUDIT.load_json(args.policy)
    AUDIT.validate_host_policy(policy)
    expected_user_agent = (
        f"{policy.get('user_agent_product')} (+{policy.get('public_contact_url')})"
    )
    user_agent = args.user_agent or expected_user_agent
    if user_agent != expected_user_agent or any(char in user_agent for char in "\r\n"):
        raise AUDIT.AuditError("PUBLIC_USER_AGENT_REQUIRED", AUDIT.EXIT_SAFETY)
    ensure_parallel_output_contract(
        args.output_dir, args, bindings, source_rows, user_agent
    )
    AUDIT.prepare_append_ledgers(args.output_dir)
    _recover_initial_spool_recovery_ledger_temps(args.output_dir)
    for name, fields in (
        ("pointer-health-attempts.tsv", AUDIT.ATTEMPT_FIELDS),
        ("record-completions.tsv", AUDIT.COMPLETION_FIELDS),
        ("attempt-dispositions.tsv", AUDIT.DISPOSITION_FIELDS),
        ("robots.tsv", AUDIT.ROBOTS_FIELDS),
        ("atomic-temp-recoveries.tsv", AUDIT.ATOMIC_TEMP_RECOVERY_FIELDS),
        (SPOOL_RECOVERY_FILE, SPOOL_RECOVERY_FIELDS),
    ):
        path = args.output_dir / name
        if not path.exists():
            AUDIT.atomic_write_tsv(path, fields, [])
    _recover_parallel_recovery_ledger_tail(args.output_dir)
    AUDIT.recover_orphan_atomic_temps(args.output_dir)
    _recover_parallel_work_quiesced(args.output_dir, source_rows)
    attempts_path = args.output_dir / "pointer-health-attempts.tsv"
    completions_path = args.output_dir / "record-completions.tsv"
    dispositions_path = args.output_dir / "attempt-dispositions.tsv"
    existing = AUDIT.read_attempts(attempts_path)
    completions = AUDIT.read_completions(completions_path)
    dispositions = AUDIT.read_dispositions(dispositions_path)
    robots = AUDIT.read_robots(args.output_dir / "robots.tsv")
    run_metadata = AUDIT.load_and_validate_invocation_metadata(
        args.output_dir, existing, completions, dispositions, robots
    )
    if any(row.get("window_id") == args.window_id for row in run_metadata):
        raise AUDIT.AuditError("WINDOW_ID_ALREADY_USED", AUDIT.EXIT_USAGE)
    AUDIT.validate_resume_state(source_rows, existing, completions, dispositions)
    _close_stale_parallel_invocations(
        args.output_dir, len(existing), len(completions)
    )
    dispositions = AUDIT.reconcile_incomplete_attempts(
        args.output_dir, existing, completions, dispositions
    )
    AUDIT.validate_resume_state(
        source_rows, existing, completions, dispositions, require_decided=True
    )
    _write_existing_parallel_execution_summaries(args.output_dir)
    _require_spool_recovery_safety_clear(args.output_dir)
    started = utc_now()
    invocation_id = hashlib.sha256(
        f"{args.window_id}\0{started}\0{time.time_ns()}\0{len(existing)}\0parallel-v04".encode()
    ).hexdigest()[:16]
    output_identity_sha256 = _output_identity_sha256(args.output_dir)
    worker_epoch = _worker_epoch(
        args.output_dir, invocation_id, args.window_id, started
    )
    by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    counts: Counter[str] = Counter()
    committed_existing = AUDIT.select_committed_attempts(existing, completions)
    completed_in_window = {
        row.get("record_id", "")
        for row in completions
        if row.get("window_id") == args.window_id
    }
    abandoned_unrecovered = AUDIT.unrecovered_abandoned_record_ids(
        existing, completions, dispositions
    )
    for attempt in committed_existing:
        by_record[attempt.get("record_id", "")].append(attempt)
    for attempt in existing:
        counts[attempt.get("record_id", "")] += 1
    eligible_rows = [
        row
        for row in candidate_rows
        if row["record_id"] not in completed_in_window
        and (
            row["record_id"] in abandoned_unrecovered
            or AUDIT.should_process(
                by_record.get(row["record_id"], []), args.retry_mode
            )
        )
    ]
    source_ordinals = {
        row["record_id"]: ordinal for ordinal, row in enumerate(source_rows)
    }
    full_schedule = _schedule_rows(candidate_rows, policy, source_ordinals)
    frozen_shard_schedule = _select_shard(
        full_schedule, args.shard_count, args.shard_index
    )
    eligible_record_ids = {row["record_id"] for row in eligible_rows}
    shard_schedule = _eligible_shard_schedule(
        frozen_shard_schedule, eligible_record_ids
    )
    selected_schedule = (
        shard_schedule[: args.max_rows]
        if args.max_rows is not None
        else shard_schedule
    )
    selected = [entry["row"] for entry in selected_schedule]
    rate_state_path = _initialize_rate_state(policy)
    metadata = {
        "schema": "coverfish.pointer-audit-run.v1",
        "status": "RUNNING",
        "invocation_id": invocation_id,
        "tool_version": AUDIT.TOOL_VERSION,
        "tool_sha256": EXPECTED_PRODUCER_SHA256,
        "phash_algorithm": AUDIT.PHASH_ALGORITHM,
        "dependencies": AUDIT.dependency_report(),
        "runtime": AUDIT.runtime_report(),
        "window_id": args.window_id,
        "scope": args.scope,
        "started_at_utc": started,
        "completed_at_utc": None,
        "rows_selected": len(selected),
        "rows_processed": 0,
        "attempts_before": len(existing),
        "attempts_after": None,
        "attempts_written": None,
        "completions_before": len(completions),
        "completions_after": None,
        "retry_mode": args.retry_mode,
        "max_rows": args.max_rows,
        "components": sorted(args.component or []),
        "final_window": args.final_window,
        "policy_sha256": file_sha256(args.policy),
        "bindings_sha256": file_sha256(args.bindings),
        "manifest_sha256": file_sha256(args.manifest) if args.manifest else None,
        "contract_sha256": file_sha256(args.output_dir / "audit-contract.json"),
        "user_agent": user_agent,
        "bytes_retained": False,
        "gpu_used": False,
        "parallel_max_inflight": args.max_inflight,
        "parallel_worker_epoch": worker_epoch,
        "parallel_scheduler": _scheduler_contract(),
        "parallel_sharding": _sharding_contract(
            args.shard_count, args.shard_index
        ),
        "parallel_frozen_candidate_rows": len(full_schedule),
        "parallel_frozen_shard_rows": len(frozen_shard_schedule),
        "parallel_eligible_rows_before_limit": len(eligible_rows),
        "parallel_shard_eligible_rows_before_limit": len(shard_schedule),
        "parallel_full_schedule_sha256": _schedule_sha256(full_schedule),
        "parallel_frozen_shard_schedule_sha256": (
            _frozen_shard_schedule_sha256(frozen_shard_schedule)
        ),
        "parallel_shard_schedule_sha256": _shard_schedule_sha256(
            shard_schedule
        ),
        "parallel_selected_schedule_sha256": _shard_schedule_sha256(
            selected_schedule
        ),
    }
    metadata_path = (
        args.output_dir
        / f"run-metadata-{args.window_id}-{invocation_id}.json"
    )
    AUDIT.atomic_write_json(metadata_path, metadata)
    processed = 0
    attempts_written = 0
    work_root = args.output_dir / WORK_ROOT_NAME
    if not work_root.exists():
        work_root.mkdir(mode=0o700)
    os.chmod(work_root, 0o700)
    if not _safe_private_directory(work_root):
        raise AUDIT.AuditError("PARALLEL_WORK_ROOT_INVALID", AUDIT.EXIT_SAFETY)
    spool_root = work_root / invocation_id
    spool_root.mkdir(mode=0o700)
    worker_epoch_path = spool_root / WORKER_EPOCH_NAME
    worker_fence_path = spool_root / WORKER_FENCE_NAME
    _write_new_json(
        worker_epoch_path,
        _worker_epoch_payload(
            output_identity_sha256,
            invocation_id,
            args.window_id,
            worker_epoch,
        ),
    )
    tasks: list[dict[str, Any]] = []
    for entry in selected_schedule:
        ordinal = int(entry["ordinal"])
        row = entry["row"]
        token = hashlib.sha256(row["record_id"].encode()).hexdigest()[:16]
        spool = spool_root / f"{ordinal:08d}-{token}"
        tasks.append(
            {
                "ordinal": ordinal,
                "row": row,
                "window_id": args.window_id,
                "invocation_id": invocation_id,
                "starting_index": counts[row["record_id"]] + 1,
                "spool": str(spool),
                "shard_ordinal": entry["shard_ordinal"],
                "global_ordinal": entry["global_ordinal"],
                "source_ordinal": entry["source_ordinal"],
                "lane_key": entry["lane_key"],
                "lane_ordinal": entry["lane_ordinal"],
            }
        )
    AUDIT.atomic_write_json(
        spool_root / "invocation-work.json",
        {
            "schema": WORK_MANIFEST_SCHEMA,
            "window_id": args.window_id,
            "invocation_id": invocation_id,
            "max_inflight": args.max_inflight,
            "worker_epoch": worker_epoch,
            "scheduler": _scheduler_contract(),
            "sharding": _sharding_contract(
                args.shard_count, args.shard_index
            ),
            "frozen_candidate_rows": len(full_schedule),
            "frozen_shard_rows": len(frozen_shard_schedule),
            "eligible_rows_before_limit": len(eligible_rows),
            "shard_eligible_rows_before_limit": len(shard_schedule),
            "max_rows": args.max_rows,
            "full_schedule_sha256": _schedule_sha256(full_schedule),
            "frozen_shard_schedule_sha256": (
                _frozen_shard_schedule_sha256(frozen_shard_schedule)
            ),
            "shard_schedule_sha256": _shard_schedule_sha256(
                shard_schedule
            ),
            "selected_schedule_sha256": _shard_schedule_sha256(
                selected_schedule
            ),
            "rows": [
                {
                    "ordinal": task["ordinal"],
                    "shard_ordinal": task["shard_ordinal"],
                    "global_ordinal": task["global_ordinal"],
                    "source_ordinal": task["source_ordinal"],
                    "record_id": task["row"]["record_id"],
                    "lane_key": task["lane_key"],
                    "lane_ordinal": task["lane_ordinal"],
                    "starting_index": task["starting_index"],
                    "spool_name": Path(task["spool"]).name,
                }
                for task in tasks
            ],
        },
    )
    robots_state_path = spool_root / "robots-cache.json"
    worker_lease_path = _worker_lease_path(args.output_dir)
    canonical_writer = CanonicalLedgerWriter(args.output_dir)
    executor: concurrent.futures.ProcessPoolExecutor | None = None
    try:
        context = multiprocessing.get_context("spawn")
        executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=args.max_inflight,
            mp_context=context,
            initializer=_worker_initialize,
            initargs=(
                policy,
                user_agent,
                args.window_id,
                invocation_id,
                str(rate_state_path),
                str(robots_state_path),
                str(worker_lease_path),
                str(worker_epoch_path),
                str(worker_fence_path),
                output_identity_sha256,
                worker_epoch,
            ),
        )
        pending: dict[concurrent.futures.Future[dict[str, Any]], int] = {}
        next_submit = 0

        def submit_available() -> None:
            nonlocal next_submit
            while next_submit < len(tasks) and len(pending) < args.max_inflight:
                Path(tasks[next_submit]["spool"]).mkdir(mode=0o700)
                future = executor.submit(_worker_audit_row, tasks[next_submit])
                pending[future] = next_submit
                next_submit += 1

        submit_available()
        while pending:
            done, _ = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            completed_events: list[tuple[int, dict[str, Any]]] = []
            for future in done:
                ordinal = pending.pop(future)
                result = future.result()
                if result.get("status") != "COMPLETE" or result.get("ordinal") != ordinal:
                    raise AUDIT.AuditError(
                        f"PARALLEL_WORKER_FAILED_{result.get('error', 'UNKNOWN')}",
                        AUDIT.EXIT_NETWORK,
                    )
                completed_events.append((ordinal, result))
            # All workers represented in ``done`` remain idle until every one
            # of their durable spools is in the canonical ledger.  Only then
            # are replacement tasks submitted, providing an explicit fsync ACK
            # without a thread-unsafe callback path.
            for ordinal, _ in completed_events:
                task = tasks[ordinal]
                row = task["row"]
                written = _commit_spool(
                    args.output_dir,
                    canonical_writer,
                    Path(task["spool"]),
                    row,
                    args.window_id,
                    invocation_id,
                    int(task["starting_index"]),
                )
                attempts_written += written
                counts[row["record_id"]] += written
                processed += 1
                if processed % 25 == 0:
                    print(
                        f"committed {processed}/{len(selected)} rows after worker fsync ACK",
                        file=sys.stderr,
                        flush=True,
                    )
            submit_available()
        if processed != len(tasks):
            raise AUDIT.AuditError("PARALLEL_COMMIT_ORDER_INCOMPLETE")
        executor.shutdown(wait=True, cancel_futures=False)
        executor = None
        summary = AUDIT.summarize_to_files(
            bindings,
            source_rows,
            args.output_dir,
            args.scope,
            args.final_window,
            {args.window_id} if args.final_window else set(),
        )
    except BaseException as exc:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        _recover_parallel_work_quiesced(args.output_dir, source_rows)
        recovered_attempts = AUDIT.read_attempts(attempts_path)
        recovered_completions = AUDIT.read_completions(completions_path)
        recovered_dispositions = AUDIT.read_dispositions(dispositions_path)
        AUDIT.validate_resume_state(
            source_rows,
            recovered_attempts,
            recovered_completions,
            recovered_dispositions,
            require_decided=True,
        )
        attempts_after = len(recovered_attempts)
        completions_after = len(recovered_completions)
        processed = completions_after - len(completions)
        metadata.update(
            {
                "status": "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "ERROR",
                "completed_at_utc": utc_now(),
                "rows_processed": processed,
                "attempts_after": attempts_after,
                "attempts_written": attempts_after - len(existing),
                "completions_after": completions_after,
                "error_code": exc.code if isinstance(exc, AUDIT.AuditError) else type(exc).__name__,
            }
        )
        AUDIT.atomic_write_json(metadata_path, metadata)
        _write_execution_summary(
            args.output_dir, metadata, args.max_inflight
        )
        raise
    attempts_after = len(AUDIT.read_attempts(attempts_path))
    completions_after = len(AUDIT.read_completions(completions_path))
    metadata.update(
        {
            "status": "COMPLETE",
            "completed_at_utc": utc_now(),
            "rows_processed": processed,
            "attempts_after": attempts_after,
            "attempts_written": attempts_after - len(existing),
            "completions_after": completions_after,
        }
    )
    AUDIT.atomic_write_json(metadata_path, metadata)
    _recover_parallel_work_quiesced(args.output_dir, source_rows)
    _write_execution_summary(args.output_dir, metadata, args.max_inflight)
    emit(
        {
            "command": "audit",
            "schema": RUNNER_SCHEMA,
            "status": summary["status"],
            "runner_version": RUNNER_VERSION,
            "window_id": args.window_id,
            "rows_selected": len(selected),
            "rows_processed": processed,
            "max_inflight": args.max_inflight,
            "summary": summary,
        }
    )
    return AUDIT.EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = RunnerArgumentParser(
        description="Run the frozen pointer audit with a process-safe five-worker executor."
    )
    parser.add_argument("--version", action="version", version=RUNNER_VERSION)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--bindings", type=Path, default=AUDIT.DEFAULT_BINDINGS)
    parser.add_argument("--scope", choices=("pilot", "archive"), required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--policy",
        type=Path,
        default=ROOT / "config/host-policy-fishbase-2s-v2.json",
    )
    parser.add_argument("--window-id", type=AUDIT.window_id_value, required=True)
    parser.add_argument(
        "--retry-mode",
        choices=("none", "transient", "nonexact", "all"),
        default="none",
    )
    parser.add_argument("--max-rows", type=AUDIT.nonnegative_int)
    parser.add_argument(
        "--component",
        action="append",
        choices=tuple(AUDIT.COMPONENT_ORDER),
    )
    parser.add_argument("--user-agent")
    parser.add_argument("--max-inflight", type=positive_worker_count, default=5)
    parser.add_argument(
        "--shard-count", type=positive_shard_count, default=1
    )
    parser.add_argument(
        "--shard-index", type=nonnegative_shard_index, default=0
    )
    parser.add_argument("--accept-network", action="store_true")
    parser.add_argument("--final-window", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return command_audit(args)
    except AUDIT.AuditError as exc:
        emit(
            {
                "schema": RUNNER_SCHEMA,
                "status": "ERROR",
                "error": exc.code,
                "runner_version": RUNNER_VERSION,
            }
        )
        return exc.exit_code
    except KeyboardInterrupt:
        emit(
            {
                "schema": RUNNER_SCHEMA,
                "status": "ERROR",
                "error": "INTERRUPTED",
                "runner_version": RUNNER_VERSION,
            }
        )
        return 130
    except Exception:
        emit(
            {
                "schema": RUNNER_SCHEMA,
                "status": "ERROR",
                "error": "UNEXPECTED_RUNTIME_ERROR",
                "runner_version": RUNNER_VERSION,
            }
        )
        return AUDIT.EXIT_NETWORK


if __name__ == "__main__":
    raise SystemExit(main())
