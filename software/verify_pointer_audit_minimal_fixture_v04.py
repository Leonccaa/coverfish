#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify the deterministic production-shaped pointer-receipt smoke fixture."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import stat
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlsplit


SCHEMA = "coverfish.pointer-audit-minimal-fixture-verifier.v1"
VERSION = "0.2.0"
EXPECTED_FILES = {
    "EXPECTED-RESULT.json",
    "README.md",
    "pair-expected.json",
    "shard-0/audit-contract.json",
    "shard-0/pointer-health-attempts.tsv",
    "shard-0/pointer-health.tsv",
    "shard-0/record-completions.tsv",
    "shard-1/audit-contract.json",
    "shard-1/pointer-health-attempts.tsv",
    "shard-1/pointer-health.tsv",
    "shard-1/record-completions.tsv",
}

HEALTH_FIELDS = (
    "record_id", "component", "active", "active_projection_status",
    "canonical_taxon_key", "fishbase25_speccode", "scientific_name",
    "source_image_url", "pointer_url", "source_host", "expected_width",
    "expected_height", "expected_sha256", "expected_phash_hex64",
    "license_normalized", "license_tier", "release_mode", "attribution",
    "attempts", "first_checked_at_utc", "last_checked_at_utc", "resolved_via",
    "final_url", "http_status", "content_type", "magic_type", "actual_bytes",
    "actual_width", "actual_height", "actual_sha256", "actual_phash_hex64",
    "phash_distance", "sha256_match", "final_class", "retryable",
    "resolution_protocol_complete", "resolution_protocol_status",
    "multiwindow_protocol_applies", "distinct_observation_windows",
    "distinct_observation_utc_dates", "observation_elapsed_hours",
    "observed_in_declared_final_window", "latest_observation_is_declared_final",
    "finality_status", "possible_placeholder_cluster", "bytes_retained",
)

ATTEMPT_FIELDS = (
    "attempt_id", "record_id", "component", "active", "window_id",
    "invocation_id", "attempt_index", "checked_at_utc", "request_kind",
    "resolved_via", "url_requested", "url_final", "host", "policy_state",
    "robots_status", "transport_status", "http_status", "redirect_count",
    "redirect_chain_json", "retry_after", "content_type", "content_type_image",
    "magic_type", "decode_status", "actual_bytes", "actual_width", "actual_height",
    "actual_sha256", "actual_phash_hex64", "phash_distance", "sha256_match",
    "identity_class", "error_code", "bytes_retained",
)

COMPLETION_FIELDS = (
    "completion_id", "record_id", "component", "window_id", "invocation_id",
    "first_attempt_index", "last_attempt_index", "attempt_count",
    "resolution_protocol_complete", "resolution_protocol_status",
    "resolver_candidate_count", "fallback_attempt_count", "completed_at_utc",
)

EXPECTED_RECORDS = (
    {
        "record_id": "fixture-active-byte-exact",
        "component": "S1",
        "active": "true",
        "active_projection_status": "active_source_record",
        "expected_sha256": "a" * 64,
        "expected_phash_hex64": "0000000000000000",
        "actual_sha256": "a" * 64,
        "actual_phash_hex64": "",
        "phash_distance": "",
        "sha256_match": "true",
        "final_class": "byte_exact",
        "finality_status": "not_applicable_exact",
        "attempts": "1",
        "resolution_protocol_complete": "true",
        "resolution_protocol_status": "direct_exact",
    },
    {
        "record_id": "fixture-retired-visual-near",
        "component": "S0",
        "active": "false",
        "active_projection_status": "retired_from_active_projection",
        "expected_sha256": "b" * 64,
        "expected_phash_hex64": "0000000000000000",
        "actual_sha256": "c" * 64,
        "actual_phash_hex64": "0000000000000001",
        "phash_distance": "1",
        "sha256_match": "false",
        "final_class": "visual_near_candidate_d0_2",
        "finality_status": "pending_policy",
        "attempts": "2",
        "resolution_protocol_complete": "false",
        "resolution_protocol_status": "pending_policy",
    },
)


class FixtureError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FixtureError("FIXTURE_JSON_INVALID") from exc
    if not isinstance(value, dict):
        raise FixtureError("FIXTURE_JSON_INVALID")
    return value


def read_tsv(path: Path, fields: Sequence[str]) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source, delimiter="\t")
            if reader.fieldnames != list(fields):
                raise FixtureError("FIXTURE_TSV_SCHEMA_INVALID")
            rows = list(reader)
    except FixtureError:
        raise
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise FixtureError("FIXTURE_TSV_SCHEMA_INVALID") from exc
    if any(
        None in row
        or any(value is None or "\n" in value or "\r" in value for value in row.values())
        for row in rows
    ):
        raise FixtureError("FIXTURE_TSV_SCHEMA_INVALID")
    return rows  # type: ignore[return-value]


def parse_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise FixtureError("FIXTURE_BOOLEAN_INVALID")


def parse_nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise FixtureError("FIXTURE_INTEGER_INVALID") from exc
    if parsed < 0 or str(parsed) != value:
        raise FixtureError("FIXTURE_INTEGER_INVALID")
    return parsed


def valid_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def valid_phash(value: str) -> bool:
    return len(value) == 16 and all(character in "0123456789abcdef" for character in value)


def phash_distance(left: str, right: str) -> int:
    if not valid_phash(left) or not valid_phash(right):
        raise FixtureError("FIXTURE_PHASH_INVALID")
    return (int(left, 16) ^ int(right, 16)).bit_count()


def valid_example_url(value: str) -> bool:
    parsed = urlsplit(value)
    return (
        parsed.scheme == "https"
        and parsed.hostname == "example.org"
        and not parsed.username
        and not parsed.password
        and not parsed.fragment
    )


def verify_inventory(root: Path) -> None:
    try:
        entries = {
            path.relative_to(root).as_posix(): path
            for path in root.rglob("*")
            if path.is_file()
        }
    except OSError as exc:
        raise FixtureError("FIXTURE_TREE_INVALID") from exc
    if set(entries) != EXPECTED_FILES | {"FILES.tsv", "SHA256SUMS"}:
        raise FixtureError("FIXTURE_TREE_INVALID")
    for path in entries.values():
        info = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise FixtureError("FIXTURE_TREE_INVALID")
    rows = read_tsv(root / "FILES.tsv", ("path", "bytes", "sha256"))
    paths = [row["path"] for row in rows]
    if paths != sorted(EXPECTED_FILES) or len(paths) != len(set(paths)):
        raise FixtureError("FIXTURE_INVENTORY_INVALID")
    for row in rows:
        path = root / row["path"]
        size = parse_nonnegative_int(row["bytes"])
        if not valid_sha256(row["sha256"]):
            raise FixtureError("FIXTURE_INVENTORY_INVALID")
        if path.stat().st_size != size or sha256_file(path) != row["sha256"]:
            raise FixtureError("FIXTURE_INVENTORY_INVALID")
    expected_sums = "".join(f"{row['sha256']}  {row['path']}\n" for row in rows)
    if (root / "SHA256SUMS").read_text(encoding="utf-8") != expected_sums:
        raise FixtureError("FIXTURE_INVENTORY_INVALID")


def classify_image_attempt(attempt: dict[str, str], expected: dict[str, str]) -> str:
    if not (
        attempt["transport_status"] == "ok"
        and attempt["http_status"] == "200"
        and attempt["content_type"] == "image/jpeg"
        and attempt["content_type_image"] == "true"
        and attempt["magic_type"] == "image/jpeg"
        and parse_nonnegative_int(attempt["actual_bytes"]) > 0
        and valid_sha256(attempt["actual_sha256"])
    ):
        raise FixtureError("FIXTURE_IMAGE_ATTEMPT_INVALID")
    exact = attempt["actual_sha256"] == expected["expected_sha256"]
    if parse_bool(attempt["sha256_match"]) != exact:
        raise FixtureError("FIXTURE_IDENTITY_INVALID")
    if exact:
        if not (
            attempt["decode_status"] == "skipped_byte_exact"
            and attempt["actual_width"] == ""
            and attempt["actual_height"] == ""
            and attempt["actual_phash_hex64"] == ""
            and attempt["phash_distance"] == ""
        ):
            raise FixtureError("FIXTURE_IMAGE_ATTEMPT_INVALID")
        return "byte_exact"
    if not (
        attempt["decode_status"] == "decoded"
        and parse_nonnegative_int(attempt["actual_width"]) > 0
        and parse_nonnegative_int(attempt["actual_height"]) > 0
        and valid_phash(attempt["actual_phash_hex64"])
    ):
        raise FixtureError("FIXTURE_IMAGE_ATTEMPT_INVALID")
    distance = phash_distance(
        expected["expected_phash_hex64"], attempt["actual_phash_hex64"]
    )
    if attempt["phash_distance"] != str(distance):
        raise FixtureError("FIXTURE_PHASH_INVALID")
    if distance <= 2:
        return "visual_near_candidate_d0_2"
    if distance <= 6:
        return "visual_related_candidate_d3_6"
    return "content_changed_candidate_d_gt6"


def verify_health(row: dict[str, str], expected: dict[str, str]) -> None:
    for key, value in expected.items():
        if row[key] != value:
            raise FixtureError("FIXTURE_HEALTH_SEMANTICS_INVALID")
    if not (
        row["canonical_taxon_key"]
        and row["fishbase25_speccode"]
        and row["scientific_name"]
        and valid_example_url(row["source_image_url"])
        and valid_example_url(row["pointer_url"])
        and valid_example_url(row["final_url"])
        and row["source_host"] == "example.org"
        and parse_nonnegative_int(row["expected_width"]) > 0
        and parse_nonnegative_int(row["expected_height"]) > 0
        and parse_nonnegative_int(row["actual_bytes"]) > 0
        and valid_sha256(row["expected_sha256"])
        and valid_sha256(row["actual_sha256"])
        and valid_phash(row["expected_phash_hex64"])
        and row["license_tier"] == "T3_pointer_only"
        and row["release_mode"] == "pointer"
        and row["http_status"] == "200"
        and row["content_type"] == "image/jpeg"
        and row["magic_type"] == "image/jpeg"
        and row["resolved_via"] == "direct"
        and row["retryable"] == "false"
        and row["possible_placeholder_cluster"] == "false"
        and row["bytes_retained"] == "false"
    ):
        raise FixtureError("FIXTURE_HEALTH_SEMANTICS_INVALID")
    exact = row["expected_sha256"] == row["actual_sha256"]
    if parse_bool(row["sha256_match"]) != exact:
        raise FixtureError("FIXTURE_IDENTITY_INVALID")
    if row["final_class"] == "byte_exact":
        if not (
            exact
            and row["finality_status"] == "not_applicable_exact"
            and row["actual_width"] == ""
            and row["actual_height"] == ""
            and row["actual_phash_hex64"] == ""
            and row["phash_distance"] == ""
        ):
            raise FixtureError("FIXTURE_IDENTITY_INVALID")
    elif row["final_class"] == "visual_near_candidate_d0_2":
        if not (
            not exact
            and parse_nonnegative_int(row["actual_width"]) > 0
            and parse_nonnegative_int(row["actual_height"]) > 0
            and valid_phash(row["actual_phash_hex64"])
            and row["phash_distance"]
            == str(
                phash_distance(
                    row["expected_phash_hex64"], row["actual_phash_hex64"]
                )
            )
            and parse_nonnegative_int(row["phash_distance"]) <= 2
        ):
            raise FixtureError("FIXTURE_IDENTITY_INVALID")
        if row["finality_status"] == "not_applicable_exact":
            raise FixtureError("FIXTURE_IDENTITY_INVALID")
    else:
        raise FixtureError("FIXTURE_IDENTITY_INVALID")


def verify_shard(root: Path, index: int) -> dict[str, Any]:
    shard = root / f"shard-{index}"
    contract = load_json(shard / "audit-contract.json")
    if contract != {
        "bytes_retained": False,
        "gpu_used": False,
        "schema": "coverfish.pointer-audit-minimal-fixture-contract.v1",
        "scope": "fixture",
        "scope_rows": 2,
        "shard_count": 2,
        "shard_index": index,
    }:
        raise FixtureError("FIXTURE_CONTRACT_INVALID")

    health = read_tsv(shard / "pointer-health.tsv", HEALTH_FIELDS)
    attempts = read_tsv(shard / "pointer-health-attempts.tsv", ATTEMPT_FIELDS)
    completions = read_tsv(shard / "record-completions.tsv", COMPLETION_FIELDS)
    expected = EXPECTED_RECORDS[index]
    if len(health) != 1 or len(completions) != 1 or len(attempts) != index + 1:
        raise FixtureError("FIXTURE_SHARD_COUNT_INVALID")
    row = health[0]
    completion = completions[0]
    verify_health(row, expected)

    expected_attempt_ids = (
        ("fixture-attempt-0",)
        if index == 0
        else ("fixture-attempt-1", "fixture-attempt-1b")
    )
    if tuple(attempt["attempt_id"] for attempt in attempts) != expected_attempt_ids:
        raise FixtureError("FIXTURE_EVENT_BINDING_INVALID")
    for attempt_index, attempt in enumerate(attempts, 1):
        if not (
            attempt["record_id"] == row["record_id"]
            and attempt["component"] == row["component"]
            and attempt["active"] == row["active"]
            and attempt["attempt_index"] == str(attempt_index)
            and attempt["window_id"] == f"fixture-shard{index}-w1"
            and attempt["invocation_id"] == f"fixture-shard{index}-inv1"
            and parse_nonnegative_int(attempt["redirect_count"]) == 0
            and attempt["redirect_chain_json"] == "[]"
            and attempt["bytes_retained"] == "false"
        ):
            raise FixtureError("FIXTURE_EVENT_BINDING_INVALID")

    derived_class = classify_image_attempt(attempts[0], expected)
    if attempts[0]["identity_class"] != derived_class or row["final_class"] != derived_class:
        raise FixtureError("FIXTURE_IDENTITY_INVALID")
    if not (
        attempts[0]["request_kind"] == "direct_image"
        and attempts[0]["resolved_via"] == "direct"
        and attempts[0]["policy_state"] == "allow_with_limits"
        and attempts[0]["robots_status"] == "allowed_no_robots"
        and attempts[0]["url_requested"] == row["source_image_url"]
        and attempts[0]["url_final"] == row["final_url"]
        and attempts[0]["host"] == row["source_host"]
        and attempts[0]["error_code"] == ""
    ):
        raise FixtureError("FIXTURE_IMAGE_ATTEMPT_INVALID")
    if index == 1:
        resolver = attempts[1]
        if not (
            resolver["request_kind"] == "resolver_page"
            and resolver["resolved_via"] == "photo_landing"
            and valid_example_url(resolver["url_requested"])
            and resolver["url_final"] == ""
            and resolver["host"] == "example.org"
            and resolver["policy_state"] == "pending_permission"
            and resolver["robots_status"] == ""
            and resolver["transport_status"] == "policy_pending"
            and resolver["http_status"] == ""
            and resolver["content_type"] == ""
            and resolver["content_type_image"] == "false"
            and resolver["magic_type"] == ""
            and resolver["decode_status"] == "not_attempted"
            and resolver["actual_bytes"] == ""
            and resolver["actual_width"] == ""
            and resolver["actual_height"] == ""
            and resolver["actual_sha256"] == ""
            and resolver["actual_phash_hex64"] == ""
            and resolver["phash_distance"] == ""
            and resolver["sha256_match"] == ""
            and resolver["identity_class"] == "resolver_error"
            and resolver["error_code"] == "SOURCE_POLICY_NOT_AUTHORIZED"
        ):
            raise FixtureError("FIXTURE_RESOLVER_ATTEMPT_INVALID")

    expected_completion = {
        "completion_id": f"fixture-completion-{index}",
        "record_id": row["record_id"],
        "component": row["component"],
        "window_id": f"fixture-shard{index}-w1",
        "invocation_id": f"fixture-shard{index}-inv1",
        "first_attempt_index": "1",
        "last_attempt_index": str(len(attempts)),
        "attempt_count": str(len(attempts)),
        "resolution_protocol_complete": row["resolution_protocol_complete"],
        "resolution_protocol_status": row["resolution_protocol_status"],
        "resolver_candidate_count": "0",
        "fallback_attempt_count": "0",
        "completed_at_utc": "2026-07-24T00:00:01Z" if index == 0 else "2026-07-24T01:00:11Z",
    }
    if completion != expected_completion:
        raise FixtureError("FIXTURE_COMPLETION_INVALID")

    return {
        "record_id": row["record_id"],
        "active": parse_bool(row["active"]),
        "final_class": row["final_class"],
        "finality_status": row["finality_status"],
        "attempt_ids": [attempt["attempt_id"] for attempt in attempts],
        "completion_id": completion["completion_id"],
        "attempt_bytes": sum(
            parse_nonnegative_int(attempt["actual_bytes"])
            if attempt["actual_bytes"]
            else 0
            for attempt in attempts
        ),
        "attempt_rows": len(attempts),
        "exact": row["final_class"] == "byte_exact" and parse_bool(row["sha256_match"]),
        "diagnostic_near": row["final_class"] == "visual_near_candidate_d0_2",
    }


def expected_result() -> dict[str, Any]:
    return {
        "aggregate": {
            "active_rows": 1,
            "attempt_bytes": 1801,
            "attempt_rows": 3,
            "completed_rows": 2,
            "diagnostic_near_rows": 1,
            "exact_rows": 1,
            "outcomes": {"byte_exact": 1, "visual_near_candidate_d0_2": 1},
            "retired_rows": 1,
        },
        "bytes_retained": False,
        "exit_policy": {"FAIL": 6, "PASS": 0},
        "fixture_rows": 2,
        "gpu_used": False,
        "network_accessed": False,
        "schema": SCHEMA,
        "scientific_status": "PENDING",
        "shard_rows": [1, 1],
        "status": "PASS",
        "tool_version": VERSION,
    }


def verify(root: Path) -> dict[str, Any]:
    if not root.is_dir() or root.is_symlink():
        raise FixtureError("FIXTURE_ROOT_INVALID")
    verify_inventory(root)
    rows = [verify_shard(root, index) for index in (0, 1)]
    attempt_ids = [attempt_id for row in rows for attempt_id in row["attempt_ids"]]
    if (
        len({row["record_id"] for row in rows}) != 2
        or len(set(attempt_ids)) != 3
        or len({row["completion_id"] for row in rows}) != 2
    ):
        raise FixtureError("FIXTURE_PAIR_IDS_INVALID")
    aggregate = {
        "active_rows": sum(row["active"] for row in rows),
        "attempt_bytes": sum(row["attempt_bytes"] for row in rows),
        "attempt_rows": sum(row["attempt_rows"] for row in rows),
        "completed_rows": len(rows),
        "diagnostic_near_rows": sum(row["diagnostic_near"] for row in rows),
        "exact_rows": sum(row["exact"] for row in rows),
        "outcomes": dict(sorted(Counter(row["final_class"] for row in rows).items())),
        "retired_rows": sum(not row["active"] for row in rows),
    }
    pair = load_json(root / "pair-expected.json")
    if pair != {
        "aggregate": aggregate,
        "coverage_complete": True,
        "scientific_status": "PENDING",
        "schema": "coverfish.pointer-audit-minimal-pair.v1",
        "shard_rows": [1, 1],
        "status": "PASS",
    }:
        raise FixtureError("FIXTURE_PAIR_INVALID")
    result = expected_result()
    if result["aggregate"] != aggregate:
        raise FixtureError("FIXTURE_AGGREGATE_INVALID")
    if load_json(root / "EXPECTED-RESULT.json") != result:
        raise FixtureError("FIXTURE_EXPECTED_RESULT_INVALID")
    return result


def minimal(error: str) -> dict[str, Any]:
    return {
        "bytes_retained": False,
        "error": error,
        "exit_policy": {"FAIL": 6, "PASS": 0},
        "gpu_used": False,
        "network_accessed": False,
        "schema": SCHEMA,
        "status": "FAIL",
        "tool_version": VERSION,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify the minimal pointer receipt fixture.")
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = verify(args.root.resolve(strict=True))
    except FixtureError as exc:
        result = minimal(exc.code)
    except Exception:
        result = minimal("FIXTURE_INTERNAL_ERROR")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "PASS" else 6


if __name__ == "__main__":
    sys.exit(main())
