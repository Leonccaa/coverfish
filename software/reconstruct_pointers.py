#!/usr/bin/env python3
"""Audit frozen COVER-Fish pointer rows without retaining image bytes.

The command line is deliberately non-interactive. Operational commands emit one
JSON object on stdout; progress is written to stderr. ``plan``, ``sample``, and
``summarize`` are network-free. ``audit`` requires an explicit network consent
flag, enforces a public-host allowlist and robots policy, and keeps response bytes
only in bounded memory while calculating integrity diagnostics.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import email.utils
import fcntl
import hashlib
import html.parser
import http.client
import ipaddress
import importlib.metadata
import json
import math
import os
import platform
import re
import signal
import socket
import ssl
import stat
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import warnings
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, NoReturn

SCHEMA = "coverfish.pointer-audit.v2"
TOOL_VERSION = "0.2.0"
PHASH_ALGORITHM = "ImageHash-4.3.2-phash-hash_size_8-highfreq_factor_4-exif_transpose-rgb"
FINALITY_MIN_WINDOWS = 3
FINALITY_MIN_ELAPSED_HOURS = 48
FINALITY_MIN_UTC_DATES = 3
TRANSIENT_CIRCUIT_THRESHOLD = 3
TRANSIENT_CIRCUIT_MIN_SECONDS = 300
TRANSIENT_BACKOFF_MAX_SECONDS = 900
SENSITIVE_QUERY_KEY = re.compile(
    r"(?:^|[-_])(?:access[-_]?token|token|auth(?:orization)?|credential|"
    r"api[-_]?key|password|secret|session|signature|sig|jwt)(?:$|[-_])|"
    r"^x-amz-(?:credential|signature|security-token)$",
    re.IGNORECASE,
)
RESOLUTION_PROTOCOL_STATUSES = frozenset(
    {
        "direct_exact",
        "fallback_exact",
        "resolver_access_or_absence_observed",
        "resolver_no_candidate",
        "exhausted_nonexact",
        "pending_resolver_adapter",
        "resolver_transient_observed",
        "resolver_invalid_response_observed",
        "resolver_http_error_observed",
        "fallback_transient_observed",
        "pending_local_response_cap",
        "pending_policy",
        "pending_local_deferral",
        "pending_candidate_cap",
        "fail_safety_review",
    }
)
RESOLUTION_PROTOCOL_COMPLETE_STATUSES = frozenset(
    {
        "direct_exact",
        "fallback_exact",
        "resolver_access_or_absence_observed",
        "resolver_no_candidate",
        "resolver_transient_observed",
        "resolver_invalid_response_observed",
        "resolver_http_error_observed",
        "fallback_transient_observed",
        "exhausted_nonexact",
    }
)

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_DEPENDENCY = 3
EXIT_SAFETY = 4
EXIT_NETWORK = 5
EXIT_INTEGRITY = 6

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINDINGS = ROOT / "inputs/input-bindings.json"
DEFAULT_POLICY = ROOT / "config/host-policy.json"

SAMPLE_QUOTAS = {"S1": 500, "S0": 160, "D0": 40, "S3": 60, "S4": 37, "S2": 3}
COMPONENT_ORDER = {name: index for index, name in enumerate(("S0", "S1", "S2", "S3", "S4", "D0"))}
COMPONENT_IMAGE_HOSTS = {
    "S0": frozenset({"static.inaturalist.org", "inaturalist-open-data.s3.amazonaws.com"}),
    "S1": frozenset({"fishbase.se", "www.fishbase.se"}),
    "S2": frozenset({"fws.gov", "www.fws.gov"}),
    "S3": frozenset(),
    "S4": frozenset({"upload.wikimedia.org"}),
    "D0": frozenset({"static.inaturalist.org", "inaturalist-open-data.s3.amazonaws.com"}),
}
COMPONENT_RESOLVER_HOSTS = {
    "S0": frozenset({"api.inaturalist.org", "www.inaturalist.org"}),
    "S1": frozenset({"fishbase.se", "www.fishbase.se"}),
    "S2": frozenset({"fws.gov", "www.fws.gov"}),
    "S3": frozenset({"db.angfa.org.au"}),
    "S4": frozenset({"commons.wikimedia.org"}),
    "D0": frozenset({"api.inaturalist.org", "www.inaturalist.org"}),
}

SAMPLE_FIELDS = (
    "record_id",
    "component",
    "active",
    "active_projection_status",
    "canonical_taxon_key",
    "fishbase25_speccode",
    "scientific_name",
    "source_page_url",
    "source_image_url",
    "pointer_url",
    "source_host",
    "pointer_host",
    "extension_class",
    "expected_width",
    "expected_height",
    "expected_min_side",
    "expected_sha256",
    "expected_phash_hex64",
    "source_pack_version",
    "evidence_id",
    "benchmark_role",
    "public_split",
    "distractor_type",
    "license_normalized",
    "license_tier",
    "release_mode",
    "attribution",
    "retirement_reason",
    "source_line_number",
    "raw_row_sha256",
    "sample_reason",
    "sample_stratum",
    "rank_digest",
    "sample_rank",
)

ATTEMPT_FIELDS = (
    "attempt_id",
    "record_id",
    "component",
    "active",
    "window_id",
    "invocation_id",
    "attempt_index",
    "checked_at_utc",
    "request_kind",
    "resolved_via",
    "url_requested",
    "url_final",
    "host",
    "policy_state",
    "robots_status",
    "transport_status",
    "http_status",
    "redirect_count",
    "redirect_chain_json",
    "retry_after",
    "content_type",
    "content_type_image",
    "magic_type",
    "decode_status",
    "actual_bytes",
    "actual_width",
    "actual_height",
    "actual_sha256",
    "actual_phash_hex64",
    "phash_distance",
    "sha256_match",
    "identity_class",
    "error_code",
    "bytes_retained",
)

HEALTH_FIELDS = (
    "record_id",
    "component",
    "active",
    "active_projection_status",
    "canonical_taxon_key",
    "fishbase25_speccode",
    "scientific_name",
    "source_image_url",
    "pointer_url",
    "source_host",
    "expected_width",
    "expected_height",
    "expected_sha256",
    "expected_phash_hex64",
    "license_normalized",
    "license_tier",
    "release_mode",
    "attribution",
    "attempts",
    "first_checked_at_utc",
    "last_checked_at_utc",
    "resolved_via",
    "final_url",
    "http_status",
    "content_type",
    "magic_type",
    "actual_bytes",
    "actual_width",
    "actual_height",
    "actual_sha256",
    "actual_phash_hex64",
    "phash_distance",
    "sha256_match",
    "final_class",
    "retryable",
    "resolution_protocol_complete",
    "resolution_protocol_status",
    "multiwindow_protocol_applies",
    "distinct_observation_windows",
    "distinct_observation_utc_dates",
    "observation_elapsed_hours",
    "observed_in_declared_final_window",
    "latest_observation_is_declared_final",
    "finality_status",
    "possible_placeholder_cluster",
    "bytes_retained",
)

COMPLETION_FIELDS = (
    "completion_id",
    "record_id",
    "component",
    "window_id",
    "invocation_id",
    "first_attempt_index",
    "last_attempt_index",
    "attempt_count",
    "resolution_protocol_complete",
    "resolution_protocol_status",
    "resolver_candidate_count",
    "fallback_attempt_count",
    "completed_at_utc",
)

DISPOSITION_FIELDS = (
    "attempt_id",
    "record_id",
    "window_id",
    "invocation_id",
    "disposition",
    "recorded_at_utc",
)

ROBOTS_FIELDS = (
    "window_id",
    "invocation_id",
    "checked_at_utc",
    "host",
    "robots_url",
    "http_status",
    "fetch_status",
    "robots_state",
    "error_code",
    "redirect_count",
    "redirect_chain_json",
    "sha256",
)

TAIL_RECOVERY_FIELDS = (
    "recovery_id",
    "ledger",
    "detected_at_utc",
    "original_size_bytes",
    "retained_size_bytes",
    "discarded_fragment_bytes",
    "discarded_fragment_sha256",
)

ATOMIC_TEMP_RECOVERY_FIELDS = (
    "recovery_id",
    "detected_at_utc",
    "temporary_name",
    "target_name",
    "size_bytes",
    "sha256",
)


class AuditError(Exception):
    def __init__(self, code: str, exit_code: int = EXIT_INTEGRITY):
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INVALID_ARGUMENTS"})
        raise SystemExit(EXIT_USAGE)


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def is_utc_timestamp(value: str) -> bool:
    if not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bytes_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise AuditError("JSON_INPUT_SYMLINK_FORBIDDEN")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuditError("JSON_INPUT_INVALID") from exc
    if not isinstance(value, dict):
        raise AuditError("JSON_INPUT_NOT_OBJECT")
    return value


def clean_text(value: object) -> str:
    return str(value or "").replace("\x00", "").strip()


def clean_url(value: object) -> str:
    url = clean_text(value)
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    return url


def sensitive_query_keys(url: str) -> set[str]:
    try:
        pairs = urllib.parse.parse_qsl(
            urllib.parse.urlsplit(url).query, keep_blank_values=True
        )
    except ValueError:
        return set()
    return {
        key
        for key, _ in pairs
        if SENSITIVE_QUERY_KEY.search(urllib.parse.unquote_plus(key))
    }


def receipt_safe_url(url: str) -> str:
    """Remove userinfo and redact any credential-like query value."""
    try:
        parsed = urllib.parse.urlsplit(clean_text(url))
        host = parsed.hostname or ""
        if not host:
            return ""
        netloc = f"[{host}]" if ":" in host else host
        if parsed.port is not None:
            netloc = f"{netloc}:{parsed.port}"
        pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        safe_pairs = [
            (
                key,
                "REDACTED"
                if SENSITIVE_QUERY_KEY.search(urllib.parse.unquote_plus(key))
                else value,
            )
            for key, value in pairs
        ]
        return urllib.parse.urlunsplit(
            (
                parsed.scheme,
                netloc,
                parsed.path,
                urllib.parse.urlencode(safe_pairs, doseq=True),
                "",
            )
        )
    except (TypeError, ValueError):
        return ""


def url_host(url: str) -> str:
    try:
        return (urllib.parse.urlsplit(url).hostname or "").lower().rstrip(".")
    except ValueError:
        return ""


def is_hex(value: str, length: int) -> bool:
    return len(value) == length and all(char in "0123456789abcdef" for char in value.lower())


def is_window_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", value))


def window_id_value(value: str) -> str:
    if not is_window_id(value):
        raise argparse.ArgumentTypeError("invalid window id")
    return value


def nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected a nonnegative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("expected a nonnegative integer")
    return parsed


def read_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if path.is_symlink():
        raise AuditError("TSV_INPUT_SYMLINK_FORBIDDEN")
    try:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source, delimiter="\t")
            if reader.fieldnames is None:
                raise AuditError("TSV_HEADER_MISSING")
            rows = list(reader)
            if any(
                None in row or any(value is None for value in row.values())
                for row in rows
            ):
                raise AuditError("TSV_ROW_WIDTH_INVALID")
            return list(reader.fieldnames), rows
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise AuditError("TSV_READ_FAILED") from exc


def bound_file(source_root: Path, relative: Path) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise AuditError("BINDING_PATH_UNSAFE")
    path = source_root / relative
    try:
        root_resolved = source_root.resolve(strict=True)
        path_resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AuditError("BOUND_FILE_MISSING") from exc
    if path.is_symlink() or not path_resolved.is_relative_to(root_resolved):
        raise AuditError("BOUND_FILE_ESCAPE_OR_SYMLINK")
    return path


def normalize_row(component: str, id_field: str, row: dict[str, str]) -> dict[str, str]:
    record_id = clean_text(row.get(id_field))
    source_image_url = clean_url(row.get("source_image_url"))
    pointer_url = clean_url(row.get("pointer_url"))
    source_page_url = clean_url(row.get("source_page_url"))
    expected_sha = clean_text(row.get("sha256")).lower()
    expected_phash = clean_text(row.get("phash_canonical_hex64")).lower()
    if not record_id:
        raise AuditError("POINTER_RECORD_ID_MISSING")
    if not source_image_url or not pointer_url:
        raise AuditError("POINTER_URL_MISSING")
    if not is_hex(expected_sha, 64):
        raise AuditError("POINTER_SHA256_INVALID")
    if not is_hex(expected_phash, 16):
        raise AuditError("POINTER_PHASH_INVALID")
    active_status = clean_text(row.get("active_projection_status"))
    active = active_status != "retired_from_active_projection"
    return {
        "record_id": record_id,
        "component": component,
        "active": "true" if active else "false",
        "active_projection_status": active_status,
        "canonical_taxon_key": clean_text(row.get("canonical_taxon_key")),
        "fishbase25_speccode": clean_text(row.get("fishbase25_speccode")),
        "scientific_name": clean_text(row.get("scientific_name") or row.get("target_scientific_name")),
        "source_page_url": source_page_url,
        "source_image_url": source_image_url,
        "pointer_url": pointer_url,
        "source_host": url_host(source_image_url),
        "pointer_host": url_host(pointer_url),
        "extension_class": extension_class(source_image_url),
        "expected_width": clean_text(row.get("width")),
        "expected_height": clean_text(row.get("height")),
        "expected_min_side": clean_text(row.get("min_side")),
        "expected_sha256": expected_sha,
        "expected_phash_hex64": expected_phash,
        "source_pack_version": clean_text(row.get("source_pack_version") or row.get("benchmark_id")),
        "evidence_id": clean_text(row.get("evidence_id")),
        "benchmark_role": clean_text(row.get("benchmark_role")),
        "public_split": clean_text(row.get("public_split")),
        "distractor_type": clean_text(row.get("distractor_type")),
        "license_normalized": clean_text(row.get("license_normalized")),
        "license_tier": clean_text(row.get("license_tier_source")),
        "release_mode": clean_text(row.get("normalized_release_mode") or row.get("pixel_release_mode")),
        "attribution": clean_text(row.get("attribution")),
        "retirement_reason": clean_text(row.get("retirement_reason")),
        "source_line_number": clean_text(row.get("source_line_number")),
        "raw_row_sha256": clean_text(row.get("raw_row_sha256")),
        "sample_reason": clean_text(row.get("sample_reason")),
        "sample_stratum": clean_text(row.get("sample_stratum")),
        "rank_digest": clean_text(row.get("rank_digest")),
        "sample_rank": clean_text(row.get("sample_rank")),
    }


def extension_class(url: str) -> str:
    suffix = Path(urllib.parse.urlsplit(url).path).suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "jpeg"
    if suffix == ".png":
        return "png"
    if suffix == ".gif":
        return "gif"
    if suffix in {".tif", ".tiff"}:
        return "tiff"
    if not suffix:
        return "no_extension"
    return suffix.lstrip(".") or "other"


EXPECTED_COUNT_FIELDS = (
    "active_staging_rows",
    "active_byte_complete_rows",
    "active_pointer_rows",
    "archive_pointer_rows",
    "e0_byte_complete_rows",
    "e0_pointer_rows",
    "r0_rows",
    "r0_byte_complete_rows",
    "r0_pointer_rows",
    "retired_pointer_rows",
)


def validated_expected_counts(bindings: dict[str, Any]) -> dict[str, int]:
    expected = bindings.get("expected")
    if not isinstance(expected, dict) or set(expected) != set(EXPECTED_COUNT_FIELDS):
        raise AuditError("EXPECTED_COUNT_BINDINGS_INVALID")
    if any(type(expected[field]) is not int or expected[field] < 0 for field in EXPECTED_COUNT_FIELDS):
        raise AuditError("EXPECTED_COUNT_VALUE_INVALID")
    if expected["archive_pointer_rows"] != expected["active_pointer_rows"] + expected["retired_pointer_rows"]:
        raise AuditError("ARCHIVE_POINTER_EXPECTED_CLOSURE_FAILED")
    if expected["active_staging_rows"] != expected["active_byte_complete_rows"] + expected["active_pointer_rows"]:
        raise AuditError("ACTIVE_STAGING_EXPECTED_CLOSURE_FAILED")
    if expected["r0_rows"] != expected["r0_byte_complete_rows"] + expected["r0_pointer_rows"]:
        raise AuditError("R0_EXPECTED_CLOSURE_FAILED")
    if expected["e0_pointer_rows"] != 0:
        raise AuditError("E0_POINTER_EXPECTED_ZERO_FAILED")
    return expected


def load_bound_rows(source_root: Path, bindings_path: Path) -> tuple[dict[str, Any], list[dict[str, str]]]:
    bindings = load_json(bindings_path)
    expected = validated_expected_counts(bindings)
    manifests = bindings.get("manifests")
    if not isinstance(manifests, list):
        raise AuditError("BINDING_MANIFESTS_INVALID")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for spec in manifests:
        if not isinstance(spec, dict):
            raise AuditError("BINDING_MANIFEST_INVALID")
        component = clean_text(spec.get("component"))
        relative_path = clean_text(spec.get("path"))
        id_field = clean_text(spec.get("id_field"))
        expected_rows = int(spec.get("rows", -1))
        expected_hash = clean_text(spec.get("sha256")).lower()
        relative = Path(relative_path)
        path = bound_file(source_root, relative)
        if not path.is_file() or file_sha256(path) != expected_hash:
            raise AuditError(f"{component}_POINTER_MANIFEST_BINDING_FAILED")
        _, source_rows = read_tsv(path)
        if len(source_rows) != expected_rows:
            raise AuditError(f"{component}_POINTER_ROW_COUNT_FAILED")
        try:
            raw_lines = path.read_bytes().splitlines()
        except OSError as exc:
            raise AuditError("TSV_RAW_READ_FAILED") from exc
        if len(raw_lines) != len(source_rows) + 1:
            raise AuditError(f"{component}_POINTER_PHYSICAL_LINE_COUNT_FAILED")
        for line_number, (source_row, raw_line) in enumerate(zip(source_rows, raw_lines[1:]), start=2):
            source_row = dict(source_row)
            source_row["source_line_number"] = str(line_number)
            source_row["raw_row_sha256"] = bytes_sha256(raw_line)
            normalized = normalize_row(component, id_field, source_row)
            if normalized["record_id"] in seen:
                raise AuditError("GLOBAL_POINTER_ID_NOT_UNIQUE")
            seen.add(normalized["record_id"])
            rows.append(normalized)
    if len(rows) != expected["archive_pointer_rows"]:
        raise AuditError("ARCHIVE_POINTER_CLOSURE_FAILED")
    active = sum(row["active"] == "true" for row in rows)
    if active != expected["active_pointer_rows"]:
        raise AuditError("ACTIVE_POINTER_CLOSURE_FAILED")
    retired = len(rows) - active
    if retired != expected["retired_pointer_rows"]:
        raise AuditError("RETIRED_POINTER_CLOSURE_FAILED")
    if sum(row["component"] == "S1" for row in rows) != expected["r0_pointer_rows"]:
        raise AuditError("R0_POINTER_CLOSURE_FAILED")
    ledgers = bindings.get("container_ledgers", {})
    if not isinstance(ledgers, dict):
        raise AuditError("CONTAINER_LEDGER_BINDINGS_INVALID")
    for name, spec in ledgers.items():
        if not isinstance(spec, dict):
            raise AuditError("CONTAINER_LEDGER_BINDING_INVALID")
        relative = Path(clean_text(spec.get("path")))
        path = bound_file(source_root, relative)
        if not path.is_file() or file_sha256(path) != clean_text(spec.get("sha256")):
            raise AuditError(f"CONTAINER_{name.upper()}_BINDING_FAILED")
    e0 = bindings.get("e0", {})
    if not isinstance(e0, dict):
        raise AuditError("E0_BINDINGS_INVALID")
    for kind, expected_rows in (
        ("bytes", expected["e0_byte_complete_rows"]),
        ("pointers", expected["e0_pointer_rows"]),
    ):
        relative = Path(clean_text(e0.get(f"{kind}_path")))
        path = bound_file(source_root, relative)
        expected_hash = clean_text(e0.get(f"{kind}_sha256"))
        if not path.is_file() or file_sha256(path) != expected_hash:
            raise AuditError(f"E0_{kind.upper()}_BINDING_FAILED")
        _, e0_rows = read_tsv(path)
        if len(e0_rows) != expected_rows:
            raise AuditError(f"E0_{kind.upper()}_ROW_COUNT_FAILED")
        try:
            physical_lines = len(path.read_bytes().splitlines())
        except OSError as exc:
            raise AuditError("E0_RAW_READ_FAILED") from exc
        if physical_lines != expected_rows + 1:
            raise AuditError(f"E0_{kind.upper()}_PHYSICAL_LINE_COUNT_FAILED")
    rows.sort(key=lambda row: (COMPONENT_ORDER[row["component"]], row["record_id"]))
    return bindings, rows


def binding_report(bindings: dict[str, Any], rows: list[dict[str, str]], bindings_path: Path) -> dict[str, Any]:
    by_component = Counter(row["component"] for row in rows)
    by_host = Counter(row["source_host"] for row in rows)
    same_urls = Counter(
        row["component"] for row in rows if row["source_image_url"] == row["pointer_url"]
    )
    missing_dimensions = Counter(
        row["component"]
        for row in rows
        if not row["expected_width"] or not row["expected_height"]
    )
    inat_taxon_pointer_rows = Counter(
        row["component"]
        for row in rows
        if row["component"] in {"S0", "D0"}
        and "/taxa/" in urllib.parse.urlsplit(row["pointer_url"]).path
    )
    return {
        "active_pointer_rows": sum(row["active"] == "true" for row in rows),
        "archive_pointer_rows": len(rows),
        "bindings_sha256": file_sha256(bindings_path),
        "by_component": dict(sorted(by_component.items())),
        "by_source_host": dict(sorted(by_host.items())),
        "dataset": bindings.get("dataset", {}),
        "e0": {
            "byte_complete_rows": bindings.get("expected", {}).get("e0_byte_complete_rows"),
            "pointer_rows": bindings.get("expected", {}).get("e0_pointer_rows"),
        },
        "missing_dimensions_by_component": dict(sorted(missing_dimensions.items())),
        "inat_taxon_pointer_rows_by_component": dict(
            sorted(inat_taxon_pointer_rows.items())
        ),
        "pointer_equals_image_by_component": dict(sorted(same_urls.items())),
        "retired_pointer_rows": sum(row["active"] == "false" for row in rows),
        "dependencies": dependency_report(),
        "runtime": runtime_report(),
    }


def dependency_report() -> dict[str, Any]:
    versions: dict[str, str | None] = {}
    expected = {
        "ImageHash": "4.3.2",
        "Pillow": "12.1.1",
        "numpy": "2.2.6",
        "scipy": "1.15.3",
    }
    for distribution in expected:
        try:
            versions[distribution] = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            versions[distribution] = None
    return {
        "installed": versions,
        "expected": expected,
        "ready": versions == expected,
    }


def runtime_report() -> dict[str, Any]:
    """Return the receipt-stable producer runtime identity."""
    return {
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "required_implementation": "CPython",
        "required_major_minor": "3.10",
        "ready": (
            platform.python_implementation() == "CPython"
            and sys.version_info[:2] == (3, 10)
        ),
    }


def deterministic_key(seed: str, component: str, record_id: str) -> str:
    return hashlib.sha256(f"{seed}\0{component}\0{record_id}".encode()).hexdigest()


def min_side_band(value: str) -> str:
    try:
        number = int(value)
    except ValueError:
        return "missing"
    if number < 256:
        return "lt256"
    if number < 512:
        return "256_511"
    if number < 1024:
        return "512_1023"
    return "ge1024"


def row_stratum(component: str, row: dict[str, str]) -> tuple[str, ...]:
    if component == "S0":
        return (
            row["active_projection_status"],
            row["pointer_host"],
            row["source_host"],
            row["extension_class"],
            row["license_normalized"],
        )
    if component == "S1":
        return (row["license_normalized"], row["extension_class"])
    if component == "S3":
        return (min_side_band(row["expected_min_side"]),)
    if component == "S4":
        return (row["license_normalized"], row["extension_class"])
    if component == "D0":
        return (
            row["benchmark_role"],
            row["public_split"],
            row["source_host"],
            row["extension_class"],
        )
    return (component,)


def add_selected(selected: dict[str, dict[str, str]], row: dict[str, str], reason: str, seed: str) -> None:
    record_id = row["record_id"]
    if record_id in selected:
        existing = selected[record_id]["sample_reason"].split("+")
        if reason not in existing:
            selected[record_id]["sample_reason"] += "+" + reason
        return
    copy = dict(row)
    copy["sample_reason"] = reason
    copy["sample_stratum"] = "|".join(row_stratum(row["component"], row))
    copy["rank_digest"] = deterministic_key(seed, row["component"], record_id)
    selected[record_id] = copy


def stratified_fill(pool: list[dict[str, str]], selected: dict[str, dict[str, str]], target: int, seed: str, reason: str) -> None:
    if len(selected) > target:
        raise AuditError("PILOT_MANDATORY_ROWS_EXCEED_QUOTA")
    available: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    selected_strata = {row_stratum(row["component"], row) for row in selected.values()}
    for row in pool:
        if row["record_id"] not in selected:
            available[row_stratum(row["component"], row)].append(row)
    for rows_in_stratum in available.values():
        rows_in_stratum.sort(key=lambda row: (deterministic_key(seed, row["component"], row["record_id"]), row["record_id"]))

    # First guarantee at least one row from every still-uncovered non-empty stratum.
    for stratum in sorted(available):
        if len(selected) >= target:
            break
        if stratum not in selected_strata and available[stratum]:
            add_selected(selected, available[stratum].pop(0), reason + "_stratum_cover", seed)
            selected_strata.add(stratum)

    remaining_slots = target - len(selected)
    while remaining_slots > 0:
        nonempty = {key: value for key, value in available.items() if value}
        if not nonempty:
            raise AuditError("PILOT_SELECTION_EXHAUSTED")
        total = sum(len(value) for value in nonempty.values())
        raw = {key: remaining_slots * len(value) / total for key, value in nonempty.items()}
        allocation = {key: min(len(nonempty[key]), int(raw[key])) for key in nonempty}
        assigned = sum(allocation.values())
        order = sorted(nonempty, key=lambda key: (-(raw[key] - int(raw[key])), key))
        for key in order:
            if assigned >= remaining_slots:
                break
            if allocation[key] < len(nonempty[key]):
                allocation[key] += 1
                assigned += 1
        if assigned == 0:
            raise AuditError("PILOT_ALLOCATION_FAILED")
        for key in sorted(allocation):
            for _ in range(allocation[key]):
                add_selected(selected, available[key].pop(0), reason + "_hamilton", seed)
        remaining_slots = target - len(selected)


def select_component_with_subquota(pool: list[dict[str, str]], mandatory: dict[str, dict[str, str]], predicate: Any, target: int, seed: str, reason: str) -> dict[str, dict[str, str]]:
    subset = [row for row in pool if predicate(row)]
    selected = {record_id: row for record_id, row in mandatory.items() if predicate(row)}
    stratified_fill(subset, selected, target, seed, reason)
    return selected


def select_sample(rows: list[dict[str, str]], seed: str) -> list[dict[str, str]]:
    by_component: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_component[row["component"]].append(row)
    selected: list[dict[str, str]] = []
    phash_counts = Counter(row["expected_phash_hex64"] for row in rows)
    for component, quota in SAMPLE_QUOTAS.items():
        pool = by_component[component]
        if len(pool) < quota:
            raise AuditError("PILOT_QUOTA_EXCEEDS_COMPONENT")
        mandatory: dict[str, dict[str, str]] = {}
        if component == "S2":
            for row in pool:
                add_selected(mandatory, row, "mandatory_all_s2", seed)
        if component == "S0":
            for row in pool:
                if row["pointer_host"] != "www.inaturalist.org":
                    add_selected(mandatory, row, "mandatory_rare_pointer_host", seed)
                if row["extension_class"] in {"gif", "no_extension"}:
                    add_selected(mandatory, row, "mandatory_rare_format", seed)
        if component == "S1":
            for row in pool:
                if not row["expected_width"] or not row["expected_height"]:
                    add_selected(mandatory, row, "mandatory_missing_dimensions", seed)
                if phash_counts[row["expected_phash_hex64"]] > 1:
                    add_selected(mandatory, row, "mandatory_phash_collision", seed)
        if component == "S4":
            for row in pool:
                if phash_counts[row["expected_phash_hex64"]] > 1:
                    add_selected(mandatory, row, "mandatory_phash_collision", seed)
                if row["extension_class"] == "png":
                    add_selected(mandatory, row, "mandatory_png", seed)
            for license_value in sorted({row["license_normalized"] for row in pool}):
                candidate = min(
                    (row for row in pool if row["license_normalized"] == license_value),
                    key=lambda row: (deterministic_key(seed, component, row["record_id"]), row["record_id"]),
                )
                add_selected(mandatory, candidate, "mandatory_license_class", seed)
        if component == "D0":
            for row in pool:
                if urllib.parse.urlsplit(row["pointer_url"]).scheme == "http":
                    add_selected(mandatory, row, "mandatory_http_landing", seed)

        if component == "S0":
            active = select_component_with_subquota(pool, mandatory, lambda row: row["active"] == "true", 145, seed, "s0_active")
            retired = select_component_with_subquota(pool, mandatory, lambda row: row["active"] == "false", 15, seed, "s0_retired")
            component_selected = active | retired
        elif component == "D0":
            positive = select_component_with_subquota(pool, mandatory, lambda row: row["benchmark_role"] == "positive", 32, seed, "d0_positive")
            distractor = select_component_with_subquota(pool, mandatory, lambda row: row["benchmark_role"] != "positive", 8, seed, "d0_distractor")
            component_selected = positive | distractor
        else:
            component_selected = dict(mandatory)
            stratified_fill(pool, component_selected, quota, seed, component.lower())
        if len(component_selected) != quota:
            raise AuditError("PILOT_COMPONENT_QUOTA_FAILED")
        selected.extend(component_selected.values())
    selected.sort(key=lambda row: (COMPONENT_ORDER[row["component"]], row["record_id"]))
    for rank, row in enumerate(selected, start=1):
        row["sample_rank"] = str(rank)
    if len(selected) != sum(SAMPLE_QUOTAS.values()):
        raise AuditError("PILOT_TOTAL_FAILED")
    return selected


def atomic_write_tsv(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames, delimiter="\t", lineterminator="\n", extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in fieldnames})
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


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
        fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_sample_manifest(path: Path) -> list[dict[str, str]]:
    fields, rows = read_tsv(path)
    required = set(SAMPLE_FIELDS) - {"sample_reason", "sample_rank"}
    if not required.issubset(fields):
        raise AuditError("SAMPLE_MANIFEST_SCHEMA_INVALID")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        record_id = clean_text(row.get("record_id"))
        if not record_id or record_id in seen:
            raise AuditError("SAMPLE_MANIFEST_ID_INVALID")
        seen.add(record_id)
        result.append({field: clean_text(row.get(field)) for field in SAMPLE_FIELDS})
    return result


@dataclass
class TransportResult:
    url_requested: str
    url_final: str = ""
    status: str = "network_error"
    http_status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    data: bytes = b""
    redirects: list[dict[str, object]] = field(default_factory=list)
    error_code: str = ""
    policy_state: str = ""
    robots_status: str = ""


@dataclass(frozen=True)
class AuditRecordResult:
    attempts: list[dict[str, str]]
    resolution_protocol_complete: bool
    resolution_protocol_status: str
    resolver_candidate_count: int
    fallback_attempt_count: int


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Expose redirect responses so every hop can pass policy and robots checks."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        del req, fp, code, msg, headers, newurl
        return None


class PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Connect to a prevalidated public address while retaining TLS SNI."""

    def __init__(self, host: str, *, pinned_addresses: tuple[str, ...], **kwargs: Any):
        kwargs.pop("key_file", None)
        kwargs.pop("cert_file", None)
        kwargs.pop("check_hostname", None)
        super().__init__(host, **kwargs)
        self.pinned_addresses = pinned_addresses

    def connect(self) -> None:
        if self._tunnel_host:
            raise OSError("HTTP tunnels are disabled")
        last_error: OSError | None = None
        for address in self.pinned_addresses:
            raw_socket: socket.socket | None = None
            try:
                raw_socket = socket.create_connection(
                    (address, self.port), self.timeout, self.source_address
                )
                self.sock = self._context.wrap_socket(
                    raw_socket, server_hostname=self.host
                )
                return
            except OSError as exc:
                last_error = exc
                if raw_socket is not None:
                    raw_socket.close()
        raise last_error or OSError("no pinned address available")


class PinnedHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, addresses: tuple[str, ...]):
        super().__init__(context=ssl.create_default_context())
        self.addresses = addresses

    def https_open(self, req: urllib.request.Request) -> Any:
        def connection(host: str, **kwargs: Any) -> PinnedHTTPSConnection:
            return PinnedHTTPSConnection(
                host, pinned_addresses=self.addresses, **kwargs
            )

        return self.do_open(
            connection,
            req,
            context=self._context,
            check_hostname=True,
        )


def validate_host_policy(policy: dict[str, Any]) -> None:
    top_keys = {
        "schema", "user_agent_product", "public_contact_url",
        "allowed_schemes", "allowed_ports", "max_response_bytes",
        "max_image_pixels", "max_image_dimension",
        "image_decode_timeout_seconds", "robots_cache_seconds",
        "request_timeout_seconds", "max_request_wall_seconds",
        "max_fallback_images_per_row", "hosts",
    }
    if set(policy) - top_keys or policy.get("schema") != "coverfish.pointer-host-policy.v1":
        raise AuditError("HOST_POLICY_SCHEMA_INVALID", EXIT_SAFETY)
    product = clean_text(policy.get("user_agent_product"))
    contact = clean_text(policy.get("public_contact_url"))
    contact_parts = urllib.parse.urlsplit(contact)
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{2,79}", product)
        or any(char in product for char in "\r\n")
        or contact_parts.scheme != "https"
        or not contact_parts.hostname
        or contact_parts.username
        or contact_parts.password
        or any(char in contact for char in "\r\n")
    ):
        raise AuditError("HOST_POLICY_USER_AGENT_INVALID", EXIT_SAFETY)
    if policy.get("allowed_schemes") != ["https"] or policy.get("allowed_ports") != [443]:
        raise AuditError("HOST_POLICY_TRANSPORT_INVALID", EXIT_SAFETY)
    numeric_limits = {
        "max_response_bytes": (1, 134_217_728),
        "max_image_pixels": (1, 25_000_000),
        "max_image_dimension": (1, 6_000),
        "image_decode_timeout_seconds": (1, 60),
        "robots_cache_seconds": (60, 86_400),
        "request_timeout_seconds": (1, 120),
        "max_request_wall_seconds": (1, 600),
        "max_fallback_images_per_row": (0, 10),
    }
    for key, (minimum, maximum) in numeric_limits.items():
        value = policy.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not minimum <= value <= maximum:
            raise AuditError(f"HOST_POLICY_{key.upper()}_INVALID", EXIT_SAFETY)
    hosts = policy.get("hosts")
    if not isinstance(hosts, dict) or not hosts:
        raise AuditError("HOST_POLICY_HOSTS_INVALID", EXIT_SAFETY)
    allowed_states = {"allow_with_limits", "blocked", "landing_only", "pending_permission"}
    allowed_roles = {"image", "landing", "resolver_api"}
    host_keys = {
        "min_interval_seconds", "policy_state", "roles", "rate_group",
        "max_bytes_per_hour", "max_bytes_per_day", "full_run_contact_recommended",
    }
    for raw_host, raw_config in hosts.items():
        host = clean_text(raw_host)
        if host != host.lower() or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", host):
            raise AuditError("HOST_POLICY_HOSTNAME_INVALID", EXIT_SAFETY)
        if not isinstance(raw_config, dict) or set(raw_config) - host_keys:
            raise AuditError("HOST_POLICY_HOST_ENTRY_INVALID", EXIT_SAFETY)
        state = raw_config.get("policy_state")
        roles = raw_config.get("roles")
        interval = raw_config.get("min_interval_seconds")
        if state not in allowed_states or not isinstance(roles, list) or not roles or set(roles) - allowed_roles:
            raise AuditError("HOST_POLICY_AUTHORITY_INVALID", EXIT_SAFETY)
        if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not 0 < interval <= 60:
            raise AuditError("HOST_POLICY_INTERVAL_INVALID", EXIT_SAFETY)
        group = raw_config.get("rate_group")
        if group is not None:
            if not isinstance(group, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", group):
                raise AuditError("HOST_POLICY_RATE_GROUP_INVALID", EXIT_SAFETY)
            for cap in ("max_bytes_per_hour", "max_bytes_per_day"):
                value = raw_config.get(cap)
                if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise AuditError("HOST_POLICY_RATE_CAP_INVALID", EXIT_SAFETY)
        elif "max_bytes_per_hour" in raw_config or "max_bytes_per_day" in raw_config:
            raise AuditError("HOST_POLICY_RATE_GROUP_MISSING", EXIT_SAFETY)


def validate_owned_directory(path: Path, private: bool) -> bool:
    try:
        metadata = os.lstat(path)
    except OSError:
        return False
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o022
        or (private and metadata.st_mode & 0o077)
    ):
        return False
    return True


def validate_output_directory(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise AuditError("OUTPUT_DIRECTORY_UNAVAILABLE", EXIT_SAFETY) from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o022
    ):
        raise AuditError("OUTPUT_DIRECTORY_SAFETY_INVALID", EXIT_SAFETY)


def secure_runtime_root() -> Path:
    uid = os.getuid()
    candidates: list[Path] = []
    xdg = clean_text(os.environ.get("XDG_RUNTIME_DIR"))
    if xdg:
        candidates.append(Path(xdg))
    candidates.append(Path("/run/user") / str(uid))
    base: Path | None = next(
        (candidate for candidate in candidates if validate_owned_directory(candidate, True)),
        None,
    )
    if base is None:
        home = Path.home()
        if not validate_owned_directory(home, False):
            raise AuditError("SECURE_RUNTIME_BASE_UNAVAILABLE", EXIT_SAFETY)
        base = home / ".cache"
        if not base.exists():
            os.mkdir(base, 0o700)
        if not validate_owned_directory(base, False):
            raise AuditError("SECURE_RUNTIME_BASE_INVALID", EXIT_SAFETY)
    root = base / "coverfish-pointer-audit-runtime"
    if not root.exists():
        os.mkdir(root, 0o700)
    if not validate_owned_directory(root, True):
        raise AuditError("SECURE_RUNTIME_DIRECTORY_INVALID", EXIT_SAFETY)
    return root


class RateLimiter:
    def __init__(self, host_policy: dict[str, Any], state_path: Path):
        self.host_policy = host_policy
        self.state_path = state_path
        self.dynamic_interval: dict[str, float] = {}
        self.group_interval: dict[str, float] = {}
        for config in host_policy.values():
            group = clean_text(config.get("rate_group"))
            if group:
                self.group_interval[group] = max(
                    self.group_interval.get(group, 0.0),
                    float(config.get("min_interval_seconds", 1.0)),
                )
        self.state: dict[str, Any] = {
            "schema": "coverfish.pointer-rate-state.v2",
            "last_request_epoch": {},
            "last_group_request_epoch": {},
            "retry_until_epoch": {},
            "transient_failures": {},
            "group_bytes": {},
            "inflight_group_bytes": {},
        }
        if state_path.is_symlink():
            raise AuditError("RATE_STATE_SYMLINK_FORBIDDEN", EXIT_SAFETY)
        if state_path.is_file():
            metadata = os.lstat(state_path)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_mode & 0o077
            ):
                raise AuditError("RATE_STATE_PERMISSIONS_INVALID", EXIT_SAFETY)
            try:
                loaded = json.loads(state_path.read_text(encoding="utf-8"))
                if (
                    not isinstance(loaded, dict)
                    or loaded.get("schema") != self.state["schema"]
                ):
                    raise AuditError("RATE_STATE_SCHEMA_INVALID", EXIT_SAFETY)
                required_maps = {
                    "last_request_epoch",
                    "last_group_request_epoch",
                    "retry_until_epoch",
                    "transient_failures",
                    "group_bytes",
                    "inflight_group_bytes",
                }
                if any(
                    not isinstance(loaded.get(key), dict)
                    for key in required_maps
                ):
                    raise AuditError("RATE_STATE_STRUCTURE_INVALID", EXIT_SAFETY)
                self.validate_loaded_state(loaded)
                self.state = loaded
            except AuditError:
                raise
            except (OSError, json.JSONDecodeError):
                raise AuditError("RATE_STATE_INVALID", EXIT_SAFETY)

    def validate_loaded_state(self, loaded: dict[str, Any]) -> None:
        required = {
            "schema",
            "last_request_epoch",
            "last_group_request_epoch",
            "retry_until_epoch",
            "transient_failures",
            "group_bytes",
            "inflight_group_bytes",
        }
        if set(loaded) != required:
            raise AuditError("RATE_STATE_STRUCTURE_INVALID", EXIT_SAFETY)
        allowed_hosts = set(self.host_policy)
        allowed_groups = set(self.group_interval)

        def finite_epoch(value: object) -> bool:
            return (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and math.isfinite(float(value))
                and float(value) >= 0
            )

        def valid_key_map(
            mapping: dict[str, Any], allowed: set[str]
        ) -> bool:
            return all(
                isinstance(key, str) and key in allowed for key in mapping
            )

        for key in ("last_request_epoch", "retry_until_epoch"):
            mapping = loaded[key]
            if not valid_key_map(mapping, allowed_hosts) or any(
                not finite_epoch(value) for value in mapping.values()
            ):
                raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)
        group_requests = loaded["last_group_request_epoch"]
        if not valid_key_map(group_requests, allowed_groups) or any(
            not finite_epoch(value) for value in group_requests.values()
        ):
            raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)

        transient_outcomes = {
            "rate_limited",
            "server_error",
            "timeout",
            "dns_error",
            "tls_error",
            "network_error",
        }
        transient = loaded["transient_failures"]
        if not valid_key_map(transient, allowed_hosts):
            raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)
        for value in transient.values():
            if (
                not isinstance(value, dict)
                or set(value)
                != {"consecutive", "defer_until_epoch", "last_outcome"}
                or not isinstance(value["consecutive"], int)
                or isinstance(value["consecutive"], bool)
                or value["consecutive"] < 0
                or not finite_epoch(value["defer_until_epoch"])
                or value["last_outcome"] not in transient_outcomes
            ):
                raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)

        usage = loaded["group_bytes"]
        if not valid_key_map(usage, allowed_groups):
            raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)
        for entries in usage.values():
            if not isinstance(entries, list):
                raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)
            for value in entries:
                if (
                    not isinstance(value, list)
                    or len(value) != 2
                    or not finite_epoch(value[0])
                    or not isinstance(value[1], int)
                    or isinstance(value[1], bool)
                    or value[1] < 0
                ):
                    raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)

        inflight = loaded["inflight_group_bytes"]
        if not valid_key_map(inflight, allowed_groups):
            raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)
        for reservations in inflight.values():
            if not isinstance(reservations, dict):
                raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)
            for reservation_id, value in reservations.items():
                if (
                    not isinstance(reservation_id, str)
                    or not is_hex(reservation_id, 24)
                    or not isinstance(value, list)
                    or len(value) != 2
                    or not finite_epoch(value[0])
                    or not isinstance(value[1], int)
                    or isinstance(value[1], bool)
                    or value[1] <= 0
                ):
                    raise AuditError("RATE_STATE_VALUE_INVALID", EXIT_SAFETY)

    def persist(self) -> None:
        atomic_write_json(self.state_path, self.state)

    def set_crawl_delay(self, host: str, seconds: float) -> None:
        self.dynamic_interval[host] = max(self.dynamic_interval.get(host, 0.0), seconds)

    def _prune_group_usage(
        self, group: str, now: float
    ) -> tuple[list[list[float | int]], dict[str, list[float | int]]]:
        raw_entries = self.state.setdefault("group_bytes", {}).setdefault(group, [])
        entries = [
            [float(stamp), int(value)]
            for stamp, value in raw_entries
            if now - float(stamp) <= 86400
        ]
        raw_inflight = self.state.setdefault("inflight_group_bytes", {}).setdefault(group, {})
        if not isinstance(raw_inflight, dict):
            raise AuditError("RATE_STATE_INFLIGHT_INVALID", EXIT_SAFETY)
        inflight = {
            clean_text(reservation_id): [float(value[0]), int(value[1])]
            for reservation_id, value in raw_inflight.items()
            if (
                clean_text(reservation_id)
                and isinstance(value, list)
                and len(value) == 2
                and now - float(value[0]) <= 86400
            )
        }
        self.state["group_bytes"][group] = entries
        self.state["inflight_group_bytes"][group] = inflight
        return entries, inflight

    def _group_budget_error(self, host: str, additional: int = 0) -> str:
        policy = self.host_policy.get(host, {})
        group = clean_text(policy.get("rate_group"))
        if not group:
            return ""
        now = time.time()
        entries, inflight = self._prune_group_usage(group, now)
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
        max_hour = int(policy.get("max_bytes_per_hour", 0) or 0)
        max_day = int(policy.get("max_bytes_per_day", 0) or 0)
        if (
            (max_hour and hour_total + additional > max_hour)
            or (max_day and day_total + additional > max_day)
        ):
            return "BANDWIDTH_BUDGET_REACHED"
        return ""

    def reserve_bytes(self, host: str, amount: int) -> tuple[str, str]:
        """Persist a conservative in-flight byte reservation before reading."""
        policy = self.host_policy.get(host, {})
        group = clean_text(policy.get("rate_group"))
        if not group or amount <= 0:
            return "", ""
        error = self._group_budget_error(host, amount)
        if error:
            self.persist()
            return "", error
        reservation_id = hashlib.sha256(
            f"{host}\0{time.time_ns()}\0{amount}".encode()
        ).hexdigest()[:24]
        self.state.setdefault("inflight_group_bytes", {}).setdefault(group, {})[
            reservation_id
        ] = [time.time(), int(amount)]
        self.persist()
        return reservation_id, ""

    def settle_byte_reservation(
        self, host: str, reservation_id: str, actual_bytes: int
    ) -> None:
        policy = self.host_policy.get(host, {})
        group = clean_text(policy.get("rate_group"))
        if not group or not reservation_id:
            return
        reservations = self.state.setdefault("inflight_group_bytes", {}).setdefault(
            group, {}
        )
        raw = reservations.get(reservation_id)
        if (
            not isinstance(raw, list)
            or len(raw) != 2
            or actual_bytes < 0
            or actual_bytes > int(raw[1])
        ):
            raise AuditError("RATE_BYTE_RESERVATION_INVALID", EXIT_SAFETY)
        del reservations[reservation_id]
        if actual_bytes:
            self.state.setdefault("group_bytes", {}).setdefault(group, []).append(
                [time.time(), int(actual_bytes)]
            )
        self.persist()

    def before(self, host: str) -> str:
        policy = self.host_policy.get(host, {})
        interval = max(float(policy.get("min_interval_seconds", 1.0)), self.dynamic_interval.get(host, 0.0))
        group = clean_text(policy.get("rate_group"))
        if group:
            interval = max(interval, self.group_interval.get(group, 0.0))
        now = time.time()
        previous = float(self.state.get("last_request_epoch", {}).get(host, 0.0) or 0.0)
        group_previous = float(
            self.state.get("last_group_request_epoch", {}).get(group, 0.0)
            or 0.0
        ) if group else 0.0
        retry_until = float(self.state.get("retry_until_epoch", {}).get(host, 0.0) or 0.0)
        transient_until = float(
            self.state.get("transient_failures", {})
            .get(host, {})
            .get("defer_until_epoch", 0.0)
            or 0.0
        )
        remaining = max(
            previous + interval - now,
            group_previous + interval - now,
            retry_until - now,
            transient_until - now,
        )
        if remaining > 60:
            if retry_until - now > 60:
                return "RETRY_AFTER_ACTIVE"
            if transient_until - now > 60:
                return "TRANSIENT_CIRCUIT_OPEN"
            return "RATE_INTERVAL_ACTIVE"
        if remaining > 0:
            time.sleep(remaining)
        if group:
            budget_error = self._group_budget_error(host)
            if budget_error:
                self.persist()
                return budget_error
        self.state.setdefault("last_request_epoch", {})[host] = time.time()
        if group:
            self.state.setdefault("last_group_request_epoch", {})[group] = time.time()
        self.persist()
        return ""

    def after(self, host: str, byte_count: int) -> None:
        self.state.setdefault("last_request_epoch", {})[host] = time.time()
        policy = self.host_policy.get(host, {})
        group = clean_text(policy.get("rate_group"))
        if group:
            self.state.setdefault("last_group_request_epoch", {})[group] = time.time()
        if group and byte_count > 0:
            self.state.setdefault("group_bytes", {}).setdefault(group, []).append(
                [time.time(), int(byte_count)]
            )
        self.persist()

    def defer(self, host: str, raw_retry_after: str) -> None:
        value = clean_text(raw_retry_after)
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
        current = float(self.state.setdefault("retry_until_epoch", {}).get(host, 0.0) or 0.0)
        self.state["retry_until_epoch"][host] = max(current, deadline)
        self.persist()

    def record_outcome(self, host: str, outcome: str) -> None:
        transient = {
            "rate_limited",
            "server_error",
            "timeout",
            "dns_error",
            "tls_error",
            "network_error",
        }
        states = self.state.setdefault("transient_failures", {})
        if outcome == "redirect":
            return
        if outcome not in transient:
            if host in states:
                del states[host]
                self.persist()
            return
        previous = states.get(host, {})
        consecutive = int(previous.get("consecutive", 0) or 0) + 1
        base = max(
            1.0,
            float(self.host_policy.get(host, {}).get("min_interval_seconds", 1.0)),
        )
        delay = min(
            float(TRANSIENT_BACKOFF_MAX_SECONDS),
            base * (2 ** min(consecutive - 1, 6)),
        )
        if consecutive >= TRANSIENT_CIRCUIT_THRESHOLD:
            delay = max(delay, float(TRANSIENT_CIRCUIT_MIN_SECONDS))
        states[host] = {
            "consecutive": consecutive,
            "defer_until_epoch": time.time() + delay,
            "last_outcome": outcome,
        }
        self.persist()


class NetworkClient:
    def __init__(
        self,
        policy: dict[str, Any],
        user_agent: str,
        window_id: str,
        invocation_id: str = "standalone",
        rate_state_path: Path | None = None,
        robots_receipt_path: Path | None = None,
    ):
        validate_host_policy(policy)
        hosts = policy.get("hosts")
        if not isinstance(hosts, dict) or not hosts:
            raise AuditError("HOST_POLICY_INVALID")
        self.policy = policy
        self.hosts: dict[str, dict[str, Any]] = {str(key).lower(): dict(value) for key, value in hosts.items()}
        self.user_agent = user_agent
        self.window_id = window_id
        self.invocation_id = invocation_id
        self.max_bytes = int(policy.get("max_response_bytes", 67_108_864))
        self.timeout = float(policy.get("request_timeout_seconds", 30))
        self.wall_timeout = int(policy["max_request_wall_seconds"])
        self.max_fallback = int(policy.get("max_fallback_images_per_row", 3))
        self.max_image_pixels = int(policy["max_image_pixels"])
        self.max_image_dimension = int(policy["max_image_dimension"])
        self.decode_timeout = int(policy["image_decode_timeout_seconds"])
        self.robots_cache_seconds = int(policy["robots_cache_seconds"])
        runtime_root = secure_runtime_root()
        self.rate = RateLimiter(
            self.hosts,
            rate_state_path or runtime_root / "rate-state.json",
        )
        self.robots_cache: dict[tuple[str, str], tuple[float, str, urllib.robotparser.RobotFileParser | None]] = {}
        self.robots_records: list[dict[str, object]] = []
        self.robots_receipt_path = robots_receipt_path

    def record_robots_observation(self, row: dict[str, object]) -> None:
        if self.robots_receipt_path is None:
            self.robots_records.append(row)
            return
        append_tsv_rows(self.robots_receipt_path, ROBOTS_FIELDS, [row])

    def validate_url(self, url: str) -> tuple[str, str, tuple[str, ...]]:
        try:
            parsed = urllib.parse.urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise AuditError("URL_PORT_INVALID", EXIT_SAFETY) from exc
        host = (parsed.hostname or "").lower().rstrip(".")
        if sensitive_query_keys(url):
            raise AuditError("URL_SENSITIVE_QUERY_FORBIDDEN", EXIT_SAFETY)
        if parsed.scheme not in set(self.policy["allowed_schemes"]) or not host or host not in self.hosts:
            raise AuditError("URL_HOST_NOT_ALLOWLISTED", EXIT_SAFETY)
        if parsed.username or parsed.password:
            raise AuditError("URL_USERINFO_FORBIDDEN", EXIT_SAFETY)
        effective_port = port or 443
        if effective_port not in set(self.policy["allowed_ports"]):
            raise AuditError("URL_PORT_FORBIDDEN", EXIT_SAFETY)
        try:
            with wall_deadline(max(1, int(self.timeout))):
                addresses = tuple(sorted({
                    item[4][0]
                    for item in socket.getaddrinfo(
                        host, effective_port, type=socket.SOCK_STREAM
                    )
                }))
        except DeadlineExceeded as exc:
            raise AuditError("DNS_RESOLUTION_TIMEOUT", EXIT_NETWORK) from exc
        except OSError as exc:
            raise AuditError("DNS_RESOLUTION_FAILED", EXIT_NETWORK) from exc
        if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
            raise AuditError("NON_PUBLIC_ADDRESS_FORBIDDEN", EXIT_SAFETY)
        return parsed.scheme, host, addresses

    def role_policy(self, host: str, role: str) -> tuple[bool, str]:
        config = self.hosts[host]
        state = clean_text(config.get("policy_state") or "allow_with_limits")
        roles = {clean_text(value) for value in config.get("roles", [])}
        if role == "robots":
            return True, state
        if state in {"blocked", "pending_permission"}:
            return False, state
        if state == "landing_only" and role not in {"landing", "resolver_api"}:
            return False, "pending_permission"
        return (not roles or role in roles), state

    def finish_request(
        self,
        host: str,
        byte_count: int,
        headers: dict[str, str],
        outcome: str,
    ) -> None:
        self.rate.after(host, byte_count)
        self.rate.defer(host, headers.get("retry-after", ""))
        self.rate.record_outcome(host, outcome)

    def record_validation_failure(self, url: str, error_code: str) -> None:
        host = url_host(url)
        if error_code in {"DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT"} and host in self.hosts:
            self.rate.record_outcome(host, "dns_error")

    def _raw_fetch_once(self, url: str, role: str, max_bytes: int, accept: str) -> TransportResult:
        try:
            _, host, addresses = self.validate_url(url)
        except AuditError as exc:
            self.record_validation_failure(url, exc.code)
            status = "dns_error" if exc.code in {"DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT"} else "safety_block"
            return TransportResult(url_requested=url, status=status, error_code=exc.code)
        allowed, policy_state = self.role_policy(host, role)
        if not allowed:
            return TransportResult(url_requested=url, status="policy_pending", error_code="SOURCE_POLICY_NOT_AUTHORIZED", policy_state=policy_state)
        budget_error = self.rate.before(host)
        if budget_error:
            return TransportResult(url_requested=url, status="bandwidth_budget_pending", error_code=budget_error, policy_state=policy_state)
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            PinnedHTTPSHandler(addresses),
            NoRedirectHandler(),
        )
        request = urllib.request.Request(
            url,
            headers={
                "Accept": accept,
                "Accept-Encoding": "identity",
                "User-Agent": self.user_agent,
            },
            method="GET",
        )
        headers: dict[str, str] = {}
        try:
            with wall_deadline(self.wall_timeout):
                response_context = opener.open(request, timeout=self.timeout)
                with response_context as response:
                    status = int(response.status)
                    headers = {str(key).lower(): str(value) for key, value in response.headers.items()}
                    try:
                        length = int(headers.get("content-length", "0") or 0)
                    except ValueError:
                        length = 0
                    if length > max_bytes:
                        result = TransportResult(url_requested=url, url_final=response.geturl(), status="oversize", http_status=status, headers=headers, error_code="CONTENT_LENGTH_EXCEEDS_LIMIT", policy_state=policy_state)
                        self.finish_request(host, 0, headers, "oversize")
                        return result
                    payload = bytearray()
                    while True:
                        if length and len(payload) >= length:
                            break
                        remaining_hint = (
                            length - len(payload)
                            if length
                            else max_bytes + 1 - len(payload)
                        )
                        read_size = min(
                            1024 * 1024,
                            max_bytes + 1 - len(payload),
                            remaining_hint,
                        )
                        reservation_id, budget_error = self.rate.reserve_bytes(
                            host, read_size
                        )
                        if budget_error:
                            self.finish_request(
                                host, 0, headers, "bandwidth_budget_pending"
                            )
                            return TransportResult(
                                url_requested=url,
                                url_final=response.geturl(),
                                status="bandwidth_budget_pending",
                                http_status=status,
                                headers=headers,
                                error_code=budget_error,
                                policy_state=policy_state,
                            )
                        try:
                            chunk = response.read(read_size)
                        except http.client.IncompleteRead as exc:
                            partial = bytes(exc.partial or b"")
                            self.rate.settle_byte_reservation(
                                host, reservation_id, len(partial)
                            )
                            self.finish_request(host, 0, headers, "network_error")
                            return TransportResult(
                                url_requested=url,
                                url_final=response.geturl(),
                                status="network_error",
                                http_status=status,
                                headers=headers,
                                error_code="INCOMPLETE_RESPONSE",
                                policy_state=policy_state,
                            )
                        self.rate.settle_byte_reservation(
                            host, reservation_id, len(chunk)
                        )
                        if not chunk:
                            break
                        payload.extend(chunk)
                        if len(payload) > max_bytes:
                            result = TransportResult(url_requested=url, url_final=response.geturl(), status="oversize", http_status=status, headers=headers, error_code="BODY_EXCEEDS_LIMIT", policy_state=policy_state)
                            self.finish_request(host, 0, headers, "oversize")
                            return result
                    if length and len(payload) != length:
                        self.finish_request(host, 0, headers, "network_error")
                        return TransportResult(
                            url_requested=url,
                            url_final=response.geturl(),
                            status="network_error",
                            http_status=status,
                            headers=headers,
                            error_code="CONTENT_LENGTH_MISMATCH",
                            policy_state=policy_state,
                        )
                    result = TransportResult(url_requested=url, url_final=response.geturl(), status="ok", http_status=status, headers=headers, data=bytes(payload), policy_state=policy_state)
                    self.finish_request(host, 0, headers, "ok")
                    return result
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            headers = {str(key).lower(): str(value) for key, value in exc.headers.items()}
            if status in {301, 302, 303, 307, 308}:
                transport = "redirect"
            elif status == 429:
                transport = "rate_limited"
            elif status in {404, 410}:
                transport = "not_found"
            elif status in {401, 403}:
                transport = "access_denied"
            elif 500 <= status <= 599:
                transport = "server_error"
            else:
                transport = "http_error"
            self.finish_request(host, 0, headers, transport)
            return TransportResult(url_requested=url, url_final=clean_text(exc.geturl()), status=transport, http_status=status, headers=headers, error_code=f"HTTP_{status}", policy_state=policy_state)
        except socket.timeout:
            self.finish_request(host, 0, headers, "timeout")
            return TransportResult(url_requested=url, status="timeout", error_code="TIMEOUT", policy_state=policy_state)
        except DeadlineExceeded:
            self.finish_request(host, 0, headers, "timeout")
            return TransportResult(url_requested=url, status="timeout", headers=headers, error_code="REQUEST_WALL_TIMEOUT", policy_state=policy_state)
        except urllib.error.URLError as exc:
            reason = clean_text(exc.reason).lower()
            if "certificate" in reason or "ssl" in reason or "tls" in reason:
                status = "tls_error"
            elif "name or service" in reason or "nodename" in reason:
                status = "dns_error"
            else:
                status = "network_error"
            self.finish_request(host, 0, headers, status)
            return TransportResult(url_requested=url, status=status, error_code=status.upper(), policy_state=policy_state)
        except http.client.IncompleteRead as exc:
            self.finish_request(host, 0, headers, "network_error")
            return TransportResult(
                url_requested=url,
                status="network_error",
                headers=headers,
                error_code="INCOMPLETE_RESPONSE",
                policy_state=policy_state,
            )
        except http.client.HTTPException:
            self.finish_request(host, 0, headers, "network_error")
            return TransportResult(
                url_requested=url,
                status="network_error",
                headers=headers,
                error_code="HTTP_PROTOCOL_ERROR",
                policy_state=policy_state,
            )
        except AuditError as exc:
            self.finish_request(host, 0, headers, "safety_block")
            return TransportResult(url_requested=url, status="safety_block", error_code=exc.code, policy_state=policy_state)
        except OSError:
            self.finish_request(host, 0, headers, "network_error")
            return TransportResult(url_requested=url, status="network_error", error_code="NETWORK_OS_ERROR", policy_state=policy_state)

    def _redirect_target(self, current: str, result: TransportResult) -> str:
        location = clean_text(result.headers.get("location", ""))
        if not location:
            return ""
        try:
            target = urllib.parse.urljoin(current, location)
            target, _ = urllib.parse.urldefrag(target)
            current_scheme = urllib.parse.urlsplit(current).scheme
            target_scheme = urllib.parse.urlsplit(target).scheme
        except ValueError:
            return ""
        if current_scheme == "https" and target_scheme != "https":
            return ""
        return target

    def _fetch_without_robots(
        self,
        url: str,
        role: str,
        max_bytes: int,
        accept: str,
        allowed_hosts: frozenset[str] | None = None,
    ) -> TransportResult:
        original = url
        current = url
        chain: list[dict[str, object]] = []
        for _ in range(11):
            if len(chain) >= 10:
                return TransportResult(url_requested=original, url_final=current, status="safety_block", redirects=chain, error_code="TOO_MANY_REDIRECTS")
            try:
                _, current_host, _ = self.validate_url(current)
            except AuditError as exc:
                self.record_validation_failure(current, exc.code)
                status = "dns_error" if exc.code in {"DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT"} else "safety_block"
                return TransportResult(
                    url_requested=original,
                    url_final=current,
                    status=status,
                    redirects=chain,
                    error_code=exc.code,
                )
            if allowed_hosts is not None and current_host not in allowed_hosts:
                return TransportResult(
                    url_requested=original,
                    url_final=current,
                    status="safety_block",
                    redirects=chain,
                    error_code="COMPONENT_ROBOTS_HOST_FORBIDDEN",
                )
            result = self._raw_fetch_once(current, role, max_bytes, accept)
            if result.status != "redirect":
                result.url_requested = original
                result.redirects = chain
                return result
            target = self._redirect_target(current, result)
            if not target:
                return TransportResult(url_requested=original, url_final=current, status="safety_block", http_status=result.http_status, headers=result.headers, redirects=chain, error_code="REDIRECT_TARGET_INVALID_OR_DOWNGRADE", policy_state=result.policy_state)
            try:
                self.validate_url(target)
            except AuditError as exc:
                self.record_validation_failure(target, exc.code)
                status = "dns_error" if exc.code in {"DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT"} else "safety_block"
                return TransportResult(url_requested=original, url_final=current, status=status, http_status=result.http_status, headers=result.headers, redirects=chain, error_code=exc.code, policy_state=result.policy_state)
            chain.append({"status": result.http_status, "from": current, "to": target})
            current = target
        return TransportResult(url_requested=original, url_final=current, status="safety_block", redirects=chain, error_code="TOO_MANY_REDIRECTS")

    def robots_decision(
        self, url: str, allowed_hosts: frozenset[str] | None = None
    ) -> str:
        try:
            scheme, host, _ = self.validate_url(url)
        except AuditError as exc:
            return f"safety_block:{exc.code}"
        key = (scheme, host)
        cached = self.robots_cache.get(key)
        if cached is None or time.time() - cached[0] >= self.robots_cache_seconds:
            robots_url = f"{scheme}://{host}/robots.txt"
            result = self._fetch_without_robots(
                robots_url,
                "robots",
                1024 * 1024,
                "text/plain,*/*;q=0.1",
                allowed_hosts=allowed_hosts,
            )
            parser: urllib.robotparser.RobotFileParser | None = None
            if result.status == "ok" and result.http_status and 200 <= result.http_status < 300:
                try:
                    text = result.data.decode("utf-8", errors="replace")
                    parser = urllib.robotparser.RobotFileParser()
                    parser.set_url(robots_url)
                    parser.parse(text.splitlines())
                    crawl_delay = parser.crawl_delay(self.policy.get("user_agent_product", ""))
                    if crawl_delay is None:
                        crawl_delay = parser.crawl_delay("*")
                    if crawl_delay is not None:
                        self.rate.set_crawl_delay(host, float(crawl_delay))
                    state = "parsed"
                except (ValueError, TypeError):
                    state = "parse_error"
            elif result.http_status in {404, 410}:
                state = "not_present_allow"
            else:
                state = "unavailable_disallow"
            self.robots_cache[key] = (time.time(), state, parser)
            self.record_robots_observation(
                {
                    "window_id": self.window_id,
                    "invocation_id": self.invocation_id,
                    "checked_at_utc": utc_now(),
                    "host": host,
                    "robots_url": robots_url,
                    "http_status": result.http_status or "",
                    "fetch_status": result.status,
                    "robots_state": state,
                    "error_code": result.error_code,
                    "redirect_count": str(len(result.redirects)),
                    "redirect_chain_json": json.dumps(
                        result.redirects,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "sha256": bytes_sha256(result.data) if result.status == "ok" else "",
                }
            )
        _, state, parser = self.robots_cache[key]
        if state == "not_present_allow":
            return "allowed_no_robots"
        if state != "parsed" or parser is None:
            return "unavailable_disallow"
        return "allowed" if parser.can_fetch(self.user_agent, url) else "disallowed"

    def fetch(
        self,
        url: str,
        role: str,
        max_bytes: int | None = None,
        accept: str = "*/*",
        allowed_hosts: frozenset[str] | None = None,
    ) -> TransportResult:
        original = url
        current = url
        chain: list[dict[str, object]] = []
        for _ in range(11):
            if len(chain) >= 10:
                return TransportResult(url_requested=original, url_final=current, status="safety_block", redirects=chain, error_code="TOO_MANY_REDIRECTS")
            try:
                _, current_host, _ = self.validate_url(current)
            except AuditError as exc:
                self.record_validation_failure(current, exc.code)
                status = "dns_error" if exc.code in {"DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT"} else "safety_block"
                return TransportResult(url_requested=original, url_final=current, status=status, redirects=chain, error_code=exc.code)
            if allowed_hosts is not None and current_host not in allowed_hosts:
                return TransportResult(
                    url_requested=original,
                    url_final=current,
                    status="safety_block",
                    redirects=chain,
                    error_code="COMPONENT_REQUEST_HOST_FORBIDDEN",
                )
            allowed, current_policy = self.role_policy(current_host, role)
            if not allowed:
                return TransportResult(url_requested=original, url_final=current, status="policy_pending", redirects=chain, error_code="SOURCE_POLICY_NOT_AUTHORIZED", policy_state=current_policy)
            current_robots = self.robots_decision(current, allowed_hosts)
            if current_robots not in {"allowed", "allowed_no_robots"}:
                return TransportResult(url_requested=original, url_final=current, status="robots_disallowed" if current_robots == "disallowed" else "robots_unavailable", redirects=chain, error_code="ROBOTS_NOT_ALLOWED", policy_state=current_policy, robots_status=current_robots)
            result = self._raw_fetch_once(current, role, max_bytes or self.max_bytes, accept)
            if result.status != "redirect":
                result.url_requested = original
                result.redirects = chain
                result.robots_status = current_robots
                return result
            target = self._redirect_target(current, result)
            if not target:
                return TransportResult(url_requested=original, url_final=current, status="safety_block", http_status=result.http_status, headers=result.headers, redirects=chain, error_code="REDIRECT_TARGET_INVALID_OR_DOWNGRADE", policy_state=result.policy_state, robots_status=current_robots)
            try:
                self.validate_url(target)
            except AuditError as exc:
                self.record_validation_failure(target, exc.code)
                status = "dns_error" if exc.code in {"DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT"} else "safety_block"
                return TransportResult(url_requested=original, url_final=current, status=status, http_status=result.http_status, headers=result.headers, redirects=chain, error_code=exc.code, policy_state=result.policy_state, robots_status=current_robots)
            chain.append({"status": result.http_status, "from": current, "to": target})
            current = target
        return TransportResult(url_requested=original, url_final=current, status="safety_block", redirects=chain, error_code="TOO_MANY_REDIRECTS")


def magic_type(payload: bytes) -> str:
    if payload.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if payload.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "image/webp"
    if payload.startswith(b"BM"):
        return "image/bmp"
    if payload.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if payload.lstrip().lower().startswith((b"<!doctype html", b"<html")):
        return "text/html"
    return "unknown"


class DeadlineExceeded(Exception):
    pass


@contextlib.contextmanager
def wall_deadline(seconds: int) -> Iterable[None]:
    if not hasattr(signal, "SIGALRM") or not hasattr(signal, "setitimer"):
        yield
        return
    try:
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)

        def timeout_handler(signum: int, frame: Any) -> None:
            del signum, frame
            raise DeadlineExceeded("wall-clock deadline exceeded")

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, float(seconds))
    except (AttributeError, ValueError):
        yield
        return
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)


def image_diagnostics(
    payload: bytes,
    expected_sha: str,
    expected_phash: str,
    max_pixels: int = 25_000_000,
    max_dimension: int = 6_000,
    timeout_seconds: int = 15,
) -> dict[str, str]:
    actual_sha = bytes_sha256(payload)
    detected = magic_type(payload)
    result = {
        "magic_type": detected,
        "actual_bytes": str(len(payload)),
        "actual_sha256": actual_sha,
        "actual_width": "",
        "actual_height": "",
        "actual_phash_hex64": "",
        "phash_distance": "",
        "sha256_match": "true" if actual_sha == expected_sha else "false",
        "decode_status": "not_attempted",
        "identity_class": "non_image",
        "error_code": "",
    }
    if actual_sha == expected_sha:
        result["decode_status"] = "skipped_byte_exact"
        result["identity_class"] = "byte_exact"
        return result
    if not payload:
        result["decode_status"] = "not_image_magic"
        result["identity_class"] = "empty_response"
        result["error_code"] = "EMPTY_RESPONSE"
        return result
    if not detected.startswith("image/"):
        result["decode_status"] = "not_image_magic"
        result["error_code"] = "NON_IMAGE_MAGIC"
        return result
    try:
        import imagehash
        from PIL import Image, ImageOps
    except ImportError as exc:
        raise AuditError("MISSING_IMAGE_DEPENDENCIES", EXIT_DEPENDENCY) from exc
    try:
        with wall_deadline(timeout_seconds), warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(payload)) as opened:
                width, height = opened.size
                if (
                    width <= 0
                    or height <= 0
                    or width > max_dimension
                    or height > max_dimension
                    or width * height > max_pixels
                ):
                    raise AuditError("IMAGE_DIMENSION_LIMIT_EXCEEDED")
                opened.verify()
            with Image.open(BytesIO(payload)) as opened:
                image = ImageOps.exif_transpose(opened)
                width, height = image.size
                if (
                    width <= 0
                    or height <= 0
                    or width > max_dimension
                    or height > max_dimension
                    or width * height > max_pixels
                ):
                    raise AuditError("IMAGE_DIMENSION_LIMIT_EXCEEDED")
                converted = image.convert("RGB")
                phash = str(
                    imagehash.phash(converted, hash_size=8, highfreq_factor=4)
                ).lower()
            distance = (int(phash, 16) ^ int(expected_phash, 16)).bit_count()
    except DeadlineExceeded:
        result["decode_status"] = "decode_error"
        result["identity_class"] = "decode_error"
        result["error_code"] = "IMAGE_DECODE_TIMEOUT"
        return result
    except AuditError as exc:
        result["decode_status"] = "decode_error"
        result["identity_class"] = "decode_error"
        result["error_code"] = exc.code
        return result
    except Exception:  # Pillow raises multiple format-specific exceptions.
        result["decode_status"] = "decode_error"
        result["identity_class"] = "decode_error"
        result["error_code"] = "IMAGE_DECODE_FAILED"
        return result
    result.update(
        {
            "actual_width": str(width),
            "actual_height": str(height),
            "actual_phash_hex64": phash,
            "phash_distance": str(distance),
            "decode_status": "decoded",
        }
    )
    if distance <= 2:
        identity = "visual_near_candidate_d0_2"
    elif distance <= 6:
        identity = "visual_related_candidate_d3_6"
    else:
        identity = "content_changed_candidate_d_gt6"
    result["identity_class"] = identity
    return result


def base_attempt(row: dict[str, str], window_id: str, invocation_id: str, attempt_index: int, request_kind: str, resolved_via: str, result: TransportResult) -> dict[str, str]:
    attempt_id = hashlib.sha256(f"{row['record_id']}\0{window_id}\0{attempt_index}".encode()).hexdigest()[:24]
    content_type = clean_text(result.headers.get("content-type", "")).split(";", 1)[0].lower()
    return {
        "attempt_id": attempt_id,
        "record_id": row["record_id"],
        "component": row["component"],
        "active": row["active"],
        "window_id": window_id,
        "invocation_id": invocation_id,
        "attempt_index": str(attempt_index),
        "checked_at_utc": utc_now(),
        "request_kind": request_kind,
        "resolved_via": resolved_via,
        "url_requested": receipt_safe_url(result.url_requested),
        "url_final": receipt_safe_url(result.url_final),
        "host": url_host(result.url_final or result.url_requested),
        "policy_state": result.policy_state,
        "robots_status": result.robots_status,
        "transport_status": result.status,
        "http_status": str(result.http_status or ""),
        "redirect_count": str(len(result.redirects)),
        "redirect_chain_json": json.dumps(
            [
                {
                    "status": item.get("status"),
                    "from": receipt_safe_url(clean_text(item.get("from"))),
                    "to": receipt_safe_url(clean_text(item.get("to"))),
                }
                for item in result.redirects
            ],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "retry_after": clean_text(result.headers.get("retry-after", "")),
        "content_type": content_type,
        "content_type_image": "true" if content_type.startswith("image/") else "false",
        "magic_type": "",
        "decode_status": "not_attempted",
        "actual_bytes": str(len(result.data)) if result.status == "ok" else "",
        "actual_width": "",
        "actual_height": "",
        "actual_sha256": bytes_sha256(result.data) if result.status == "ok" else "",
        "actual_phash_hex64": "",
        "phash_distance": "",
        "sha256_match": "",
        "identity_class": "not_evaluated",
        "error_code": result.error_code,
        "bytes_retained": "false",
    }


def image_attempt(row: dict[str, str], client: NetworkClient, url: str, window_id: str, attempt_index: int, request_kind: str, resolved_via: str) -> dict[str, str]:
    if row["component"] == "S3":
        result = TransportResult(
            url_requested=url,
            status="policy_pending",
            error_code="S3_IMAGE_FETCH_NOT_AUTHORIZED",
            policy_state="pending_permission",
        )
    else:
        result = client.fetch(
            url,
            "image",
            accept="image/*,*/*;q=0.1",
            allowed_hosts=COMPONENT_IMAGE_HOSTS[row["component"]],
        )
    attempt = base_attempt(row, window_id, client.invocation_id, attempt_index, request_kind, resolved_via, result)
    if result.status == "ok":
        attempt.update(
            image_diagnostics(
                result.data,
                row["expected_sha256"],
                row["expected_phash_hex64"],
                client.max_image_pixels,
                client.max_image_dimension,
                client.decode_timeout,
            )
        )
        if attempt["content_type_image"] != "true" and attempt["magic_type"].startswith("image/"):
            attempt["error_code"] = "CONTENT_TYPE_MISMATCH"
    else:
        attempt["identity_class"] = {
            "policy_pending": "policy_pending_permission",
            "robots_disallowed": "robots_disallowed",
            "robots_unavailable": "robots_unavailable",
            "not_found": "not_found_404_410",
            "access_denied": "access_denied_401_403",
            "rate_limited": "rate_limited_429",
            "server_error": "transient_5xx",
            "timeout": "timeout",
            "dns_error": "dns_error",
            "tls_error": "tls_error",
            "oversize": "oversize",
            "bandwidth_budget_pending": "bandwidth_budget_pending",
            "safety_block": "safety_review_required",
        }.get(result.status, "network_or_transport_error")
    return attempt


class ImageLinkParser(html.parser.HTMLParser):
    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): clean_text(value) for key, value in attrs}
        candidate = ""
        if tag.lower() == "meta" and values.get("property", "").lower() in {"og:image", "twitter:image"}:
            candidate = values.get("content", "")
        elif tag.lower() in {"img", "source"}:
            candidate = values.get("src", "") or values.get("data-src", "")
        elif tag.lower() == "a":
            candidate = values.get("href", "")
        if candidate:
            self.urls.append(urllib.parse.urljoin(self.base_url, candidate))


def is_image_candidate(url: str) -> bool:
    try:
        path = urllib.parse.urlsplit(url).path.lower()
    except ValueError:
        return False
    return any(path.endswith(extension) for extension in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".tif", ".tiff"))


def resolver_attempt(row: dict[str, str], window_id: str, invocation_id: str, attempt_index: int, request_kind: str, resolved_via: str, result: TransportResult) -> dict[str, str]:
    attempt = base_attempt(row, window_id, invocation_id, attempt_index, request_kind, resolved_via, result)
    attempt["identity_class"] = "resolver_response" if result.status == "ok" else "resolver_error"
    return attempt


def inat_candidates(row: dict[str, str], client: NetworkClient, window_id: str, next_index: int) -> tuple[list[dict[str, str]], list[str]]:
    observation_match = re.search(r"/observations/(\d+)", row["pointer_url"])
    photo_match = re.search(r"/photos/(\d+)/", row["source_image_url"])
    if not photo_match:
        return [], []
    if not observation_match:
        photo_id = photo_match.group(1)
        attempts, candidates = html_candidates(
            row,
            client,
            window_id,
            next_index,
            f"https://www.inaturalist.org/photos/{photo_id}",
            "photo_landing",
            False,
        )
        marker = f"/photos/{photo_id}/"
        return attempts, [
            candidate
            for candidate in candidates
            if marker in urllib.parse.urlsplit(candidate).path
        ]
    api_url = f"https://api.inaturalist.org/v1/observations/{observation_match.group(1)}"
    result = client.fetch(
        api_url,
        "resolver_api",
        max_bytes=4 * 1024 * 1024,
        accept="application/json",
        allowed_hosts=COMPONENT_RESOLVER_HOSTS[row["component"]],
    )
    attempts = [resolver_attempt(row, window_id, client.invocation_id, next_index, "resolver_api", "source_api", result)]
    if result.status != "ok":
        return attempts, []
    try:
        payload = json.loads(result.data)
        if not isinstance(payload, dict):
            raise TypeError
        observations = payload.get("results", [])
        if not isinstance(observations, list):
            raise TypeError
        observation = observations[0] if observations else {}
        if not isinstance(observation, dict):
            raise TypeError
        photos = observation.get("photos", [])
        if not isinstance(photos, list):
            raise TypeError
    except (json.JSONDecodeError, AttributeError, IndexError, TypeError):
        attempts[0]["identity_class"] = "resolver_error"
        attempts[0]["error_code"] = "INAT_API_JSON_INVALID"
        return attempts, []
    photo_id = int(photo_match.group(1))
    candidates: list[str] = []
    for photo in photos:
        if not isinstance(photo, dict) or str(photo.get("id", "")) != str(photo_id):
            continue
        url = clean_url(photo.get("url"))
        if url:
            candidates.append(re.sub(r"/(square|small|medium|large|original)\.", "/large.", url))
    return attempts, candidates


def commons_candidates(row: dict[str, str], client: NetworkClient, window_id: str, next_index: int) -> tuple[list[dict[str, str]], list[str]]:
    parsed = urllib.parse.urlsplit(row["pointer_url"])
    marker = "/wiki/"
    if marker not in parsed.path:
        return [], []
    title = urllib.parse.unquote(parsed.path.split(marker, 1)[1])
    query = urllib.parse.urlencode(
        {
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "prop": "imageinfo",
            "iiprop": "url|size|sha1|mime",
            "maxlag": "1",
            "titles": title,
        }
    )
    api_url = f"https://commons.wikimedia.org/w/api.php?{query}"
    result = client.fetch(
        api_url,
        "resolver_api",
        max_bytes=4 * 1024 * 1024,
        accept="application/json",
        allowed_hosts=COMPONENT_RESOLVER_HOSTS[row["component"]],
    )
    attempts = [resolver_attempt(row, window_id, client.invocation_id, next_index, "resolver_api", "source_api", result)]
    if result.status != "ok":
        return attempts, []
    try:
        payload = json.loads(result.data)
        if not isinstance(payload, dict):
            raise TypeError
        error = payload.get("error")
        if isinstance(error, dict):
            attempts[0]["identity_class"] = "resolver_error"
            attempts[0]["error_code"] = (
                "COMMONS_API_MAXLAG"
                if clean_text(error.get("code")).lower() == "maxlag"
                else "COMMONS_API_ERROR"
            )
            return attempts, []
        query_payload = payload.get("query", {})
        if not isinstance(query_payload, dict):
            raise TypeError
        pages = query_payload.get("pages", [])
        if not isinstance(pages, list):
            raise TypeError
        page = pages[0] if pages else {}
        if not isinstance(page, dict):
            raise TypeError
        imageinfo = page.get("imageinfo", [])
        if not isinstance(imageinfo, list):
            raise TypeError
        candidates = [
            clean_url(item.get("url"))
            for item in imageinfo
            if isinstance(item, dict)
        ]
    except (json.JSONDecodeError, AttributeError, IndexError, TypeError):
        attempts[0]["identity_class"] = "resolver_error"
        attempts[0]["error_code"] = "COMMONS_API_JSON_INVALID"
        return attempts, []
    return attempts, [url for url in candidates if url]


def html_candidates(row: dict[str, str], client: NetworkClient, window_id: str, next_index: int, page_url: str, resolved_via: str, exact_basename: bool) -> tuple[list[dict[str, str]], list[str]]:
    result = client.fetch(
        page_url,
        "landing",
        max_bytes=4 * 1024 * 1024,
        accept="text/html,*/*;q=0.1",
        allowed_hosts=COMPONENT_RESOLVER_HOSTS[row["component"]],
    )
    attempts = [resolver_attempt(row, window_id, client.invocation_id, next_index, "resolver_page", resolved_via, result)]
    if result.status != "ok":
        return attempts, []
    try:
        text = result.data.decode("utf-8", errors="replace")
        parser = ImageLinkParser(result.url_final or page_url)
        parser.feed(text)
    except Exception:
        attempts[0]["identity_class"] = "resolver_error"
        attempts[0]["error_code"] = "HTML_PARSE_FAILED"
        return attempts, []
    candidates = [url for url in parser.urls if clean_url(url) and is_image_candidate(url)]
    if exact_basename:
        expected_name = Path(urllib.parse.urlsplit(row["source_image_url"]).path).name.lower()
        candidates = [url for url in candidates if Path(urllib.parse.urlsplit(url).path).name.lower() == expected_name]
    deduplicated: list[str] = []
    seen: set[str] = set()
    for url in candidates:
        if url not in seen:
            deduplicated.append(url)
            seen.add(url)
    return attempts, deduplicated


def resolve_candidates(row: dict[str, str], client: NetworkClient, window_id: str, next_index: int) -> tuple[list[dict[str, str]], list[str], str]:
    component = row["component"]
    if component in {"S0", "D0"}:
        attempts, candidates = inat_candidates(row, client, window_id, next_index)
        via = (
            "source_api"
            if re.search(r"/observations/\d+", row["pointer_url"])
            else "photo_landing"
        )
        return attempts, candidates, via
    if component == "S4":
        attempts, candidates = commons_candidates(row, client, window_id, next_index)
        return attempts, candidates, "source_api"
    if component == "S1":
        code = urllib.parse.quote(row["fishbase25_speccode"], safe="")
        page = f"https://www.fishbase.se/photos/ThumbnailsSummary.php?ID={code}"
        attempts, candidates = html_candidates(row, client, window_id, next_index, page, "landing_page", True)
        return attempts, candidates, "landing_page"
    page = row["pointer_url"] or row["source_page_url"]
    attempts, candidates = html_candidates(row, client, window_id, next_index, page, "landing_page", component in {"S2", "S3"})
    return attempts, candidates, "landing_page"


def audit_one(
    row: dict[str, str],
    client: NetworkClient,
    window_id: str,
    starting_index: int,
    on_attempt: Callable[[dict[str, str]], None] | None = None,
) -> AuditRecordResult:
    attempts: list[dict[str, str]] = []

    def record(attempt: dict[str, str]) -> None:
        attempts.append(attempt)
        if on_attempt is not None:
            on_attempt(attempt)

    direct = image_attempt(row, client, row["source_image_url"], window_id, starting_index, "direct_image", "direct")
    record(direct)
    if direct["identity_class"] == "byte_exact":
        return AuditRecordResult(attempts, True, "direct_exact", 0, 0)
    if direct["identity_class"] == "safety_review_required":
        return AuditRecordResult(
            attempts, False, "fail_safety_review", 0, 0
        )
    resolver_rows, candidates, via = resolve_candidates(row, client, window_id, starting_index + len(attempts))
    for resolver_row in resolver_rows:
        record(resolver_row)
    if not resolver_rows:
        return AuditRecordResult(
            attempts, False, "pending_resolver_adapter", 0, 0
        )
    resolver = resolver_rows[-1]
    resolver_transport = resolver.get("transport_status", "")
    resolver_ready = (
        resolver_transport == "ok"
        and resolver.get("identity_class") == "resolver_response"
    )
    if not resolver_ready:
        if resolver_transport == "safety_block":
            return AuditRecordResult(
                attempts, False, "fail_safety_review", 0, 0
            )
        if direct["identity_class"] in {
            "policy_pending_permission",
            "robots_disallowed",
            "robots_unavailable",
        }:
            return AuditRecordResult(
                attempts, False, "pending_policy", 0, 0
            )
        if direct["identity_class"] == "bandwidth_budget_pending":
            return AuditRecordResult(
                attempts, False, "pending_local_deferral", 0, 0
            )
        if direct["identity_class"] == "oversize":
            return AuditRecordResult(
                attempts, False, "pending_local_response_cap", 0, 0
            )
        if resolver_transport in {"not_found", "access_denied"}:
            return AuditRecordResult(
                attempts, True, "resolver_access_or_absence_observed", 0, 0
            )
        if resolver_transport in {
            "policy_pending",
            "robots_disallowed",
            "robots_unavailable",
        }:
            status = "pending_policy"
        elif resolver_transport == "bandwidth_budget_pending":
            status = "pending_local_deferral"
        elif resolver_transport == "oversize":
            return AuditRecordResult(
                attempts, False, "pending_local_response_cap", 0, 0
            )
        elif resolver.get("error_code") == "COMMONS_API_MAXLAG":
            return AuditRecordResult(
                attempts, True, "resolver_transient_observed", 0, 0
            )
        elif resolver_transport in {
            "rate_limited",
            "server_error",
            "timeout",
            "dns_error",
            "tls_error",
            "network_error",
        }:
            return AuditRecordResult(
                attempts, True, "resolver_transient_observed", 0, 0
            )
        elif resolver_transport == "ok":
            return AuditRecordResult(
                attempts, True, "resolver_invalid_response_observed", 0, 0
            )
        else:
            return AuditRecordResult(
                attempts, True, "resolver_http_error_observed", 0, 0
            )
        return AuditRecordResult(attempts, False, status, 0, 0)
    seen = {row["source_image_url"]}
    candidate_queue: list[str] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        candidate_queue.append(candidate)
    fallback_attempts = 0
    for candidate in candidate_queue[: client.max_fallback]:
        attempt = image_attempt(row, client, candidate, window_id, starting_index + len(attempts), "fallback_image", via)
        record(attempt)
        fallback_attempts += 1
        if attempt["identity_class"] == "byte_exact":
            return AuditRecordResult(
                attempts,
                True,
                "fallback_exact",
                len(candidate_queue),
                fallback_attempts,
            )
    image_classes = {
        attempt.get("identity_class", "")
        for attempt in attempts
        if attempt.get("request_kind") in {"direct_image", "fallback_image"}
    }
    if "safety_review_required" in image_classes:
        protocol_status = "fail_safety_review"
        protocol_complete = False
    elif image_classes & {
        "policy_pending_permission",
        "robots_disallowed",
        "robots_unavailable",
    }:
        protocol_status = "pending_policy"
        protocol_complete = False
    elif "bandwidth_budget_pending" in image_classes:
        protocol_status = "pending_local_deferral"
        protocol_complete = False
    elif "oversize" in image_classes:
        protocol_status = "pending_local_response_cap"
        protocol_complete = False
    elif len(candidate_queue) > client.max_fallback:
        protocol_status = "pending_candidate_cap"
        protocol_complete = False
    elif image_classes & {
        "rate_limited_429",
        "transient_5xx",
        "timeout",
        "dns_error",
        "tls_error",
        "network_or_transport_error",
    }:
        protocol_status = "fallback_transient_observed"
        protocol_complete = True
    elif candidate_queue:
        protocol_status = "exhausted_nonexact"
        protocol_complete = True
    else:
        protocol_status = "resolver_no_candidate"
        protocol_complete = True
    return AuditRecordResult(
        attempts,
        protocol_complete,
        protocol_status,
        len(candidate_queue),
        fallback_attempts,
    )


IDENTITY_PRIORITY = {
    "byte_exact": 100,
    "visual_near_candidate_d0_2": 80,
    "visual_related_candidate_d3_6": 70,
    "content_changed_candidate_d_gt6": 60,
    "decode_error": 45,
    "empty_response": 41,
    "non_image": 40,
    "not_found_404_410": 30,
    "access_denied_401_403": 25,
    "rate_limited_429": 20,
    "transient_5xx": 19,
    "timeout": 18,
    "dns_error": 17,
    "tls_error": 16,
    "bandwidth_budget_pending": 15,
    "robots_disallowed": 10,
    "robots_unavailable": 9,
    "policy_pending_permission": 8,
    "safety_review_required": 7,
    "network_or_transport_error": 5,
}

RETRYABLE_CLASSES = {
    "rate_limited_429",
    "transient_5xx",
    "timeout",
    "dns_error",
    "tls_error",
    "bandwidth_budget_pending",
    "robots_unavailable",
    "network_or_transport_error",
    "empty_response",
}

POLICY_PENDING_CLASSES = {
    "policy_pending_permission",
    "robots_disallowed",
    "robots_unavailable",
}

FINALITY_NON_OBSERVATION_CLASSES = {
    *POLICY_PENDING_CLASSES,
    "bandwidth_budget_pending",
    "oversize",
    "safety_review_required",
    "not_audited",
}

RETRYABLE_RESOLVER_TRANSPORT = {
    "rate_limited", "server_error", "timeout", "dns_error", "tls_error",
    "bandwidth_budget_pending", "robots_unavailable", "network_error",
}


def read_attempts(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    fields, rows = read_tsv(path)
    if tuple(fields) != ATTEMPT_FIELDS:
        raise AuditError("ATTEMPTS_SCHEMA_INVALID")
    return rows


def image_attempts_for_record(attempts: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in attempts if row.get("request_kind") in {"direct_image", "fallback_image"}]


def best_attempt(attempts: list[dict[str, str]]) -> dict[str, str] | None:
    candidates = image_attempts_for_record(attempts)
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            IDENTITY_PRIORITY.get(row.get("identity_class", ""), 0),
            int(row.get("attempt_index", "0") or 0),
        ),
    )


def record_retryable(attempts: list[dict[str, str]], best: dict[str, str] | None = None) -> bool:
    selected = best if best is not None else best_attempt(attempts)
    if selected is None:
        return True
    final_class = selected.get("identity_class", "")
    if final_class in {"byte_exact", "policy_pending_permission"}:
        return False
    if final_class in RETRYABLE_CLASSES:
        return True
    return any(
        row.get("request_kind") in {"resolver_api", "resolver_page"}
        and (
            row.get("transport_status") in RETRYABLE_RESOLVER_TRANSPORT
            or row.get("error_code") == "COMMONS_API_MAXLAG"
        )
        for row in attempts
    )


def should_process(prior: list[dict[str, str]], retry_mode: str) -> bool:
    if not prior:
        return True
    best = best_attempt(prior)
    if best is None:
        return True
    final_class = best.get("identity_class", "")
    if retry_mode == "all":
        return True
    if retry_mode == "nonexact":
        return final_class != "byte_exact" and final_class != "policy_pending_permission"
    if retry_mode == "transient":
        return record_retryable(prior, best)
    return False


def unrecovered_abandoned_record_ids(
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
) -> set[str]:
    attempts_by_id = {row["attempt_id"]: row for row in attempts}
    latest_disposed: dict[str, int] = defaultdict(int)
    for disposition in dispositions:
        attempt = attempts_by_id.get(disposition.get("attempt_id", ""))
        if attempt is not None:
            latest_disposed[attempt["record_id"]] = max(
                latest_disposed[attempt["record_id"]],
                int(attempt["attempt_index"]),
            )
    latest_completed: dict[str, int] = defaultdict(int)
    for completion in completions:
        latest_completed[completion["record_id"]] = max(
            latest_completed[completion["record_id"]],
            int(completion["last_attempt_index"]),
        )
    return {
        record_id
        for record_id, disposed_index in latest_disposed.items()
        if disposed_index > latest_completed.get(record_id, 0)
    }


def build_health_rows(
    source_rows: list[dict[str, str]],
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    final_window_ids: set[str] | None = None,
) -> list[dict[str, str]]:
    declared_final_window_ids = set(final_window_ids or set())
    by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for attempt in attempts:
        by_record[attempt.get("record_id", "")].append(attempt)
    completions_by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for completion in completions:
        completions_by_record[completion.get("record_id", "")].append(
            completion
        )
    actual_sha_expected: dict[str, set[str]] = defaultdict(set)
    for row in source_rows:
        for attempt in image_attempts_for_record(by_record.get(row["record_id"], [])):
            if attempt.get("actual_sha256"):
                actual_sha_expected[attempt["actual_sha256"]].add(row["expected_sha256"])
    suspicious = {sha for sha, expected in actual_sha_expected.items() if len(expected) >= 3}
    health: list[dict[str, str]] = []
    for row in source_rows:
        record_attempts = by_record.get(row["record_id"], [])
        record_completions = completions_by_record.get(row["record_id"], [])
        latest_completion = max(
            record_completions,
            key=lambda item: (
                int(item.get("last_attempt_index", "0") or 0),
                item.get("completed_at_utc", ""),
            ),
            default=None,
        )
        best = best_attempt(record_attempts)
        result = {field: "" for field in HEALTH_FIELDS}
        result.update({field: row.get(field, "") for field in HEALTH_FIELDS})
        result["attempts"] = str(len(record_attempts))
        timestamps = sorted(item.get("checked_at_utc", "") for item in record_attempts if item.get("checked_at_utc"))
        result["first_checked_at_utc"] = timestamps[0] if timestamps else ""
        result["last_checked_at_utc"] = timestamps[-1] if timestamps else ""
        result["bytes_retained"] = "false"
        if best is None:
            result["final_class"] = "not_audited"
            result["retryable"] = "true"
        else:
            mapping = {
                "resolved_via": "resolved_via",
                "url_final": "final_url",
                "http_status": "http_status",
                "content_type": "content_type",
                "magic_type": "magic_type",
                "actual_bytes": "actual_bytes",
                "actual_width": "actual_width",
                "actual_height": "actual_height",
                "actual_sha256": "actual_sha256",
                "actual_phash_hex64": "actual_phash_hex64",
                "phash_distance": "phash_distance",
                "sha256_match": "sha256_match",
                "identity_class": "final_class",
            }
            for source, target in mapping.items():
                result[target] = best.get(source, "")
            result["retryable"] = "true" if record_retryable(record_attempts, best) else "false"
            result["possible_placeholder_cluster"] = "true" if best.get("actual_sha256") in suspicious else "false"
        if latest_completion is None:
            result["resolution_protocol_complete"] = "false"
            result["resolution_protocol_status"] = "not_audited"
        else:
            result["resolution_protocol_complete"] = latest_completion.get(
                "resolution_protocol_complete", "false"
            )
            result["resolution_protocol_status"] = latest_completion.get(
                "resolution_protocol_status", ""
            )
        if (
            result["resolution_protocol_complete"] != "true"
            and result["resolution_protocol_status"]
            not in {"pending_policy", "fail_safety_review"}
        ):
            result["retryable"] = "true"
        multiwindow_applies = result["final_class"] not in {
            "byte_exact",
            *FINALITY_NON_OBSERVATION_CLASSES,
        }
        metrics = retry_protocol_metrics(
            record_attempts, record_completions, declared_final_window_ids
        )
        result["multiwindow_protocol_applies"] = (
            "true" if multiwindow_applies else "false"
        )
        result["distinct_observation_windows"] = str(metrics["distinct_windows"])
        result["distinct_observation_utc_dates"] = str(
            metrics["distinct_utc_dates"]
        )
        result["observation_elapsed_hours"] = f"{metrics['elapsed_hours']:.6f}"
        result["observed_in_declared_final_window"] = (
            "true" if metrics["final_window_observed"] else "false"
        )
        result["latest_observation_is_declared_final"] = (
            "true" if metrics["latest_observation_is_final"] else "false"
        )
        if result["final_class"] == "byte_exact":
            result["finality_status"] = "not_applicable_exact"
        elif result["resolution_protocol_status"] == "fail_safety_review":
            result["finality_status"] = "fail_safety_review"
        elif (
            result["final_class"] in POLICY_PENDING_CLASSES
            or result["resolution_protocol_status"] == "pending_policy"
        ):
            result["finality_status"] = "pending_policy"
        elif (
            result["final_class"] == "bandwidth_budget_pending"
            or result["resolution_protocol_status"]
            in {"pending_local_deferral", "pending_local_response_cap"}
        ):
            result["finality_status"] = "pending_local_deferral"
        elif result["final_class"] == "safety_review_required":
            result["finality_status"] = "fail_safety_review"
        elif result["final_class"] == "not_audited":
            result["finality_status"] = "pending_not_audited"
        elif result["resolution_protocol_complete"] != "true":
            result["finality_status"] = "pending_resolution_protocol"
        else:
            result["finality_status"] = (
                "satisfied" if metrics["satisfied"] else "pending"
            )
        health.append(result)
    return health


def nested_counts(rows: list[dict[str, str]], field: str, group: str) -> dict[str, dict[str, int]]:
    result: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        result[row[group]][row[field]] += 1
    return {key: dict(sorted(value.items())) for key, value in sorted(result.items())}


def retry_protocol_metrics(
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    final_window_ids: set[str],
) -> dict[str, Any]:
    """Evaluate the dated retry protocol from committed attempts only."""
    protocol_complete_windows = {
        row.get("window_id", "")
        for row in completions
        if row.get("resolution_protocol_complete") == "true"
    }
    observations = [
        row
        for row in attempts
        if row.get("request_kind") in {"direct_image", "fallback_image"}
        and row.get("identity_class") not in FINALITY_NON_OBSERVATION_CLASSES
        and row.get("window_id") in protocol_complete_windows
    ]
    windows = {
        row.get("window_id", "")
        for row in observations
        if row.get("window_id")
    }
    dated_attempts = [
        (utc_timestamp(row["checked_at_utc"]), row.get("window_id", ""))
        for row in observations
        if row.get("checked_at_utc")
    ]
    timestamps = sorted(stamp for stamp, _ in dated_attempts)
    utc_dates = {
        datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()
        for stamp in timestamps
    }
    elapsed_hours = (
        (timestamps[-1] - timestamps[0]) / 3600
        if len(timestamps) >= 2
        else 0.0
    )
    final_window_observed = bool(windows & final_window_ids)
    latest_windows = {
        window_id
        for stamp, window_id in dated_attempts
        if timestamps and stamp == timestamps[-1]
    }
    latest_observation_is_final = bool(latest_windows) and latest_windows.issubset(
        final_window_ids
    )
    satisfied = (
        len(windows) >= FINALITY_MIN_WINDOWS
        and len(utc_dates) >= FINALITY_MIN_UTC_DATES
        and elapsed_hours >= FINALITY_MIN_ELAPSED_HOURS
        and latest_observation_is_final
    )
    return {
        "distinct_windows": len(windows),
        "distinct_utc_dates": len(utc_dates),
        "elapsed_hours": elapsed_hours,
        "final_window_observed": final_window_observed,
        "latest_observation_is_final": latest_observation_is_final,
        "satisfied": satisfied,
        "protocol_complete_windows": len(protocol_complete_windows),
    }


def summary_payload(
    bindings: dict[str, Any],
    source_rows: list[dict[str, str]],
    attempts: list[dict[str, str]],
    committed_attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
    robots: list[dict[str, str]],
    health: list[dict[str, str]],
    scope: str,
    final_window: bool,
    contract: dict[str, Any],
    final_window_ids: set[str] | None = None,
    tail_recoveries: list[dict[str, str]] | None = None,
    atomic_temp_recoveries: list[dict[str, str]] | None = None,
    run_metadata: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    expected = validated_expected_counts(bindings)
    declared_final_window_ids = set(final_window_ids or set())
    classes = Counter(row["final_class"] for row in health)
    active_rows = [row for row in health if row["active"] == "true"]
    active_classes = Counter(row["final_class"] for row in active_rows)
    exact_archive = classes.get("byte_exact", 0)
    exact_active = active_classes.get("byte_exact", 0)
    s1_rows = [row for row in health if row["component"] == "S1"]
    s1_exact = sum(row["final_class"] == "byte_exact" for row in s1_rows)
    s1_near = sum(row["final_class"] == "visual_near_candidate_d0_2" for row in s1_rows)
    protocol_statuses = Counter(
        row["resolution_protocol_status"] for row in health
    )
    protocol_incomplete = sum(
        row["resolution_protocol_complete"] != "true" for row in health
    )
    pending_policy = sum(
        row["finality_status"] == "pending_policy" for row in health
    )
    pending_local_deferral = sum(
        row["finality_status"] == "pending_local_deferral" for row in health
    )
    safety_review_ids = {
        row["record_id"]
        for row in attempts
        if row.get("identity_class") == "safety_review_required"
        or row.get("transport_status") == "safety_block"
    }
    safety_review_ids.update(
        row["record_id"]
        for row in health
        if row.get("resolution_protocol_status") == "fail_safety_review"
    )
    robots_safety_rows = [
        row for row in robots if row.get("fetch_status") == "safety_block"
    ]
    retryable = sum(row["retryable"] == "true" for row in health)
    not_audited = classes.get("not_audited", 0)
    completed_ids = {row.get("record_id", "") for row in completions}
    completed_health = [row for row in health if row["record_id"] in completed_ids]
    completed_active = [row for row in completed_health if row["active"] == "true"]
    completed_exact = sum(row["final_class"] == "byte_exact" for row in completed_health)
    completed_active_exact = sum(row["final_class"] == "byte_exact" for row in completed_active)
    uncompleted = len(source_rows) - len(completed_ids)
    covered_attempts = completion_coverage(completions)
    disposed_ids = {row.get("attempt_id", "") for row in dispositions}
    uncommitted_attempts = sum(
        (
            row.get("record_id", ""),
            row.get("window_id", ""),
            int(row.get("attempt_index", "0") or 0),
        )
        not in covered_attempts
        and row.get("attempt_id", "") not in disposed_ids
        for row in attempts
    )
    timestamps = sorted(
        row["checked_at_utc"]
        for row in committed_attempts
        if row.get("checked_at_utc")
    )
    committed_by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for attempt in committed_attempts:
        committed_by_record[attempt["record_id"]].append(attempt)
    completions_by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for completion in completions:
        completions_by_record[completion["record_id"]].append(completion)
    retryable_ids = {
        row["record_id"] for row in health if row["retryable"] == "true"
    }
    finality_ids = {
        row["record_id"]
        for row in health
        if row["final_class"] not in {"byte_exact", *FINALITY_NON_OBSERVATION_CLASSES}
    }
    finality_satisfied = 0
    finality_unsatisfied = 0
    finality_observed_in_final_window = 0
    finality_latest_in_final_window = 0
    for record_id in finality_ids:
        record_attempts = committed_by_record[record_id]
        metrics = retry_protocol_metrics(
            record_attempts,
            completions_by_record[record_id],
            declared_final_window_ids,
        )
        if metrics["final_window_observed"]:
            finality_observed_in_final_window += 1
        if metrics["latest_observation_is_final"]:
            finality_latest_in_final_window += 1
        if metrics["satisfied"]:
            finality_satisfied += 1
        else:
            finality_unsatisfied += 1
    scope_complete = uncompleted == 0 and not not_audited and uncommitted_attempts == 0
    archive_population_complete = (
        scope == "archive"
        and scope_complete
        and len(health) == expected["archive_pointer_rows"]
    )
    active_population_complete = (
        archive_population_complete
        and len(active_rows) == expected["active_pointer_rows"]
    )
    r0_population_complete = (
        archive_population_complete
        and len(s1_rows) == expected["r0_pointer_rows"]
    )
    status = "FAIL" if safety_review_ids or robots_safety_rows else "PASS"
    if not safety_review_ids and not robots_safety_rows and (
        not scope_complete
        or pending_policy
        or pending_local_deferral
        or protocol_incomplete
        or (final_window and not declared_final_window_ids)
        or (finality_ids and (not final_window or finality_unsatisfied))
    ):
        status = "PENDING"
    return {
        "schema": "coverfish.pointer-health-summary.v2",
        "status": status,
        "audit_integrity": "PASS" if len(health) == len(source_rows) and scope_complete else "PENDING",
        "tool_version": TOOL_VERSION,
        "tool_sha256": contract.get("tool_sha256"),
        "phash_algorithm": PHASH_ALGORITHM,
        "dependencies": contract.get("dependencies", {}),
        "runtime": contract.get("runtime", {}),
        "scope": scope,
        "final_window": final_window,
        "dataset": bindings.get("dataset", {}),
        "rows": {
            "scope": len(health),
            "archive_expected": bindings.get("expected", {}).get("archive_pointer_rows"),
            "active_in_scope": len(active_rows),
            "retired_in_scope": len(health) - len(active_rows),
        },
        "outcomes": dict(sorted(classes.items())),
        "active_outcomes": dict(sorted(active_classes.items())),
        "by_component": nested_counts(health, "final_class", "component"),
        "by_source_host": nested_counts(health, "final_class", "source_host"),
        "by_resolved_via": dict(sorted(Counter(row["resolved_via"] for row in health).items())),
        "by_resolution_protocol_status": dict(sorted(protocol_statuses.items())),
        "by_final_host": dict(sorted(Counter(url_host(row["final_url"]) for row in health).items())),
        "by_http_status": dict(sorted(Counter(row["http_status"] for row in health).items())),
        "attempt_aggregations": {
            "by_host_and_transport": nested_counts(
                attempts, "transport_status", "host"
            ),
            "by_transport_status": dict(
                sorted(Counter(row["transport_status"] for row in attempts).items())
            ),
            "by_error_code": dict(
                sorted(Counter(row["error_code"] for row in attempts).items())
            ),
            "by_redirect_count": dict(
                sorted(Counter(row["redirect_count"] for row in attempts).items())
            ),
        },
        "robots_aggregations": {
            "rows": len(robots),
            "by_fetch_status": dict(
                sorted(Counter(row["fetch_status"] for row in robots).items())
            ),
            "by_robots_state": dict(
                sorted(Counter(row["robots_state"] for row in robots).items())
            ),
            "safety_review_rows": len(robots_safety_rows),
        },
        "run_retry_modes": dict(
            sorted(
                Counter(
                    clean_text(row.get("retry_mode"))
                    for row in (run_metadata or [])
                ).items()
            )
        ),
        "host_rate_policy": contract.get("host_rate_policy", {}),
        "transient_backoff": contract.get("transient_backoff", {}),
        "attempts": len(attempts),
        "attempt_bytes": sum(int(row.get("actual_bytes") or 0) for row in attempts),
        "committed_attempts": len(committed_attempts),
        "committed_attempt_bytes": sum(int(row.get("actual_bytes") or 0) for row in committed_attempts),
        "receipt": {
            "completion_rows": len(completions),
            "abandoned_attempt_rows": len(dispositions),
            "completed_scope_rows": len(completed_ids),
            "uncompleted_scope_rows": uncompleted,
            "uncommitted_attempt_rows": uncommitted_attempts,
            "tail_recovery_rows": len(tail_recoveries or []),
            "atomic_temp_recovery_rows": len(atomic_temp_recoveries or []),
            "bindings_sha256": contract.get("bindings_sha256"),
            "manifest_sha256": contract.get("manifest_sha256"),
            "policy_sha256": contract.get("policy_sha256"),
            "contract_sha256": contract.get("_sha256"),
        },
        "observation": {
            "first_checked_at_utc": timestamps[0] if timestamps else None,
            "last_checked_at_utc": timestamps[-1] if timestamps else None,
            "window_ids": sorted({row["window_id"] for row in committed_attempts}),
        },
        "finality": {
            "declared_final_window": final_window,
            "declared_final_window_ids": sorted(declared_final_window_ids),
            "minimum_distinct_windows": FINALITY_MIN_WINDOWS,
            "minimum_distinct_utc_dates": FINALITY_MIN_UTC_DATES,
            "minimum_elapsed_hours": FINALITY_MIN_ELAPSED_HOURS,
            "retryable_rows": len(retryable_ids),
            "nonexact_rows_requiring_retry_protocol": len(finality_ids),
            "rows_observed_in_declared_final_window": finality_observed_in_final_window,
            "rows_whose_latest_observation_is_declared_final": finality_latest_in_final_window,
            "rows_satisfying_retry_protocol": finality_satisfied,
            "rows_pending_retry_protocol": finality_unsatisfied,
        },
        "resolution_protocol": {
            "complete_rows": len(health) - protocol_incomplete,
            "incomplete_rows": protocol_incomplete,
            "status_counts": dict(sorted(protocol_statuses.items())),
        },
        "exact": {
            "scope_rows": exact_archive,
            "active_scope_rows": exact_active,
            "archive_rows": exact_archive if archive_population_complete else None,
            "active_rows": exact_active if active_population_complete else None,
            "scope_rate": exact_archive / len(health) if scope_complete and health else None,
            "active_scope_rate": exact_active / len(active_rows) if scope_complete and active_rows else None,
            "archive_rate": exact_archive / expected["archive_pointer_rows"] if archive_population_complete else None,
            "active_rate": exact_active / expected["active_pointer_rows"] if active_population_complete else None,
            "completed_rows": completed_exact,
            "completed_rate": completed_exact / len(completed_health) if completed_health else None,
            "completed_active_rows": completed_active_exact,
            "completed_active_rate": completed_active_exact / len(completed_active) if completed_active else None,
        },
        "r0": {
            "frozen_byte_complete_rows": expected["r0_byte_complete_rows"],
            "pointer_rows": expected["r0_pointer_rows"],
            "pointer_rows_in_scope": len(s1_rows),
            "pointer_byte_exact_rows_in_scope": s1_exact,
            "pointer_visual_near_candidate_rows_in_scope": s1_near,
            "pointer_byte_exact_rows": s1_exact if r0_population_complete else None,
            "pointer_visual_near_candidate_rows": s1_near if r0_population_complete else None,
            "strict_exact_byte_availability_rows": expected["r0_byte_complete_rows"] + s1_exact if r0_population_complete else None,
            "strict_exact_byte_availability_rate": (expected["r0_byte_complete_rows"] + s1_exact) / expected["r0_rows"] if r0_population_complete else None,
            "population_complete": r0_population_complete,
        },
        "active_staging": {
            "frozen_byte_complete_rows": expected["active_byte_complete_rows"],
            "pointer_rows": expected["active_pointer_rows"],
            "pointer_rows_in_scope": len(active_rows),
            "pointer_byte_exact_rows_in_scope": exact_active,
            "pointer_byte_exact_rows": exact_active if active_population_complete else None,
            "strict_exact_byte_availability_rows": expected["active_byte_complete_rows"] + exact_active if active_population_complete else None,
            "strict_exact_byte_availability_rate": (expected["active_byte_complete_rows"] + exact_active) / expected["active_staging_rows"] if active_population_complete else None,
            "population_complete": active_population_complete,
        },
        "e0": {
            "byte_complete_rows": bindings.get("expected", {}).get("e0_byte_complete_rows"),
            "pointer_rows": bindings.get("expected", {}).get("e0_pointer_rows"),
        },
        "pending": {
            "not_audited_rows": not_audited,
            "uncompleted_scope_rows": uncompleted,
            "policy_rows": pending_policy,
            "local_deferral_rows": pending_local_deferral,
            "resolution_protocol_rows": protocol_incomplete,
            "safety_review_rows": len(safety_review_ids) + len(robots_safety_rows),
            "safety_review_record_rows": len(safety_review_ids),
            "safety_review_robots_rows": len(robots_safety_rows),
            "retryable_rows": retryable,
        },
        "possible_placeholder_cluster_rows": sum(row["possible_placeholder_cluster"] == "true" for row in health),
        "bytes_retained": False,
        "frozen_tier_mutated": False,
    }


def summarize_to_files(
    bindings: dict[str, Any],
    source_rows: list[dict[str, str]],
    output_dir: Path,
    scope: str,
    final_window: bool,
    final_window_ids: set[str] | None = None,
) -> dict[str, Any]:
    attempts_path = output_dir / "pointer-health-attempts.tsv"
    attempts = read_attempts(attempts_path)
    completions = read_completions(output_dir / "record-completions.tsv")
    dispositions = read_dispositions(output_dir / "attempt-dispositions.tsv")
    robots = read_robots(output_dir / "robots.tsv")
    tail_recoveries = read_tail_recoveries(output_dir / "tail-recoveries.tsv")
    atomic_temp_recoveries = read_atomic_temp_recoveries(
        output_dir / "atomic-temp-recoveries.tsv"
    )
    run_metadata = load_and_validate_invocation_metadata(
        output_dir, attempts, completions, dispositions, robots
    )
    committed_attempts = select_committed_attempts(attempts, completions)
    health = build_health_rows(
        source_rows, committed_attempts, completions, final_window_ids
    )
    atomic_write_tsv(output_dir / "pointer-health.tsv", HEALTH_FIELDS, health)
    contract = load_json(output_dir / "audit-contract.json")
    contract["_sha256"] = file_sha256(output_dir / "audit-contract.json")
    summary = summary_payload(
        bindings,
        source_rows,
        attempts,
        committed_attempts,
        completions,
        dispositions,
        robots,
        health,
        scope,
        final_window,
        contract,
        final_window_ids,
        tail_recoveries,
        atomic_temp_recoveries,
        run_metadata,
    )
    atomic_write_json(output_dir / "pointer-health-summary.json", summary)
    return summary


def command_plan(args: argparse.Namespace) -> int:
    bindings, rows = load_bound_rows(args.source_root, args.bindings)
    result = binding_report(bindings, rows, args.bindings)
    result.update({"command": "plan", "schema": SCHEMA, "status": "PASS", "tool_version": TOOL_VERSION, "network_accessed": False})
    emit(result)
    return EXIT_OK


def command_sample(args: argparse.Namespace) -> int:
    _, rows = load_bound_rows(args.source_root, args.bindings)
    selected = select_sample(rows, args.seed)
    atomic_write_tsv(args.output, SAMPLE_FIELDS, selected)
    result = {
        "command": "sample",
        "schema": SCHEMA,
        "status": "PASS",
        "tool_version": TOOL_VERSION,
        "network_accessed": False,
        "rows": len(selected),
        "by_component": dict(sorted(Counter(row["component"] for row in selected).items())),
        "output_sha256": file_sha256(args.output),
        "seed": args.seed,
    }
    emit(result)
    return EXIT_OK


def append_tsv_rows(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = (
        os.O_CREAT | os.O_WRONLY | os.O_APPEND
        | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o644)
    except OSError as exc:
        raise AuditError("APPEND_OUTPUT_OPEN_FAILED", EXIT_SAFETY) from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o022
    ):
        os.close(descriptor)
        raise AuditError("APPEND_OUTPUT_SAFETY_INVALID", EXIT_SAFETY)
    empty = metadata.st_size == 0
    with os.fdopen(descriptor, "a", newline="", encoding="utf-8", buffering=1) as target:
        writer = csv.DictWriter(
            target,
            fieldnames=fieldnames,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        if empty:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
        target.flush()
        os.fsync(target.fileno())
    if empty:
        fsync_directory(path.parent)


def append_attempts(path: Path, rows: list[dict[str, str]]) -> None:
    append_tsv_rows(path, ATTEMPT_FIELDS, rows)


def recover_incomplete_tsv_tail(
    path: Path,
    expected_fields: Sequence[str],
    output_dir: Path,
) -> None:
    """Quarantine the hash/length of a crash-truncated final row, then truncate it."""
    if not path.exists():
        return
    if path.is_symlink():
        raise AuditError("APPEND_LEDGER_SYMLINK_FORBIDDEN", EXIT_SAFETY)
    read_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, read_flags)
    except OSError as exc:
        raise AuditError("APPEND_LEDGER_READ_FAILED") from exc
    try:
        metadata = os.fstat(descriptor)
        original_size = metadata.st_size
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
        ):
            raise AuditError("APPEND_LEDGER_TYPE_OR_OWNER_INVALID", EXIT_SAFETY)
        if original_size == 0 or os.pread(descriptor, 1, original_size - 1) == b"\n":
            return
        expected_header = "\t".join(expected_fields).encode() + b"\n"
        if os.pread(descriptor, len(expected_header), 0) != expected_header:
            raise AuditError("APPEND_LEDGER_HEADER_INVALID")
        position = original_size
        last_newline = -1
        while position > 0 and last_newline < 0:
            start = max(0, position - 64 * 1024)
            block = os.pread(descriptor, position - start, start)
            index = block.rfind(b"\n")
            if index >= 0:
                last_newline = start + index
            position = start
        if last_newline < 0:
            raise AuditError("APPEND_LEDGER_HEADER_TRUNCATED")
        digest = hashlib.sha256()
        offset = last_newline + 1
        while offset < original_size:
            block = os.pread(
                descriptor,
                min(1024 * 1024, original_size - offset),
                offset,
            )
            if not block:
                raise AuditError("APPEND_LEDGER_READ_FAILED")
            digest.update(block)
            offset += len(block)
        fragment_sha = digest.hexdigest()
    finally:
        os.close(descriptor)
    retained_size = last_newline + 1
    fragment_size = original_size - retained_size
    recovery_id = hashlib.sha256(
        f"{path.name}\0{original_size}\0{retained_size}\0{fragment_sha}".encode()
    ).hexdigest()[:24]
    recovery_path = output_dir / "tail-recoveries.tsv"
    existing = read_tail_recoveries(recovery_path)
    if recovery_id not in {row.get("recovery_id", "") for row in existing}:
        recovery = {
            "recovery_id": recovery_id,
            "ledger": path.name,
            "detected_at_utc": utc_now(),
            "original_size_bytes": str(original_size),
            "retained_size_bytes": str(retained_size),
            "discarded_fragment_bytes": str(fragment_size),
            "discarded_fragment_sha256": fragment_sha,
        }
        atomic_write_tsv(
            recovery_path,
            TAIL_RECOVERY_FIELDS,
            [*existing, recovery],
        )
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AuditError("APPEND_LEDGER_RECOVERY_OPEN_FAILED", EXIT_SAFETY) from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_size != original_size
        ):
            raise AuditError("APPEND_LEDGER_CHANGED_DURING_RECOVERY", EXIT_SAFETY)
        os.ftruncate(descriptor, retained_size)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def prepare_append_ledgers(output_dir: Path) -> None:
    for name, fields in (
        ("pointer-health-attempts.tsv", ATTEMPT_FIELDS),
        ("record-completions.tsv", COMPLETION_FIELDS),
        ("attempt-dispositions.tsv", DISPOSITION_FIELDS),
        ("robots.tsv", ROBOTS_FIELDS),
        ("atomic-temp-recoveries.tsv", ATOMIC_TEMP_RECOVERY_FIELDS),
    ):
        recover_incomplete_tsv_tail(output_dir / name, fields, output_dir)


def read_completions(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    fields, rows = read_tsv(path)
    if tuple(fields) != COMPLETION_FIELDS:
        raise AuditError("COMPLETIONS_SCHEMA_INVALID")
    return rows


def read_dispositions(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    fields, rows = read_tsv(path)
    if tuple(fields) != DISPOSITION_FIELDS:
        raise AuditError("DISPOSITIONS_SCHEMA_INVALID")
    return rows


def read_robots(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    fields, rows = read_tsv(path)
    if tuple(fields) != ROBOTS_FIELDS:
        raise AuditError("ROBOTS_RECEIPT_SCHEMA_INVALID")
    return rows


def read_tail_recoveries(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    fields, rows = read_tsv(path)
    if tuple(fields) != TAIL_RECOVERY_FIELDS:
        raise AuditError("TAIL_RECOVERY_SCHEMA_INVALID")
    allowed_ledgers = {
        "pointer-health-attempts.tsv",
        "record-completions.tsv",
        "attempt-dispositions.tsv",
        "robots.tsv",
        "atomic-temp-recoveries.tsv",
    }
    seen: set[str] = set()
    for row in rows:
        try:
            original = int(row["original_size_bytes"])
            retained = int(row["retained_size_bytes"])
            discarded = int(row["discarded_fragment_bytes"])
        except ValueError as exc:
            raise AuditError("TAIL_RECOVERY_SEMANTICS_INVALID") from exc
        expected_id = hashlib.sha256(
            f"{row['ledger']}\0{original}\0{retained}\0{row['discarded_fragment_sha256']}".encode()
        ).hexdigest()[:24]
        if (
            row["ledger"] not in allowed_ledgers
            or row["recovery_id"] in seen
            or row["recovery_id"] != expected_id
            or original != retained + discarded
            or retained <= 0
            or discarded <= 0
            or not is_hex(row["discarded_fragment_sha256"], 64)
            or not is_utc_timestamp(row["detected_at_utc"])
        ):
            raise AuditError("TAIL_RECOVERY_SEMANTICS_INVALID")
        seen.add(row["recovery_id"])
    return rows


ATOMIC_TEMP_NAME = re.compile(
    r"^\.(?P<target>(?:audit-contract\.json|pointer-health\.tsv|"
    r"pointer-health-summary\.json|tail-recoveries\.tsv|robots\.tsv|"
    r"run-metadata-[A-Za-z0-9._-]+-[0-9a-f]{16}\.json))\."
    r"[A-Za-z0-9_-]+\.tmp$"
)


def read_atomic_temp_recoveries(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    fields, rows = read_tsv(path)
    if tuple(fields) != ATOMIC_TEMP_RECOVERY_FIELDS:
        raise AuditError("ATOMIC_TEMP_RECOVERY_SCHEMA_INVALID")
    seen: set[str] = set()
    for row in rows:
        try:
            size = int(row["size_bytes"])
        except ValueError as exc:
            raise AuditError("ATOMIC_TEMP_RECOVERY_SEMANTICS_INVALID") from exc
        match = ATOMIC_TEMP_NAME.fullmatch(row["temporary_name"])
        expected_id = hashlib.sha256(
            (
                f"{row['temporary_name']}\0{row['target_name']}\0{size}\0"
                f"{row['sha256']}"
            ).encode()
        ).hexdigest()[:24]
        if (
            match is None
            or match.group("target") != row["target_name"]
            or row["recovery_id"] in seen
            or row["recovery_id"] != expected_id
            or size < 0
            or not is_hex(row["sha256"], 64)
            or not is_utc_timestamp(row["detected_at_utc"])
        ):
            raise AuditError("ATOMIC_TEMP_RECOVERY_SEMANTICS_INVALID")
        seen.add(row["recovery_id"])
    return rows


def recover_orphan_atomic_temps(output_dir: Path) -> list[dict[str, str]]:
    """Audit and remove only strictly recognized orphan atomic-write temps."""
    recovery_path = output_dir / "atomic-temp-recoveries.tsv"
    existing = read_atomic_temp_recoveries(recovery_path)
    by_id = {row["recovery_id"]: row for row in existing}
    added: list[dict[str, str]] = []
    for path in sorted(output_dir.iterdir(), key=lambda item: item.name):
        if not (path.name.startswith(".") and path.name.endswith(".tmp")):
            continue
        match = ATOMIC_TEMP_NAME.fullmatch(path.name)
        if match is None:
            raise AuditError("UNRECOGNIZED_ATOMIC_TEMP_FILE", EXIT_SAFETY)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise AuditError("ATOMIC_TEMP_OPEN_FAILED", EXIT_SAFETY) from exc
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_nlink != 1
                or metadata.st_mode & 0o077
            ):
                raise AuditError("ATOMIC_TEMP_SAFETY_INVALID", EXIT_SAFETY)
            digest = hashlib.sha256()
            while True:
                block = os.read(descriptor, 1024 * 1024)
                if not block:
                    break
                digest.update(block)
        finally:
            os.close(descriptor)
        sha256 = digest.hexdigest()
        size = int(metadata.st_size)
        target = match.group("target")
        recovery_id = hashlib.sha256(
            f"{path.name}\0{target}\0{size}\0{sha256}".encode()
        ).hexdigest()[:24]
        row = {
            "recovery_id": recovery_id,
            "detected_at_utc": utc_now(),
            "temporary_name": path.name,
            "target_name": target,
            "size_bytes": str(size),
            "sha256": sha256,
        }
        prior = by_id.get(recovery_id)
        if prior is None:
            append_tsv_rows(
                recovery_path, ATOMIC_TEMP_RECOVERY_FIELDS, [row]
            )
            existing.append(row)
            by_id[recovery_id] = row
            added.append(row)
        elif any(
            prior.get(field, "") != row[field]
            for field in (
                "temporary_name",
                "target_name",
                "size_bytes",
                "sha256",
            )
        ):
            raise AuditError("ATOMIC_TEMP_RECOVERY_REPLAY_INVALID", EXIT_SAFETY)
        try:
            path.unlink()
        except OSError as exc:
            raise AuditError("ATOMIC_TEMP_REMOVE_FAILED", EXIT_SAFETY) from exc
        fsync_directory(output_dir)
    return [*existing]


def append_dispositions(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    append_tsv_rows(path, DISPOSITION_FIELDS, rows)


def append_completion(
    path: Path,
    row: dict[str, str],
    window_id: str,
    invocation_id: str,
    audit_result: AuditRecordResult,
) -> None:
    new_attempts = audit_result.attempts
    if not new_attempts:
        raise AuditError("EMPTY_RECORD_COMPLETION")
    first_index = new_attempts[0]["attempt_index"]
    last_index = new_attempts[-1]["attempt_index"]
    protocol_complete = (
        "true" if audit_result.resolution_protocol_complete else "false"
    )
    completion_id = hashlib.sha256(
        (
            f"{row['record_id']}\0{window_id}\0{invocation_id}\0"
            f"{first_index}\0{last_index}\0{protocol_complete}\0"
            f"{audit_result.resolution_protocol_status}\0"
            f"{audit_result.resolver_candidate_count}\0"
            f"{audit_result.fallback_attempt_count}"
        ).encode()
    ).hexdigest()[:24]
    payload = {
        "completion_id": completion_id,
        "record_id": row["record_id"],
        "component": row["component"],
        "window_id": window_id,
        "invocation_id": invocation_id,
        "first_attempt_index": first_index,
        "last_attempt_index": last_index,
        "attempt_count": str(len(new_attempts)),
        "resolution_protocol_complete": protocol_complete,
        "resolution_protocol_status": audit_result.resolution_protocol_status,
        "resolver_candidate_count": str(audit_result.resolver_candidate_count),
        "fallback_attempt_count": str(audit_result.fallback_attempt_count),
        "completed_at_utc": utc_now(),
    }
    append_tsv_rows(path, COMPLETION_FIELDS, [payload])


def scope_ids_sha256(rows: list[dict[str, str]]) -> str:
    return bytes_sha256("".join(f"{row['record_id']}\n" for row in rows).encode())


def ensure_output_contract(
    output_dir: Path,
    args: argparse.Namespace,
    bindings: dict[str, Any],
    source_rows: list[dict[str, str]],
    user_agent: str,
) -> dict[str, Any]:
    policy = load_json(args.policy)
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
    contract = {
        "schema": "coverfish.pointer-audit-contract.v1",
        "tool_version": TOOL_VERSION,
        "tool_sha256": file_sha256(Path(__file__)),
        "requirements_sha256": file_sha256(ROOT / "requirements-pointer-audit.txt"),
        "dependencies": dependency_report(),
        "runtime": runtime_report(),
        "phash_algorithm": PHASH_ALGORITHM,
        "scope": args.scope,
        "scope_rows": len(source_rows),
        "scope_ids_sha256": scope_ids_sha256(source_rows),
        "dataset": bindings.get("dataset", {}),
        "bindings_sha256": file_sha256(args.bindings),
        "manifest_sha256": file_sha256(args.manifest) if args.manifest is not None else None,
        "policy_sha256": file_sha256(args.policy),
        "host_rate_policy": host_rate_policy,
        "transient_backoff": {
            "strategy": "exponential_per_host_persisted",
            "base": "configured_host_min_interval_seconds",
            "circuit_threshold": TRANSIENT_CIRCUIT_THRESHOLD,
            "circuit_min_seconds": TRANSIENT_CIRCUIT_MIN_SECONDS,
            "maximum_seconds": TRANSIENT_BACKOFF_MAX_SECONDS,
            "reset_on_nontransient_response": True,
        },
        "user_agent": user_agent,
        "bytes_retained": False,
        "gpu_used": False,
    }
    path = output_dir / "audit-contract.json"
    if path.exists():
        existing = load_json(path)
        if existing != contract:
            raise AuditError("OUTPUT_CONTRACT_MISMATCH", EXIT_SAFETY)
    else:
        occupied = any(
            (output_dir / name).exists()
            for name in (
                "pointer-health-attempts.tsv",
                "record-completions.tsv",
                "attempt-dispositions.tsv",
                "robots.tsv",
                "tail-recoveries.tsv",
                "atomic-temp-recoveries.tsv",
                "pointer-health.tsv",
                "pointer-health-summary.json",
            )
        )
        if occupied:
            raise AuditError("OUTPUT_CONTRACT_MISSING", EXIT_SAFETY)
        atomic_write_json(path, contract)
    return contract


def invocation_metadata_files(output_dir: Path) -> list[Path]:
    return sorted(output_dir.glob("run-metadata-*.json"))


def load_and_validate_invocation_metadata(
    output_dir: Path,
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
    robots: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    metadata_rows: list[dict[str, Any]] = []
    pairs: set[tuple[str, str]] = set()
    windows: dict[str, str] = {}
    invocations: dict[str, str] = {}
    for path in invocation_metadata_files(output_dir):
        metadata = load_json(path)
        window_id = clean_text(metadata.get("window_id"))
        invocation_id = clean_text(metadata.get("invocation_id"))
        expected_name = f"run-metadata-{window_id}-{invocation_id}.json"
        if (
            metadata.get("schema") != "coverfish.pointer-audit-run.v1"
            or not is_window_id(window_id)
            or not re.fullmatch(r"[0-9a-f]{16}", invocation_id)
            or path.name != expected_name
            or (window_id, invocation_id) in pairs
        ):
            raise AuditError("INVOCATION_METADATA_IDENTITY_INVALID")
        pairs.add((window_id, invocation_id))
        if (
            (window_id in windows and windows[window_id] != invocation_id)
            or (
                invocation_id in invocations
                and invocations[invocation_id] != window_id
            )
        ):
            raise AuditError("WINDOW_INVOCATION_NOT_ONE_TO_ONE")
        windows[window_id] = invocation_id
        invocations[invocation_id] = window_id
        metadata_rows.append(metadata)
    for row in [*attempts, *completions, *dispositions, *(robots or [])]:
        pair = (row.get("window_id", ""), row.get("invocation_id", ""))
        if pair not in pairs:
            raise AuditError("LEDGER_INVOCATION_METADATA_MISSING")
    return metadata_rows


def close_stale_invocations(
    output_dir: Path,
    attempts_count: int,
    completions_count: int,
) -> None:
    for path in invocation_metadata_files(output_dir):
        metadata = load_json(path)
        if metadata.get("status") != "RUNNING":
            continue
        attempts_before = int(metadata.get("attempts_before", 0))
        completions_before = int(metadata.get("completions_before", 0))
        metadata.update(
            {
                "status": "ABANDONED_BY_RESUME",
                "completed_at_utc": utc_now(),
                "attempts_after": attempts_count,
                "completions_after": completions_count,
                "rows_processed": max(0, completions_count - completions_before),
                "attempts_written": max(0, attempts_count - attempts_before),
            }
        )
        atomic_write_json(path, metadata)


def completion_coverage(
    completions: list[dict[str, str]],
) -> set[tuple[str, str, int]]:
    covered: set[tuple[str, str, int]] = set()
    for completion in completions:
        try:
            first = int(completion.get("first_attempt_index", ""))
            last = int(completion.get("last_attempt_index", ""))
        except ValueError:
            continue
        for index in range(first, last + 1):
            covered.add(
                (
                    completion.get("record_id", ""),
                    completion.get("window_id", ""),
                    index,
                )
            )
    return covered


def select_committed_attempts(
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
) -> list[dict[str, str]]:
    covered = completion_coverage(completions)
    return [
        row
        for row in attempts
        if (row["record_id"], row["window_id"], int(row["attempt_index"]))
        in covered
    ]


def reconcile_incomplete_attempts(
    output_dir: Path,
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
) -> list[dict[str, str]]:
    covered = completion_coverage(completions)
    disposed_ids = {row.get("attempt_id", "") for row in dispositions}
    metadata_by_invocation: dict[str, dict[str, Any]] = {}
    for path in invocation_metadata_files(output_dir):
        metadata = load_json(path)
        invocation_id = clean_text(metadata.get("invocation_id"))
        if invocation_id in metadata_by_invocation:
            raise AuditError("INVOCATION_METADATA_DUPLICATE")
        metadata_by_invocation[invocation_id] = metadata
    new_rows: list[dict[str, str]] = []
    for attempt in attempts:
        key = (
            attempt.get("record_id", ""),
            attempt.get("window_id", ""),
            int(attempt.get("attempt_index", "0") or 0),
        )
        attempt_id = attempt.get("attempt_id", "")
        if key in covered or attempt_id in disposed_ids:
            continue
        metadata = metadata_by_invocation.get(attempt.get("invocation_id", ""))
        if metadata is None or metadata.get("status") not in {
            "ERROR", "INTERRUPTED", "ABANDONED_BY_RESUME"
        }:
            raise AuditError("UNDECIDED_ATTEMPT_WITHOUT_FAILED_INVOCATION")
        new_rows.append(
            {
                "attempt_id": attempt_id,
                "record_id": attempt["record_id"],
                "window_id": attempt["window_id"],
                "invocation_id": attempt["invocation_id"],
                "disposition": "abandoned_incomplete_transaction",
                "recorded_at_utc": utc_now(),
            }
        )
    append_dispositions(output_dir / "attempt-dispositions.tsv", new_rows)
    all_dispositions = [*dispositions, *new_rows]
    disposition_counts = Counter(
        row["invocation_id"] for row in all_dispositions
    )
    for path in invocation_metadata_files(output_dir):
        metadata = load_json(path)
        count = disposition_counts.get(clean_text(metadata.get("invocation_id")), 0)
        if count and metadata.get("abandoned_attempts_recorded") != count:
            metadata["abandoned_attempts_recorded"] = count
            atomic_write_json(path, metadata)
    return all_dispositions


def validate_resume_state(
    source_rows: list[dict[str, str]],
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
    require_decided: bool = False,
) -> None:
    frozen = {row["record_id"]: row for row in source_rows}
    attempt_ids: set[str] = set()
    attempts_by_key: dict[tuple[str, str, int], dict[str, str]] = {}
    indexes_by_record: dict[str, set[int]] = defaultdict(set)
    for attempt in attempts:
        record_id = attempt.get("record_id", "")
        record = frozen.get(record_id)
        if record is None or attempt.get("component") != record["component"] or attempt.get("active") != record["active"]:
            raise AuditError("RESUME_ATTEMPT_FROZEN_BINDING_FAILED")
        try:
            index = int(attempt.get("attempt_index", ""))
        except ValueError as exc:
            raise AuditError("RESUME_ATTEMPT_INDEX_INVALID") from exc
        window_id = attempt.get("window_id", "")
        invocation_id = attempt.get("invocation_id", "")
        if (
            index <= 0
            or not is_window_id(window_id)
            or not re.fullmatch(r"[0-9a-f]{16}", invocation_id)
            or not is_utc_timestamp(attempt.get("checked_at_utc", ""))
        ):
            raise AuditError("RESUME_ATTEMPT_IDENTITY_INVALID")
        expected_id = hashlib.sha256(
            f"{record_id}\0{window_id}\0{index}".encode()
        ).hexdigest()[:24]
        attempt_id = attempt.get("attempt_id", "")
        key = (record_id, window_id, index)
        if attempt_id != expected_id or attempt_id in attempt_ids or key in attempts_by_key:
            raise AuditError("RESUME_ATTEMPT_IDENTITY_INVALID")
        attempt_ids.add(attempt_id)
        attempts_by_key[key] = attempt
        indexes_by_record[record_id].add(index)
    for record_id, indexes in indexes_by_record.items():
        if indexes != set(range(1, len(indexes) + 1)):
            raise AuditError(f"RESUME_ATTEMPT_INDEX_GAP_{record_id}")
    completion_ids: set[str] = set()
    covered: set[tuple[str, str, int]] = set()
    for completion in completions:
        record_id = completion.get("record_id", "")
        window_id = completion.get("window_id", "")
        record = frozen.get(record_id)
        try:
            first = int(completion.get("first_attempt_index", ""))
            last = int(completion.get("last_attempt_index", ""))
            count = int(completion.get("attempt_count", ""))
            resolver_candidate_count = int(
                completion.get("resolver_candidate_count", "")
            )
            fallback_attempt_count = int(
                completion.get("fallback_attempt_count", "")
            )
        except ValueError as exc:
            raise AuditError("RESUME_COMPLETION_RANGE_INVALID") from exc
        invocation_id = completion.get("invocation_id", "")
        protocol_complete = completion.get("resolution_protocol_complete", "")
        protocol_status = completion.get("resolution_protocol_status", "")
        expected_id = hashlib.sha256(
            (
                f"{record_id}\0{window_id}\0{invocation_id}\0{first}\0{last}\0"
                f"{protocol_complete}\0{protocol_status}\0"
                f"{resolver_candidate_count}\0{fallback_attempt_count}"
            ).encode()
        ).hexdigest()[:24]
        completion_id = completion.get("completion_id", "")
        if (
            record is None
            or completion.get("component") != record["component"]
            or not re.fullmatch(r"[0-9a-f]{16}", invocation_id)
            or not is_window_id(window_id)
            or first <= 0
            or last < first
            or count != last - first + 1
            or protocol_complete not in {"true", "false"}
            or protocol_status not in RESOLUTION_PROTOCOL_STATUSES
            or resolver_candidate_count < 0
            or fallback_attempt_count < 0
            or fallback_attempt_count > resolver_candidate_count
            or completion_id != expected_id
            or completion_id in completion_ids
            or not is_utc_timestamp(completion.get("completed_at_utc", ""))
        ):
            raise AuditError("RESUME_COMPLETION_BINDING_FAILED")
        completion_ids.add(completion_id)
        for index in range(first, last + 1):
            key = (record_id, window_id, index)
            if (
                key not in attempts_by_key
                or attempts_by_key[key].get("invocation_id") != invocation_id
                or key in covered
            ):
                raise AuditError("RESUME_COMPLETION_ATTEMPT_BINDING_FAILED")
            covered.add(key)
        covered_attempts = [
            attempts_by_key[(record_id, window_id, index)]
            for index in range(first, last + 1)
        ]
        actual_fallback_count = sum(
            attempt.get("request_kind") == "fallback_image"
            for attempt in covered_attempts
        )
        image_classes = {
            attempt.get("identity_class", "")
            for attempt in covered_attempts
            if attempt.get("request_kind")
            in {"direct_image", "fallback_image"}
        }
        direct_classes = [
            attempt.get("identity_class", "")
            for attempt in covered_attempts
            if attempt.get("request_kind") == "direct_image"
        ]
        fallback_classes = [
            attempt.get("identity_class", "")
            for attempt in covered_attempts
            if attempt.get("request_kind") == "fallback_image"
        ]
        resolver_attempts = [
            attempt
            for attempt in covered_attempts
            if attempt.get("request_kind") in {"resolver_api", "resolver_page"}
        ]
        if (
            actual_fallback_count != fallback_attempt_count
            or (protocol_complete == "true")
            != (protocol_status in RESOLUTION_PROTOCOL_COMPLETE_STATUSES)
            or len(direct_classes) != 1
            or fallback_attempt_count > 10
            or (
                protocol_status == "direct_exact"
                and not (
                    direct_classes == ["byte_exact"]
                    and len(covered_attempts) == 1
                    and resolver_candidate_count == 0
                )
            )
            or (
                protocol_status == "fallback_exact"
                and "byte_exact" not in fallback_classes
            )
            or (
                protocol_status == "pending_resolver_adapter"
                and resolver_attempts
            )
            or (
                protocol_status == "pending_candidate_cap"
                and resolver_candidate_count <= fallback_attempt_count
            )
            or (
                protocol_status == "fail_safety_review"
                and "safety_review_required" not in image_classes
                and not any(
                    attempt.get("transport_status") == "safety_block"
                    for attempt in resolver_attempts
                )
            )
        ):
            raise AuditError("RESUME_COMPLETION_PROTOCOL_BINDING_FAILED")
    disposition_ids: set[str] = set()
    attempts_by_id = {row["attempt_id"]: row for row in attempts}
    for disposition in dispositions:
        attempt_id = disposition.get("attempt_id", "")
        attempt = attempts_by_id.get(attempt_id)
        if (
            attempt is None
            or attempt_id in disposition_ids
            or disposition.get("record_id") != attempt["record_id"]
            or disposition.get("window_id") != attempt["window_id"]
            or disposition.get("invocation_id") != attempt["invocation_id"]
            or disposition.get("disposition") != "abandoned_incomplete_transaction"
            or not is_utc_timestamp(disposition.get("recorded_at_utc", ""))
            or (
                attempt["record_id"],
                attempt["window_id"],
                int(attempt["attempt_index"]),
            )
            in covered
        ):
            raise AuditError("RESUME_DISPOSITION_BINDING_FAILED")
        disposition_ids.add(attempt_id)
    if require_decided:
        undecided = [
            row
            for row in attempts
            if (
                row["record_id"], row["window_id"], int(row["attempt_index"])
            )
            not in covered
            and row["attempt_id"] not in disposition_ids
        ]
        if undecided:
            raise AuditError("RESUME_UNDECIDED_ATTEMPTS")


def write_robots_receipt(client: NetworkClient, output_dir: Path) -> None:
    if not client.robots_records:
        return
    robots_path = output_dir / "robots.tsv"
    existing_robots: list[dict[str, str]] = []
    if robots_path.exists():
        fields, existing_robots = read_tsv(robots_path)
        if tuple(fields) != ROBOTS_FIELDS:
            raise AuditError("ROBOTS_RECEIPT_SCHEMA_INVALID")
    atomic_write_tsv(
        robots_path,
        ROBOTS_FIELDS,
        [*existing_robots, *client.robots_records],
    )
    client.robots_records.clear()


@contextlib.contextmanager
def exclusive_audit_lock() -> Iterable[None]:
    runtime_root = secure_runtime_root()
    lock_path = runtime_root / "audit-process.lock"
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise AuditError("AUDIT_LOCK_OPEN_FAILED", EXIT_SAFETY) from exc
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o077
    ):
        os.close(descriptor)
        raise AuditError("AUDIT_LOCK_PERMISSIONS_INVALID", EXIT_SAFETY)
    with os.fdopen(descriptor, "a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AuditError("AUDIT_PROCESS_ALREADY_RUNNING", EXIT_SAFETY) from exc
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def validate_audit_mode(args: argparse.Namespace) -> None:
    if args.final_window and (args.component or args.max_rows is not None):
        raise AuditError("FINAL_WINDOW_MUST_COVER_SCOPE", EXIT_USAGE)
    if args.final_window and args.retry_mode != "nonexact":
        raise AuditError("FINAL_WINDOW_REQUIRES_NONEXACT_RETRY", EXIT_USAGE)


def command_audit(args: argparse.Namespace) -> int:
    validate_audit_mode(args)
    if not args.accept_network:
        emit({"command": "audit", "schema": SCHEMA, "status": "PENDING", "error": "NETWORK_CONFIRMATION_REQUIRED", "tool_version": TOOL_VERSION})
        return EXIT_SAFETY
    args.output_dir.mkdir(parents=True, exist_ok=True)
    validate_output_directory(args.output_dir)
    with exclusive_audit_lock():
        return command_audit_locked(args)


def command_audit_locked(args: argparse.Namespace) -> int:
    bindings, archive_rows = load_bound_rows(args.source_root, args.bindings)
    if not dependency_report()["ready"]:
        raise AuditError("POINTER_AUDIT_DEPENDENCIES_NOT_PINNED", EXIT_DEPENDENCY)
    if not runtime_report()["ready"]:
        raise AuditError("POINTER_AUDIT_RUNTIME_NOT_SUPPORTED", EXIT_DEPENDENCY)
    if args.scope == "pilot":
        if args.manifest is None:
            raise AuditError("PILOT_MANIFEST_REQUIRED", EXIT_USAGE)
        pilot_binding = bindings.get("pilot", {})
        if (
            not isinstance(pilot_binding, dict)
            or file_sha256(args.manifest) != clean_text(pilot_binding.get("sha256"))
            or int(pilot_binding.get("rows", -1)) != 800
        ):
            raise AuditError("PILOT_MANIFEST_BINDING_FAILED")
        sample = load_sample_manifest(args.manifest)
        archive_by_id = {row["record_id"]: row for row in archive_rows}
        if any(
            row["record_id"] not in archive_by_id
            or row["component"] != archive_by_id[row["record_id"]]["component"]
            for row in sample
        ):
            raise AuditError("PILOT_ROW_NOT_IN_FROZEN_INPUT")
        source_rows = [archive_by_id[row["record_id"]] | {"sample_reason": row.get("sample_reason", ""), "sample_rank": row.get("sample_rank", "")} for row in sample]
    else:
        source_rows = archive_rows
    if args.component:
        allowed_components = set(args.component)
        candidate_rows = [row for row in source_rows if row["component"] in allowed_components]
    else:
        candidate_rows = source_rows
    policy = load_json(args.policy)
    validate_host_policy(policy)
    expected_user_agent = f"{policy.get('user_agent_product')} (+{policy.get('public_contact_url')})"
    user_agent = args.user_agent or expected_user_agent
    if user_agent != expected_user_agent or any(char in user_agent for char in "\r\n"):
        raise AuditError("PUBLIC_USER_AGENT_REQUIRED", EXIT_SAFETY)
    contract = ensure_output_contract(
        args.output_dir, args, bindings, source_rows, user_agent
    )
    prepare_append_ledgers(args.output_dir)
    recover_orphan_atomic_temps(args.output_dir)
    attempts_path = args.output_dir / "pointer-health-attempts.tsv"
    completions_path = args.output_dir / "record-completions.tsv"
    dispositions_path = args.output_dir / "attempt-dispositions.tsv"
    existing = read_attempts(attempts_path)
    completions = read_completions(completions_path)
    dispositions = read_dispositions(dispositions_path)
    robots = read_robots(args.output_dir / "robots.tsv")
    run_metadata = load_and_validate_invocation_metadata(
        args.output_dir, existing, completions, dispositions, robots
    )
    if any(row.get("window_id") == args.window_id for row in run_metadata):
        raise AuditError("WINDOW_ID_ALREADY_USED", EXIT_USAGE)
    validate_resume_state(source_rows, existing, completions, dispositions)
    close_stale_invocations(args.output_dir, len(existing), len(completions))
    dispositions = reconcile_incomplete_attempts(
        args.output_dir, existing, completions, dispositions
    )
    validate_resume_state(
        source_rows, existing, completions, dispositions, require_decided=True
    )
    started = utc_now()
    invocation_id = hashlib.sha256(
        f"{args.window_id}\0{started}\0{time.time_ns()}\0{len(existing)}".encode()
    ).hexdigest()[:16]
    client = NetworkClient(
        policy,
        user_agent,
        args.window_id,
        invocation_id,
        robots_receipt_path=args.output_dir / "robots.tsv",
    )
    by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    committed_existing = select_committed_attempts(existing, completions)
    completed_in_window = {
        row.get("record_id", "")
        for row in completions
        if row.get("window_id") == args.window_id
    }
    abandoned_unrecovered = unrecovered_abandoned_record_ids(
        existing, completions, dispositions
    )
    counts: Counter[str] = Counter()
    for attempt in committed_existing:
        by_record[attempt.get("record_id", "")].append(attempt)
    for attempt in existing:
        counts[attempt.get("record_id", "")] += 1
    selected = [
        row for row in candidate_rows
        if row["record_id"] not in completed_in_window
        and (
            row["record_id"] in abandoned_unrecovered
            or should_process(by_record.get(row["record_id"], []), args.retry_mode)
        )
    ]
    if args.max_rows is not None:
        selected = selected[: args.max_rows]
    processed = 0
    metadata = {
        "schema": "coverfish.pointer-audit-run.v1",
        "status": "RUNNING",
        "invocation_id": invocation_id,
        "tool_version": TOOL_VERSION,
        "tool_sha256": file_sha256(Path(__file__)),
        "phash_algorithm": PHASH_ALGORITHM,
        "dependencies": dependency_report(),
        "runtime": runtime_report(),
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
        "manifest_sha256": file_sha256(args.manifest) if args.manifest is not None else None,
        "contract_sha256": file_sha256(args.output_dir / "audit-contract.json"),
        "user_agent": user_agent,
        "bytes_retained": False,
        "gpu_used": False,
    }
    metadata_path = args.output_dir / f"run-metadata-{args.window_id}-{invocation_id}.json"
    atomic_write_json(metadata_path, metadata)
    try:
        for row in selected:
            start_index = counts[row["record_id"]] + 1
            audit_result = audit_one(
                row,
                client,
                args.window_id,
                start_index,
                on_attempt=lambda attempt: append_attempts(
                    attempts_path, [attempt]
                ),
            )
            new_attempts = audit_result.attempts
            append_completion(
                completions_path,
                row,
                args.window_id,
                invocation_id,
                audit_result,
            )
            by_record[row["record_id"]].extend(new_attempts)
            counts[row["record_id"]] += len(new_attempts)
            processed += 1
            if processed % 25 == 0:
                print(f"audited {processed}/{len(selected)} rows in window {args.window_id}", file=sys.stderr, flush=True)
        write_robots_receipt(client, args.output_dir)
        summary = summarize_to_files(
            bindings,
            source_rows,
            args.output_dir,
            args.scope,
            args.final_window,
            {args.window_id} if args.final_window else set(),
        )
    except BaseException as exc:
        try:
            write_robots_receipt(client, args.output_dir)
        except Exception:
            pass
        attempts_after = len(read_attempts(attempts_path))
        completions_after = len(read_completions(completions_path))
        metadata.update(
            {
                "status": "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "ERROR",
                "completed_at_utc": utc_now(),
                "rows_processed": processed,
                "attempts_after": attempts_after,
                "attempts_written": attempts_after - len(existing),
                "completions_after": completions_after,
                "error_code": exc.code if isinstance(exc, AuditError) else type(exc).__name__,
            }
        )
        atomic_write_json(metadata_path, metadata)
        raise
    attempts_after = len(read_attempts(attempts_path))
    completions_after = len(read_completions(completions_path))
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
    atomic_write_json(metadata_path, metadata)
    emit({"command": "audit", "schema": SCHEMA, "status": summary["status"], "tool_version": TOOL_VERSION, "window_id": args.window_id, "rows_selected": len(selected), "rows_processed": processed, "summary": summary})
    return EXIT_OK


def command_summarize(args: argparse.Namespace) -> int:
    validate_output_directory(args.output_dir)
    with exclusive_audit_lock():
        return command_summarize_locked(args)


def command_summarize_locked(args: argparse.Namespace) -> int:
    bindings, archive_rows = load_bound_rows(args.source_root, args.bindings)
    if not dependency_report()["ready"]:
        raise AuditError("POINTER_AUDIT_DEPENDENCIES_NOT_PINNED", EXIT_DEPENDENCY)
    if not runtime_report()["ready"]:
        raise AuditError("POINTER_AUDIT_RUNTIME_NOT_SUPPORTED", EXIT_DEPENDENCY)
    if args.scope == "pilot":
        if args.manifest is None:
            raise AuditError("PILOT_MANIFEST_REQUIRED", EXIT_USAGE)
        pilot_binding = bindings.get("pilot", {})
        if (
            not isinstance(pilot_binding, dict)
            or file_sha256(args.manifest) != clean_text(pilot_binding.get("sha256"))
        ):
            raise AuditError("PILOT_MANIFEST_BINDING_FAILED")
        sample = load_sample_manifest(args.manifest)
        archive_by_id = {row["record_id"]: row for row in archive_rows}
        if any(
            row["record_id"] not in archive_by_id
            or row["component"] != archive_by_id[row["record_id"]]["component"]
            for row in sample
        ):
            raise AuditError("PILOT_ROW_NOT_IN_FROZEN_INPUT")
        source_rows = [archive_by_id[row["record_id"]] for row in sample]
    else:
        source_rows = archive_rows
    policy = load_json(args.policy)
    validate_host_policy(policy)
    user_agent = f"{policy.get('user_agent_product')} (+{policy.get('public_contact_url')})"
    ensure_output_contract(
        args.output_dir, args, bindings, source_rows, user_agent
    )
    prepare_append_ledgers(args.output_dir)
    recover_orphan_atomic_temps(args.output_dir)
    attempts = read_attempts(args.output_dir / "pointer-health-attempts.tsv")
    completions = read_completions(args.output_dir / "record-completions.tsv")
    dispositions = read_dispositions(args.output_dir / "attempt-dispositions.tsv")
    robots = read_robots(args.output_dir / "robots.tsv")
    load_and_validate_invocation_metadata(
        args.output_dir, attempts, completions, dispositions, robots
    )
    validate_resume_state(source_rows, attempts, completions, dispositions)
    close_stale_invocations(args.output_dir, len(attempts), len(completions))
    dispositions = reconcile_incomplete_attempts(
        args.output_dir, attempts, completions, dispositions
    )
    validate_resume_state(
        source_rows, attempts, completions, dispositions, require_decided=True
    )
    run_metadata = load_and_validate_invocation_metadata(
        args.output_dir, attempts, completions, dispositions, robots
    )
    derived_final = any(
        row.get("status") == "COMPLETE" and row.get("final_window") is True
        for row in run_metadata
    )
    if args.final_window != derived_final:
        raise AuditError("FINAL_WINDOW_METADATA_MISMATCH", EXIT_USAGE)
    final_window_ids = {
        row.get("window_id", "")
        for row in run_metadata
        if row.get("status") == "COMPLETE" and row.get("final_window") is True
    }
    summary = summarize_to_files(
        bindings,
        source_rows,
        args.output_dir,
        args.scope,
        args.final_window,
        final_window_ids,
    )
    emit({"command": "summarize", "schema": SCHEMA, "status": summary["status"], "tool_version": TOOL_VERSION, "network_accessed": False, "summary": summary})
    return EXIT_OK


def add_input_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-root", type=Path, required=True, help="Extracted frozen RC2 root.")
    parser.add_argument("--bindings", type=Path, default=DEFAULT_BINDINGS, help="Frozen input binding JSON.")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Audit frozen COVER-Fish pointer availability without retaining bytes.")
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Validate frozen manifests and report counts without network access.")
    add_input_arguments(plan)
    plan.set_defaults(handler=command_plan)

    sample = subparsers.add_parser("sample", help="Write the deterministic 800-row pilot manifest without network access.")
    add_input_arguments(sample)
    sample.add_argument("--output", type=Path, required=True)
    sample.add_argument("--seed", default="coverfish-pointer-pilot-v1")
    sample.set_defaults(handler=command_sample)

    audit = subparsers.add_parser("audit", help="Run one timestamped live audit window.")
    add_input_arguments(audit)
    audit.add_argument("--scope", choices=("pilot", "archive"), required=True)
    audit.add_argument("--manifest", type=Path)
    audit.add_argument("--output-dir", type=Path, required=True)
    audit.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    audit.add_argument("--window-id", type=window_id_value, required=True)
    audit.add_argument("--retry-mode", choices=("none", "transient", "nonexact", "all"), default="none")
    audit.add_argument("--max-rows", type=nonnegative_int)
    audit.add_argument("--component", action="append", choices=tuple(COMPONENT_ORDER), help="Restrict this window invocation; repeat for multiple components.")
    audit.add_argument("--user-agent")
    audit.add_argument("--accept-network", action="store_true")
    audit.add_argument("--final-window", action="store_true")
    audit.set_defaults(handler=command_audit)

    summarize = subparsers.add_parser("summarize", help="Rebuild final rows and summary from append-only attempts.")
    add_input_arguments(summarize)
    summarize.add_argument("--scope", choices=("pilot", "archive"), required=True)
    summarize.add_argument("--manifest", type=Path)
    summarize.add_argument("--output-dir", type=Path, required=True)
    summarize.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    summarize.add_argument("--final-window", action="store_true")
    summarize.set_defaults(handler=command_summarize)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except AuditError as exc:
        emit({"schema": SCHEMA, "status": "ERROR", "error": exc.code, "tool_version": TOOL_VERSION})
        return exc.exit_code
    except KeyboardInterrupt:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INTERRUPTED", "tool_version": TOOL_VERSION})
        return 130
    except Exception:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "UNEXPECTED_RUNTIME_ERROR", "tool_version": TOOL_VERSION})
        return EXIT_NETWORK


if __name__ == "__main__":
    raise SystemExit(main())
