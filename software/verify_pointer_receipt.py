#!/usr/bin/env python3
"""Fail-closed, offline verifier for a COVER-Fish pointer-health receipt.

The verifier deliberately does not import the networked producer.  It binds the
frozen RC2 inputs to constants compiled into this file, reconstructs every
derived health and summary field from the append-only attempt ledger, and emits
exactly one JSON object on stdout.  It uses only the Python standard library and
never opens a network connection.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import ipaddress
import json
import os
import re
import stat
import sys
import tempfile
import urllib.parse
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NoReturn

SCHEMA = "coverfish.pointer-receipt-verifier.v1"
TOOL_VERSION = "0.2.1"
PRODUCER_VERSION = "0.2.0"

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_INTEGRITY = 6

ATTEMPT_FIELDS = (
    "attempt_id", "record_id", "component", "active", "window_id",
    "invocation_id", "attempt_index", "checked_at_utc", "request_kind", "resolved_via",
    "url_requested", "url_final", "host", "policy_state", "robots_status",
    "transport_status", "http_status", "redirect_count", "redirect_chain_json",
    "retry_after", "content_type", "content_type_image", "magic_type",
    "decode_status", "actual_bytes", "actual_width", "actual_height",
    "actual_sha256", "actual_phash_hex64", "phash_distance", "sha256_match",
    "identity_class", "error_code", "bytes_retained",
)

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
    "finality_status",
    "possible_placeholder_cluster", "bytes_retained",
)

ROBOTS_FIELDS = (
    "window_id", "invocation_id", "checked_at_utc", "host", "robots_url", "http_status",
    "fetch_status", "robots_state", "error_code", "redirect_count",
    "redirect_chain_json", "sha256",
)

TAIL_RECOVERY_FIELDS = (
    "recovery_id", "ledger", "detected_at_utc", "original_size_bytes",
    "retained_size_bytes", "discarded_fragment_bytes",
    "discarded_fragment_sha256",
)

APPEND_LEDGERS = {
    "pointer-health-attempts.tsv",
    "record-completions.tsv",
    "attempt-dispositions.tsv",
    "robots.tsv",
    "atomic-temp-recoveries.tsv",
}

COMPLETION_FIELDS = (
    "completion_id", "record_id", "component", "window_id", "invocation_id",
    "first_attempt_index", "last_attempt_index", "attempt_count",
    "resolution_protocol_complete", "resolution_protocol_status",
    "resolver_candidate_count", "fallback_attempt_count", "completed_at_utc",
)

ATOMIC_TEMP_RECOVERY_FIELDS = (
    "recovery_id", "detected_at_utc", "temporary_name", "target_name",
    "size_bytes", "sha256",
)

DISPOSITION_FIELDS = (
    "attempt_id", "record_id", "window_id", "invocation_id", "disposition",
    "recorded_at_utc",
)

SAMPLE_FIELDS = (
    "record_id", "component", "active", "active_projection_status",
    "canonical_taxon_key", "fishbase25_speccode", "scientific_name",
    "source_page_url", "source_image_url", "pointer_url", "source_host",
    "pointer_host", "extension_class", "expected_width", "expected_height",
    "expected_min_side", "expected_sha256", "expected_phash_hex64",
    "source_pack_version", "evidence_id", "benchmark_role", "public_split",
    "distractor_type", "license_normalized", "license_tier", "release_mode",
    "attribution", "retirement_reason", "source_line_number", "raw_row_sha256",
    "sample_reason", "sample_stratum", "rank_digest", "sample_rank",
)

EXPECTED_DATASET = {
    "doi": "10.57967/hf/9706",
    "repository": "COVER-Fish/COVER-Fish",
    "revision": "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8",
    "tag": "rev023-rc2-20260714",
}
EXPECTED_COUNTS = {
    "active_staging_rows": 115780,
    "active_byte_complete_rows": 73835,
    "active_pointer_rows": 41945,
    "archive_pointer_rows": 42387,
    "e0_byte_complete_rows": 6719,
    "e0_pointer_rows": 0,
    "r0_rows": 49140,
    "r0_byte_complete_rows": 15052,
    "r0_pointer_rows": 34088,
    "retired_pointer_rows": 442,
}
PILOT_SHA256 = "cf7011d1744b0fb383c82953a019d0ba967c6fcf886b466010abcb072d21fbd0"
PILOT_QUOTAS = {"S1": 500, "S0": 160, "S3": 60, "D0": 40, "S4": 37, "S2": 3}

MANIFESTS = {
    "S0": ("packs/S0/pointers.tsv", "record_id", 7461, "a95e8d04cbcb5035dc59339b5c11abad1cf2c24bd283ec091b2326de43a87f9f"),
    "S1": ("packs/S1/pointers.tsv", "record_id", 34088, "f6ca665d472d247578f16903562da11406c373ecd1c5899c5ad6625018a9e8cf"),
    "S2": ("packs/S2/pointers.tsv", "record_id", 3, "3f501f694acd3b1a41ee567b9f8e3a6ac7b3c50a0f292ca05114d1ec6591282e"),
    "S3": ("packs/S3/pointers.tsv", "record_id", 451, "5ef936128356b63b4688278e36f057076780f9404c7085db871c02530533e73f"),
    "S4": ("packs/S4/pointers.tsv", "record_id", 132, "6a043e12bfa63cf080a1827a16c086868ac01eeff640b25bf7a2acbe23f4e858"),
    "D0": ("packs/D0/pointers.tsv", "public_id", 252, "b9c2ad57df18b10144fb54c3fe1a30746707482f36bbb0c2d094511c8cf49bd7"),
}
CONTAINER_LEDGERS = {
    "files_tsv": ("FILES.tsv", "95e4489d56ab00f814954b1c99e66398bd2f717ff763fd6b8a56d70458a21630"),
    "sha256sums": ("SHA256SUMS", "f521caf23aa1806132f781a5b6dd215908fea0ec367258c7717e499fb309bc10"),
}
E0_FILES = {
    "bytes": ("packs/E0/bytes.tsv", 6719, "acbc85857da640c539679ad52243b7503995c58ddfc4e6cf602df6f3ed8b99b9"),
    "pointers": ("packs/E0/pointers.tsv", 0, "87565e38fbfe0160c2be5e68f566a54d58267e6aa0cb328516130f5ffb1f46ad"),
}

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
    "rate_limited_429", "transient_5xx", "timeout", "dns_error", "tls_error",
    "bandwidth_budget_pending", "robots_unavailable", "network_or_transport_error",
    "empty_response",
}
RETRYABLE_RESOLVER_TRANSPORT = {
    "rate_limited", "server_error", "timeout", "dns_error", "tls_error",
    "bandwidth_budget_pending", "robots_unavailable", "network_error",
}
IMAGE_REQUESTS = {"direct_image", "fallback_image"}
RESOLVER_REQUESTS = {"resolver_api", "resolver_page"}
TRANSPORT_IDENTITY = {
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
    "http_error": "network_or_transport_error",
    "network_error": "network_or_transport_error",
}

POLICY_PENDING_CLASSES = {
    "policy_pending_permission", "robots_disallowed", "robots_unavailable",
}
FINALITY_NON_OBSERVATION_CLASSES = {
    *POLICY_PENDING_CLASSES,
    "bandwidth_budget_pending", "oversize", "safety_review_required",
    "not_audited",
}

RESOLUTION_PROTOCOL_STATUSES = frozenset({
    "direct_exact", "fallback_exact", "resolver_access_or_absence_observed",
    "resolver_no_candidate", "exhausted_nonexact", "pending_resolver_adapter",
    "resolver_transient_observed", "resolver_invalid_response_observed",
    "resolver_http_error_observed", "fallback_transient_observed",
    "pending_local_response_cap", "pending_policy", "pending_local_deferral",
    "pending_candidate_cap", "fail_safety_review",
})
RESOLUTION_PROTOCOL_COMPLETE_STATUSES = frozenset({
    "direct_exact", "fallback_exact", "resolver_access_or_absence_observed",
    "resolver_no_candidate", "resolver_transient_observed",
    "resolver_invalid_response_observed", "resolver_http_error_observed",
    "fallback_transient_observed", "exhausted_nonexact",
})

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

SENSITIVE_QUERY_KEY = re.compile(
    r"(?:^|[-_])(?:access[-_]?token|token|auth(?:orization)?|credential|"
    r"api[-_]?key|password|secret|session|signature|sig|jwt)(?:$|[-_])|"
    r"^x-amz-(?:credential|signature|security-token)$",
    re.IGNORECASE,
)

WINDOW_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
UTC_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
EXPECTED_DEPENDENCIES = {
    "installed": {
        "ImageHash": "4.3.2", "Pillow": "12.1.1",
        "numpy": "2.2.6", "scipy": "1.15.3",
    },
    "expected": {
        "ImageHash": "4.3.2", "Pillow": "12.1.1",
        "numpy": "2.2.6", "scipy": "1.15.3",
    },
    "ready": True,
}
REQUIREMENTS_SHA256 = "38582028a376dad0914650f12dd5ebd58c2614e544badf94b7762e6a94d70009"
EXPECTED_PRODUCER_SHA256 = "14a0b11025256f22a79883d7337687cfd7b02af8646fb38e6351209cc7bca522"
PHASH_ALGORITHM = "ImageHash-4.3.2-phash-hash_size_8-highfreq_factor_4-exif_transpose-rgb"
EXPECTED_RUNTIME = {
    "python_implementation": "CPython",
    "python_version": "3.10.12",
    "required_implementation": "CPython",
    "required_major_minor": "3.10",
    "ready": True,
}
TRANSIENT_BACKOFF = {
    "strategy": "exponential_per_host_persisted",
    "base": "configured_host_min_interval_seconds",
    "circuit_threshold": 3,
    "circuit_min_seconds": 300,
    "maximum_seconds": 900,
    "reset_on_nontransient_response": True,
}


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INVALID_ARGUMENTS"})
        raise SystemExit(EXIT_USAGE)


class Checks:
    def __init__(self) -> None:
        self.rows: list[dict[str, str]] = []

    def add(self, check_id: str, passed: bool, observed: object, expected: object) -> None:
        self.rows.append({
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "observed": stable_value(observed),
            "expected": stable_value(expected),
        })

    @property
    def passed(self) -> bool:
        return bool(self.rows) and all(row["status"] == "PASS" for row in self.rows)


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def stable_value(value: object) -> str:
    if isinstance(value, (dict, list, tuple, set)):
        normalized = sorted(value) if isinstance(value, set) else value
        return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))[:4000]
    return str(value)[:4000]


def clean(value: object) -> str:
    return str(value or "").replace("\x00", "").strip()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("not object")
    return value


def read_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames is None or None in reader.fieldnames:
            raise ValueError("invalid header")
        rows = list(reader)
    if any(None in row for row in rows):
        raise ValueError("extra columns")
    return list(reader.fieldnames), rows


def is_hex(value: str, length: int) -> bool:
    return len(value) == length and all(char in "0123456789abcdef" for char in value)


def as_int(value: str, *, minimum: int = 0) -> int:
    if not re.fullmatch(r"0|[1-9]\d*", value):
        raise ValueError("invalid integer")
    result = int(value)
    if result < minimum:
        raise ValueError("integer below minimum")
    return result


def parse_utc(value: str) -> datetime:
    if not UTC_RE.fullmatch(value):
        raise ValueError("invalid timestamp")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def url_host(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    return (parsed.hostname or "").lower().rstrip(".")


def valid_public_url_shape(value: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(value)
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
    )


def request_role(request_kind: str) -> str:
    if request_kind in IMAGE_REQUESTS:
        return "image"
    if request_kind == "resolver_api":
        return "resolver_api"
    return "landing"


def sensitive_query_keys(value: str) -> set[str]:
    try:
        pairs = urllib.parse.parse_qsl(
            urllib.parse.urlsplit(value).query, keep_blank_values=True
        )
    except ValueError:
        return set()
    return {
        key
        for key, _ in pairs
        if SENSITIVE_QUERY_KEY.search(urllib.parse.unquote_plus(key))
    }


def component_hosts(component: str, request_kind: str) -> frozenset[str]:
    if request_kind in IMAGE_REQUESTS:
        return COMPONENT_IMAGE_HOSTS.get(component, frozenset())
    if request_kind in RESOLVER_REQUESTS:
        return COMPONENT_RESOLVER_HOSTS.get(component, frozenset())
    return frozenset()


def offline_url_policy_error(value: str, policy: dict[str, Any]) -> str:
    """Return the first producer-equivalent URL-policy error visible offline.

    DNS answers are intentionally not replayed.  A syntactically valid,
    allowlisted URL therefore returns an empty string even though a live run
    may subsequently report DNS_RESOLUTION_FAILED or
    NON_PUBLIC_ADDRESS_FORBIDDEN.  Literal IP hosts are still rejected here.
    """
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return "URL_PORT_INVALID"
    host = (parsed.hostname or "").lower().rstrip(".")
    if sensitive_query_keys(value):
        return "URL_SENSITIVE_QUERY_FORBIDDEN"
    if (
        parsed.scheme not in set(policy.get("allowed_schemes", []))
        or not host
        or host not in policy.get("hosts", {})
    ):
        return "URL_HOST_NOT_ALLOWLISTED"
    if parsed.username is not None or parsed.password is not None:
        return "URL_USERINFO_FORBIDDEN"
    effective_port = port or 443
    if effective_port not in set(policy.get("allowed_ports", [])):
        return "URL_PORT_FORBIDDEN"
    if parsed.fragment:
        return "URL_FRAGMENT_FORBIDDEN"
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None and not literal.is_global:
        return "NON_PUBLIC_ADDRESS_FORBIDDEN"
    return ""


def role_policy(policy: dict[str, Any], host: str, role: str) -> tuple[bool, str]:
    config = policy.get("hosts", {}).get(host)
    if not isinstance(config, dict):
        return False, ""
    state = clean(config.get("policy_state") or "allow_with_limits")
    roles = {clean(value) for value in config.get("roles", [])}
    if role == "robots":
        return True, state
    if state in {"blocked", "pending_permission"}:
        return False, state
    if state == "landing_only" and role not in {"landing", "resolver_api"}:
        return False, "pending_permission"
    return (not roles or role in roles), state


def prepolicy_safety_block_is_consistent(
    row: dict[str, str], policy: dict[str, Any]
) -> bool:
    """Validate a blank-policy safety receipt without pretending to replay DNS."""
    if (
        row.get("transport_status") != "safety_block"
        or row.get("policy_state")
        or row.get("robots_status")
        or row.get("http_status")
    ):
        return False
    current = row.get("url_final") or row.get("url_requested", "")
    visible_error = offline_url_policy_error(current, policy)
    error_code = row.get("error_code", "")
    if visible_error:
        return error_code == visible_error
    return error_code == "NON_PUBLIC_ADDRESS_FORBIDDEN"


def prepolicy_dns_failure_is_consistent(
    row: dict[str, str], policy: dict[str, Any]
) -> bool:
    if (
        row.get("transport_status") != "dns_error"
        or row.get("policy_state")
        or row.get("robots_status") not in {"", "allowed", "allowed_no_robots"}
        or row.get("http_status")
        or row.get("error_code") not in {
            "DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT",
        }
    ):
        return False
    current = row.get("url_final") or row.get("url_requested", "")
    return not offline_url_policy_error(current, policy)


def component_safety_block_is_consistent(
    row: dict[str, str], allowed_hosts: frozenset[str]
) -> bool:
    if (
        row.get("transport_status") != "safety_block"
        or row.get("error_code") != "COMPONENT_REQUEST_HOST_FORBIDDEN"
        or row.get("policy_state")
        or row.get("robots_status")
        or row.get("http_status")
    ):
        return False
    current = row.get("url_final") or row.get("url_requested", "")
    try:
        return url_host(current) not in allowed_hosts
    except ValueError:
        return False


def expected_bindings() -> dict[str, Any]:
    return {
        "schema": "coverfish.pointer-input-bindings.v1",
        "dataset": EXPECTED_DATASET,
        "expected": EXPECTED_COUNTS,
        "pilot": {
            "seed": "coverfish-pointer-pilot-v1",
            "rows": 800,
            "sha256": PILOT_SHA256,
        },
        "manifests": [
            {"component": component, "id_field": id_field, "path": path, "rows": rows, "sha256": sha}
            for component, (path, id_field, rows, sha) in MANIFESTS.items()
        ],
        "container_ledgers": {
            name: {"path": path, "sha256": sha}
            for name, (path, sha) in CONTAINER_LEDGERS.items()
        },
        "e0": {
            "bytes_path": E0_FILES["bytes"][0],
            "bytes_sha256": E0_FILES["bytes"][2],
            "pointers_path": E0_FILES["pointers"][0],
            "pointers_sha256": E0_FILES["pointers"][2],
        },
    }


def normalize_frozen(component: str, id_field: str, row: dict[str, str]) -> dict[str, str]:
    image_url = clean(row.get("source_image_url"))
    pointer_url = clean(row.get("pointer_url"))
    status = clean(row.get("active_projection_status"))
    return {
        "record_id": clean(row.get(id_field)),
        "component": component,
        "active": "false" if status == "retired_from_active_projection" else "true",
        "active_projection_status": status,
        "canonical_taxon_key": clean(row.get("canonical_taxon_key")),
        "fishbase25_speccode": clean(row.get("fishbase25_speccode")),
        "scientific_name": clean(row.get("scientific_name") or row.get("target_scientific_name")),
        "source_page_url": clean(row.get("source_page_url")),
        "source_image_url": image_url,
        "pointer_url": pointer_url,
        "source_host": url_host(image_url),
        "expected_width": clean(row.get("width")),
        "expected_height": clean(row.get("height")),
        "expected_sha256": clean(row.get("sha256")).lower(),
        "expected_phash_hex64": clean(row.get("phash_canonical_hex64")).lower(),
        "license_normalized": clean(row.get("license_normalized")),
        "license_tier": clean(row.get("license_tier_source")),
        "release_mode": clean(row.get("normalized_release_mode") or row.get("pixel_release_mode")),
        "attribution": clean(row.get("attribution")),
    }


def load_frozen(source_root: Path, bindings: dict[str, Any], checks: Checks) -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    checks.add("bindings_hardcoded_contract", bindings == expected_bindings(), bindings, expected_bindings())
    closures = {
        "archive": EXPECTED_COUNTS["archive_pointer_rows"]
        == EXPECTED_COUNTS["active_pointer_rows"]
        + EXPECTED_COUNTS["retired_pointer_rows"],
        "active_staging": EXPECTED_COUNTS["active_staging_rows"]
        == EXPECTED_COUNTS["active_byte_complete_rows"]
        + EXPECTED_COUNTS["active_pointer_rows"],
        "r0": EXPECTED_COUNTS["r0_rows"]
        == EXPECTED_COUNTS["r0_byte_complete_rows"]
        + EXPECTED_COUNTS["r0_pointer_rows"],
        "e0_zero_pointer": EXPECTED_COUNTS["e0_pointer_rows"] == 0,
    }
    checks.add(
        "expected_count_closures",
        all(closures.values()),
        closures,
        {key: True for key in closures},
    )
    ordered: list[dict[str, str]] = []
    by_id: dict[str, dict[str, str]] = {}
    component_counts: Counter[str] = Counter()
    invalid_rows = 0
    duplicate_ids = 0
    for component, (relative, id_field, expected_rows, expected_sha) in MANIFESTS.items():
        path = source_root / relative
        observed_sha = file_sha256(path) if path.is_file() else "missing"
        checks.add(f"frozen_{component.lower()}_sha256", observed_sha == expected_sha, observed_sha, expected_sha)
        if not path.is_file():
            continue
        fields, rows = read_tsv(path)
        checks.add(f"frozen_{component.lower()}_rows", len(rows) == expected_rows, len(rows), expected_rows)
        checks.add(f"frozen_{component.lower()}_id_field", id_field in fields, id_field in fields, True)
        for raw in rows:
            row = normalize_frozen(component, id_field, raw)
            record_id = row["record_id"]
            if (
                not record_id
                or not is_hex(row["expected_sha256"], 64)
                or not is_hex(row["expected_phash_hex64"], 16)
                or not valid_public_url_shape(row["source_image_url"])
                or not valid_public_url_shape(row["pointer_url"])
            ):
                invalid_rows += 1
            if record_id in by_id:
                duplicate_ids += 1
            else:
                by_id[record_id] = row
                ordered.append(row)
            component_counts[component] += 1
    checks.add("frozen_rows_valid", invalid_rows == 0, invalid_rows, 0)
    checks.add("frozen_global_ids_unique", duplicate_ids == 0, duplicate_ids, 0)
    checks.add("frozen_archive_rows", len(ordered) == 42387, len(ordered), 42387)
    checks.add("frozen_component_counts", dict(component_counts) == {key: value[2] for key, value in MANIFESTS.items()}, dict(component_counts), {key: value[2] for key, value in MANIFESTS.items()})
    checks.add(
        "frozen_r0_pointer_rows",
        component_counts["S1"] == EXPECTED_COUNTS["r0_pointer_rows"],
        component_counts["S1"],
        EXPECTED_COUNTS["r0_pointer_rows"],
    )
    active = sum(row["active"] == "true" for row in ordered)
    checks.add("frozen_active_rows", active == 41945, active, 41945)
    checks.add("frozen_retired_rows", len(ordered) - active == 442, len(ordered) - active, 442)

    for name, (relative, expected_sha) in CONTAINER_LEDGERS.items():
        path = source_root / relative
        observed = file_sha256(path) if path.is_file() else "missing"
        checks.add(f"frozen_container_{name}_sha256", observed == expected_sha, observed, expected_sha)
    for name, (relative, expected_rows, expected_sha) in E0_FILES.items():
        path = source_root / relative
        observed = file_sha256(path) if path.is_file() else "missing"
        checks.add(f"frozen_e0_{name}_sha256", observed == expected_sha, observed, expected_sha)
        row_count = len(read_tsv(path)[1]) if path.is_file() else -1
        checks.add(f"frozen_e0_{name}_rows", row_count == expected_rows, row_count, expected_rows)
    return ordered, by_id


def load_scope(scope: str, manifest: Path | None, frozen_order: list[dict[str, str]], frozen: dict[str, dict[str, str]], checks: Checks) -> list[dict[str, str]]:
    if scope == "archive":
        checks.add("archive_manifest_omitted", manifest is None, manifest is None, True)
        return frozen_order
    if manifest is None or not manifest.is_file():
        checks.add("pilot_manifest_present", False, "missing", "present")
        return []
    manifest_sha = file_sha256(manifest)
    checks.add("pilot_manifest_hardcoded_sha256", manifest_sha == PILOT_SHA256, manifest_sha, PILOT_SHA256)
    fields, rows = read_tsv(manifest)
    checks.add("pilot_manifest_schema", tuple(fields) == SAMPLE_FIELDS, fields, SAMPLE_FIELDS)
    checks.add("pilot_manifest_rows", len(rows) == 800, len(rows), 800)
    ids = [clean(row.get("record_id")) for row in rows]
    checks.add("pilot_ids_unique", len(ids) == len(set(ids)), len(set(ids)), len(ids))
    bound_failures = 0
    selected: list[dict[str, str]] = []
    bound_fields = (
        "record_id", "component", "active", "active_projection_status",
        "canonical_taxon_key", "fishbase25_speccode", "scientific_name",
        "source_image_url", "pointer_url", "source_host", "expected_width",
        "expected_height", "expected_sha256", "expected_phash_hex64",
        "license_normalized", "license_tier", "release_mode", "attribution",
    )
    for sample in rows:
        record_id = clean(sample.get("record_id"))
        expected = frozen.get(record_id)
        if expected is None:
            bound_failures += 1
            continue
        if any(clean(sample.get(field)) != expected[field] for field in bound_fields):
            bound_failures += 1
        selected.append(expected)
    checks.add("pilot_rows_bound_to_frozen", bound_failures == 0, bound_failures, 0)
    counts = Counter(clean(row.get("component")) for row in rows)
    checks.add("pilot_component_counts", dict(counts) == PILOT_QUOTAS, dict(counts), PILOT_QUOTAS)
    checks.add("pilot_sample_rank", all(clean(row.get("sample_rank")) == str(index) for index, row in enumerate(rows, 1)), "sequential" if all(clean(row.get("sample_rank")) == str(index) for index, row in enumerate(rows, 1)) else "invalid", "sequential")
    return selected


POLICY_CONTRACT = {
    "api.inaturalist.org": ("allow_with_limits", 1.0, {"resolver_api"}),
    "commons.wikimedia.org": ("allow_with_limits", 1.0, {"landing", "resolver_api"}),
    "conabio.inaturalist.org": ("blocked", 2.0, {"landing"}),
    "db.angfa.org.au": ("landing_only", 10.0, {"image", "landing"}),
    "fishbase.se": ("allow_with_limits", 10.0, {"image", "landing"}),
    "fws.gov": ("allow_with_limits", 1.0, {"image", "landing"}),
    "inaturalist-open-data.s3.amazonaws.com": ("allow_with_limits", 2.0, {"image"}),
    "inaturalist.ca": ("blocked", 2.0, {"landing"}),
    "naturewatch.org.nz": ("blocked", 2.0, {"landing"}),
    "static.inaturalist.org": ("allow_with_limits", 2.0, {"image"}),
    "upload.wikimedia.org": ("allow_with_limits", 1.0, {"image"}),
    "www.fishbase.se": ("allow_with_limits", 10.0, {"image", "landing"}),
    "www.fws.gov": ("allow_with_limits", 1.0, {"image", "landing"}),
    "www.inaturalist.org": ("blocked", 2.0, {"landing"}),
}

FISHBASE_HOSTS = frozenset({"fishbase.se", "www.fishbase.se"})
FISHBASE_10S_CONFIG = {
    "min_interval_seconds": 10,
    "policy_state": "allow_with_limits",
    "roles": ["image", "landing"],
    "full_run_contact_recommended": True,
}
FISHBASE_2S_CONFIG = {
    "min_interval_seconds": 2,
    "policy_state": "allow_with_limits",
    "rate_group": "fishbase_media",
    "max_bytes_per_hour": 2_000_000_000,
    "max_bytes_per_day": 10_000_000_000,
    "roles": ["image", "landing"],
    "full_run_contact_recommended": True,
}
POLICY_PROFILE_CANONICAL_SHA256 = {
    "fishbase_10s_v1": "319bdad94a2340499e4524176f8c9fea0440f86cafb98b9f3b555b9f71202f36",
    "fishbase_2s_v2": "386a0746cbd7303e15a280b2bdc7b2d848b5cbfc40e858a1b570451f1fbdbad4",
}
POLICY_PROFILE_FILE_SHA256 = {
    "fishbase_10s_v1": "711505f4abb446200215f69a9c06a1ca924c8211ebb5613625567ce09fa416fe",
    "fishbase_2s_v2": "2987dffde63b8a0fa1e4a795142267d964d6644bd23d22c35b76b337112687a9",
}


def policy_canonical_sha256(policy: object) -> str:
    payload = json.dumps(
        policy,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def policy_profile_name(policy: object) -> str:
    digest = policy_canonical_sha256(policy)
    return next(
        (
            name
            for name, expected in POLICY_PROFILE_CANONICAL_SHA256.items()
            if digest == expected
        ),
        "",
    )


def host_rate_policy_snapshot(policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    keys = (
        "policy_state", "roles", "min_interval_seconds", "rate_group",
        "max_bytes_per_hour", "max_bytes_per_day",
        "full_run_contact_recommended",
    )
    return {
        host: {key: config[key] for key in keys if key in config}
        for host, config in sorted(policy.get("hosts", {}).items())
        if isinstance(config, dict)
    }


def verify_policy(
    policy: dict[str, Any],
    checks: Checks,
    policy_file_sha256: str | None = None,
) -> str:
    hosts = policy.get("hosts")
    expected_top_keys = {
        "schema", "user_agent_product", "public_contact_url", "allowed_schemes",
        "allowed_ports", "max_response_bytes", "max_image_pixels",
        "max_image_dimension", "image_decode_timeout_seconds",
        "robots_cache_seconds", "request_timeout_seconds",
        "max_request_wall_seconds", "max_fallback_images_per_row", "hosts",
    }
    checks.add("policy_top_level_schema", set(policy) == expected_top_keys, set(policy), expected_top_keys)
    checks.add("policy_schema", policy.get("schema") == "coverfish.pointer-host-policy.v1", policy.get("schema"), "coverfish.pointer-host-policy.v1")
    checks.add("policy_hosts_exact", isinstance(hosts, dict) and set(hosts) == set(POLICY_CONTRACT), set(hosts) if isinstance(hosts, dict) else "invalid", set(POLICY_CONTRACT))
    failures = 0
    if isinstance(hosts, dict):
        for host, (state, minimum, roles) in POLICY_CONTRACT.items():
            config = hosts.get(host, {})
            expected_keys = {"min_interval_seconds", "policy_state", "roles"}
            if host in FISHBASE_HOSTS:
                expected_keys.add("full_run_contact_recommended")
            if host in {"static.inaturalist.org", "inaturalist-open-data.s3.amazonaws.com"}:
                expected_keys.update({"rate_group", "max_bytes_per_hour", "max_bytes_per_day"})
            try:
                interval = float(config.get("min_interval_seconds"))
            except (TypeError, ValueError):
                interval = -1
            fishbase_exact = False
            if host in FISHBASE_HOSTS:
                fishbase_exact = config == FISHBASE_10S_CONFIG or config == FISHBASE_2S_CONFIG
            if (
                (host in FISHBASE_HOSTS and not fishbase_exact)
                or (
                    host not in FISHBASE_HOSTS
                    and (
                        set(config) != expected_keys
                        or config.get("policy_state") != state
                        or interval < minimum
                        or set(config.get("roles", [])) != roles
                    )
                )
            ):
                failures += 1
        for host in FISHBASE_HOSTS:
            if hosts.get(host, {}).get("full_run_contact_recommended") is not True:
                failures += 1
        for host in ("static.inaturalist.org", "inaturalist-open-data.s3.amazonaws.com"):
            config = hosts.get(host, {})
            if (
                config.get("rate_group") != "inaturalist_media"
                or not 0 < int(config.get("max_bytes_per_hour", 0)) <= 4_000_000_000
                or not 0 < int(config.get("max_bytes_per_day", 0)) <= 20_000_000_000
            ):
                failures += 1
    checks.add("policy_host_safety_contract", failures == 0, failures, 0)
    profile = policy_profile_name(policy)
    checks.add(
        "policy_exact_profile",
        bool(profile),
        profile or policy_canonical_sha256(policy),
        sorted(POLICY_PROFILE_CANONICAL_SHA256),
    )
    if policy_file_sha256 is not None:
        expected_file_sha = POLICY_PROFILE_FILE_SHA256.get(profile, "")
        checks.add(
            "policy_file_sha256_allowlist",
            bool(expected_file_sha) and policy_file_sha256 == expected_file_sha,
            policy_file_sha256,
            expected_file_sha or sorted(POLICY_PROFILE_FILE_SHA256.values()),
        )
    try:
        common_ok = (
            0 < int(policy.get("max_response_bytes", 0)) <= 67_108_864
            and policy.get("allowed_schemes") == ["https"]
            and policy.get("allowed_ports") == [443]
            and int(policy.get("max_image_pixels", 0)) == 25_000_000
            and int(policy.get("max_image_dimension", 0)) == 6_000
            and 0 < float(policy.get("image_decode_timeout_seconds", 0)) <= 15
            and 0 < int(policy.get("robots_cache_seconds", 0)) <= 86_400
            and 1 <= float(policy.get("request_timeout_seconds", 0)) <= 60
            and 1 <= float(policy.get("max_request_wall_seconds", 0)) <= 120
            and 0 <= int(policy.get("max_fallback_images_per_row", -1)) <= 3
            and policy.get("user_agent_product") == "COVER-Fish-pointer-audit/1.0"
            and policy.get("public_contact_url") == "https://github.com/Leonccaa/coverfish"
        )
    except (TypeError, ValueError):
        common_ok = False
    checks.add("policy_common_safety_contract", common_ok, common_ok, True)
    return f"{policy.get('user_agent_product')} (+{policy.get('public_contact_url')})"


def validate_redirect(
    row: dict[str, str],
    policy: dict[str, Any],
    role: str,
    allowed_hosts: frozenset[str] | None = None,
    *,
    component_error_code: str = "COMPONENT_REQUEST_HOST_FORBIDDEN",
) -> bool:
    try:
        count = as_int(row["redirect_count"])
        chain = json.loads(row["redirect_chain_json"])
    except (ValueError, TypeError, json.JSONDecodeError):
        return False
    if not isinstance(chain, list) or len(chain) != count or count > 10:
        return False
    too_many = (
        row["transport_status"] == "safety_block"
        and row["error_code"] == "TOO_MANY_REDIRECTS"
    )
    if (count == 10) != too_many:
        return False
    current = row["url_requested"]
    initial_component_stop = (
        allowed_hosts is not None
        and url_host(current) not in allowed_hosts
        and not chain
        and row["transport_status"] == "safety_block"
        and row["error_code"] == component_error_code
        and (not row["url_final"] or row["url_final"] == current)
    )
    if allowed_hosts is not None and url_host(current) not in allowed_hosts:
        return initial_component_stop
    for position, item in enumerate(chain):
        if not isinstance(item, dict) or set(item) != {"status", "from", "to"}:
            return False
        if item["from"] != current or not isinstance(item["status"], int) or item["status"] not in {301, 302, 303, 307, 308}:
            return False
        if not isinstance(item["to"], str):
            return False
        if offline_url_policy_error(current, policy) or offline_url_policy_error(item["to"], policy):
            return False
        current_host = url_host(current)
        target_host = url_host(item["to"])
        current_allowed, _ = role_policy(policy, current_host, role)
        target_allowed, target_state = role_policy(policy, target_host, role)
        if not current_allowed:
            return False
        target_is_terminal_component_stop = (
            allowed_hosts is not None
            and target_host not in allowed_hosts
            and position == len(chain) - 1
            and row["transport_status"] == "safety_block"
            and row["error_code"] == component_error_code
            and row["url_final"] == item["to"]
        )
        if (
            allowed_hosts is not None
            and target_host not in allowed_hosts
            and not target_is_terminal_component_stop
        ):
            return False
        target_is_terminal_policy_stop = (
            position == len(chain) - 1
            and row["transport_status"] == "policy_pending"
            and row["url_final"] == item["to"]
            and row["policy_state"] == target_state
        )
        if (
            not target_allowed
            and not target_is_terminal_policy_stop
            and not target_is_terminal_component_stop
        ):
            return False
        current = item["to"]
    if chain:
        return row["url_final"] == current
    return not row["url_final"] or row["url_final"] == row["url_requested"]


def canonical_receipt_url(value: str) -> str:
    """Return the producer's deterministic, credential-safe receipt form."""
    try:
        parsed = urllib.parse.urlsplit(clean(value))
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


def expected_resolver_url(record: dict[str, str]) -> str:
    component = record["component"]
    if component in {"S0", "D0"}:
        match = re.search(r"/observations/(\d+)", record["pointer_url"])
        if match:
            raw = f"https://api.inaturalist.org/v1/observations/{match.group(1)}"
        else:
            photo = re.search(r"/photos/(\d+)/", record["source_image_url"])
            raw = f"https://www.inaturalist.org/photos/{photo.group(1)}" if photo else ""
    elif component == "S4":
        parsed = urllib.parse.urlsplit(record["pointer_url"])
        if "/wiki/" not in parsed.path:
            return ""
        title = urllib.parse.unquote(parsed.path.split("/wiki/", 1)[1])
        query = urllib.parse.urlencode({
            "action": "query", "format": "json", "formatversion": "2",
            "prop": "imageinfo", "iiprop": "url|size|sha1|mime",
            "maxlag": "1", "titles": title,
        })
        raw = f"https://commons.wikimedia.org/w/api.php?{query}"
    elif component == "S1":
        code = urllib.parse.quote(record["fishbase25_speccode"], safe="")
        raw = f"https://www.fishbase.se/photos/ThumbnailsSummary.php?ID={code}"
    else:
        raw = record["pointer_url"] or record["source_page_url"]
    return canonical_receipt_url(raw)


def classify_attempt(row: dict[str, str], frozen: dict[str, str]) -> tuple[bool, str]:
    request_kind = row["request_kind"]
    transport = row["transport_status"]
    identity = row["identity_class"]
    if request_kind in RESOLVER_REQUESTS:
        expected = "resolver_response" if transport == "ok" else "resolver_error"
        resolver_errors = {
            "INAT_API_JSON_INVALID", "COMMONS_API_JSON_INVALID", "COMMONS_API_ERROR",
            "COMMONS_API_MAXLAG", "HTML_PARSE_FAILED",
        }
        if transport == "ok" and row["error_code"] in resolver_errors:
            expected = "resolver_error"
        resolver_empty = all(not row[field] for field in ("actual_width", "actual_height", "actual_phash_hex64", "phash_distance", "sha256_match", "magic_type"))
        if transport == "ok":
            payload_ok = (
                (bool(row["actual_bytes"]) and is_hex(row["actual_sha256"], 64))
                or (not row["actual_bytes"] and not row["actual_sha256"])
            )
            error_ok = (expected == "resolver_response" and not row["error_code"]) or (
                expected == "resolver_error"
                and row["error_code"] in resolver_errors
            )
        else:
            payload_ok = not row["actual_bytes"] and not row["actual_sha256"]
            error_ok = bool(row["error_code"])
        return identity == expected and resolver_empty and payload_ok and error_ok and row["decode_status"] == "not_attempted", expected
    if request_kind not in IMAGE_REQUESTS:
        return False, "known request_kind"
    if transport != "ok":
        expected = TRANSPORT_IDENTITY.get(transport, "")
        empty = all(not row[field] for field in (
            "actual_bytes", "actual_width", "actual_height", "actual_sha256",
            "actual_phash_hex64", "phash_distance", "sha256_match", "magic_type",
        ))
        return (
            bool(expected)
            and identity == expected
            and empty
            and row["decode_status"] == "not_attempted"
            and bool(row["error_code"])
        ), expected

    actual_sha = row["actual_sha256"]
    if not is_hex(actual_sha, 64) or not row["actual_bytes"]:
        return False, "valid payload SHA and size"
    match = "true" if actual_sha == frozen["expected_sha256"] else "false"
    if row["sha256_match"] != match:
        return False, f"sha256_match={match}"
    if match == "true":
        diagnostics_empty = all(
            not row[field]
            for field in ("actual_width", "actual_height", "actual_phash_hex64", "phash_distance")
        )
        expected_error = (
            "CONTENT_TYPE_MISMATCH"
            if row["content_type_image"] != "true" and row["magic_type"].startswith("image/")
            else ""
        )
        return (
            identity == "byte_exact"
            and row["decode_status"] == "skipped_byte_exact"
            and diagnostics_empty
            and as_int(row["actual_bytes"], minimum=1) > 0
            and row["error_code"] == expected_error
        ), "byte_exact"
    if row["decode_status"] == "decoded":
        actual_phash = row["actual_phash_hex64"]
        if not is_hex(actual_phash, 16):
            return False, "valid actual pHash"
        distance = (int(frozen["expected_phash_hex64"], 16) ^ int(actual_phash, 16)).bit_count()
        if row["phash_distance"] != str(distance):
            return False, f"phash_distance={distance}"
        expected = (
            "visual_near_candidate_d0_2" if distance <= 2
            else "visual_related_candidate_d3_6" if distance <= 6
            else "content_changed_candidate_d_gt6"
        )
        dimensions = all(as_int(row[field], minimum=1) > 0 for field in ("actual_bytes", "actual_width", "actual_height"))
        magic_ok = row["magic_type"].startswith("image/")
        expected_error = "CONTENT_TYPE_MISMATCH" if row["content_type_image"] != "true" else ""
        return identity == expected and dimensions and magic_ok and row["error_code"] == expected_error, expected
    if row["decode_status"] == "decode_error":
        expected = "decode_error"
        empty = not row["actual_phash_hex64"] and not row["phash_distance"] and not row["actual_width"] and not row["actual_height"]
        return identity == expected and empty and row["magic_type"].startswith("image/") and row["error_code"] in {"IMAGE_DECODE_FAILED", "IMAGE_DECODE_TIMEOUT", "IMAGE_DIMENSION_LIMIT_EXCEEDED"}, expected
    if row["decode_status"] == "not_image_magic":
        expected = "empty_response" if row["actual_bytes"] == "0" else "non_image"
        empty = not row["actual_phash_hex64"] and not row["phash_distance"] and not row["actual_width"] and not row["actual_height"]
        expected_error = "EMPTY_RESPONSE" if expected == "empty_response" else "NON_IMAGE_MAGIC"
        empty_sha_ok = expected != "empty_response" or row["actual_sha256"] == hashlib.sha256(b"").hexdigest()
        return identity == expected and empty and empty_sha_ok and not row["magic_type"].startswith("image/") and row["error_code"] == expected_error, expected
    return False, "known decode_status"


def verify_attempts(attempts: list[dict[str, str]], scope_ids: set[str], frozen: dict[str, dict[str, str]], policy: dict[str, Any], checks: Checks) -> None:
    ids = [row["attempt_id"] for row in attempts]
    checks.add("attempt_ids_unique_nonempty", all(ids) and len(ids) == len(set(ids)), len(set(ids)), len(ids))
    failures: Counter[str] = Counter()
    by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    policy_hosts = policy.get("hosts", {})
    for row in attempts:
        record_id = row["record_id"]
        record = frozen.get(record_id)
        if record is None or record_id not in scope_ids:
            failures["scope"] += 1
            continue
        try:
            index = as_int(row["attempt_index"], minimum=1)
            parse_utc(row["checked_at_utc"])
        except ValueError:
            failures["index_or_time"] += 1
            continue
        expected_id = hashlib.sha256(f"{record_id}\0{row['window_id']}\0{index}".encode()).hexdigest()[:24]
        if (
            not WINDOW_RE.fullmatch(row["window_id"])
            or not re.fullmatch(r"[0-9a-f]{16}", row["invocation_id"])
            or row["attempt_id"] != expected_id
        ):
            failures["id_or_window"] += 1
        if row["component"] != record["component"] or row["active"] != record["active"]:
            failures["frozen_binding"] += 1
        if row["bytes_retained"] != "false":
            failures["retention"] += 1
        role = request_role(row["request_kind"])
        s3_image_pending = (
            record["component"] == "S3"
            and row["request_kind"] in IMAGE_REQUESTS
            and row["transport_status"] == "policy_pending"
            and row["error_code"] == "S3_IMAGE_FETCH_NOT_AUTHORIZED"
            and row["policy_state"] == "pending_permission"
            and not row["robots_status"]
            and not row["http_status"]
        )
        if record["component"] == "S3" and row["request_kind"] in IMAGE_REQUESTS:
            allowed_component_hosts = None
            if not s3_image_pending:
                failures["s3_image_policy"] += 1
        else:
            allowed_component_hosts = component_hosts(
                record["component"], row["request_kind"]
            )
        prepolicy_safety = prepolicy_safety_block_is_consistent(row, policy)
        prepolicy_dns = prepolicy_dns_failure_is_consistent(row, policy)
        component_safety = (
            component_safety_block_is_consistent(row, allowed_component_hosts)
            if allowed_component_hosts is not None
            else False
        )
        prepolicy_failure = (
            prepolicy_safety or prepolicy_dns or component_safety
            or s3_image_pending
        )
        requested_error = offline_url_policy_error(row["url_requested"], policy)
        final_error = (
            offline_url_policy_error(row["url_final"], policy)
            if row["url_final"]
            else ""
        )
        if requested_error and not prepolicy_failure:
            failures["requested_url"] += 1
        if final_error and not prepolicy_failure:
            failures["final_url"] += 1
        try:
            expected_host = url_host(row["url_final"] or row["url_requested"])
        except ValueError:
            expected_host = ""
        if row["host"] != expected_host:
            failures["host"] += 1
        elif prepolicy_failure:
            pass
        elif expected_host not in policy_hosts:
            failures["host"] += 1
        else:
            role_allowed, expected_policy_state = role_policy(
                policy, expected_host, role
            )
            if row["policy_state"] != expected_policy_state:
                failures["policy_state"] += 1
            if role_allowed == (row["transport_status"] == "policy_pending"):
                failures["policy_enforcement"] += 1
        if row["transport_status"] == "safety_block" and not row["policy_state"]:
            if not prepolicy_safety and not component_safety and not (
                row["error_code"] == "TOO_MANY_REDIRECTS"
                and row["robots_status"] == ""
                and row["http_status"] == ""
            ):
                failures["safety_block"] += 1
        elif row["transport_status"] == "safety_block":
            if (
                row["error_code"] not in {
                    "REDIRECT_TARGET_INVALID_OR_DOWNGRADE",
                    "URL_PORT_INVALID", "URL_HOST_NOT_ALLOWLISTED",
                    "URL_USERINFO_FORBIDDEN", "URL_PORT_FORBIDDEN",
                    "URL_SENSITIVE_QUERY_FORBIDDEN",
                    "NON_PUBLIC_ADDRESS_FORBIDDEN",
                    "COMPONENT_REQUEST_HOST_FORBIDDEN",
                }
                or row["robots_status"] not in {"allowed", "allowed_no_robots"}
            ):
                failures["safety_block"] += 1
        if row["transport_status"] == "dns_error" and not prepolicy_dns:
            validation_dns = row["error_code"] in {
                "DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT",
            }
            runtime_dns = row["error_code"] == "DNS_ERROR"
            if (
                row["policy_state"] == ""
                or row["robots_status"] not in {"allowed", "allowed_no_robots"}
                or not (validation_dns or runtime_dns)
                or (validation_dns and row["http_status"] not in {"301", "302", "303", "307", "308"})
                or (runtime_dns and row["http_status"])
            ):
                failures["dns_error_semantics"] += 1
        if not validate_redirect(
            row, policy, role, allowed_component_hosts
        ):
            failures["redirect"] += 1
        content_image = "true" if row["content_type"].startswith("image/") else "false"
        if row["content_type_image"] != content_image:
            failures["content_type"] += 1
        try:
            if row["http_status"]:
                http_status = as_int(row["http_status"], minimum=100)
                if http_status > 599:
                    raise ValueError("status")
            else:
                http_status = 0
            if row["actual_bytes"]:
                as_int(row["actual_bytes"])
        except ValueError:
            failures["numeric"] += 1
            http_status = -1
        expected_http = {
            "ok": 200 <= http_status <= 299,
            "not_found": http_status in {404, 410},
            "access_denied": http_status in {401, 403},
            "rate_limited": http_status == 429,
            "server_error": 500 <= http_status <= 599,
            "policy_pending": http_status == 0,
            "robots_disallowed": http_status == 0,
            "robots_unavailable": http_status == 0,
            "timeout": http_status == 0,
            "dns_error": (
                http_status == 0
                if not row["policy_state"] or row["error_code"] == "DNS_ERROR"
                else http_status in {301, 302, 303, 307, 308}
            ),
            "tls_error": http_status == 0,
            "bandwidth_budget_pending": http_status == 0 or 200 <= http_status <= 299,
            "network_error": http_status == 0 or 200 <= http_status <= 299,
            "oversize": 200 <= http_status <= 299,
            "http_error": (
                300 <= http_status <= 499
                and http_status not in {
                    301, 302, 303, 307, 308, 401, 403, 404, 410, 429,
                }
            ),
            "safety_block": (
                http_status == 0
                if not row["policy_state"]
                else http_status in {301, 302, 303, 307, 308}
            ),
        }.get(row["transport_status"], True)
        if not expected_http:
            failures["transport_http"] += 1
        if row["transport_status"] == "oversize" and row["error_code"] not in {
            "CONTENT_LENGTH_EXCEEDS_LIMIT", "BODY_EXCEEDS_LIMIT",
        }:
            failures["transport_error_code"] += 1
        if row["transport_status"] == "http_error" and row["error_code"] != f"HTTP_{http_status}":
            failures["transport_error_code"] += 1
        robots = row["robots_status"]
        transport = row["transport_status"]
        if prepolicy_dns and not robots:
            pass
        elif transport == "policy_pending" and robots:
            failures["robots_policy_order"] += 1
        elif transport == "robots_disallowed" and robots != "disallowed":
            failures["robots_transport"] += 1
        elif transport == "robots_unavailable" and robots != "unavailable_disallow":
            failures["robots_transport"] += 1
        elif transport not in {"policy_pending", "robots_disallowed", "robots_unavailable", "safety_block"} and robots not in {"allowed", "allowed_no_robots"}:
            failures["robots_transport"] += 1
        try:
            classified, _ = classify_attempt(row, record)
        except ValueError:
            classified = False
        if not classified:
            failures["classification"] += 1
        if row["request_kind"] == "direct_image" and (row["resolved_via"] != "direct" or row["url_requested"] != record["source_image_url"]):
            failures["direct_binding"] += 1
        if row["request_kind"] == "resolver_api" and row["resolved_via"] != "source_api":
            failures["resolver_via"] += 1
        if row["request_kind"] == "resolver_page" and row["resolved_via"] not in {"landing_page", "photo_landing"}:
            failures["resolver_via"] += 1
        if row["request_kind"] in RESOLVER_REQUESTS and row["url_requested"] != expected_resolver_url(record):
            failures["resolver_url"] += 1
        if row["request_kind"] == "fallback_image" and row["resolved_via"] not in {"source_api", "landing_page", "photo_landing"}:
            failures["fallback_via"] += 1
        by_record[record_id].append(row)

    try:
        global_times = [parse_utc(row["checked_at_utc"]) for row in attempts]
        if global_times != sorted(global_times):
            failures["global_time_order"] += 1
        future_limit = datetime.now(timezone.utc).timestamp() + 300
        if any(value.timestamp() > future_limit for value in global_times):
            failures["future_timestamp"] += 1
    except ValueError:
        failures["global_time_order"] += 1

    for record_id, rows in by_record.items():
        indexes = [int(row["attempt_index"]) for row in rows]
        if indexes != list(range(1, len(rows) + 1)):
            failures["record_index_sequence"] += 1
        timestamps = [row["checked_at_utc"] for row in rows]
        if timestamps != sorted(timestamps):
            failures["record_time_order"] += 1
        by_window: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            by_window[(row["window_id"], row["invocation_id"])].append(row)
        for window_rows in by_window.values():
            if window_rows[0]["request_kind"] != "direct_image" or sum(row["request_kind"] == "direct_image" for row in window_rows) != 1:
                failures["window_direct_order"] += 1
            kinds = [row["request_kind"] for row in window_rows]
            if kinds.count("fallback_image") > 3:
                failures["fallback_limit"] += 1
            if "fallback_image" in kinds:
                first_fallback = kinds.index("fallback_image")
                if not any(row["request_kind"] in RESOLVER_REQUESTS and row["identity_class"] == "resolver_response" for row in window_rows[:first_fallback]):
                    failures["fallback_without_resolver"] += 1
            exact_positions = [index for index, row in enumerate(window_rows) if row["identity_class"] == "byte_exact"]
            if exact_positions and exact_positions[-1] != len(window_rows) - 1:
                failures["attempt_after_exact"] += 1
    checks.add("attempt_full_semantics", not failures, dict(failures), {})


def derive_resolution_protocol(
    segment: list[dict[str, str]], resolver_candidate_count: int
) -> tuple[bool, str, int] | None:
    """Derive the producer's two-level resolution result from one transaction.

    Candidate bodies are intentionally absent from the public receipt.  Their
    declared count is therefore treated as an input, but it is tightly bound to
    the observable number/order of fallback attempts and to the deterministic
    producer precedence rules.
    """
    if not segment or resolver_candidate_count < 0:
        return None
    direct_rows = [row for row in segment if row["request_kind"] == "direct_image"]
    resolver_rows = [row for row in segment if row["request_kind"] in RESOLVER_REQUESTS]
    fallback_rows = [row for row in segment if row["request_kind"] == "fallback_image"]
    if (
        len(direct_rows) != 1
        or segment[0] is not direct_rows[0]
        or len(resolver_rows) > 1
        or any(
            row["request_kind"] not in IMAGE_REQUESTS | RESOLVER_REQUESTS
            for row in segment
        )
    ):
        return None
    direct = direct_rows[0]
    direct_class = direct["identity_class"]
    if direct_class == "byte_exact":
        return (
            (True, "direct_exact", 0)
            if len(segment) == 1 and resolver_candidate_count == 0
            else None
        )
    if direct_class == "safety_review_required":
        return (
            (False, "fail_safety_review", 0)
            if len(segment) == 1 and resolver_candidate_count == 0
            else None
        )
    if not resolver_rows:
        return (
            (False, "pending_resolver_adapter", 0)
            if len(segment) == 1
            and not fallback_rows
            and resolver_candidate_count == 0
            else None
        )
    resolver = resolver_rows[0]
    resolver_position = segment.index(resolver)
    if resolver_position != 1 or any(
        row["request_kind"] != "fallback_image"
        for row in segment[resolver_position + 1 :]
    ):
        return None
    resolver_ready = (
        resolver["transport_status"] == "ok"
        and resolver["identity_class"] == "resolver_response"
    )
    if not resolver_ready:
        if fallback_rows or resolver_candidate_count != 0 or len(segment) != 2:
            return None
        resolver_transport = resolver["transport_status"]
        if resolver_transport == "safety_block":
            return False, "fail_safety_review", 0
        if direct_class in POLICY_PENDING_CLASSES:
            return False, "pending_policy", 0
        if direct_class == "bandwidth_budget_pending":
            return False, "pending_local_deferral", 0
        if direct_class == "oversize":
            return False, "pending_local_response_cap", 0
        if resolver_transport in {"not_found", "access_denied"}:
            return True, "resolver_access_or_absence_observed", 0
        if resolver_transport in {
            "policy_pending", "robots_disallowed", "robots_unavailable",
        }:
            return False, "pending_policy", 0
        if resolver_transport == "bandwidth_budget_pending":
            return False, "pending_local_deferral", 0
        if resolver_transport == "oversize":
            return False, "pending_local_response_cap", 0
        if (
            resolver["error_code"] == "COMMONS_API_MAXLAG"
            or resolver_transport in {
                "rate_limited", "server_error", "timeout", "dns_error",
                "tls_error", "network_error",
            }
        ):
            return True, "resolver_transient_observed", 0
        if resolver_transport == "ok":
            return True, "resolver_invalid_response_observed", 0
        return True, "resolver_http_error_observed", 0

    fallback_count = len(fallback_rows)
    if fallback_count > 3 or fallback_count > resolver_candidate_count:
        return None
    exact_positions = [
        index
        for index, row in enumerate(fallback_rows)
        if row["identity_class"] == "byte_exact"
    ]
    if exact_positions:
        if (
            exact_positions != [fallback_count - 1]
            or fallback_count == 0
            or fallback_count > min(resolver_candidate_count, 3)
        ):
            return None
        return True, "fallback_exact", fallback_count
    if fallback_count != min(resolver_candidate_count, 3):
        return None
    image_classes = {
        row["identity_class"]
        for row in segment
        if row["request_kind"] in IMAGE_REQUESTS
    }
    if "safety_review_required" in image_classes:
        return False, "fail_safety_review", fallback_count
    if image_classes & POLICY_PENDING_CLASSES:
        return False, "pending_policy", fallback_count
    if "bandwidth_budget_pending" in image_classes:
        return False, "pending_local_deferral", fallback_count
    if "oversize" in image_classes:
        return False, "pending_local_response_cap", fallback_count
    if resolver_candidate_count > 3:
        return False, "pending_candidate_cap", fallback_count
    if image_classes & {
        "rate_limited_429", "transient_5xx", "timeout", "dns_error",
        "tls_error", "network_or_transport_error",
    }:
        return True, "fallback_transient_observed", fallback_count
    if resolver_candidate_count:
        return True, "exhausted_nonexact", fallback_count
    return True, "resolver_no_candidate", fallback_count


def verify_transactions(
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
    scope_ids: set[str],
    frozen: dict[str, dict[str, str]],
    checks: Checks,
) -> list[dict[str, str]]:
    failures: Counter[str] = Counter()
    attempts_by_key: dict[tuple[str, str, int], dict[str, str]] = {}
    attempts_by_id: dict[str, dict[str, str]] = {}
    position_by_id: dict[str, int] = {}
    for position, attempt in enumerate(attempts):
        try:
            key = (attempt["record_id"], attempt["window_id"], as_int(attempt["attempt_index"], minimum=1))
        except ValueError:
            failures["attempt_key"] += 1
            continue
        if key in attempts_by_key or attempt["attempt_id"] in attempts_by_id:
            failures["attempt_key_duplicate"] += 1
        attempts_by_key[key] = attempt
        attempts_by_id[attempt["attempt_id"]] = attempt
        position_by_id[attempt["attempt_id"]] = position

    covered: set[tuple[str, str, int]] = set()
    completion_ids: set[str] = set()
    completed_record_windows: set[tuple[str, str]] = set()
    committed_ids: set[str] = set()
    previous_completion_position = -1
    for completion in completions:
        record_id = completion["record_id"]
        record = frozen.get(record_id)
        try:
            first = as_int(completion["first_attempt_index"], minimum=1)
            last = as_int(completion["last_attempt_index"], minimum=1)
            count = as_int(completion["attempt_count"], minimum=1)
            resolver_candidate_count = as_int(
                completion["resolver_candidate_count"]
            )
            fallback_attempt_count = as_int(
                completion["fallback_attempt_count"]
            )
            completed_at = parse_utc(completion["completed_at_utc"])
        except ValueError:
            failures["completion_types"] += 1
            continue
        invocation = completion["invocation_id"]
        window = completion["window_id"]
        protocol_complete = completion["resolution_protocol_complete"]
        protocol_status = completion["resolution_protocol_status"]
        expected_id = hashlib.sha256(
            (
                f"{record_id}\0{window}\0{invocation}\0{first}\0{last}\0"
                f"{protocol_complete}\0{protocol_status}\0"
                f"{resolver_candidate_count}\0{fallback_attempt_count}"
            ).encode()
        ).hexdigest()[:24]
        record_window = (record_id, window)
        if (
            record is None
            or record_id not in scope_ids
            or completion["component"] != record["component"]
            or not WINDOW_RE.fullmatch(window)
            or not re.fullmatch(r"[0-9a-f]{16}", invocation)
            or last < first
            or count != last - first + 1
            or protocol_complete not in {"true", "false"}
            or protocol_status not in RESOLUTION_PROTOCOL_STATUSES
            or (protocol_complete == "true")
            != (protocol_status in RESOLUTION_PROTOCOL_COMPLETE_STATUSES)
            or fallback_attempt_count > resolver_candidate_count
            or completion["completion_id"] != expected_id
            or completion["completion_id"] in completion_ids
            or record_window in completed_record_windows
        ):
            failures["completion_binding"] += 1
        completion_ids.add(completion["completion_id"])
        completed_record_windows.add(record_window)
        positions: list[int] = []
        segment: list[dict[str, str]] = []
        for index in range(first, last + 1):
            key = (record_id, window, index)
            attempt = attempts_by_key.get(key)
            if (
                attempt is None
                or attempt["invocation_id"] != invocation
                or key in covered
            ):
                failures["completion_coverage"] += 1
                continue
            try:
                if parse_utc(attempt["checked_at_utc"]) > completed_at:
                    failures["completion_time"] += 1
            except ValueError:
                failures["completion_time"] += 1
            covered.add(key)
            committed_ids.add(attempt["attempt_id"])
            positions.append(position_by_id[attempt["attempt_id"]])
            segment.append(attempt)
        derived = derive_resolution_protocol(segment, resolver_candidate_count)
        if (
            derived is None
            or derived
            != (
                protocol_complete == "true",
                protocol_status,
                fallback_attempt_count,
            )
        ):
            failures["completion_protocol_semantics"] += 1
        if positions:
            if positions != list(range(positions[0], positions[0] + len(positions))):
                failures["completion_not_contiguous"] += 1
            if positions[0] <= previous_completion_position:
                failures["completion_file_order"] += 1
            previous_completion_position = positions[-1]

    disposed_ids: set[str] = set()
    for disposition in dispositions:
        attempt_id = disposition["attempt_id"]
        attempt = attempts_by_id.get(attempt_id)
        try:
            recorded_at = parse_utc(disposition["recorded_at_utc"])
        except ValueError:
            failures["disposition_time"] += 1
            continue
        if (
            attempt is None
            or attempt_id in disposed_ids
            or attempt_id in committed_ids
            or disposition["record_id"] != (attempt or {}).get("record_id")
            or disposition["window_id"] != (attempt or {}).get("window_id")
            or disposition["invocation_id"] != (attempt or {}).get("invocation_id")
            or disposition["disposition"] != "abandoned_incomplete_transaction"
        ):
            failures["disposition_binding"] += 1
        elif recorded_at < parse_utc(attempt["checked_at_utc"]):
            failures["disposition_time"] += 1
        disposed_ids.add(attempt_id)

    undecided = set(attempts_by_id) - committed_ids - disposed_ids
    overlap = committed_ids & disposed_ids
    if undecided:
        failures["undecided_attempts"] += len(undecided)
    if overlap:
        failures["decision_overlap"] += len(overlap)
    checks.add("transaction_full_binding", not failures, dict(failures), {})
    return [attempt for attempt in attempts if attempt["attempt_id"] in committed_ids]


def scope_ids_sha256(rows: list[dict[str, str]]) -> str:
    return hashlib.sha256("".join(f"{row['record_id']}\n" for row in rows).encode()).hexdigest()


def verify_contract(
    audit_dir: Path,
    source_rows: list[dict[str, str]],
    scope: str,
    policy: dict[str, Any],
    bindings_sha: str,
    policy_sha: str,
    manifest_sha: str | None,
    user_agent: str,
    checks: Checks,
) -> tuple[dict[str, Any], str]:
    path = audit_dir / "audit-contract.json"
    contract = load_json(path)
    producer = Path(__file__).with_name("reconstruct_pointers.py")
    requirements = Path(__file__).resolve().parents[1] / "requirements-pointer-audit.txt"
    producer_sha = file_sha256(producer) if producer.is_file() else "missing"
    requirements_sha = file_sha256(requirements) if requirements.is_file() else "missing"
    expected = {
        "schema": "coverfish.pointer-audit-contract.v1",
        "tool_version": PRODUCER_VERSION,
        "tool_sha256": EXPECTED_PRODUCER_SHA256,
        "requirements_sha256": REQUIREMENTS_SHA256,
        "dependencies": EXPECTED_DEPENDENCIES,
        "runtime": EXPECTED_RUNTIME,
        "phash_algorithm": PHASH_ALGORITHM,
        "scope": scope,
        "scope_rows": len(source_rows),
        "scope_ids_sha256": scope_ids_sha256(source_rows),
        "dataset": EXPECTED_DATASET,
        "bindings_sha256": bindings_sha,
        "manifest_sha256": manifest_sha,
        "policy_sha256": policy_sha,
        "host_rate_policy": host_rate_policy_snapshot(policy),
        "transient_backoff": TRANSIENT_BACKOFF,
        "user_agent": user_agent,
        "bytes_retained": False,
        "gpu_used": False,
    }
    checks.add(
        "producer_hardcoded_sha256",
        producer_sha == EXPECTED_PRODUCER_SHA256,
        producer_sha,
        EXPECTED_PRODUCER_SHA256,
    )
    checks.add("requirements_hardcoded_sha256", requirements_sha == REQUIREMENTS_SHA256, requirements_sha, REQUIREMENTS_SHA256)
    checks.add("audit_contract_full_binding", contract == expected, contract, expected)
    return contract, file_sha256(path)


def verify_tail_recoveries(audit_dir: Path, checks: Checks) -> list[dict[str, str]]:
    path = audit_dir / "tail-recoveries.tsv"
    if not path.is_file():
        checks.add("tail_recoveries_optional", True, "absent", "absent or valid")
        return []
    fields, rows = read_tsv(path)
    checks.add("tail_recoveries_schema", tuple(fields) == TAIL_RECOVERY_FIELDS, fields, TAIL_RECOVERY_FIELDS)
    failures: Counter[str] = Counter()
    recovery_ids: set[str] = set()
    for row in rows:
        try:
            parse_utc(row["detected_at_utc"])
            original = as_int(row["original_size_bytes"])
            retained = as_int(row["retained_size_bytes"], minimum=1)
            discarded = as_int(row["discarded_fragment_bytes"], minimum=1)
        except ValueError:
            failures["types"] += 1
            continue
        ledger = row["ledger"]
        fragment_sha = row["discarded_fragment_sha256"]
        expected_id = hashlib.sha256(
            f"{ledger}\0{original}\0{retained}\0{fragment_sha}".encode()
        ).hexdigest()[:24]
        if ledger not in APPEND_LEDGERS:
            failures["ledger"] += 1
        if original != retained + discarded:
            failures["size_closure"] += 1
        if not is_hex(fragment_sha, 64):
            failures["sha256"] += 1
        if row["recovery_id"] != expected_id or row["recovery_id"] in recovery_ids:
            failures["identity"] += 1
        recovery_ids.add(row["recovery_id"])
    checks.add("tail_recoveries_full_semantics", not failures, dict(failures), {})
    return rows


ATOMIC_TEMP_NAME = re.compile(
    r"^\.(?P<target>(?:audit-contract\.json|pointer-health\.tsv|"
    r"pointer-health-summary\.json|tail-recoveries\.tsv|robots\.tsv|"
    r"run-metadata-[A-Za-z0-9._-]+-[0-9a-f]{16}\.json))\."
    r"[A-Za-z0-9_-]+\.tmp$"
)


def verify_atomic_temp_recoveries(
    audit_dir: Path, checks: Checks
) -> list[dict[str, str]]:
    path = audit_dir / "atomic-temp-recoveries.tsv"
    if not path.is_file():
        checks.add(
            "atomic_temp_recoveries_optional",
            True,
            "absent",
            "absent or valid",
        )
        return []
    fields, rows = read_tsv(path)
    checks.add(
        "atomic_temp_recoveries_schema",
        tuple(fields) == ATOMIC_TEMP_RECOVERY_FIELDS,
        fields,
        ATOMIC_TEMP_RECOVERY_FIELDS,
    )
    failures: Counter[str] = Counter()
    recovery_ids: set[str] = set()
    for row in rows:
        try:
            parse_utc(row["detected_at_utc"])
            size = as_int(row["size_bytes"])
        except ValueError:
            failures["types"] += 1
            continue
        match = ATOMIC_TEMP_NAME.fullmatch(row["temporary_name"])
        digest = row["sha256"]
        expected_id = hashlib.sha256(
            (
                f"{row['temporary_name']}\0{row['target_name']}\0{size}\0"
                f"{digest}"
            ).encode()
        ).hexdigest()[:24]
        if match is None or match.group("target") != row["target_name"]:
            failures["name_binding"] += 1
        if not is_hex(digest, 64):
            failures["sha256"] += 1
        if row["recovery_id"] != expected_id or row["recovery_id"] in recovery_ids:
            failures["identity"] += 1
        recovery_ids.add(row["recovery_id"])
    checks.add(
        "atomic_temp_recoveries_full_semantics",
        not failures,
        dict(failures),
        {},
    )
    return rows


def robots_fetch_semantics(row: dict[str, str]) -> bool:
    try:
        status = as_int(row["http_status"], minimum=100) if row["http_status"] else 0
        if status > 599:
            return False
    except ValueError:
        return False
    fetch = row["fetch_status"]
    state = row["robots_state"]
    sha = row["sha256"]
    if fetch == "ok":
        fetch_ok = 200 <= status <= 299
    elif fetch == "not_found":
        fetch_ok = status in {404, 410}
    elif fetch == "access_denied":
        fetch_ok = status in {401, 403}
    elif fetch == "rate_limited":
        fetch_ok = status == 429
    elif fetch == "server_error":
        fetch_ok = 500 <= status <= 599
    elif fetch == "oversize":
        fetch_ok = 200 <= status <= 299
    elif fetch == "http_error":
        fetch_ok = 300 <= status <= 499 and status not in {
            301, 302, 303, 307, 308, 401, 403, 404, 410, 429,
        }
    elif fetch == "safety_block":
        fetch_ok = status == 0 or status in {301, 302, 303, 307, 308}
    elif fetch == "dns_error":
        fetch_ok = status == 0 or status in {301, 302, 303, 307, 308}
    elif fetch in {"timeout", "tls_error", "policy_pending"}:
        fetch_ok = status == 0
    elif fetch in {"network_error", "bandwidth_budget_pending"}:
        fetch_ok = status == 0 or 200 <= status <= 299
    else:
        fetch_ok = False
    error = row["error_code"]
    if fetch == "ok":
        error_ok = not error
    elif fetch in {
        "not_found", "access_denied", "rate_limited", "server_error",
        "http_error",
    }:
        error_ok = error == f"HTTP_{status}"
    elif fetch == "oversize":
        error_ok = error in {
            "CONTENT_LENGTH_EXCEEDS_LIMIT", "BODY_EXCEEDS_LIMIT",
        }
    elif fetch == "timeout":
        error_ok = error in {"TIMEOUT", "REQUEST_WALL_TIMEOUT"}
    elif fetch == "dns_error":
        error_ok = error in {
            "DNS_ERROR", "DNS_RESOLUTION_FAILED", "DNS_RESOLUTION_TIMEOUT",
        }
    elif fetch == "tls_error":
        error_ok = error == "TLS_ERROR"
    elif fetch == "network_error":
        error_ok = error in {
            "NETWORK_ERROR", "NETWORK_OS_ERROR", "INCOMPLETE_RESPONSE",
            "CONTENT_LENGTH_MISMATCH", "HTTP_PROTOCOL_ERROR",
        }
    elif fetch == "bandwidth_budget_pending":
        error_ok = error in {
            "BANDWIDTH_BUDGET_REACHED", "RETRY_AFTER_ACTIVE",
            "TRANSIENT_CIRCUIT_OPEN", "RATE_INTERVAL_ACTIVE",
        }
    elif fetch == "policy_pending":
        error_ok = error == "SOURCE_POLICY_NOT_AUTHORIZED"
    elif fetch == "safety_block":
        error_ok = error in {
            "TOO_MANY_REDIRECTS", "REDIRECT_TARGET_INVALID_OR_DOWNGRADE",
            "COMPONENT_ROBOTS_HOST_FORBIDDEN", "URL_PORT_INVALID",
            "URL_HOST_NOT_ALLOWLISTED", "URL_USERINFO_FORBIDDEN",
            "URL_PORT_FORBIDDEN", "URL_SENSITIVE_QUERY_FORBIDDEN",
            "NON_PUBLIC_ADDRESS_FORBIDDEN",
        }
    else:
        error_ok = False
    if not fetch_ok or not error_ok:
        return False
    if state in {"parsed", "parse_error"}:
        return fetch == "ok" and 200 <= status <= 299 and is_hex(sha, 64)
    if state == "not_present_allow":
        return fetch == "not_found" and status in {404, 410} and not sha
    if state == "unavailable_disallow":
        return fetch not in {"ok", "not_found"} and not sha
    return False


def verify_robots(
    audit_dir: Path,
    attempts: list[dict[str, str]],
    metadata: list[dict[str, Any]],
    policy: dict[str, Any],
    checks: Checks,
) -> list[dict[str, str]]:
    path = audit_dir / "robots.tsv"
    if not path.is_file():
        robots_needed = sum(bool(row["robots_status"]) for row in attempts)
        checks.add("robots_present_when_referenced", robots_needed == 0, "missing", "optional only when no attempt references robots")
        return []
    fields, rows = read_tsv(path)
    checks.add("robots_schema", tuple(fields) == ROBOTS_FIELDS, fields, ROBOTS_FIELDS)
    failures: Counter[str] = Counter()
    metadata_by_pair = {
        (str(item.get("window_id", "")), str(item.get("invocation_id", ""))): item
        for item in metadata
    }
    attempts_by_pair: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    related_hosts: dict[tuple[str, str], set[str]] = defaultdict(set)
    allowed_hosts_by_pair: dict[tuple[str, str], set[str]] = defaultdict(set)
    for attempt in attempts:
        pair = (attempt["window_id"], attempt["invocation_id"])
        attempts_by_pair[pair].append(attempt)
        allowed_hosts_by_pair[pair].update(
            component_hosts(attempt["component"], attempt["request_kind"])
        )
        urls = [attempt["url_requested"], attempt["url_final"]]
        try:
            chain = json.loads(attempt["redirect_chain_json"])
            if isinstance(chain, list):
                for item in chain:
                    if isinstance(item, dict):
                        urls.extend([str(item.get("from", "")), str(item.get("to", ""))])
        except json.JSONDecodeError:
            pass
        related_hosts[pair].update(url_host(url) for url in urls if url)
    observations: dict[tuple[str, str, str], list[tuple[datetime, dict[str, str]]]] = defaultdict(list)
    for row in rows:
        host = row["host"]
        try:
            checked_at = parse_utc(row["checked_at_utc"])
        except ValueError:
            failures["timestamp"] += 1
            continue
        pair = (row["window_id"], row["invocation_id"])
        metadata_item = metadata_by_pair.get(pair)
        if metadata_item is None:
            failures["metadata_binding"] += 1
        if host not in policy.get("hosts", {}):
            failures["host"] += 1
        if (
            row["robots_url"] != f"https://{host}/robots.txt"
            or offline_url_policy_error(row["robots_url"], policy)
        ):
            failures["url"] += 1
        if not robots_fetch_semantics(row):
            failures["fetch_state"] += 1
        if pair in attempts_by_pair:
            allowed_component_hosts = frozenset(allowed_hosts_by_pair[pair])
        else:
            components = (
                metadata_item.get("components", [])
                if isinstance(metadata_item, dict)
                else []
            )
            if not components:
                components = sorted(MANIFESTS)
            allowed_component_hosts = frozenset().union(*(
                COMPONENT_IMAGE_HOSTS.get(str(component), frozenset())
                | COMPONENT_RESOLVER_HOSTS.get(str(component), frozenset())
                for component in components
            ))
        try:
            chain_value = json.loads(row["redirect_chain_json"])
            final_url = (
                str(chain_value[-1]["to"])
                if isinstance(chain_value, list) and chain_value
                else row["robots_url"]
            )
        except (json.JSONDecodeError, KeyError, TypeError):
            final_url = ""
        redirect_row = {
            "redirect_count": row["redirect_count"],
            "redirect_chain_json": row["redirect_chain_json"],
            "transport_status": row["fetch_status"],
            "error_code": row["error_code"],
            "url_requested": row["robots_url"],
            "url_final": final_url,
            "policy_state": "",
        }
        if not validate_redirect(
            redirect_row,
            policy,
            "robots",
            allowed_component_hosts,
            component_error_code="COMPONENT_ROBOTS_HOST_FORBIDDEN",
        ):
            failures["redirect"] += 1
        if pair in attempts_by_pair:
            if host not in related_hosts[pair]:
                failures["attempt_host_binding"] += 1
        elif metadata_item is None or metadata_item.get("status") not in {
            "ERROR", "INTERRUPTED", "ABANDONED_BY_RESUME",
        }:
            failures["robots_only_invocation"] += 1
        observations[(row["window_id"], row["invocation_id"], host)].append(
            (checked_at, row)
        )
    cache_seconds = int(policy.get("robots_cache_seconds", 0) or 0)
    for key, items in observations.items():
        timestamps = [stamp for stamp, _ in items]
        if timestamps != sorted(timestamps) or len(timestamps) != len(set(timestamps)):
            failures["observation_order"] += 1
    for attempt in attempts:
        if attempt["robots_status"]:
            key = (attempt["window_id"], attempt["invocation_id"], attempt["host"])
            try:
                attempt_time = parse_utc(attempt["checked_at_utc"])
            except ValueError:
                failures["attempt_timestamp"] += 1
                continue
            eligible = [
                item for item in observations.get(key, [])
                if item[0] <= attempt_time
                and (attempt_time - item[0]).total_seconds() <= cache_seconds
            ]
            if not eligible:
                failures["attempt_receipt_missing_or_stale"] += 1
                continue
            receipt = max(eligible, key=lambda item: item[0])[1]
            compatible_states = {
                "allowed": {"parsed"},
                "allowed_no_robots": {"not_present_allow"},
                "disallowed": {"parsed"},
                "unavailable_disallow": {"unavailable_disallow", "parse_error"},
            }.get(attempt["robots_status"], set())
            if receipt["robots_state"] not in compatible_states:
                failures["attempt_state_binding"] += 1
    checks.add("robots_full_semantics", not failures, dict(failures), {})
    return rows


RUN_KEYS = {
    "schema", "status", "invocation_id", "tool_version", "tool_sha256",
    "phash_algorithm", "dependencies", "runtime", "window_id", "scope", "started_at_utc",
    "completed_at_utc", "rows_selected", "rows_processed", "attempts_before",
    "attempts_after", "attempts_written", "completions_before", "completions_after",
    "retry_mode", "max_rows", "components", "final_window", "policy_sha256",
    "bindings_sha256", "manifest_sha256", "contract_sha256", "user_agent",
    "bytes_retained", "gpu_used",
}


def verify_run_metadata(
    audit_dir: Path,
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
    scope: str,
    manifest_sha: str | None,
    policy_sha: str,
    bindings_sha: str,
    contract: dict[str, Any],
    contract_sha: str,
    user_agent: str,
    checks: Checks,
) -> tuple[bool, set[str], list[dict[str, Any]]]:
    metadata_files = sorted(audit_dir.glob("run-metadata-*.json"))
    robots = read_tsv(audit_dir / "robots.tsv")[1] if (audit_dir / "robots.tsv").is_file() else []
    metadata: list[dict[str, Any]] = []
    failures: Counter[str] = Counter()
    for path in metadata_files:
        item = load_json(path)
        metadata.append(item)
        window = item.get("window_id")
        invocation = item.get("invocation_id")
        status = item.get("status")
        disposition_count = sum(row["invocation_id"] == invocation for row in dispositions)
        expected_keys = set(RUN_KEYS)
        if status in {"ERROR", "INTERRUPTED"}:
            expected_keys.add("error_code")
        if disposition_count:
            expected_keys.add("abandoned_attempts_recorded")
        if (
            set(item) != expected_keys
            or not isinstance(window, str)
            or not WINDOW_RE.fullmatch(window)
            or not isinstance(invocation, str)
            or not re.fullmatch(r"[0-9a-f]{16}", invocation)
            or status not in {"COMPLETE", "ERROR", "INTERRUPTED", "ABANDONED_BY_RESUME"}
        ):
            failures["schema"] += 1
            continue
        if path.name != f"run-metadata-{window}-{invocation}.json":
            failures["filename"] += 1
        try:
            started = parse_utc(str(item["started_at_utc"]))
            completed = parse_utc(str(item["completed_at_utc"]))
            before = int(item["attempts_before"])
            after = int(item["attempts_after"])
            written = int(item["attempts_written"])
            completions_before = int(item["completions_before"])
            completions_after = int(item["completions_after"])
            selected = int(item["rows_selected"])
            processed = int(item["rows_processed"])
            if not all(isinstance(item[key], int) and not isinstance(item[key], bool) for key in (
                "attempts_before", "attempts_after", "attempts_written",
                "completions_before", "completions_after", "rows_selected", "rows_processed",
            )):
                raise ValueError("non integer")
        except (ValueError, TypeError, KeyError):
            failures["types"] += 1
            continue
        if not (
            started <= completed
            and 0 <= before <= after <= len(attempts)
            and written == after - before
            and 0 <= completions_before <= completions_after <= len(completions)
            and 0 <= processed <= selected
            and processed == completions_after - completions_before
            and (status != "COMPLETE" or processed == selected)
        ):
            failures["ranges"] += 1
        components = item.get("components")
        max_rows = item.get("max_rows")
        components_ok = (
            isinstance(components, list)
            and all(component in MANIFESTS for component in components)
            and components == sorted(set(components))
        )
        max_rows_ok = max_rows is None or (isinstance(max_rows, int) and not isinstance(max_rows, bool) and max_rows >= 0)
        if (
            item["schema"] != "coverfish.pointer-audit-run.v1"
            or item["tool_version"] != PRODUCER_VERSION
            or item["tool_sha256"] != contract.get("tool_sha256")
            or item["phash_algorithm"] != PHASH_ALGORITHM
            or item["dependencies"] != EXPECTED_DEPENDENCIES
            or item["runtime"] != EXPECTED_RUNTIME
            or item["scope"] != scope
            or item["retry_mode"] not in {"none", "all", "nonexact", "transient"}
            or not isinstance(item["final_window"], bool)
            or item["policy_sha256"] != policy_sha
            or item["bindings_sha256"] != bindings_sha
            or item["manifest_sha256"] != manifest_sha
            or item["contract_sha256"] != contract_sha
            or item["user_agent"] != user_agent
            or item["bytes_retained"] is not False
            or item["gpu_used"] is not False
            or not components_ok
            or not max_rows_ok
            or (item["final_window"] and (components or max_rows is not None))
            or (item["final_window"] and item["retry_mode"] != "nonexact")
            or (status in {"ERROR", "INTERRUPTED"} and not clean(item.get("error_code")))
            or (disposition_count and item.get("abandoned_attempts_recorded") != disposition_count)
            or (disposition_count and status not in {"ERROR", "INTERRUPTED", "ABANDONED_BY_RESUME"})
        ):
            failures["binding"] += 1
        segment = attempts[before:after]
        completion_segment = completions[completions_before:completions_after]
        if (
            any(row["invocation_id"] != invocation or row["window_id"] != window for row in segment)
            or any(row["invocation_id"] == invocation for row in attempts[:before] + attempts[after:])
            or any(row["invocation_id"] != invocation or row["window_id"] != window for row in completion_segment)
            or any(row["invocation_id"] == invocation for row in completions[:completions_before] + completions[completions_after:])
        ):
            failures["attempt_segment"] += 1
        completed_records = {row["record_id"] for row in completion_segment}
        if len(completed_records) != processed:
            failures["processed_count"] += 1
        if (
            any(not (started <= parse_utc(row["checked_at_utc"]) <= completed) for row in segment)
            or any(not (started <= parse_utc(row["completed_at_utc"]) <= completed) for row in completion_segment)
            or any(
                not (started <= parse_utc(row["checked_at_utc"]) <= completed)
                or row["window_id"] != window
                for row in robots
                if row["invocation_id"] == invocation
            )
        ):
            failures["time_window"] += 1
    invocation_ids = {row["invocation_id"] for row in attempts} | {row["invocation_id"] for row in completions} | {row["invocation_id"] for row in dispositions}
    metadata_ids = {str(item.get("invocation_id", "")) for item in metadata}
    if not invocation_ids.issubset(metadata_ids) or len(metadata_ids) != len(metadata):
        failures["invocation_set"] += 1
    windows_to_invocations: dict[str, set[str]] = defaultdict(set)
    invocations_to_windows: dict[str, set[str]] = defaultdict(set)
    for item in metadata:
        window_value = str(item.get("window_id", ""))
        invocation_value = str(item.get("invocation_id", ""))
        windows_to_invocations[window_value].add(invocation_value)
        invocations_to_windows[invocation_value].add(window_value)
    if (
        any(len(invocations) != 1 for invocations in windows_to_invocations.values())
        or any(len(windows) != 1 for windows in invocations_to_windows.values())
    ):
        failures["window_id_reused_across_invocations"] += 1
    positive = sorted((item for item in metadata if isinstance(item.get("attempts_before"), int) and item.get("attempts_after") > item.get("attempts_before")), key=lambda item: item["attempts_before"])
    cursor = 0
    for item in positive:
        if item["attempts_before"] != cursor:
            failures["partition"] += 1
        cursor = item["attempts_after"]
    if cursor != len(attempts):
        failures["partition"] += 1
    positive_completions = sorted(
        (item for item in metadata if isinstance(item.get("completions_before"), int) and item.get("completions_after") > item.get("completions_before")),
        key=lambda item: item["completions_before"],
    )
    cursor = 0
    for item in positive_completions:
        if item["completions_before"] != cursor:
            failures["completion_partition"] += 1
        cursor = item["completions_after"]
    if cursor != len(completions):
        failures["completion_partition"] += 1
    checks.add("run_metadata_full_binding", not failures, dict(failures), {})
    if not metadata:
        checks.add("run_metadata_present", not attempts, 0, ">=1 when attempts exist")
        return False, set(), []
    final_window_ids = {
        str(item["window_id"])
        for item in metadata
        if item.get("status") == "COMPLETE" and item.get("final_window") is True
    }
    return bool(final_window_ids), final_window_ids, metadata


def best_attempt(rows: Iterable[dict[str, str]]) -> dict[str, str] | None:
    candidates = [row for row in rows if row["request_kind"] in IMAGE_REQUESTS]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            IDENTITY_PRIORITY.get(row["identity_class"], 0),
            int(row["attempt_index"] or 0),
        ),
    )


def record_retryable(rows: list[dict[str, str]], best: dict[str, str] | None) -> bool:
    if best is None:
        return True
    final_class = best["identity_class"]
    if final_class in {"byte_exact", "policy_pending_permission"}:
        return False
    if final_class in RETRYABLE_CLASSES:
        return True
    return any(
        row["request_kind"] in RESOLVER_REQUESTS
        and (
            row["transport_status"] in RETRYABLE_RESOLVER_TRANSPORT
            or row["error_code"] == "COMMONS_API_MAXLAG"
        )
        for row in rows
    )


def retry_protocol_metrics(
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    final_window_ids: set[str],
) -> tuple[int, int, float, bool, bool, bool]:
    protocol_complete_windows = {
        row["window_id"]
        for row in completions
        if row["resolution_protocol_complete"] == "true"
    }
    observations = [
        row for row in attempts
        if row["request_kind"] in IMAGE_REQUESTS
        and row["identity_class"] not in FINALITY_NON_OBSERVATION_CLASSES
        and row["window_id"] in protocol_complete_windows
    ]
    windows = {row["window_id"] for row in observations if row["window_id"]}
    dated_attempts = [
        (parse_utc(row["checked_at_utc"]).timestamp(), row["window_id"])
        for row in observations if row["checked_at_utc"]
    ]
    timestamps = sorted(stamp for stamp, _ in dated_attempts)
    utc_dates = {
        datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()
        for stamp in timestamps
    }
    elapsed = (timestamps[-1] - timestamps[0]) / 3600 if len(timestamps) >= 2 else 0.0
    final_observed = bool(windows & final_window_ids)
    latest_windows = {
        window for stamp, window in dated_attempts if timestamps and stamp == timestamps[-1]
    }
    latest_is_final = bool(latest_windows) and latest_windows.issubset(final_window_ids)
    satisfied = (
        len(windows) >= 3 and len(utc_dates) >= 3 and elapsed >= 48 and latest_is_final
    )
    return len(windows), len(utc_dates), elapsed, final_observed, latest_is_final, satisfied


def rebuild_health(
    source_rows: list[dict[str, str]],
    attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    final_window_ids: set[str] | None = None,
) -> list[dict[str, str]]:
    declared_final_window_ids = set(final_window_ids or set())
    by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for attempt in attempts:
        by_record[attempt["record_id"]].append(attempt)
    completions_by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for completion in completions:
        completions_by_record[completion["record_id"]].append(completion)
    actual_to_expected: dict[str, set[str]] = defaultdict(set)
    for source in source_rows:
        for attempt in by_record[source["record_id"]]:
            if attempt["request_kind"] in IMAGE_REQUESTS and attempt["actual_sha256"]:
                actual_to_expected[attempt["actual_sha256"]].add(source["expected_sha256"])
    suspicious = {actual for actual, expected in actual_to_expected.items() if len(expected) >= 3}
    result_rows: list[dict[str, str]] = []
    for source in source_rows:
        record_attempts = by_record[source["record_id"]]
        record_completions = completions_by_record[source["record_id"]]
        latest_completion = max(
            record_completions,
            key=lambda item: (
                int(item["last_attempt_index"]), item["completed_at_utc"]
            ),
            default=None,
        )
        result = {field: "" for field in HEALTH_FIELDS}
        for field in HEALTH_FIELDS:
            if field in source:
                result[field] = source[field]
        result["attempts"] = str(len(record_attempts))
        timestamps = sorted(row["checked_at_utc"] for row in record_attempts if row["checked_at_utc"])
        result["first_checked_at_utc"] = timestamps[0] if timestamps else ""
        result["last_checked_at_utc"] = timestamps[-1] if timestamps else ""
        result["bytes_retained"] = "false"
        best = best_attempt(record_attempts)
        if best is None:
            result["final_class"] = "not_audited"
            result["retryable"] = "true"
        else:
            for source_field, target in {
                "resolved_via": "resolved_via", "url_final": "final_url",
                "http_status": "http_status", "content_type": "content_type",
                "magic_type": "magic_type", "actual_bytes": "actual_bytes",
                "actual_width": "actual_width", "actual_height": "actual_height",
                "actual_sha256": "actual_sha256", "actual_phash_hex64": "actual_phash_hex64",
                "phash_distance": "phash_distance", "sha256_match": "sha256_match",
                "identity_class": "final_class",
            }.items():
                result[target] = best[source_field]
            result["retryable"] = "true" if record_retryable(record_attempts, best) else "false"
            result["possible_placeholder_cluster"] = "true" if best["actual_sha256"] in suspicious else "false"
        if latest_completion is None:
            result["resolution_protocol_complete"] = "false"
            result["resolution_protocol_status"] = "not_audited"
        else:
            result["resolution_protocol_complete"] = latest_completion[
                "resolution_protocol_complete"
            ]
            result["resolution_protocol_status"] = latest_completion[
                "resolution_protocol_status"
            ]
        if (
            result["resolution_protocol_complete"] != "true"
            and result["resolution_protocol_status"]
            not in {"pending_policy", "fail_safety_review"}
        ):
            result["retryable"] = "true"
        multiwindow_applies = result["final_class"] not in {
            "byte_exact", *FINALITY_NON_OBSERVATION_CLASSES,
        }
        distinct_windows, distinct_dates, elapsed_hours, final_observed, latest_is_final, satisfied = retry_protocol_metrics(
            record_attempts, record_completions, declared_final_window_ids
        )
        result["multiwindow_protocol_applies"] = "true" if multiwindow_applies else "false"
        result["distinct_observation_windows"] = str(distinct_windows)
        result["distinct_observation_utc_dates"] = str(distinct_dates)
        result["observation_elapsed_hours"] = f"{elapsed_hours:.6f}"
        result["observed_in_declared_final_window"] = "true" if final_observed else "false"
        result["latest_observation_is_declared_final"] = "true" if latest_is_final else "false"
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
            result["finality_status"] = "satisfied" if satisfied else "pending"
        result_rows.append(result)
    return result_rows


def compare_health(actual: list[dict[str, str]], rebuilt: list[dict[str, str]], checks: Checks) -> None:
    mismatched = 0
    fields: Counter[str] = Counter()
    if len(actual) != len(rebuilt):
        mismatched += abs(len(actual) - len(rebuilt))
    for observed, expected in zip(actual, rebuilt):
        if observed != expected:
            mismatched += 1
            for field in HEALTH_FIELDS:
                if observed.get(field) != expected.get(field):
                    fields[field] += 1
    checks.add("health_full_rebuild", mismatched == 0, {"rows": mismatched, "fields": dict(fields)}, {"rows": 0, "fields": {}})


def nested_counts(rows: list[dict[str, str]], field: str, group: str) -> dict[str, dict[str, int]]:
    result: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        result[row[group]][row[field]] += 1
    return {key: dict(sorted(values.items())) for key, values in sorted(result.items())}


def rebuild_summary(
    health: list[dict[str, str]],
    attempts: list[dict[str, str]],
    committed_attempts: list[dict[str, str]],
    completions: list[dict[str, str]],
    dispositions: list[dict[str, str]],
    robots: list[dict[str, str]],
    scope: str,
    final_window: bool,
    contract: dict[str, Any],
    contract_sha: str,
    final_window_ids: set[str] | None = None,
    tail_recoveries: list[dict[str, str]] | None = None,
    atomic_temp_recoveries: list[dict[str, str]] | None = None,
    run_metadata: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    declared_final_window_ids = set(final_window_ids or set())
    classes = Counter(row["final_class"] for row in health)
    active = [row for row in health if row["active"] == "true"]
    active_classes = Counter(row["final_class"] for row in active)
    s1 = [row for row in health if row["component"] == "S1"]
    exact_archive = classes.get("byte_exact", 0)
    exact_active = active_classes.get("byte_exact", 0)
    s1_exact = sum(row["final_class"] == "byte_exact" for row in s1)
    s1_near = sum(row["final_class"] == "visual_near_candidate_d0_2" for row in s1)
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
        if row["identity_class"] == "safety_review_required"
        or row["transport_status"] == "safety_block"
    }
    safety_review_ids.update(
        row["record_id"]
        for row in health
        if row["resolution_protocol_status"] == "fail_safety_review"
    )
    robots_safety_rows = [
        row for row in robots if row["fetch_status"] == "safety_block"
    ]
    retryable = sum(row["retryable"] == "true" for row in health)
    not_audited = classes.get("not_audited", 0)
    completed_ids = {row["record_id"] for row in completions}
    completed_health = [row for row in health if row["record_id"] in completed_ids]
    completed_active = [row for row in completed_health if row["active"] == "true"]
    completed_exact = sum(row["final_class"] == "byte_exact" for row in completed_health)
    completed_active_exact = sum(row["final_class"] == "byte_exact" for row in completed_active)
    uncompleted = len(health) - len(completed_ids)
    covered: set[tuple[str, str, int]] = set()
    for completion in completions:
        for index in range(int(completion["first_attempt_index"]), int(completion["last_attempt_index"]) + 1):
            covered.add((completion["record_id"], completion["window_id"], index))
    disposed_ids = {row["attempt_id"] for row in dispositions}
    uncommitted = sum(
        (row["record_id"], row["window_id"], int(row["attempt_index"])) not in covered
        and row["attempt_id"] not in disposed_ids
        for row in attempts
    )
    timestamps = sorted(row["checked_at_utc"] for row in committed_attempts if row["checked_at_utc"])
    committed_by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for attempt in committed_attempts:
        committed_by_record[attempt["record_id"]].append(attempt)
    completions_by_record: dict[str, list[dict[str, str]]] = defaultdict(list)
    for completion in completions:
        completions_by_record[completion["record_id"]].append(completion)
    retryable_ids = {row["record_id"] for row in health if row["retryable"] == "true"}
    finality_ids = {
        row["record_id"]
        for row in health
        if row["final_class"] not in {"byte_exact", *FINALITY_NON_OBSERVATION_CLASSES}
    }
    finality_satisfied = 0
    finality_unsatisfied = 0
    finality_observed = 0
    finality_latest = 0
    for record_id in finality_ids:
        record_attempts = committed_by_record[record_id]
        _, _, _, observed, latest, satisfied = retry_protocol_metrics(
            record_attempts,
            completions_by_record[record_id],
            declared_final_window_ids,
        )
        if observed:
            finality_observed += 1
        if latest:
            finality_latest += 1
        if satisfied:
            finality_satisfied += 1
        else:
            finality_unsatisfied += 1
    scope_complete = uncompleted == 0 and not not_audited and uncommitted == 0
    archive_population_complete = (
        scope == "archive"
        and scope_complete
        and len(health) == EXPECTED_COUNTS["archive_pointer_rows"]
    )
    active_population_complete = (
        archive_population_complete
        and len(active) == EXPECTED_COUNTS["active_pointer_rows"]
    )
    r0_population_complete = (
        archive_population_complete
        and len(s1) == EXPECTED_COUNTS["r0_pointer_rows"]
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
        "audit_integrity": "PASS" if len(health) == (800 if scope == "pilot" else 42387) and scope_complete else "PENDING",
        "tool_version": PRODUCER_VERSION,
        "tool_sha256": contract.get("tool_sha256"),
        "phash_algorithm": PHASH_ALGORITHM,
        "dependencies": EXPECTED_DEPENDENCIES,
        "runtime": EXPECTED_RUNTIME,
        "scope": scope,
        "final_window": final_window,
        "dataset": EXPECTED_DATASET,
        "rows": {
            "scope": len(health), "archive_expected": 42387,
            "active_in_scope": len(active), "retired_in_scope": len(health) - len(active),
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
            "by_host_and_transport": nested_counts(attempts, "transport_status", "host"),
            "by_transport_status": dict(sorted(Counter(row["transport_status"] for row in attempts).items())),
            "by_error_code": dict(sorted(Counter(row["error_code"] for row in attempts).items())),
            "by_redirect_count": dict(sorted(Counter(row["redirect_count"] for row in attempts).items())),
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
        "run_retry_modes": dict(sorted(Counter(clean(row.get("retry_mode")) for row in (run_metadata or [])).items())),
        "host_rate_policy": contract.get("host_rate_policy", {}),
        "transient_backoff": contract.get("transient_backoff", {}),
        "attempts": len(attempts),
        "attempt_bytes": sum(int(row["actual_bytes"] or 0) for row in attempts),
        "committed_attempts": len(committed_attempts),
        "committed_attempt_bytes": sum(int(row["actual_bytes"] or 0) for row in committed_attempts),
        "receipt": {
            "completion_rows": len(completions),
            "abandoned_attempt_rows": len(dispositions),
            "completed_scope_rows": len(completed_ids),
            "uncompleted_scope_rows": uncompleted,
            "uncommitted_attempt_rows": uncommitted,
            "tail_recovery_rows": len(tail_recoveries or []),
            "atomic_temp_recovery_rows": len(atomic_temp_recoveries or []),
            "bindings_sha256": contract.get("bindings_sha256"),
            "manifest_sha256": contract.get("manifest_sha256"),
            "policy_sha256": contract.get("policy_sha256"),
            "contract_sha256": contract_sha,
        },
        "observation": {
            "first_checked_at_utc": timestamps[0] if timestamps else None,
            "last_checked_at_utc": timestamps[-1] if timestamps else None,
            "window_ids": sorted({row["window_id"] for row in committed_attempts}),
        },
        "finality": {
            "declared_final_window": final_window,
            "declared_final_window_ids": sorted(declared_final_window_ids),
            "minimum_distinct_windows": 3,
            "minimum_distinct_utc_dates": 3,
            "minimum_elapsed_hours": 48,
            "retryable_rows": len(retryable_ids),
            "nonexact_rows_requiring_retry_protocol": len(finality_ids),
            "rows_observed_in_declared_final_window": finality_observed,
            "rows_whose_latest_observation_is_declared_final": finality_latest,
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
            "active_scope_rate": exact_active / len(active) if scope_complete and active else None,
            "archive_rate": exact_archive / EXPECTED_COUNTS["archive_pointer_rows"] if archive_population_complete else None,
            "active_rate": exact_active / EXPECTED_COUNTS["active_pointer_rows"] if active_population_complete else None,
            "completed_rows": completed_exact,
            "completed_rate": completed_exact / len(completed_health) if completed_health else None,
            "completed_active_rows": completed_active_exact,
            "completed_active_rate": completed_active_exact / len(completed_active) if completed_active else None,
        },
        "r0": {
            "frozen_byte_complete_rows": EXPECTED_COUNTS["r0_byte_complete_rows"],
            "pointer_rows": EXPECTED_COUNTS["r0_pointer_rows"],
            "pointer_rows_in_scope": len(s1),
            "pointer_byte_exact_rows_in_scope": s1_exact,
            "pointer_visual_near_candidate_rows_in_scope": s1_near,
            "pointer_byte_exact_rows": s1_exact if r0_population_complete else None,
            "pointer_visual_near_candidate_rows": s1_near if r0_population_complete else None,
            "strict_exact_byte_availability_rows": EXPECTED_COUNTS["r0_byte_complete_rows"] + s1_exact if r0_population_complete else None,
            "strict_exact_byte_availability_rate": (EXPECTED_COUNTS["r0_byte_complete_rows"] + s1_exact) / EXPECTED_COUNTS["r0_rows"] if r0_population_complete else None,
            "population_complete": r0_population_complete,
        },
        "active_staging": {
            "frozen_byte_complete_rows": EXPECTED_COUNTS["active_byte_complete_rows"],
            "pointer_rows": EXPECTED_COUNTS["active_pointer_rows"],
            "pointer_rows_in_scope": len(active),
            "pointer_byte_exact_rows_in_scope": exact_active,
            "pointer_byte_exact_rows": exact_active if active_population_complete else None,
            "strict_exact_byte_availability_rows": EXPECTED_COUNTS["active_byte_complete_rows"] + exact_active if active_population_complete else None,
            "strict_exact_byte_availability_rate": (EXPECTED_COUNTS["active_byte_complete_rows"] + exact_active) / EXPECTED_COUNTS["active_staging_rows"] if active_population_complete else None,
            "population_complete": active_population_complete,
        },
        "e0": {"byte_complete_rows": 6719, "pointer_rows": 0},
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


def scan_output_tree(audit_dir: Path, output: Path, checks: Checks) -> None:
    allowed = {
        "pointer-health-attempts.tsv", "pointer-health.tsv", "pointer-health-summary.json",
        "record-completions.tsv", "attempt-dispositions.tsv", "audit-contract.json",
        "robots.tsv", "tail-recoveries.tsv", "atomic-temp-recoveries.tsv",
        "independent-checks.tsv",
    }
    findings: Counter[str] = Counter()
    try:
        output_location_ok = (
            output.name == "independent-checks.tsv"
            and output.parent.resolve() == audit_dir.resolve()
        )
    except OSError:
        output_location_ok = False
    if not output_location_ok:
        findings["verifier_output_location"] += 1
    private_pattern = re.compile(
        rb"(?:^|(?<=[\s=:'\"(]))/(?:home|Users|mnt|srv)/"
        rb"[A-Za-z0-9._-]+(?:/|(?=$|[\s\"']))|"
        rb"(?:^|(?<=[\s=:'\"(]))[A-Za-z]:\\\\Users\\\\[^\\\\\r\n]+\\\\|"
        rb"(?:^|(?<=[\s=:'\"(]))\\\\\\\\[^\\\\\r\n]+\\\\[^\\\\\r\n]+\\\\",
        re.IGNORECASE,
    )
    secret_pattern = re.compile(
        rb"(?:authorization\s*:\s*(?:bearer|token)|"
        rb"(?:api[_-]?key|access[_-]?token)\s*[:=]\s*"
        rb"(?!REDACTED(?:[&\s\"']|$))[^&\s\"']+|"
        rb"(?:hf|ghp|github_pat)_[A-Za-z0-9_-]{16,})",
        re.IGNORECASE,
    )
    image_magic = (b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a", b"RIFF")
    for path in audit_dir.rglob("*"):
        relative = path.relative_to(audit_dir)
        if path.is_symlink():
            findings["symlink"] += 1
            continue
        if path.is_dir():
            findings["nested_directory"] += 1
            if path.name == "__pycache__":
                findings["pycache"] += 1
            continue
        if not path.is_file():
            findings["special"] += 1
            continue
        try:
            metadata = path.lstat()
        except OSError:
            findings["lstat"] += 1
            continue
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            findings["unsafe_regular_file"] += 1
            continue
        if path.name.endswith((".pyc", ".pyo")):
            findings["bytecode"] += 1
        if (
            relative.parent != Path(".")
            or (
                path.name not in allowed
                and not re.fullmatch(
                    r"run-metadata-[A-Za-z0-9][A-Za-z0-9._-]{0,63}-[0-9a-f]{16}\.json",
                    path.name,
                )
            )
        ):
            findings["unexpected_file"] += 1
        with path.open("rb") as source:
            first = source.read(16)
            if first.startswith(image_magic[:4]) or (first.startswith(b"RIFF") and first[8:12] == b"WEBP"):
                findings["image"] += 1
            previous = b""
            source.seek(0)
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                scan = previous + chunk
                private_hits = len(private_pattern.findall(scan))
                secret_hits = len(secret_pattern.findall(scan))
                if private_hits:
                    findings["private"] += private_hits
                if secret_hits:
                    findings["secret"] += secret_hits
                previous = scan[-256:]
    checks.add("output_tree_safe", not findings, dict(findings), {})


def preflight_receipt_paths(audit_dir: Path, output: Path, checks: Checks) -> bool:
    findings: Counter[str] = Counter()
    try:
        directory = audit_dir.lstat()
        if (
            not stat.S_ISDIR(directory.st_mode)
            or directory.st_uid != os.getuid()
            or audit_dir.is_symlink()
            or directory.st_mode & 0o022
        ):
            findings["audit_directory"] += 1
    except OSError:
        findings["audit_directory"] += 1
    expected_names = {
        "pointer-health-attempts.tsv", "pointer-health.tsv",
        "pointer-health-summary.json", "record-completions.tsv",
        "attempt-dispositions.tsv", "audit-contract.json", "robots.tsv",
        "tail-recoveries.tsv", "atomic-temp-recoveries.tsv",
        "independent-checks.tsv",
    }
    try:
        children = list(audit_dir.iterdir()) if not findings else []
    except OSError:
        findings["audit_directory_read"] += 1
        children = []
    for path in children:
        if path.name not in expected_names and not re.fullmatch(
            r"run-metadata-[A-Za-z0-9][A-Za-z0-9._-]{0,63}-[0-9a-f]{16}\.json",
            path.name,
        ):
            continue
        try:
            metadata = path.lstat()
        except OSError:
            findings["receipt_lstat"] += 1
            continue
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            findings["unsafe_receipt_file"] += 1
    try:
        same_parent = Path(os.path.abspath(output.parent)) == Path(os.path.abspath(audit_dir))
    except OSError:
        same_parent = False
    if output.name != "independent-checks.tsv" or not same_parent:
        findings["verifier_output_location"] += 1
    checks.add("receipt_paths_preflight", not findings, dict(findings), {})
    return not findings


def write_checks(path: Path, rows: list[dict[str, str]]) -> None:
    parent_metadata = path.parent.lstat()
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or path.parent.is_symlink()
        or parent_metadata.st_uid != os.getuid()
        or parent_metadata.st_mode & 0o022
    ):
        raise OSError("unsafe output directory")
    original: tuple[int, int] | None = None
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if (
            not stat.S_ISREG(existing.st_mode)
            or existing.st_uid != os.getuid()
            or existing.st_nlink != 1
            or existing.st_mode & 0o022
        ):
            raise OSError("unsafe existing output")
        original = (existing.st_dev, existing.st_ino)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".independent-checks-", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=("check_id", "status", "observed", "expected"), delimiter="\t", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            target.flush()
            os.fsync(target.fileno())
        try:
            current = path.lstat()
        except FileNotFoundError:
            current = None
        current_identity = (current.st_dev, current.st_ino) if current is not None else None
        if current_identity != original or (
            current is not None
            and (
                not stat.S_ISREG(current.st_mode)
                or current.st_uid != os.getuid()
                or current.st_nlink != 1
                or current.st_mode & 0o022
            )
        ):
            raise OSError("output changed during write")
        os.replace(temporary, path)
        directory_fd = os.open(
            path.parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def command_verify(args: argparse.Namespace) -> int:
    checks = Checks()
    summary: dict[str, Any] = {}
    preflight_ok = preflight_receipt_paths(args.audit_dir, args.output, checks)
    try:
        if not preflight_ok:
            raise ValueError("unsafe receipt paths")
        bindings = load_json(args.bindings)
        policy = load_json(args.policy)
        policy_sha = file_sha256(args.policy)
        frozen_order, frozen = load_frozen(args.source_root, bindings, checks)
        source_rows = load_scope(args.scope, args.manifest, frozen_order, frozen, checks)
        scope_ids = {row["record_id"] for row in source_rows}
        attempt_fields, attempts = read_tsv(args.audit_dir / "pointer-health-attempts.tsv")
        completion_path = args.audit_dir / "record-completions.tsv"
        disposition_path = args.audit_dir / "attempt-dispositions.tsv"
        if completion_path.is_file():
            completion_fields, completions = read_tsv(completion_path)
        else:
            completion_fields, completions = [], []
        if disposition_path.is_file():
            disposition_fields, dispositions = read_tsv(disposition_path)
        else:
            disposition_fields, dispositions = [], []
        health_fields, health = read_tsv(args.audit_dir / "pointer-health.tsv")
        summary = load_json(args.audit_dir / "pointer-health-summary.json")
        checks.add("attempt_schema", tuple(attempt_fields) == ATTEMPT_FIELDS, attempt_fields, ATTEMPT_FIELDS)
        checks.add("completion_schema", tuple(completion_fields) == COMPLETION_FIELDS, completion_fields, COMPLETION_FIELDS)
        checks.add("disposition_schema", not disposition_fields or tuple(disposition_fields) == DISPOSITION_FIELDS, disposition_fields, f"optional {DISPOSITION_FIELDS}")
        checks.add("health_schema", tuple(health_fields) == HEALTH_FIELDS, health_fields, HEALTH_FIELDS)
        user_agent = verify_policy(policy, checks, policy_sha)
        verify_attempts(attempts, scope_ids, frozen, policy, checks)
        committed_attempts = verify_transactions(
            attempts, completions, dispositions, scope_ids, frozen, checks
        )
        tail_recoveries = verify_tail_recoveries(args.audit_dir, checks)
        atomic_temp_recoveries = verify_atomic_temp_recoveries(
            args.audit_dir, checks
        )
        bindings_sha = file_sha256(args.bindings)
        manifest_sha = file_sha256(args.manifest) if args.manifest is not None else None
        contract, contract_sha = verify_contract(
            args.audit_dir, source_rows, args.scope, policy, bindings_sha, policy_sha,
            manifest_sha, user_agent, checks,
        )
        final_window, final_window_ids, metadata = verify_run_metadata(
            args.audit_dir, attempts, completions, dispositions, args.scope,
            manifest_sha, policy_sha, bindings_sha, contract, contract_sha,
            user_agent, checks,
        )
        robots = verify_robots(
            args.audit_dir, attempts, metadata, policy, checks
        )
        rebuilt_health = rebuild_health(
            source_rows, committed_attempts, completions, final_window_ids
        )
        compare_health(health, rebuilt_health, checks)
        rebuilt_summary = rebuild_summary(
            rebuilt_health, attempts, committed_attempts, completions,
            dispositions, robots, args.scope, final_window, contract,
            contract_sha, final_window_ids, tail_recoveries,
            atomic_temp_recoveries, metadata,
        )
        checks.add("summary_full_rebuild", summary == rebuilt_summary, summary, rebuilt_summary)
        scan_output_tree(args.audit_dir, args.output, checks)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, csv.Error) as exc:
        checks.add("verifier_runtime", False, type(exc).__name__, "no exception")
    try:
        write_checks(args.output, checks.rows)
    except OSError:
        emit({"schema": SCHEMA, "tool_version": TOOL_VERSION, "status": "ERROR", "error": "CHECK_OUTPUT_WRITE_FAILED", "network_accessed": False})
        return EXIT_INTEGRITY
    result = {
        "schema": SCHEMA,
        "tool_version": TOOL_VERSION,
        "status": "PASS" if checks.passed else "FAIL",
        "checks": len(checks.rows),
        "passed": sum(row["status"] == "PASS" for row in checks.rows),
        "failed": sum(row["status"] == "FAIL" for row in checks.rows),
        "health_status": summary.get("status", "UNKNOWN"),
        "network_accessed": False,
    }
    emit(result)
    return EXIT_OK if checks.passed else EXIT_INTEGRITY


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Independently verify a COVER-Fish pointer-health receipt.")
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--bindings", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--scope", choices=("pilot", "archive"), required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.set_defaults(handler=command_verify)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except KeyboardInterrupt:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INTERRUPTED", "network_accessed": False})
        return 130
    except Exception:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "UNEXPECTED_RUNTIME_ERROR", "network_accessed": False})
        return EXIT_INTEGRITY


if __name__ == "__main__":
    raise SystemExit(main())
