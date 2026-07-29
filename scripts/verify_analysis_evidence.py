#!/usr/bin/env python3
"""Verify the compact REV035 COVER-Fish analysis-evidence supplement."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path, PurePosixPath
from typing import NoReturn, Sequence

TOOL_VERSION = "1.0.0"
SCHEMA = "coverfish.analysis-evidence.verify.v1"
EXPECTED_ALIGNMENT = "REV035"
EXPECTED_MODULES = 13
EXPECTED_ARTIFACTS = 169
EXPECTED_CLAIMS = 14
EXPECTED_ARCHIVE_SHA256 = (
    "a83cea63de116c6b895551401f55a97af9b38bcc750006063a217aae44022a01"
)
EXPECTED_FILES_LEDGER_SHA256 = (
    "77ab909068dbbcd0b01ae268e877a18b30833a2f71e107693195502f9d276d00"
)
REQUIRED_OUTER_FILES = (
    "README.md",
    "RELEASE.json",
    "CLAIM-ARTIFACT-LEDGER.tsv",
    "analysis-evidence.tar.zst",
    "FILES.tsv",
    "SHA256SUMS",
)

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_DEPENDENCY = 3
EXIT_RUNTIME = 5
EXIT_INTEGRITY = 6


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INVALID_ARGUMENTS"})
        raise SystemExit(EXIT_USAGE)


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_checksum_ledger(path: Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        parts = raw.split(maxsplit=1)
        if len(parts) != 2 or len(parts[0]) != 64:
            raise ValueError("invalid checksum row")
        name = parts[1].lstrip(" *")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or len(relative.parts) != 1:
            raise ValueError("unsafe checksum path")
        if name in rows:
            raise ValueError("duplicate checksum path")
        rows[name] = parts[0]
    return rows


def safe_archive_member(name: str) -> bool:
    path = PurePosixPath(name)
    return (
        not path.is_absolute()
        and ".." not in path.parts
        and bool(path.parts)
        and path.parts[0] == "analysis-evidence"
    )


def list_archive(archive: Path) -> list[str]:
    completed = subprocess.run(
        ["tar", "--zstd", "-tf", str(archive)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError("archive listing failed")
    members = [line for line in completed.stdout.splitlines() if line]
    if not members or any(not safe_archive_member(name) for name in members):
        raise ValueError("unsafe or empty archive topology")
    return members


def extract_archive(archive: Path, destination: Path) -> Path:
    completed = subprocess.run(
        ["tar", "--zstd", "-xf", str(archive), "-C", str(destination)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError("archive extraction failed")
    root = destination / "analysis-evidence"
    if not root.is_dir():
        raise ValueError("archive root missing")
    return root


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source, delimiter="\t"))


def verify_inner(root: Path) -> dict[str, int]:
    release = json.loads((root / "RELEASE.json").read_text(encoding="utf-8"))
    if release.get("manuscript_alignment") != EXPECTED_ALIGNMENT:
        raise ValueError("manuscript alignment mismatch")
    if release.get("module_count") != EXPECTED_MODULES:
        raise ValueError("module count mismatch")
    if release.get("artifact_count") != EXPECTED_ARTIFACTS:
        raise ValueError("artifact count mismatch")
    if release.get("scientific_object_mutated") is not False:
        raise ValueError("scientific mutation flag mismatch")
    if release.get("new_model_inference_or_image_encoding") is not False:
        raise ValueError("inference flag mismatch")

    inventory = read_tsv(root / "ARTIFACT-INVENTORY.tsv")
    if len(inventory) != EXPECTED_ARTIFACTS:
        raise ValueError("artifact inventory length mismatch")
    seen: set[str] = set()
    modules: set[str] = set()
    for row in inventory:
        name = row.get("path", "")
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or name in seen:
            raise ValueError("unsafe or duplicate artifact path")
        path = root.joinpath(*relative.parts)
        if path.is_symlink() or not path.is_file():
            raise ValueError("artifact missing or unsafe")
        if path.stat().st_size != int(row.get("bytes", "-1")):
            raise ValueError("artifact size mismatch")
        if sha256(path) != row.get("sha256"):
            raise ValueError("artifact hash mismatch")
        seen.add(name)
        modules.add(row.get("module", ""))
    if len(modules) != EXPECTED_MODULES:
        raise ValueError("inventory module count mismatch")

    claims = read_tsv(root / "CLAIM-ARTIFACT-LEDGER.tsv")
    if len(claims) != EXPECTED_CLAIMS:
        raise ValueError("claim ledger length mismatch")
    claim_ids: set[str] = set()
    for row in claims:
        claim_id = row.get("claim_id", "")
        name = row.get("artifact_path", "")
        relative = PurePosixPath(name)
        if (
            not claim_id
            or claim_id in claim_ids
            or relative.is_absolute()
            or ".." in relative.parts
            or name not in seen
        ):
            raise ValueError("claim ledger mismatch")
        claim_ids.add(claim_id)

    return {"artifacts": len(inventory), "claims": len(claims), "modules": len(modules)}


def verify(root: Path) -> dict[str, object]:
    missing = [name for name in REQUIRED_OUTER_FILES if not (root / name).is_file()]
    if missing:
        raise ValueError("outer files missing")
    checksums = parse_checksum_ledger(root / "SHA256SUMS")
    expected_names = set(REQUIRED_OUTER_FILES) - {"FILES.tsv", "SHA256SUMS"}
    if set(checksums) != expected_names:
        raise ValueError("outer checksum file set mismatch")
    for name, expected in checksums.items():
        if sha256(root / name) != expected:
            raise ValueError("outer hash mismatch")
    files_ledger = read_tsv(root / "FILES.tsv")
    if {row.get("filename") for row in files_ledger} != expected_names:
        raise ValueError("outer file inventory set mismatch")
    for row in files_ledger:
        name = row.get("filename", "")
        if (
            int(row.get("bytes", "-1")) != (root / name).stat().st_size
            or row.get("sha256") != checksums[name]
        ):
            raise ValueError("outer file inventory mismatch")

    archive = root / "analysis-evidence.tar.zst"
    if sha256(archive) != EXPECTED_ARCHIVE_SHA256:
        raise ValueError("archive release hash mismatch")
    if sha256(root / "FILES.tsv") != EXPECTED_FILES_LEDGER_SHA256:
        raise ValueError("outer file inventory hash mismatch")
    members = list_archive(archive)
    with tempfile.TemporaryDirectory(prefix="coverfish-analysis-evidence-") as temporary:
        inner = extract_archive(archive, Path(temporary))
        counts = verify_inner(inner)
        if (inner / "CLAIM-ARTIFACT-LEDGER.tsv").read_bytes() != (
            root / "CLAIM-ARTIFACT-LEDGER.tsv"
        ).read_bytes():
            raise ValueError("claim ledger copies differ")
    return {**counts, "archive_members": len(members), "outer_files": len(REQUIRED_OUTER_FILES)}


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    parser.add_argument("--root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        counts = verify(args.root)
    except FileNotFoundError:
        emit({"schema": SCHEMA, "status": "FAIL", "error": "MISSING_FILE"})
        return EXIT_INTEGRITY
    except json.JSONDecodeError:
        emit({"schema": SCHEMA, "status": "FAIL", "error": "INVALID_JSON"})
        return EXIT_INTEGRITY
    except (ValueError, UnicodeDecodeError):
        emit({"schema": SCHEMA, "status": "FAIL", "error": "INTEGRITY_MISMATCH"})
        return EXIT_INTEGRITY
    except OSError:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "MISSING_DEPENDENCY"})
        return EXIT_DEPENDENCY
    except RuntimeError:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "ARCHIVE_RUNTIME"})
        return EXIT_RUNTIME
    emit({"schema": SCHEMA, "status": "PASS", "tool_version": TOOL_VERSION, **counts})
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
