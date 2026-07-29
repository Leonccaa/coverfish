#!/usr/bin/env python3
"""Plan, download, and verify the historical immutable COVER-Fish base release.

Operational commands emit exactly one JSON document on stdout. Progress is sent
to stderr. The fixed manifest below is independent of the moving Hub default
branch and binds every public release file to its byte size and SHA-256 digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from collections.abc import Iterable, Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

SCHEMA = "coverfish.download.v1"
TOOL_VERSION = "1.0.0"
DATASET_REPO = "COVER-Fish/COVER-Fish"
DATASET_REVISION = "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8"
DATASET_TAG = "rev023-rc2-20260714"
DATASET_DOI = "10.57967/hf/9706"
LARGE_DOWNLOAD_BYTES = 5_000_000_000

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_DEPENDENCY = 3
EXIT_SAFETY = 4
EXIT_TRANSFER = 5
EXIT_INTEGRITY = 6


@dataclass(frozen=True)
class FileSpec:
    filename: str
    component: str
    bytes: int
    sha256: str


FILES: tuple[FileSpec, ...] = (
    FileSpec("UPLOAD-MANIFEST.tsv", "CONTROL", 4116, "631947358df25dec626e8494f37a89e0b55162804601be97e7446df65d125a25"),
    FileSpec("verify_s4_reassembly.sh", "CONTROL", 982, "cd15765c3fc36b492a3b40d5464c2d7d6dd33d371c120faa8e2d8c749c9ad357"),
    FileSpec("ARCHIVES-RC2.tsv", "CONTROL", 1445, "0a5f9b6c87e8e23212917d1e9fc32746e63142d011336b05b493850d1b6fa2db"),
    FileSpec("MD5SUMS", "CONTROL", 1761, "f6c80c91377d5ef1b298062f1a599ea81d46888e0714166242463fba2f0a380d"),
    FileSpec("SHA256SUMS", "CONTROL", 2433, "6c02901707f85b83bb0e31a098878fe07c89394f879c8cc771b40d433a7aa975"),
    FileSpec("S4-PARTS.tsv", "CONTROL", 2833, "1f4e7b7b8586ebdb98b483287e12ed2777977371af4aa32c27541eee17225003"),
    FileSpec("DATA-FILES.tsv", "CONTROL", 3627, "491b476f7ba161c9cfa1bfba5394eef242a1fdd62b1ad83318105d3d1bcccdac"),
    FileSpec("README.md", "CONTROL", 5528, "a9915ec4cf01db9cb979026000fd837311702e240381495878b0a403849bba26"),
    FileSpec("coverfish-rev023-S3-angfa-pointers-rc2-20260714.tar.zst", "S3", 57951, "bdacf7110740426db65a290e90a12027cc89c6fa221ac8fae9245a2b3cf9b3d0"),
    FileSpec("coverfish-rev023-D0-qint-development-rc2-20260714.tar.zst", "D0", 403653155, "2fff4709d70e5bcf2ea993b41004b3c688d1d4b20486672709d00c9dc36bfc51"),
    FileSpec("coverfish-rev023-core-metadata-index-code-rc2-20260714.tar.zst", "CORE", 499593954, "3bf4295ae3bbc8d853095575be9f0f87dea73d31160e1a168ff164595fbd6554"),
    FileSpec("coverfish-rev023-S2-usfws-rc2-20260714.tar.zst", "S2", 548073297, "5f34c0d1b874ec6c51dbc5639336a6da7278102427976d59ce2512b5e22b9c92"),
    FileSpec("coverfish-rev023-S1-fishbase-r0-rc2-20260714.tar.zst", "S1", 1036917323, "8212d8efbd48f46f83d22fecdc4b96e53eb602e6ea7602f97283e0a436330d59"),
    FileSpec("coverfish-rev023-E0-qt26-qc-rc2-20260714.tar.zst", "E0", 3683785882, "1574f1f121f31f2a5b213b42a1ef40cdc5f0a708c8aa466d661cbf55f37152ff"),
    FileSpec("coverfish-rev023-S0-inat-rg-archive-rc2-20260714.tar.zst", "S0", 10443428913, "12d64dfd2330388c1810df095d8c1f14f5d2e6b62f392e49bed6abf24bad76b6"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-009-of-009", "S4", 2637933197, "e8b8dd2712c79c9ce6a42dda0c06242803d7db5f4cd48d54cd6f6810dc67ca94"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-001-of-009", "S4", 8000000000, "a787c00a48fc62ecf45a93632cc72782a1f1b1bab4f71d70c1be8e2ec58c2e20"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-002-of-009", "S4", 8000000000, "a1b7cbe8f340f0bd5ada579cafd5fc63d7c91ebf65884b8455ce8c4a2bd08efb"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-003-of-009", "S4", 8000000000, "924d9db59cbd51839a33d285ffd5acfa54bfa79e0cddd75a3e7b37179badec76"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-004-of-009", "S4", 8000000000, "a2a27578cba03f4ed452c0fb1c62eb010795ecf2bec6b3b02a044aa9663ea274"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-005-of-009", "S4", 8000000000, "ad0095029a531bdcd467154b8dea49afe0831d63533b1765bd2ea6cbed736f82"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-006-of-009", "S4", 8000000000, "93d2709329a05e6ddef5576e859516349c78a01afd530226a0a9995245cf6d61"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-007-of-009", "S4", 8000000000, "15e1d12d19bd1064aa5394f3bdeaff6f4bcf4f23973a48b363cb279d1ca8f62d"),
    FileSpec("coverfish-rev023-S4-commons-rc2-20260714.tar.zst.part-008-of-009", "S4", 8000000000, "d4de99251848b2968622fc6042ccc548b259d675e40497ca2175443db0034579"),
)

PROFILE_COMPONENTS: dict[str, frozenset[str]] = {
    "control": frozenset({"CONTROL"}),
    "core": frozenset({"CONTROL", "CORE"}),
    "smoke": frozenset({"CONTROL", "CORE", "D0"}),
    "d0": frozenset({"CONTROL", "D0"}),
    "e0": frozenset({"CONTROL", "E0"}),
    "s0": frozenset({"CONTROL", "S0"}),
    "s1": frozenset({"CONTROL", "S1"}),
    "s2": frozenset({"CONTROL", "S2"}),
    "s3": frozenset({"CONTROL", "S3"}),
    "s4": frozenset({"CONTROL", "S4"}),
    "all": frozenset({"CONTROL", "CORE", "D0", "E0", "S0", "S1", "S2", "S3", "S4"}),
}


class JsonArgumentParser(argparse.ArgumentParser):
    """Emit a stable machine-readable error for invalid invocations."""

    def error(self, message: str) -> NoReturn:
        del message
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INVALID_ARGUMENTS"})
        raise SystemExit(EXIT_USAGE)


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def release_identity() -> dict[str, str]:
    return {
        "dataset_repo": DATASET_REPO,
        "doi": DATASET_DOI,
        "revision": DATASET_REVISION,
        "tag": DATASET_TAG,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def specs_for_profile(profile: str) -> tuple[FileSpec, ...]:
    components = PROFILE_COMPONENTS[profile]
    return tuple(spec for spec in FILES if spec.component in components)


def nearest_existing_path(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def capacity_status(output: Path, transfer_bytes: int) -> tuple[str, int]:
    if transfer_bytes <= 0:
        return "PASS", 0
    margin = max(64 * 1024 * 1024, transfer_bytes // 20)
    required = transfer_bytes + margin
    try:
        available = shutil.disk_usage(nearest_existing_path(output)).free
    except OSError:
        return "UNKNOWN", required
    return ("PASS" if available >= required else "FAIL"), required


def inspect_for_plan(output: Path, specs: Iterable[FileSpec]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in specs:
        path = output / spec.filename
        if not path.is_file():
            state = "missing"
        elif path.stat().st_size == spec.bytes:
            state = "present_unverified"
        else:
            state = "size_mismatch"
        rows.append(
            {
                "bytes": spec.bytes,
                "component": spec.component,
                "filename": spec.filename,
                "sha256": spec.sha256,
                "state": state,
            }
        )
    return rows


def verify_specs(output: Path, specs: Sequence[FileSpec], progress: bool) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    rows: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    for spec in specs:
        if progress:
            print(f"verify {spec.filename}", file=sys.stderr, flush=True)
        path = output / spec.filename
        if not path.is_file():
            state = "missing"
        elif path.stat().st_size != spec.bytes:
            state = "size_mismatch"
        elif file_sha256(path) != spec.sha256:
            state = "sha256_mismatch"
        else:
            state = "verified"
        rows.append(
            {
                "bytes": spec.bytes,
                "component": spec.component,
                "filename": spec.filename,
                "state": state,
            }
        )
        if state != "verified":
            failures.append({"filename": spec.filename, "reason": state})
    return rows, failures


def base_result(command: str, profile: str, specs: Sequence[FileSpec]) -> dict[str, object]:
    expected_bytes = sum(spec.bytes for spec in specs)
    return {
        "command": command,
        "expected_bytes": expected_bytes,
        "file_count": len(specs),
        "large_download_confirmation_required": expected_bytes >= LARGE_DOWNLOAD_BYTES,
        "profile": profile,
        "release": release_identity(),
        "schema": SCHEMA,
        "tool_version": TOOL_VERSION,
    }


def command_plan(args: argparse.Namespace) -> int:
    specs = specs_for_profile(args.profile)
    files = inspect_for_plan(args.output, specs)
    transfer_bytes = sum(
        spec.bytes
        for spec, row in zip(specs, files)
        if row["state"] in {"missing", "size_mismatch"}
    )
    capacity, required = capacity_status(args.output, transfer_bytes)
    result = base_result("plan", args.profile, specs)
    result.update(
        {
            "capacity_check": capacity,
            "capacity_scope": "known missing or size-mismatched files; use verify for SHA-256",
            "files": files,
            "known_transfer_bytes": transfer_bytes,
            "required_capacity_bytes": required,
            "status": "PASS" if capacity != "FAIL" else "FAIL",
        }
    )
    emit(result)
    return EXIT_OK if capacity != "FAIL" else EXIT_SAFETY


def command_verify(args: argparse.Namespace) -> int:
    specs = specs_for_profile(args.profile)
    files, failures = verify_specs(args.output, specs, progress=True)
    result = base_result("verify", args.profile, specs)
    result.update(
        {
            "failures": failures,
            "files": files,
            "status": "PASS" if not failures else "FAIL",
            "verified_files": len(specs) - len(failures),
        }
    )
    emit(result)
    return EXIT_OK if not failures else EXIT_INTEGRITY


def command_download(args: argparse.Namespace) -> int:
    specs = specs_for_profile(args.profile)
    result = base_result("download", args.profile, specs)
    if result["large_download_confirmation_required"] and not args.accept_large_download:
        result.update({"status": "PENDING", "error": "LARGE_DOWNLOAD_CONFIRMATION_REQUIRED"})
        emit(result)
        return EXIT_SAFETY

    existing, _ = verify_specs(args.output, specs, progress=False)
    states = {row["filename"]: row["state"] for row in existing}
    transfer_specs = [spec for spec in specs if states[spec.filename] != "verified"]
    transfer_bytes = sum(spec.bytes for spec in transfer_specs)
    capacity, required = capacity_status(args.output, transfer_bytes)
    if capacity == "FAIL":
        result.update(
            {
                "status": "FAIL",
                "error": "INSUFFICIENT_CAPACITY",
                "required_capacity_bytes": required,
                "transfer_bytes": transfer_bytes,
            }
        )
        emit(result)
        return EXIT_SAFETY

    if transfer_specs:
        try:
            from huggingface_hub import hf_hub_download
        except ImportError:
            result.update({"status": "ERROR", "error": "MISSING_HUGGINGFACE_HUB"})
            emit(result)
            return EXIT_DEPENDENCY

        args.output.mkdir(parents=True, exist_ok=True)
        try:
            for spec in transfer_specs:
                print(f"download {spec.filename}", file=sys.stderr, flush=True)
                with redirect_stdout(sys.stderr):
                    hf_hub_download(
                        repo_id=DATASET_REPO,
                        repo_type="dataset",
                        revision=DATASET_REVISION,
                        filename=spec.filename,
                        local_dir=str(args.output),
                        force_download=(states[spec.filename] != "missing"),
                        token=False,
                    )
        # Collapse transport-library details so stdout cannot disclose local state.
        except Exception:  # noqa: BLE001
            result.update({"status": "ERROR", "error": "DOWNLOAD_FAILED"})
            emit(result)
            return EXIT_TRANSFER

    if transfer_specs:
        transferred_rows, failures = verify_specs(
            args.output, transfer_specs, progress=True
        )
        transferred_by_name = {row["filename"]: row for row in transferred_rows}
        existing_by_name = {row["filename"]: row for row in existing}
        files = [
            transferred_by_name.get(spec.filename, existing_by_name[spec.filename])
            for spec in specs
        ]
    else:
        files = existing
        failures = []
    result.update(
        {
            "failures": failures,
            "files": files,
            "status": "PASS" if not failures else "FAIL",
            "transferred_files": len(transfer_specs),
            "verified_files": len(specs) - len(failures),
        }
    )
    emit(result)
    return EXIT_OK if not failures else EXIT_INTEGRITY


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(
        description="Plan, download, or verify the immutable COVER-Fish base release.",
    )
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--profile",
            choices=tuple(PROFILE_COMPONENTS),
            default="control",
            help="Release subset; component profiles always include control files.",
        )
        subparser.add_argument(
            "--output",
            type=Path,
            default=Path("coverfish-release"),
            help="Local destination directory (default: coverfish-release).",
        )

    plan = subparsers.add_parser("plan", help="Report exact files and sizes without network access.")
    add_common(plan)
    plan.set_defaults(handler=command_plan)

    download = subparsers.add_parser("download", help="Download missing files and verify SHA-256.")
    add_common(download)
    download.add_argument(
        "--accept-large-download",
        action="store_true",
        help="Explicitly allow a selected profile of 5 GB or more.",
    )
    download.set_defaults(handler=command_download)

    verify = subparsers.add_parser("verify", help="Verify local sizes and SHA-256 without network access.")
    add_common(verify)
    verify.set_defaults(handler=command_verify)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except KeyboardInterrupt:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INTERRUPTED"})
        return 130
    # The CLI boundary deliberately emits a non-sensitive stable error code.
    except Exception:  # noqa: BLE001
        emit({"schema": SCHEMA, "status": "ERROR", "error": "UNEXPECTED_RUNTIME_ERROR"})
        return EXIT_TRANSFER


if __name__ == "__main__":
    raise SystemExit(main())
