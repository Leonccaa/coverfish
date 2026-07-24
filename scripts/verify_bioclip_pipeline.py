#!/usr/bin/env python3
"""Verify one public D0 image against the frozen COVER-Fish BioCLIP index.

``plan`` performs byte and dependency checks without importing ML frameworks or
using the network. ``run`` loads an immutable BioCLIP snapshot, re-encodes one
full-frame image, compares it with the frozen fp16 query row, and checks the
top-1 prototype. Operational commands emit one JSON document on stdout.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import shutil
import sys
from collections.abc import Sequence
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

SCHEMA = "coverfish.bioclip-smoke.v1"
TOOL_VERSION = "1.0.0"
DEFAULT_RECORD_ID = "coverfish-qint-v1.0-0721"

MODEL_REPO = "imageomics/bioclip-2.5-vith14"
MODEL_REVISION = "191d741545e4c741cdef4b22c6eb69c945c1e592"
MODEL_SNAPSHOT_MANIFEST_SHA256 = "260ab46d37906a0f90d2821936867f883f2e397ec901367b2d904dc1b612ba32"
MODEL_CONFIG_SHA256 = "4c131846348300c77b5bee06690b0e94ee85ca9ce9a30a853b36084d00dcd25c"

EXPECTED_QUERY_ROWS = 1044
EXPECTED_PROTOTYPE_ROWS = 19144
EXPECTED_EMBEDDING_DIM = 1024
DEFAULT_MIN_COSINE = 0.9999
DEFAULT_MAX_ABS = 0.005
DEFAULT_MAX_NORM_ERROR = 0.002

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_DEPENDENCY = 3
EXIT_SAFETY = 4
EXIT_RUNTIME = 5
EXIT_INTEGRITY = 6
EXIT_COMPARISON = 7


@dataclass(frozen=True)
class BoundFile:
    relative_path: str
    bytes: int
    sha256: str


CORE_FILES: tuple[BoundFile, ...] = (
    BoundFile("index/d0-query-features-fp16.npy", 2138240, "f1e520bb1264adc6069c63d818152ac42901f0d58803a6e3ad9400ddfc284676"),
    BoundFile("index/d0-query-rowmap.tsv", 164679, "894d01fa33cdc918e7661b92408435bd1f4e335d090d07831ab3e9aa6ade4891"),
    BoundFile("index/final-species-centroids-f64.npy", 156827776, "328a1835c78a5d87e65cbed3cde2b00c82f2247b2a40d05cad232a03f397364f"),
    BoundFile("index/species-prototype-map.tsv", 750956, "39bda6a4e607b8c9da08cc5d86fbbac851670e00ca6d6f258455759a3e411631"),
)
D0_MANIFEST = BoundFile("packs/D0/manifest.tsv", 715715, "49d3c5a1419db65de757096e84353cba82275a7a0be6d568027b56aa1249f6b0")

MODEL_FILES: tuple[BoundFile, ...] = (
    BoundFile("open_clip_config.json", 560, MODEL_CONFIG_SHA256),
    BoundFile("open_clip_model.safetensors", 3944517804, "ac2e37c2f89ef8e6b889176a9a3f418970ad9db15a218bd29e3321e95c46ae97"),
)

FROZEN_DEPENDENCIES: dict[str, str] = {
    "numpy": "2.2.6",
    "open_clip_torch": "3.3.0",
    "torch": "2.11.0+cu128",
    "torchvision": "0.26.0+cu128",
}
IMPORT_NAMES: dict[str, str] = {
    "numpy": "numpy",
    "open_clip_torch": "open_clip",
    "torch": "torch",
    "torchvision": "torchvision",
}


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INVALID_ARGUMENTS"})
        raise SystemExit(EXIT_USAGE)


class ValidationError(Exception):
    def __init__(self, code: str, exit_code: int = EXIT_INTEGRITY):
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


def emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_bound_file(root: Path, spec: BoundFile) -> dict[str, object]:
    path = root / spec.relative_path
    if not path.is_file():
        state = "missing"
    elif path.stat().st_size != spec.bytes:
        state = "size_mismatch"
    elif file_sha256(path) != spec.sha256:
        state = "sha256_mismatch"
    else:
        state = "verified"
    return {"bytes": spec.bytes, "file": spec.relative_path, "state": state}


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames is None:
            raise ValidationError("TSV_HEADER_MISSING")
        return list(reader)


def unique_row(rows: list[dict[str, str]], field: str, value: str, error_prefix: str) -> dict[str, str]:
    matches = [row for row in rows if row.get(field) == value]
    if len(matches) != 1:
        raise ValidationError(f"{error_prefix}_NOT_UNIQUE")
    return matches[0]


def safe_payload_path(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if not relative_path or relative.is_absolute() or ".." in relative.parts:
        raise ValidationError("UNSAFE_PAYLOAD_PATH")
    root_resolved = root.resolve()
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise ValidationError("UNSAFE_PAYLOAD_PATH") from exc
    return candidate


def dependency_report() -> dict[str, object]:
    installed: dict[str, str | None] = {}
    matches: dict[str, bool] = {}
    for distribution, expected in FROZEN_DEPENDENCIES.items():
        try:
            actual = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            actual = None
        installed[distribution] = actual
        matches[distribution] = actual == expected
    python_expected = "3.10.12"
    python_actual = ".".join(str(value) for value in sys.version_info[:3])
    matches["cpython"] = python_actual == python_expected
    return {
        "frozen_match": all(matches.values()),
        "installed": {**installed, "cpython": python_actual},
        "matches": matches,
        "required": {**FROZEN_DEPENDENCIES, "cpython": python_expected},
    }


def model_report(model_dir: Path) -> dict[str, Any]:
    files = [verify_bound_file(model_dir, spec) for spec in MODEL_FILES]
    missing_bytes = sum(
        spec.bytes for spec, row in zip(MODEL_FILES, files) if row["state"] != "verified"
    )
    return {
        "download_confirmation_required": missing_bytes > 0,
        "download_required_bytes": missing_bytes,
        "files": files,
        "ready": missing_bytes == 0,
        "repository": MODEL_REPO,
        "revision": MODEL_REVISION,
        "paper_snapshot_manifest_sha256": MODEL_SNAPSHOT_MANIFEST_SHA256,
    }


def audit_data(core_dir: Path, d0_dir: Path, record_id: str) -> dict[str, Any]:
    files = [verify_bound_file(core_dir, spec) for spec in CORE_FILES]
    manifest_file = verify_bound_file(d0_dir, D0_MANIFEST)
    files.append(manifest_file)
    failed = [row for row in files if row["state"] != "verified"]
    if failed:
        raise ValidationError("FROZEN_DATA_BINDING_FAILED")

    row_map = read_tsv(core_dir / "index/d0-query-rowmap.tsv")
    row = unique_row(row_map, "record_id", record_id, "ROW_MAP_RECORD")
    manifest = read_tsv(d0_dir / D0_MANIFEST.relative_path)
    manifest_row = unique_row(manifest, "public_id", record_id, "D0_MANIFEST_RECORD")
    if row.get("component_id") != "D0" or row.get("row_role") != "selected_target":
        raise ValidationError("ROW_MAP_ROLE_MISMATCH")
    if manifest_row.get("normalized_release_mode") != "bytes":
        raise ValidationError("RECORD_HAS_NO_PIXEL_PAYLOAD", EXIT_SAFETY)
    if row.get("sha256") != manifest_row.get("p5_payload_sha256"):
        raise ValidationError("ROW_MANIFEST_SHA_MISMATCH")

    try:
        tensor_row = int(row["tensor_row"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("INVALID_TENSOR_ROW") from exc
    if not 0 <= tensor_row < EXPECTED_QUERY_ROWS:
        raise ValidationError("TENSOR_ROW_OUT_OF_RANGE")

    image = safe_payload_path(d0_dir, manifest_row.get("p5_payload_rel_path", ""))
    if not image.is_file():
        raise ValidationError("PAYLOAD_IMAGE_MISSING")
    image_sha = file_sha256(image)
    if image_sha != row["sha256"]:
        raise ValidationError("PAYLOAD_IMAGE_SHA_MISMATCH")
    try:
        expected_bytes = int(manifest_row["p5_payload_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("INVALID_PAYLOAD_SIZE") from exc
    if image.stat().st_size != expected_bytes:
        raise ValidationError("PAYLOAD_IMAGE_SIZE_MISMATCH")

    return {
        "files": files + [{"bytes": expected_bytes, "file": "selected D0 payload", "state": "verified"}],
        "image": image,
        "image_sha256": image_sha,
        "record_id": record_id,
        "target_taxon": manifest_row.get("canonical_taxon_key", ""),
        "tensor_row": tensor_row,
    }


def public_data_report(audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "files": audit["files"],
        "image_sha256": audit["image_sha256"],
        "record_id": audit["record_id"],
        "status": "PASS",
        "target_taxon": audit["target_taxon"],
        "tensor_row": audit["tensor_row"],
    }


def base_result(command: str, record_id: str) -> dict[str, object]:
    return {
        "command": command,
        "record_id": record_id,
        "schema": SCHEMA,
        "tool_version": TOOL_VERSION,
        "validation_scope": "single-record encoder and prototype-ranking smoke test",
    }


def require_model_consent(report: dict[str, Any], accept_download: bool, offline: bool) -> None:
    if report["ready"]:
        return
    if offline:
        raise ValidationError("MODEL_NOT_AVAILABLE_OFFLINE", EXIT_SAFETY)
    if not accept_download:
        raise ValidationError("MODEL_DOWNLOAD_CONFIRMATION_REQUIRED", EXIT_SAFETY)


def download_model(
    model_dir: Path, report: dict[str, Any], accept_download: bool, offline: bool
) -> dict[str, Any]:
    require_model_consent(report, accept_download, offline)
    if report["ready"]:
        return report
    try:
        from huggingface_hub import hf_hub_download
    except ImportError as exc:
        raise ValidationError("MISSING_HUGGINGFACE_HUB", EXIT_DEPENDENCY) from exc

    transfer_bytes = int(report["download_required_bytes"])
    margin = max(64 * 1024 * 1024, transfer_bytes // 20)
    candidate = model_dir
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    try:
        enough_capacity = shutil.disk_usage(candidate).free >= transfer_bytes + margin
    except OSError:
        enough_capacity = True
    if not enough_capacity:
        raise ValidationError("INSUFFICIENT_MODEL_CAPACITY", EXIT_SAFETY)

    model_dir.mkdir(parents=True, exist_ok=True)
    states = {row["file"]: row["state"] for row in report["files"]}
    try:
        for spec in MODEL_FILES:
            if states[spec.relative_path] == "verified":
                continue
            print(f"download model file {spec.relative_path}", file=sys.stderr, flush=True)
            with redirect_stdout(sys.stderr):
                hf_hub_download(
                    repo_id=MODEL_REPO,
                    revision=MODEL_REVISION,
                    filename=spec.relative_path,
                    local_dir=str(model_dir),
                    force_download=(states[spec.relative_path] != "missing"),
                    token=False,
                )
    except Exception as exc:
        raise ValidationError("MODEL_DOWNLOAD_FAILED", EXIT_RUNTIME) from exc
    verified = model_report(model_dir)
    if not verified["ready"]:
        raise ValidationError("MODEL_INTEGRITY_FAILED")
    return verified


def require_runtime_dependencies() -> None:
    missing: list[str] = []
    for distribution, import_name in IMPORT_NAMES.items():
        try:
            importlib.metadata.version(distribution)
            with redirect_stdout(sys.stderr):
                __import__(import_name)
        # Binary/import mismatches can raise beyond ImportError.
        except Exception:  # noqa: BLE001
            missing.append(distribution)
    try:
        importlib.metadata.version("Pillow")
        with redirect_stdout(sys.stderr):
            __import__("PIL.Image")
    except Exception:  # noqa: BLE001 - Pillow binary/import mismatch
        missing.append("Pillow")
    if missing:
        raise ValidationError("MISSING_RUNTIME_DEPENDENCIES", EXIT_DEPENDENCY)


def select_device(torch: Any, requested: str) -> str:
    if requested == "cpu":
        return "cpu"
    if not torch.cuda.is_available():
        raise ValidationError("CUDA_NOT_AVAILABLE", EXIT_RUNTIME)
    return "cuda"


def normalized_numpy(vector: Any, np: Any) -> Any:
    array = np.asarray(vector, dtype=np.float64)
    norm = float(np.linalg.norm(array))
    if not np.isfinite(array).all() or not np.isfinite(norm) or norm <= 0:
        raise ValidationError("NONFINITE_OR_ZERO_FEATURE")
    return array / norm


def prototype_identity(rows: list[dict[str, str]], row_index: int) -> dict[str, object]:
    row = rows[row_index]
    try:
        declared = int(row["prototype_row"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("INVALID_PROTOTYPE_MAP") from exc
    if declared != row_index:
        raise ValidationError("NONCONTIGUOUS_PROTOTYPE_MAP")
    return {
        "canonical_taxon_key": row.get("canonical_taxon_key", ""),
        "prototype_row": row_index,
        "scientific_name": row.get("scientific_name", ""),
    }


def compare_embeddings(
    *,
    np: Any,
    current: Any,
    frozen: Any,
    centroids: Any,
    species: list[dict[str, str]],
    min_cosine: float,
    max_abs: float,
    max_norm_error: float,
) -> dict[str, object]:
    frozen_unit = normalized_numpy(frozen, np)
    current_unit = normalized_numpy(current, np)
    frozen_top1_row = int(np.argmax(centroids @ frozen_unit))
    current_top1_row = int(np.argmax(centroids @ current_unit))
    frozen_top1 = prototype_identity(species, frozen_top1_row)
    current_top1 = prototype_identity(species, current_top1_row)
    cosine = float(np.dot(current_unit, frozen_unit))
    observed_max_abs = float(np.max(np.abs(current - frozen)))
    norm_error = abs(float(np.linalg.norm(current.astype(np.float64))) - 1.0)
    checks = {
        "cosine": cosine >= min_cosine,
        "max_abs": observed_max_abs <= max_abs,
        "norm": norm_error <= max_norm_error,
        "top1": current_top1_row == frozen_top1_row,
    }
    return {
        "checks": checks,
        "frozen_top1": frozen_top1,
        "observed": {
            "cosine": cosine,
            "max_abs": observed_max_abs,
            "norm_error": norm_error,
        },
        "reencoded_top1": current_top1,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "thresholds": {
            "max_abs": max_abs,
            "max_norm_error": max_norm_error,
            "min_cosine": min_cosine,
        },
    }


def run_pipeline(
    *,
    core_dir: Path,
    model_dir: Path,
    audit: dict[str, Any],
    requested_device: str,
    min_cosine: float,
    max_abs: float,
    max_norm_error: float,
) -> dict[str, object]:
    with redirect_stdout(sys.stderr):
        import numpy as np
        import open_clip
        import torch
        from PIL import Image

    query_features = np.load(
        core_dir / "index/d0-query-features-fp16.npy", mmap_mode="r", allow_pickle=False
    )
    centroids = np.load(
        core_dir / "index/final-species-centroids-f64.npy", mmap_mode="r", allow_pickle=False
    )
    if query_features.shape != (EXPECTED_QUERY_ROWS, EXPECTED_EMBEDDING_DIM):
        raise ValidationError("QUERY_TENSOR_SHAPE_MISMATCH")
    if str(query_features.dtype) != "float16":
        raise ValidationError("QUERY_TENSOR_DTYPE_MISMATCH")
    if centroids.shape != (EXPECTED_PROTOTYPE_ROWS, EXPECTED_EMBEDDING_DIM):
        raise ValidationError("PROTOTYPE_TENSOR_SHAPE_MISMATCH")
    if str(centroids.dtype) != "float64":
        raise ValidationError("PROTOTYPE_TENSOR_DTYPE_MISMATCH")

    species = read_tsv(core_dir / "index/species-prototype-map.tsv")
    if len(species) != EXPECTED_PROTOTYPE_ROWS:
        raise ValidationError("PROTOTYPE_MAP_ROW_COUNT_MISMATCH")

    tensor_row = int(audit["tensor_row"])
    frozen = np.asarray(query_features[tensor_row], dtype=np.float32)

    device = select_device(torch, requested_device)
    precision = "fp16" if device == "cuda" else "fp32"
    try:
        with redirect_stdout(sys.stderr):
            model, _, preprocess = open_clip.create_model_and_transforms(
                f"local-dir:{model_dir}", device=device, precision=precision
            )
            model.eval()
            with Image.open(audit["image"]) as opened:
                image = opened.convert("RGB")
            try:
                conv1 = getattr(model.visual, "conv1", None)
                model_dtype = (
                    conv1.weight.dtype
                    if conv1 is not None
                    else next(model.visual.parameters()).dtype
                )
                batch = preprocess(image).unsqueeze(0).to(device=device, dtype=model_dtype)
                with torch.inference_mode():
                    encoded = model.encode_image(batch)
                    encoded = torch.nn.functional.normalize(encoded.float(), dim=-1)
                current = encoded.detach().cpu().to(torch.float16).float().numpy()[0]
            finally:
                image.close()
    except ValidationError:
        raise
    except Exception as exc:
        raise ValidationError("MODEL_EXECUTION_FAILED", EXIT_RUNTIME) from exc

    if current.shape != (EXPECTED_EMBEDDING_DIM,):
        raise ValidationError("ENCODED_FEATURE_SHAPE_MISMATCH")
    comparison = compare_embeddings(
        np=np,
        current=current,
        frozen=frozen,
        centroids=centroids,
        species=species,
        min_cosine=min_cosine,
        max_abs=max_abs,
        max_norm_error=max_norm_error,
    )
    comparison["device_category"] = device
    return comparison


def command_plan(args: argparse.Namespace) -> int:
    result = base_result("plan", args.record_id)
    try:
        audit = audit_data(args.core_dir, args.d0_dir, args.record_id)
    except ValidationError as exc:
        result.update({"status": "FAIL", "error": exc.code})
        emit(result)
        return exc.exit_code
    dependencies = dependency_report()
    model = model_report(args.model_dir)
    result.update(
        {
            "data": public_data_report(audit),
            "dependencies": dependencies,
            "model": model,
            "status": "PASS" if model["ready"] else "PENDING",
        }
    )
    emit(result)
    return EXIT_OK


def command_run(args: argparse.Namespace) -> int:
    result = base_result("run", args.record_id)
    try:
        audit = audit_data(args.core_dir, args.d0_dir, args.record_id)
        dependencies = dependency_report()
        if args.require_frozen_environment and not dependencies["frozen_match"]:
            raise ValidationError("FROZEN_ENVIRONMENT_REQUIRED", EXIT_DEPENDENCY)
        model_preflight = model_report(args.model_dir)
        require_model_consent(model_preflight, args.accept_model_download, args.offline)
        require_runtime_dependencies()
        model = download_model(
            args.model_dir, model_preflight, args.accept_model_download, args.offline
        )
        pipeline = run_pipeline(
            core_dir=args.core_dir,
            model_dir=args.model_dir,
            audit=audit,
            requested_device=args.device,
            min_cosine=args.min_cosine,
            max_abs=args.max_abs,
            max_norm_error=args.max_norm_error,
        )
    except ValidationError as exc:
        result.update({"status": "PENDING" if exc.exit_code == EXIT_SAFETY else "FAIL", "error": exc.code})
        emit(result)
        return exc.exit_code
    result.update(
        {
            "data": public_data_report(audit),
            "dependencies": dependencies,
            "model": model,
            "pipeline": pipeline,
            "status": pipeline["status"],
        }
    )
    emit(result)
    return EXIT_OK if pipeline["status"] == "PASS" else EXIT_COMPARISON


def positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be numeric") from exc
    if not 0 < number <= 1:
        raise argparse.ArgumentTypeError("must be greater than zero and at most one")
    return number


def add_common(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument("--core-dir", type=Path, required=True, help="Extracted CORE archive directory.")
    subparser.add_argument("--d0-dir", type=Path, required=True, help="Extracted D0 archive directory.")
    subparser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("coverfish-model"),
        help="Pinned model files directory (default: coverfish-model).",
    )
    subparser.add_argument(
        "--record-id", default=DEFAULT_RECORD_ID, help=f"Byte-bearing D0 record (default: {DEFAULT_RECORD_ID})."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = JsonArgumentParser(description="Verify a COVER-Fish BioCLIP encoding and ranking anchor.")
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Audit inputs and report model/dependency readiness without network access.")
    add_common(plan)
    plan.set_defaults(handler=command_plan)

    run = subparsers.add_parser("run", help="Re-encode one D0 image and compare with the frozen index.")
    add_common(run)
    run.add_argument(
        "--device",
        choices=("cpu", "cuda"),
        default="cpu",
        help="Execution category (default: cpu; CUDA is never selected implicitly).",
    )
    run.add_argument("--offline", action="store_true", help="Forbid model downloads.")
    run.add_argument(
        "--accept-model-download",
        action="store_true",
        help="Allow download of the pinned model files when absent (about 3.95 GB).",
    )
    run.add_argument("--require-frozen-environment", action="store_true")
    run.add_argument("--min-cosine", type=positive_float, default=DEFAULT_MIN_COSINE)
    run.add_argument("--max-abs", type=positive_float, default=DEFAULT_MAX_ABS)
    run.add_argument("--max-norm-error", type=positive_float, default=DEFAULT_MAX_NORM_ERROR)
    run.set_defaults(handler=command_run)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if getattr(args, "offline", False) and getattr(args, "accept_model_download", False):
        emit({"schema": SCHEMA, "status": "ERROR", "error": "CONFLICTING_DOWNLOAD_OPTIONS"})
        return EXIT_USAGE
    try:
        return int(args.handler(args))
    except KeyboardInterrupt:
        emit({"schema": SCHEMA, "status": "ERROR", "error": "INTERRUPTED"})
        return 130
    # The CLI boundary deliberately emits a non-sensitive stable error code.
    except Exception:  # noqa: BLE001
        emit({"schema": SCHEMA, "status": "ERROR", "error": "UNEXPECTED_RUNTIME_ERROR"})
        return EXIT_RUNTIME


if __name__ == "__main__":
    raise SystemExit(main())
