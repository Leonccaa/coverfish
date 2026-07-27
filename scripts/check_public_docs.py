#!/usr/bin/env python3
"""Check the COVER-Fish public GitHub baseline without external dependencies."""

from __future__ import annotations

import ast
import hashlib
import json
import re
import runpy
import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = (
    "README.md",
    "CITATION.md",
    "VERSION.md",
    "LICENSE.md",
    "LICENSES/Apache-2.0.txt",
    "LICENSES/CC-BY-4.0.txt",
    "docs/SCIENTIFIC_SCOPE.md",
    "docs/BENCHMARKS.md",
    "docs/DATA_AVAILABILITY.md",
    "docs/REPRODUCTION.md",
    "docs/POINTER_AUDIT.md",
    "docs/POINTER_AUDIT_PROTOCOL.md",
    "docs/POINTER_SOURCE_POLICY.md",
    "docs/RIGHTS_AND_LICENSING.md",
    "AGENTS.md",
    ".gitignore",
    "requirements-model.txt",
    "requirements-pointer-audit.txt",
    "requirements-pointer-audit-parallel-v04.txt",
    "config/pointer-audit-public-surface-v04.json",
    "config/host-policy-fishbase-2s-v2.json",
    "inputs/input-bindings.json",
    "schema/SCHEMA.md",
    "schema/pointer-audit-schema.json",
    "scripts/download_release.py",
    "scripts/verify_bioclip_pipeline.py",
    "software/reconstruct_pointers.py",
    "software/reconstruct_pointers_parallel_v04.py",
    "software/verify_pointer_receipt.py",
    "software/verify_pointer_receipt_parallel_v04.py",
    "software/verify_pointer_receipt_pair_v04.py",
    "software/verify_pointer_audit_minimal_fixture_v04.py",
    "tests/test_download_release.py",
    "tests/test_verify_bioclip_pipeline.py",
    "tests/test_pointer_fixture.py",
    "tests/test_pointer_tools.py",
    "tests/test_pointer_public_surface.py",
)

REQUIRED_README_TEXT = (
    "49,140",
    "30,796",
    "107,722",
    "19,144",
    "6,719",
    "+1.35",
    "+1.72",
    "-3.52",
    "117,640",
    "75,253",
    "42,387",
    "115,780",
    "10.57967/hf/9706",
    "Hugging Face",
    "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8",
    "rev023-rc2-20260714",
    "Liang Li",
    "Apache-2.0",
    "CC BY 4.0",
    "download_release.py",
    "verify_bioclip_pipeline.py",
    "docs/POINTER_AUDIT.md",
    "4--5 days",
    "2--3 days",
)

STALE_OR_UNSUPPORTED_TEXT = (
    "116,082-image / 18,928-species",
    "6,734 QC-curated queries / 3,126 species",
    "Q-INT hidden test",
    "Hidden contamination-protected benchmark",
    "species-level max pooling",
    "python scripts/download_index.py",
    "python scripts/query.py",
    "A preprint and DOI are in preparation. For now, please cite",
    "author       = {",
    "COVER-Fish-Val | Public seed",
    "selected publication design is Zenodo",
    "Zenodo will hold the complete immutable data archive",
    "No project-authored repository licence has yet been approved",
    "UPLOADED / PRIVATE / NOT PUBLIC / NOT DOI",
    "The Hub repository remains private",
    "DOI generation remains pending",
    "A DOI will be added",
    "Local review branch",
)

PRIVATE_PATTERNS = (
    re.compile(r"/(?:home|Users|mnt|srv)/[A-Za-z0-9._-]+(?:/|\b)"),
    re.compile(r"\b[A-Z]:\\Users\\[A-Za-z0-9._-]+\\", re.IGNORECASE),
    re.compile(r"\b[a-z0-9][a-z0-9.-]*\.lan\b", re.IGNORECASE),
    re.compile(
        r"\b(?:10(?:\.\d{1,3}){3}|172\.(?:1[6-9]|2\d|3[01])"
        r"(?:\.\d{1,3}){2}|192\.168(?:\.\d{1,3}){2})\b"
    ),
    re.compile(r"\bhf_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(
        r"(?:api[_-]?key|access[_-]?token|private[_-]?key)\s*[:=]\s*"
        r"['\"]?[A-Za-z0-9._~-]{20,}",
        re.IGNORECASE,
    ),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"https?://[^\s/:@]+:[^\s/@]+@", re.IGNORECASE),
)

MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")

FORBIDDEN_IDENTITY_CALLS = (
    "socket.gethostname",
    "platform.node",
    "getpass.getuser",
    "os.getlogin",
    "os.uname",
    "platform.uname",
    "pwd.getpwuid",
    "torch.cuda.get_device_name",
)

PINNED_ENVIRONMENT_READS = {
    "software/reconstruct_pointers.py": {"XDG_RUNTIME_DIR"},
    "software/verify_pointer_receipt_pair_v04.py": {"*copy*"},
    "tests/test_pointer_tools.py": {"COVERFISH_RC2_ROOT"},
}

POINTER_SURFACE_PATH = "config/pointer-audit-public-surface-v04.json"
POINTER_SURFACE_SCHEMA = "coverfish.pointer-audit-public-surface.v1"
POINTER_SURFACE_PATHS = frozenset(
    {
        "config/host-policy-fishbase-2s-v2.json",
        "inputs/input-bindings.json",
        "requirements-pointer-audit.txt",
        "requirements-pointer-audit-parallel-v04.txt",
        "schema/SCHEMA.md",
        "schema/pointer-audit-schema.json",
        "software/reconstruct_pointers.py",
        "software/reconstruct_pointers_parallel_v04.py",
        "software/verify_pointer_receipt.py",
        "software/verify_pointer_receipt_parallel_v04.py",
        "software/verify_pointer_receipt_pair_v04.py",
        "software/verify_pointer_audit_minimal_fixture_v04.py",
        "fixtures/minimal-pointer-receipt-v04/EXPECTED-RESULT.json",
        "fixtures/minimal-pointer-receipt-v04/FILES.tsv",
        "fixtures/minimal-pointer-receipt-v04/README.md",
        "fixtures/minimal-pointer-receipt-v04/SHA256SUMS",
        "fixtures/minimal-pointer-receipt-v04/pair-expected.json",
        "fixtures/minimal-pointer-receipt-v04/shard-0/audit-contract.json",
        "fixtures/minimal-pointer-receipt-v04/shard-0/pointer-health-attempts.tsv",
        "fixtures/minimal-pointer-receipt-v04/shard-0/pointer-health.tsv",
        "fixtures/minimal-pointer-receipt-v04/shard-0/record-completions.tsv",
        "fixtures/minimal-pointer-receipt-v04/shard-1/audit-contract.json",
        "fixtures/minimal-pointer-receipt-v04/shard-1/pointer-health-attempts.tsv",
        "fixtures/minimal-pointer-receipt-v04/shard-1/pointer-health.tsv",
        "fixtures/minimal-pointer-receipt-v04/shard-1/record-completions.tsv",
    }
)

FORBIDDEN_PUBLIC_PATHS = (
    "inputs/pilot-manifest.tsv",
    "software/reconstruct_pointers_parallel_v03.py",
    "software/verify_pointer_receipt_parallel_v03.py",
    "software/verify_scientific_nonmutation.py",
    "software/build_pointer_audit_final_package_v04.py",
    "software/verify_pointer_audit_final_package_v04.py",
)

FORBIDDEN_PUBLIC_DIRECTORIES = {
    "outputs",
    "receipts",
    "runtime",
    "runtime-formal",
}

FORBIDDEN_DATA_SUFFIXES = {
    ".bmp", ".gif", ".jpeg", ".jpg", ".npy", ".npz", ".parquet",
    ".png", ".pt", ".safetensors", ".tar", ".tif", ".tiff", ".zst",
}

INTERNAL_OPERATION_PATTERNS = (
    re.compile(r"\bper[- ]egress\b", re.IGNORECASE),
    re.compile(r"\bper[- ]IP\b", re.IGNORECASE),
    re.compile(r"\b(?:dual|two)[- ]machine\b", re.IGNORECASE),
    re.compile(r"\bproxy (?:chain|route)\b", re.IGNORECASE),
    re.compile(r"\bactual (?:machine|host|IP|address)\b", re.IGNORECASE),
    re.compile(r"\bauthor[- ]confirmed\b", re.IGNORECASE),
    re.compile(r"\bhow we (?:ran|executed|deployed)\b", re.IGNORECASE),
    re.compile(
        r"\binternal (?:approval|deployment|host|machine|IP|proxy)\b",
        re.IGNORECASE,
    ),
)


def markdown_files() -> list[Path]:
    return sorted(path for path in ROOT.rglob("*.md") if ".git" not in path.parts)


def public_text_files() -> list[Path]:
    suffixes = {
        ".csv", ".json", ".md", ".py", ".sh", ".toml", ".tsv",
        ".txt", ".yaml", ".yml",
    }
    return sorted(
        path
        for path in ROOT.rglob("*")
        if path.is_file()
        and ".git" not in path.parts
        and "__pycache__" not in path.parts
        and ".venv" not in path.parts
        and (path.suffix in suffixes or path.name.startswith("requirements"))
    )


def qualified_name(node: ast.AST) -> str:
    parts: list[str] = []
    cursor = node
    while isinstance(cursor, ast.Attribute):
        parts.append(cursor.attr)
        cursor = cursor.value
    if isinstance(cursor, ast.Name):
        parts.append(cursor.id)
        return ".".join(reversed(parts))
    return ""


class RuntimeUseVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.identity_calls: list[tuple[int, str]] = []
        self.environment_reads: list[tuple[int, str]] = []
        self.path_home_calls: list[int] = []

    def visit_Call(self, node: ast.Call) -> None:
        name = qualified_name(node.func)
        if name in FORBIDDEN_IDENTITY_CALLS:
            self.identity_calls.append((node.lineno, name))
        if name == "Path.home":
            self.path_home_calls.append(node.lineno)
        if name in {"os.environ.get", "os.getenv"}:
            key = (
                node.args[0].value
                if node.args and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                else "*dynamic*"
            )
            self.environment_reads.append((node.lineno, key))
        if (
            name == "dict"
            and node.args
            and qualified_name(node.args[0]) == "os.environ"
        ):
            self.environment_reads.append((node.lineno, "*copy*"))
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if qualified_name(node.value) == "os.environ":
            key = (
                node.slice.value
                if isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
                else "*dynamic*"
            )
            self.environment_reads.append((node.lineno, key))
        self.generic_visit(node)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_required_files(errors: list[str]) -> None:
    missing = [name for name in REQUIRED_FILES if not (ROOT / name).is_file()]
    if missing:
        errors.append("missing required files: " + ", ".join(missing))


def check_relative_links(errors: list[str]) -> None:
    for path in markdown_files():
        text = path.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK.findall(text):
            target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
            if target.startswith(("https://", "http://", "mailto:", "#")):
                continue
            target_path = target.split("#", 1)[0]
            if not target_path:
                continue
            resolved = (path.parent / target_path).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                errors.append(f"{path.relative_to(ROOT)}: link escapes repository: {target}")
                continue
            if not resolved.exists():
                errors.append(f"{path.relative_to(ROOT)}: missing link target: {target}")


def check_public_boundary(errors: list[str]) -> None:
    for path in public_text_files():
        relative = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            errors.append(f"{relative}: public text is not readable UTF-8")
            continue
        for pattern in PRIVATE_PATTERNS:
            if match := pattern.search(text):
                errors.append(
                    f"{relative}: private-boundary match: {match.group(0)!r}"
                )
        if path.suffix != ".py" or path.name == "check_public_docs.py":
            continue
        try:
            tree = ast.parse(text, filename=relative)
        except SyntaxError as exc:
            errors.append(f"{relative}: Python syntax invalid: {exc.msg}")
            continue
        visitor = RuntimeUseVisitor()
        visitor.visit(tree)
        for line, name in visitor.identity_calls:
            errors.append(
                f"{relative}:{line}: forbidden runtime identity collection: {name}"
            )
        allowed = PINNED_ENVIRONMENT_READS.get(relative, set())
        for line, key in visitor.environment_reads:
            if key not in allowed:
                errors.append(
                    f"{relative}:{line}: unapproved environment read: {key}"
                )
        for line in visitor.path_home_calls:
            if relative != "software/reconstruct_pointers.py":
                errors.append(f"{relative}:{line}: unapproved Path.home use")


def check_pointer_surface(errors: list[str]) -> None:
    manifest_path = ROOT / POINTER_SURFACE_PATH
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        errors.append(f"{POINTER_SURFACE_PATH}: invalid JSON: {type(exc).__name__}")
        return
    if not isinstance(value, dict) or value.get("schema") != POINTER_SURFACE_SCHEMA:
        errors.append(f"{POINTER_SURFACE_PATH}: schema mismatch")
        return
    dataset = value.get("dataset")
    if not isinstance(dataset, dict) or dataset.get("revision") != (
        "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8"
    ):
        errors.append(f"{POINTER_SURFACE_PATH}: dataset revision mismatch")
    rows = value.get("files")
    totals = value.get("totals")
    if not isinstance(rows, list) or not isinstance(totals, dict):
        errors.append(f"{POINTER_SURFACE_PATH}: inventory shape invalid")
        return
    paths = [row.get("path") for row in rows if isinstance(row, dict)]
    if len(paths) != len(rows) or any(not isinstance(path, str) for path in paths):
        errors.append(f"{POINTER_SURFACE_PATH}: file path values invalid")
        return
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        errors.append(f"{POINTER_SURFACE_PATH}: paths are not unique and sorted")
    if set(paths) != POINTER_SURFACE_PATHS:
        errors.append(f"{POINTER_SURFACE_PATH}: exact public file set changed")
    total_bytes = 0
    for row in rows:
        relative_text = row.get("path")
        relative = PurePosixPath(relative_text)
        size = row.get("bytes")
        sha256 = row.get("sha256")
        role = row.get("role")
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            or not isinstance(role, str)
            or not role
        ):
            errors.append(f"{POINTER_SURFACE_PATH}: invalid row for {relative_text!r}")
            continue
        path = ROOT.joinpath(*relative.parts)
        if path.is_symlink() or not path.is_file():
            errors.append(f"{relative_text}: missing or unsafe public-surface file")
            continue
        actual_size = path.stat().st_size
        actual_sha256 = file_sha256(path)
        if actual_size != size or actual_sha256 != sha256:
            errors.append(f"{relative_text}: public-surface bytes or SHA-256 changed")
        total_bytes += size
    if totals.get("files") != len(rows) or totals.get("bytes") != total_bytes:
        errors.append(f"{POINTER_SURFACE_PATH}: totals do not close")


def check_excluded_surfaces(errors: list[str]) -> None:
    for relative in FORBIDDEN_PUBLIC_PATHS:
        if (ROOT / relative).exists():
            errors.append(f"forbidden public path is present: {relative}")
    ignored_roots = {".git", ".venv", "artifacts", "coverfish-model",
                     "coverfish-release", "pointer-audit-work", "pointer-receipts"}
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(ROOT)
        if relative.parts and relative.parts[0] in ignored_roots:
            continue
        lowered_parts = {part.lower() for part in relative.parts}
        if lowered_parts & FORBIDDEN_PUBLIC_DIRECTORIES or any(
            part.startswith("runtime-") for part in lowered_parts
        ):
            errors.append(f"forbidden generated-data directory is present: {relative}")
        if path.suffix.lower() in FORBIDDEN_DATA_SUFFIXES:
            errors.append(f"forbidden image/archive/array file is present: {relative}")


def check_tool_contract(errors: list[str]) -> None:
    downloader = runpy.run_path(str(ROOT / "scripts/download_release.py"))
    files = downloader["FILES"]
    if len(files) != 24:
        errors.append(f"download manifest has {len(files)} files, expected 24")
    if sum(item.bytes for item in files) != 83_253_466_397:
        errors.append("download manifest byte total is not 83,253,466,397")
    if downloader["DATASET_REVISION"] != "0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8":
        errors.append("downloader fixed dataset revision changed")
    for profile, expected in {"control": 22_725, "smoke": 903_269_834}.items():
        specs = downloader["specs_for_profile"](profile)
        if sum(item.bytes for item in specs) != expected:
            errors.append(f"downloader {profile} profile byte total changed")

    verifier = runpy.run_path(str(ROOT / "scripts/verify_bioclip_pipeline.py"))
    if verifier["MODEL_REVISION"] != "191d741545e4c741cdef4b22c6eb69c945c1e592":
        errors.append("BioCLIP fixed model revision changed")
    if verifier["EXPECTED_QUERY_ROWS"] != 1044:
        errors.append("BioCLIP verifier D0 row count changed")
    if verifier["EXPECTED_PROTOTYPE_ROWS"] != 19144:
        errors.append("BioCLIP verifier prototype row count changed")
    if verifier["FROZEN_DEPENDENCIES"].get("torch") != "2.11.0+cu128":
        errors.append("BioCLIP verifier frozen torch build changed")
    if verifier["FROZEN_DEPENDENCIES"].get("torchvision") != "0.26.0+cu128":
        errors.append("BioCLIP verifier frozen torchvision build changed")
    if verifier["DEFAULT_RECORD_ID"] != "coverfish-qint-v1.0-0721":
        errors.append("BioCLIP verifier default record changed")
    args = verifier["build_parser"]().parse_args(
        ["run", "--core-dir", "core", "--d0-dir", "d0"]
    )
    if args.device != "cpu":
        errors.append("BioCLIP verifier no longer defaults to CPU")


def check_claims(errors: list[str]) -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for token in REQUIRED_README_TEXT:
        if token not in readme:
            errors.append(f"README.md: missing frozen/public token: {token!r}")

    public_text = "\n".join(path.read_text(encoding="utf-8") for path in markdown_files())
    for token in STALE_OR_UNSUPPORTED_TEXT:
        if token in public_text:
            errors.append(f"stale or unsupported public wording remains: {token!r}")
    for pattern in INTERNAL_OPERATION_PATTERNS:
        if match := pattern.search(public_text):
            errors.append(
                f"internal execution wording remains in public docs: {match.group(0)!r}"
            )

    pointer = (ROOT / "docs/POINTER_AUDIT.md").read_text(encoding="utf-8")
    for token in (
        "42,387",
        "41,945",
        "442",
        "4--5 days",
        "2--3 days",
        'test "$(python3.10 --version)" = "Python 3.10.12"',
        "--accept-network",
        "bytes_retained=false",
        "verify_pointer_receipt_parallel_v04.py",
        "verify_pointer_receipt_pair_v04.py",
        "verify_pointer_audit_minimal_fixture_v04.py",
    ):
        if token not in pointer:
            errors.append(f"docs/POINTER_AUDIT.md: missing contract token: {token!r}")
    for forbidden in ("--device cuda", "--component", "--max-rows"):
        if forbidden in readme:
            errors.append(f"README.md: pointer implementation detail is too prominent: {forbidden!r}")

    scope = (ROOT / "LICENSE.md").read_text(encoding="utf-8")
    for token in (
        "Apache License 2.0",
        "Creative Commons Attribution 4.0 International",
        "single blanket",
        "Liang Li",
        "license: other",
        "do not relicense third-party images",
        "fixtures/minimal-pointer-receipt-v04/",
    ):
        if token not in scope:
            errors.append(f"LICENSE.md: missing mixed-rights token: {token!r}")

    apache = (ROOT / "LICENSES/Apache-2.0.txt").read_text(encoding="utf-8")
    if "Apache License" not in apache or "Version 2.0, January 2004" not in apache:
        errors.append("Apache-2.0 full text is missing or malformed")
    cc = (ROOT / "LICENSES/CC-BY-4.0.txt").read_text(encoding="utf-8")
    if (
        "Creative Commons Attribution 4.0 International Public License" not in cc
        or "Section 8 -- Interpretation" not in cc
    ):
        errors.append("CC-BY-4.0 full text is missing or malformed")

    citation = (ROOT / "CITATION.md").read_text(encoding="utf-8")
    for token in (
        "10.57967/hf/9706",
        "COVER-Fish` (`Organizational`)",
        "Version 0ee47b2",
        "Hugging Face",
    ):
        if token not in citation:
            errors.append(f"CITATION.md: missing verified DOI token: {token!r}")

    availability = (ROOT / "docs/DATA_AVAILABILITY.md").read_text(encoding="utf-8")
    for token in (
        "117,640",
        "75,253",
        "42,387",
        "115,780",
        "73,835",
        "41,945",
        "107,722",
        "19,144",
        "451 pointer-only rows",
    ):
        if token not in availability:
            errors.append(f"docs/DATA_AVAILABILITY.md: missing release token: {token!r}")

    if (ROOT / "CITATION.cff").exists():
        errors.append("CITATION.cff is outside the approved REV024 alignment surface")


def main() -> int:
    errors: list[str] = []
    check_required_files(errors)
    if not errors:
        check_relative_links(errors)
        check_public_boundary(errors)
        check_excluded_surfaces(errors)
        check_pointer_surface(errors)
        check_claims(errors)
        check_tool_contract(errors)

    if errors:
        print(f"FAIL: {len(errors)} documentation check(s) failed", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print("PASS: required public documentation files are present")
    print("PASS: repository-relative Markdown links resolve")
    print("PASS: no private path, host, network address, credential, or identity collection found")
    print("PASS: excluded receipts, source rows, outputs, images, archives, and arrays are absent")
    print("PASS: pointer-audit public surface hashes are fixed")
    print("PASS: frozen paper values and public dataset identifiers are present")
    print("PASS: enumerated stale or unsupported public wording is absent")
    print("PASS: Apache-2.0 and CC-BY-4.0 texts and mixed-rights scope are present")
    print("PASS: downloader and BioCLIP verifier identities and manifest totals are fixed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
