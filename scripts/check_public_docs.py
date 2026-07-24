#!/usr/bin/env python3
"""Check the COVER-Fish public GitHub baseline without external dependencies."""

from __future__ import annotations

import re
import runpy
import sys
from pathlib import Path

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
    "docs/RIGHTS_AND_LICENSING.md",
    "AGENTS.md",
    ".gitignore",
    "requirements-model.txt",
    "scripts/download_release.py",
    "scripts/verify_bioclip_pipeline.py",
    "tests/test_download_release.py",
    "tests/test_verify_bioclip_pipeline.py",
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
    "NOT UPLOADED",
    "selected publication design is Zenodo",
    "exact Zenodo version",
    "Zenodo will hold the complete immutable data archive",
    "No project-authored repository licence has yet been approved",
    "No final creator list",
    "UPLOADED / PRIVATE / NOT PUBLIC / NOT DOI",
    "It remains **PRIVATE, NOT PUBLIC",
    "The Hub repository remains private",
    "Included in the private Hub package; not public",
    "blocked by public availability",
    "private release candidate, not a public",
    "X4 COLD VERIFICATION PENDING",
    "X4 PENDING",
    "X4 pending",
    "full X4 cold verification remains pending",
    "Full X4 public cold extraction/replay remains pending",
    "NOT DOI-bearing",
    "DOI generation remains pending",
    "No DOI or aligned",
    "A DOI will be added",
    "committed only in the local Git",
    "has not been pushed",
    "remains local and unpushed",
    "Public historical landing page",
    "pushing the local alignment commit",
    "Inspected public repository baseline",
    "Local review branch",
    "approved the project-authored validation/reproduction code",
    "explicitly accepted this record",
    "author-accepted",
    "X1 metadata",
    "X4-lite",
    "full 83 GB redownload",
    "deletion is deferred",
    "internal receipt",
    "force-with-lease",
    "candidate remains local",
)

PRIVATE_PATTERNS = (
    re.compile(r"/(?:home|mnt|Users)/", re.IGNORECASE),
    re.compile(r"\b[A-Z]:\\Users\\", re.IGNORECASE),
    re.compile(r"\b[a-z0-9][a-z0-9.-]*\.lan\b", re.IGNORECASE),
    re.compile(r"\b(?:10|172\.(?:1[6-9]|2\d|3[01])|192\.168)(?:\.\d{1,3}){2}\b"),
    re.compile(r"(?:api[_-]?key|access[_-]?token|private[_-]?key)\s*[:=]", re.IGNORECASE),
)

MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")

FORBIDDEN_RUNTIME_INTROSPECTION = (
    "socket.gethostname(",
    "platform.node(",
    "getpass.getuser(",
    "Path.home(",
    "os.environ",
    "os.getenv(",
    "nvidia-smi",
    "torch.cuda.get_device_name(",
)


def markdown_files() -> list[Path]:
    return sorted(path for path in ROOT.rglob("*.md") if ".git" not in path.parts)


def public_text_files() -> list[Path]:
    suffixes = {".md", ".py", ".txt"}
    return sorted(
        path
        for path in ROOT.rglob("*")
        if path.is_file()
        and ".git" not in path.parts
        and (path.suffix in suffixes or path.name.startswith("requirements"))
    )


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
        text = path.read_text(encoding="utf-8")
        for pattern in PRIVATE_PATTERNS:
            if match := pattern.search(text):
                errors.append(
                    f"{path.relative_to(ROOT)}: private-boundary match: {match.group(0)!r}"
                )
        if path.suffix == ".py" and path.name != "check_public_docs.py":
            for token in FORBIDDEN_RUNTIME_INTROSPECTION:
                if token in text:
                    errors.append(
                        f"{path.relative_to(ROOT)}: forbidden runtime introspection: {token!r}"
                    )


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

    scope = (ROOT / "LICENSE.md").read_text(encoding="utf-8")
    for token in (
        "Apache License 2.0",
        "Creative Commons Attribution 4.0 International",
        "single blanket",
        "Liang Li",
        "license: other",
        "do not relicense third-party images",
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
        check_claims(errors)
        check_tool_contract(errors)

    if errors:
        print(f"FAIL: {len(errors)} documentation check(s) failed", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print("PASS: required public documentation files are present")
    print("PASS: repository-relative Markdown links resolve")
    print("PASS: no private local path, internal host, or private-network address found")
    print("PASS: frozen paper values and public dataset identifiers are present")
    print("PASS: enumerated stale or unsupported public wording is absent")
    print("PASS: Apache-2.0 and CC-BY-4.0 texts and mixed-rights scope are present")
    print("PASS: downloader and BioCLIP verifier identities and manifest totals are fixed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
