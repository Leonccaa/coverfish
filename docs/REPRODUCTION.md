# Reproduction

## Repository check

Clone this repository and run:

```bash
python3 scripts/check_public_docs.py
```

The check verifies required files, repository-relative links, frozen headline
values, release identifiers, licence texts, and the absence of private paths or
service identifiers. It does not download data or recompute paper estimates.

## Load and score QT26-QC

The canonical evaluation split is directly loadable:

```python
from datasets import load_dataset

test = load_dataset("COVER-Fish/QT26-QC", split="test")
assert len(test) == 6_719
```

The dataset card links the fixed submission schema, FishBase 25.04 mapping rules,
fixed-denominator scorer, 20,000-replicate species-cluster bootstrap, byte verifier
and the byte-identical E0 archive path. The 6,734-row construction parent is metadata
only and must not be substituted for `test`.

## Reference-update replay

The canonical control plane is published in the Reference-Update Zenodo record,
DOI `10.5281/zenodo.21735011`. Its README separates two canonical tracks: gallery-state
replay and aggregation replay. Source manifests, R0–R4 and later source-layer
views, licence-aware row lists and transition ledgers are available without
downloading image bytes.

For the 16-query Level A fixture, download the control and frozen CORE archives
from the same Zenodo record, then run its minimal replay example.

## Paper analysis evidence

Download `cover-fish-analysis-evidence-v1.tar.zst` from the Reference-Update
Zenodo record without pulling the source-image archives:

```bash
tar --zstd -xf cover-fish-analysis-evidence-v1.tar.zst
python3 scripts/verify_analysis_evidence.py \
  --root cover-fish-analysis-evidence-v1/release
```

The verifier uses only the Python standard library plus GNU tar with zstd support.
It checks the four files in the supplement checksum ledger, rejects unsafe archive
paths, verifies all 169 rows in `ARTIFACT-INVENTORY.tsv`, confirms all 14 claim
paths in `CLAIM-ARTIFACT-LEDGER.tsv`, and checks the frozen release metadata. It
does not execute the deposited analysis programs or recompute scores.

## Optional historical-payload fallback

The large-object downloader remains available for the byte-identical historical
Hugging Face fallback. It is not the current citation or dependency surface. Use
tag `rev023-rc2-20260714` or fixed revision
`0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`, rather than an unpinned moving branch.
The package README, file map, `SHA256SUMS`, and offline verifiers define the expected
inventory and integrity checks.

The downloader embeds the fixed 24-file manifest. `plan` and `verify` use only the
Python standard library and do not access the network. `download` additionally
requires `huggingface_hub`:

```bash
python3 -m pip install "huggingface_hub>=0.34,<2"
python3 scripts/download_release.py plan --profile smoke
python3 scripts/download_release.py download --profile smoke --output coverfish-release
python3 scripts/download_release.py verify --profile smoke --output coverfish-release
```

Every component profile includes the eight control files.

| Profile | Files | Exact bytes | Purpose |
| --- | ---: | ---: | --- |
| `control` | 8 | 22,725 | Ledgers, checksums, package README, and S4 verifier |
| `core` | 9 | 499,616,679 | Control files and metadata/index/code CORE |
| `smoke` | 10 | 903,269,834 | Control files, CORE, and D0 model-smoke inputs |
| `d0` | 9 | 403,675,880 | Q-INT development archive |
| `e0` | 9 | 3,683,808,607 | QT26-QC primary-evaluation archive |
| `s0` | 9 | 10,443,451,638 | iNat-RG Archive |
| `s1` | 9 | 1,036,940,048 | FishBase R0 |
| `s2` | 9 | 548,096,022 | USFWS |
| `s3` | 9 | 80,676 | ANGFA pointer-only layer |
| `s4` | 17 | 66,637,955,922 | Commons transport parts and controls |
| `all` | 24 | 83,253,466,397 | Complete published surface |

Profiles of 5 GB or more are refused unless `download` receives
`--accept-large-download`. The program does not prompt, discover credentials, or
fall back to a moving revision. A public release download does not require a Hub
token.

### Command contract

- Successful operational commands exit `0`.
- Invalid arguments exit `2`.
- A missing optional dependency exits `3`.
- A required large-download confirmation or insufficient capacity exits `4`.
- A transfer/runtime failure exits `5`.
- A size or SHA-256 failure exits `6`.
- stdout contains one JSON document; progress is confined to stderr.

`--help` and `--version` are the only human-text stdout modes. JSON schema
`coverfish.download.v1` is intended for agents and workflow runners.

## Pointer reconstruction audit

The repository also provides a byte-discarding, dated availability audit for
all `42,387` pointer rows. Its offline fixture, CORE download, single-run,
fixed two-shard, retry, and receipt-verification commands are in [Pointer
reconstruction audit](POINTER_AUDIT.md).

Allow approximately four to five days for one complete initial run or two to
three days for the fixed two-shard workflow. Transient responses and later
dated retry windows can extend those estimates. The audit does not change the
frozen dataset or its reproducibility tiers.

The complete dated receipt and the direct-only 18-row recheck are included in
`cover-fish-pointer-audit-receipts-v1.tar.zst` in the Reference-Update Zenodo
record. Download and checksum instructions are in [Pointer reconstruction
audit](POINTER_AUDIT.md#published-receipts).

## Extract the smoke inputs

With GNU tar and zstd support:

```bash
mkdir -p artifacts/core artifacts/d0
tar --zstd -xf \
  coverfish-release/coverfish-rev023-core-metadata-index-code-rc2-20260714.tar.zst \
  -C artifacts/core
tar --zstd -xf \
  coverfish-release/coverfish-rev023-D0-qint-development-rc2-20260714.tar.zst \
  -C artifacts/d0
```

Archive-level SHA-256 must pass before extraction. The model verifier then checks
the exact hashes of its extracted CORE inputs, D0 manifest, and selected image.

## BioCLIP encoder and index smoke test

The public model check is deliberately small. It re-encodes one byte-bearing D0
image with the pinned BioCLIP 2.5 ViT-H/14 snapshot, compares the result with its
frozen 1,024-dimensional fp16 row, and checks the top-1 result against all 19,144
frozen species prototypes. It does not train a model, rebuild the gallery, pool
Q-INT with QT26-QC, or claim to recompute every paper result.

The paper's frozen scientific environment used CPython 3.10.12, NumPy 2.2.6,
PyTorch 2.11.0+cu128, Torchvision 0.26.0+cu128, and OpenCLIP 3.3.0. For a safe
portable CPU smoke test, install the corresponding base versions from the CPU
wheel index:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --index-url https://download.pytorch.org/whl/cpu \
  torch==2.11.0 torchvision==0.26.0
python3 -m pip install -r requirements-model.txt
```

This CPU-first sequence avoids implicitly installing or using an accelerator
runtime. A CUDA-specific PyTorch wheel should be substituted only when accelerator
use has been approved. The CPU wheels intentionally report
`dependencies.frozen_match == false`; `--require-frozen-environment` requires the
exact `+cu128` builds and is separate from a portable CPU smoke test.

First run a network-free plan. It audits the input bytes and reports dependency and
model readiness as JSON without importing PyTorch or OpenCLIP:

```bash
python3 scripts/verify_bioclip_pipeline.py plan \
  --core-dir artifacts/core \
  --d0-dir artifacts/d0 \
  --model-dir coverfish-model
```

The pinned model contributes a 3,944,517,804-byte safetensors file and a 560-byte
configuration. The first networked run therefore requires explicit consent:

```bash
python3 scripts/verify_bioclip_pipeline.py run \
  --core-dir artifacts/core \
  --d0-dir artifacts/d0 \
  --model-dir coverfish-model \
  --device cpu \
  --accept-model-download
```

Subsequent runs can add `--offline`. CPU is the safe default, and CUDA is used only
when `--device cuda` is explicit. `--require-frozen-environment` converts any
declared version mismatch into an error. The default comparison requires cosine
similarity at least `0.9999`, maximum absolute element difference at most `0.005`,
norm error at most `0.002`, and identical top-1 prototype.

The model tool uses JSON schema `coverfish.bioclip-smoke.v1` and the same exit codes
as the downloader, plus exit `7` for a completed but failed numerical comparison.
It reports only release identifiers, file hashes, dependency versions, device
category, thresholds, and scientific check results. It does not collect usernames,
hostnames, device model names, environment variables, or absolute paths.

## Public package layers

The public package separates:

| Layer | Contents |
| --- | --- |
| Metadata and views | Taxonomy, source/projection/view counts, rights tiers, fixed denominators, and hashes |
| Frozen arrays and row maps | Tensor identity, active selectors, embeddings, indices, and species prototypes |
| Compact replay evidence | Fixed query examples, top-k decisions, representative flips, and audit bindings |

The independent frozen-result audit reports 3,639/3,639 checks passing. It
reaggregates denominators, metrics, flips, and bootstrap point estimates from frozen
row-level outputs. It does not rerun image encoding or publish full dense
prediction/score matrices.

## Frozen computation contract

The paper's current recognition contract is:

1. map reference and mapped query targets to FishBase 25.04;
2. process full-frame images with the frozen native BioCLIP 2.5 ViT-H/14 processor;
3. L2-normalise image embeddings;
4. sum embeddings within each species and renormalise once to form an
   image-count-weighted species centroid;
5. rank every species centroid by exact query-centroid dot product; and
6. score the complete fixed query denominator at top-1, top-5, top-10, and top-20.

Approximate-nearest-neighbour search and image-level max pooling are not the frozen
paper method.

## Frozen checkpoints

- FishBase R0: 49,140 rows / 18,733 taxa;
- iNat-RG Archive: 32,656 / 618;
- iNat-RG-Pre: 30,796 / 615;
- retired temporal rows: 1,860;
- R0--R4 rows: 49,140 / 58,490 / 63,080 / 67,235 / 79,936;
- final clean gallery: 107,722 rows / 19,144 species centroids;
- D0 / Q-INT: 1,044 records;
- E0 / QT26-QC: 6,719 records; and
- archive tiers: 75,253 / 0 / 42,387.
