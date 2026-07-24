# COVER-Fish

**Auditable Gallery-Only Adaptation and Its Cross-Domain Limits in Fish Recognition**

COVER-Fish studies whether a frozen biological image encoder can improve fish
species recognition through governed reference-gallery updates, without retraining,
and where that strategy fails across source and query domains.

[Dataset](https://huggingface.co/datasets/COVER-Fish/COVER-Fish) ·
[DOI](https://doi.org/10.57967/hf/9706) ·
[Data availability](docs/DATA_AVAILABILITY.md) ·
[Reproduction](docs/REPRODUCTION.md) ·
[Citation](CITATION.md)

## Study at a glance

The frozen system uses FishBase 25.04 as its taxonomy backbone, BioCLIP 2.5
ViT-H/14 with its native full-frame processor, L2-normalised embeddings, one
image-count-weighted centroid per species, and exact prototype ranking.

| Frozen object or result | Paper-aligned value |
| --- | ---: |
| FishBase R0 breadth base | 49,140 references / 18,733 taxa |
| iNat-RG Archive | 32,656 references / 618 taxa |
| Active pre-cutoff iNat-RG-Pre | 30,796 references / 615 taxa |
| R4 endpoint | 79,936 gallery rows |
| Final clean source-layer view | 107,722 rows / 19,144 species centroids |
| QT26-QC primary evaluation | 6,719 queries / 3,121 taxa |
| R0 to R4 query-micro top-1 | +1.35 percentage points, 95% CI +0.30 to +2.43 |
| R0 to R4 species-macro top-1 | +1.72 points, 95% CI +0.77 to +2.70 |
| Commons step on Fish-Vista species-macro top-1 | -3.52 points, 95% CI -4.41 to -2.64 |

The result is deliberately two-sided. Temporally separated iNaturalist gallery
updates improve the fixed post-cutoff natural-photo roster. A later Wikimedia
Commons layer improves natural-photo recognition but harms museum-specimen
recognition under the same frozen single-centroid rule. The supported deployment
boundary is therefore **source pack × aggregation rule × query domain**.

## Public resources

The complete paper-data and reproducibility package is hosted on Hugging Face:

- repository: [`COVER-Fish/COVER-Fish`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish);
- DOI: [`10.57967/hf/9706`](https://doi.org/10.57967/hf/9706);
- project version: `REV023-RC2-20260714`;
- immutable tag: `rev023-rc2-20260714`; and
- fixed Hub revision: `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`.

The archive release core contains 117,640 records: 75,253 byte-bearing rows and
42,387 pointer rows. The active projection contains 115,780 records. ANGFA remains
a 451-row pointer-only source; pointer records are not redistributed image pixels.

This GitHub repository is the lightweight REV024 baseline for project
documentation, scientific scope, citation, rights information, and verification
code. Large image archives and tensor arrays are distributed through the DOI-bound
dataset, not through GitHub.

## Safe automated use

Inspect an exact download plan without network access or local writes:

```bash
python3 scripts/download_release.py plan --profile smoke
```

Operational commands return one stable JSON document on stdout and send progress
to stderr. The `smoke` profile contains control files, CORE, and D0 (903,269,834
bytes). The complete 83,253,466,397-byte surface and every profile of 5 GB or more
require the explicit `--accept-large-download` flag.

After extracting CORE and D0, audit a byte-bound query and the pinned BioCLIP model
requirements without downloading the model:

```bash
python3 scripts/verify_bioclip_pipeline.py plan \
  --core-dir artifacts/core \
  --d0-dir artifacts/d0
```

See [Reproduction](docs/REPRODUCTION.md) for complete commands and [Agent operating
contract](AGENTS.md) for machine-facing status and safety rules.

## Evidence boundaries

- The 615 intervention taxa were deliberately selected; they are not a random or
  representative sample of FishBase.
- Q-INT is the development benchmark used to select the encoder and aggregation.
  QT26-QC is the fixed primary evaluation roster. Their roles are not pooled.
- Fishial is an end-to-end native-pipeline comparator. COVER-Fish is lower overall
  at top-1 on the fixed matched subset, but higher at top-5 and top-20.
- FishNet and Fish-Vista are third-party query benchmarks and never enter the
  COVER-Fish reference gallery.
- Query-gallery identity checks are source-conditional and do not establish
  separation from BioCLIP pretraining.
- Closed-set ranking is not open-set abstention.

See [Scientific scope](docs/SCIENTIFIC_SCOPE.md) and
[Benchmark roles](docs/BENCHMARKS.md) for the complete interpretation boundary.

## Repository contents

- [Scientific scope](docs/SCIENTIFIC_SCOPE.md)
- [Benchmark roles](docs/BENCHMARKS.md)
- [Data availability](docs/DATA_AVAILABILITY.md)
- [Reproduction](docs/REPRODUCTION.md)
- [Rights and licensing](docs/RIGHTS_AND_LICENSING.md)
- [Citation](CITATION.md)
- [Version map](VERSION.md)
- [Agent operating contract](AGENTS.md)
- [`download_release.py`](scripts/download_release.py): pinned, profile-aware downloader
- [`verify_bioclip_pipeline.py`](scripts/verify_bioclip_pipeline.py): one-record encoder/index smoke test

Run the documentation consistency check with:

```bash
python3 scripts/check_public_docs.py
```

## Licensing

Project-authored verification and reproduction software is licensed under
Apache-2.0. Project-authored explanatory documentation is licensed under CC BY 4.0.
Third-party images, metadata, and database content remain governed by their
file-specific or row-level rights information. See [Licence scope](LICENSE.md) and
[Rights and licensing](docs/RIGHTS_AND_LICENSING.md).

## Citation

The dataset citation is:

> COVER-Fish. (2026). *COVER-Fish* (Version 0ee47b2) [Dataset]. Hugging Face.
> <https://doi.org/10.57967/HF/9706>

Liang Li (ORCID [`0009-0004-0467-7032`](https://orcid.org/0009-0004-0467-7032))
is the manuscript author and contact. Dataset and manuscript citation details are
kept separate in [CITATION.md](CITATION.md).
