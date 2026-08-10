# Data availability

## Citable data objects

The canonical public release is organized as three linked Zenodo records with
different roles.

### QT26-QC evaluation roster

The QT26-QC Zenodo record publishes the canonical 6,719-query, 3,121-taxon
`test` split as directly loadable Parquet shards and as the canonical E0 archive.
Every row carries encoded image bytes, a FishBase 25.04 target, provenance,
licence, attribution and image SHA-256.

- DOI: `10.5281/zenodo.21734785`
- version: `1.0.0`

The 6,734-row parent roster and its 15-row Q-INT overlap ledger are included only
as construction lineage. They do not define a second scoring split. The
[`COVER-Fish/QT26-QC`](https://huggingface.co/datasets/COVER-Fish/QT26-QC)
mirror is an optional `load_dataset()` access path, not a competing citation
object.

### Reference-Update Benchmark

The Reference-Update Zenodo record publishes the source archives and versioned
control plane for gallery-state and aggregation replay: source manifests, R0–R4
and later source-layer views, tensor bindings, transition ledgers, protocols,
licence-aware row lists and frozen CORE tensors.

- DOI: `10.5281/zenodo.21735011`
- version: `1.0.0`

Level A ranking and aggregation replay requires only the approximately 75.8 MB
control archive and the frozen CORE archive. Commons and QT26-QC are separate
companion records with their own citation and version boundaries.

### Commons Scale-Out Layer

The Commons Zenodo record publishes the S4 source manifest, 132 pointer-only
records, and 26,454 byte-bearing rows in three independently extractable archives:
public-domain/CC0, CC BY and CC BY-SA.

- DOI: `10.5281/zenodo.21386770`
- version: `1.0.0`

The metadata bundle contains a 26,454-row source-to-tier crosswalk and a direct
equivalence receipt. Original S4 and the union of the three tier archives match on
all 26,454 image byte sequences; missing, extra and duplicate rows are zero. The
compressed containers are intentionally different and are not represented as
binary-identical archives.

## Analysis evidence

The Reference-Update record includes
`cover-fish-analysis-evidence-v1.tar.zst` (2,278,902 bytes; SHA-256
`30d6ae64149f7f51d404c3932d5b139e546d5c669b0515e3d2c781d517b00cea`).
Its verified nested release contains:

- `analysis-evidence.tar.zst`: 2,272,506 bytes, SHA-256
  `a83cea63de116c6b895551401f55a97af9b38bcc750006063a217aae44022a01`;
- 169 artifacts across 13 modules;
- a 14-claim `CLAIM-ARTIFACT-LEDGER.tsv`; and
- `FILES.tsv`, `SHA256SUMS`, and `RELEASE.json` controls.

The package reuses frozen manifests, embeddings, queries, aggregators, predictions,
and scores. It introduces no new training, image encoding, or canonical-score
change. Deposited text copies replace absolute workstation and storage paths with
portable role placeholders; identifiers, scores, intervals, and source hashes are
unchanged. Run `scripts/verify_analysis_evidence.py` after download to check the
outer controls, safe archive topology, all 169 artifact hashes, and every claim
path.

The same record includes
`cover-fish-frozen-result-audit-v1.tar.zst` (47,495 bytes; SHA-256
`0d7db77cb9bd6bce6f07b38a9e02df591a7554e2f4c24acfbd30fd832bb6b8b1`).
This separately implemented audit reaggregated frozen row-level outputs without
importing the main scoring implementation and passed 3,639 of 3,639 checks.

## Historical archive and dated pointer audit

The Reference-Update record publishes the complete dated receipts as
`cover-fish-pointer-audit-receipts-v1.tar.zst` (12,226,488 bytes; SHA-256
`aa05519748fcf585abeb816e0960d95eaa3cb6093872c1a87e7125b0c62d3755`).
The package preserves both the complete two-shard audit and the direct 404
recheck. The pointer observation does not rewrite scientific objects or frozen
reproducibility tiers.

Earlier Hugging Face DOI snapshots remain immutable provenance and fallback
access, not alternative current citations. Their fixed identifiers are listed
only in [Archive history](ARCHIVE_HISTORY.md).

## Release-core accounting

The archive release core contains 117,640 records:

```text
109,877 source-pack records
+ 1,044 D0 / Q-INT records
+ 6,719 E0 / QT26-QC records
= 117,640 archive release-core records
```

The active projection replaces the 32,656-row iNat-RG Archive with the 30,796-row
iNat-RG-Pre projection, leaving 115,780 active records. The final clean gallery is a
separate 107,722-row derived view with 19,144 species prototypes.

| Surface | Byte-complete | Pointer-reconstructable | Pointer-fragile | Total |
| --- | ---: | ---: | ---: | ---: |
| Archive | 75,253 | 0 | 42,387 | 117,640 |
| Active projection | 73,835 | 0 | 41,945 | 115,780 |

`Byte-complete` means that bytes, size, and SHA-256 are bound by the release
manifests. It does not grant a blanket licence. `Pointer-reconstructable` requires a
successful release-time fetch and exact-hash match; no pointer met that frozen
definition. `Pointer-fragile` retains metadata and an upstream locator without that
reconstruction receipt.

The later dated audit observes 41,917/42,387 archive pointers and
41,476/41,945 active pointers as byte-exact. Those observations do not
retroactively change the tier counts above. ANGFA contributes 451 pointer-only rows
and no redistributed ANGFA image payload.

## Repository roles

Zenodo is the canonical DOI and citation surface for QT26-QC, Reference-Update,
and Commons. Hugging Face provides a convenient QT26-QC access mirror and retains
historical immutable snapshots. GitHub carries lightweight documentation,
scientific and benchmark scope, rights information, citation metadata, and public
verification code. A fixed GitHub content commit will be inserted before
submission so the code citation does not resolve to a moving branch.

GitHub Git and GitHub Releases do not mirror the large data archives. Row-level
source, attribution, URL, licence, and rights fields remain authoritative for
third-party material.
