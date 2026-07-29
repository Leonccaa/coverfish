# Data availability

## Current paper-aligned release

The COVER-Fish paper-data and reproducibility package is public in the Hugging Face
dataset repository
[`COVER-Fish/COVER-Fish`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish).

- current DOI: [`10.57967/hf/9776`](https://doi.org/10.57967/hf/9776)
- DOI snapshot commit: `4e437b6a2bf5f9a12a200bbe3a93411fe713db1f`
- DataCite version: `4e437b6`
- manuscript alignment: `REV035`
- dataset creator: `COVER-Fish` (`Organizational`)

The DOI snapshot retains every large object from the earlier base release
byte-for-byte and adds the compact analysis evidence needed by REV035. The dataset
card is mutable discovery metadata and is intentionally excluded from the root
checksum seal; the DOI snapshot commit and root checksum ledgers identify the
immutable release surface.

## Analysis evidence

The current diagnostic package is fixed at
[`supplements/analysis-evidence/`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish/tree/4e437b6a2bf5f9a12a200bbe3a93411fe713db1f/supplements/analysis-evidence).
It contains:

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

## Historical base and dated pointer audit

The byte-identical base objects remain independently citable as historical
provenance:

- DOI: [`10.57967/hf/9706`](https://doi.org/10.57967/hf/9706)
- tag: `rev023-rc2-20260714`
- fixed revision: `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`

The complete pointer-audit receipts are separately fixed under tag
[`pointer-audit-v04-20260726`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish/tree/pointer-audit-v04-20260726/supplements/pointer-audit-v04-20260726)
and revision `70284660ee40128ff1d34ccec12e5c3e78f83f25`. The pointer observation
does not rewrite the base tag, scientific objects, or frozen reproducibility tiers.

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

Hugging Face carries the image/query packs, frozen arrays and indices, manifests,
checksums, analysis evidence, and full dated pointer receipts. GitHub carries
lightweight documentation, scientific and benchmark scope, rights information,
citation metadata, and public verification code.

GitHub Git and GitHub Releases do not mirror the large data archives. Row-level
source, attribution, URL, licence, and rights fields remain authoritative for
third-party material.
