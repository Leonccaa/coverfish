# Data availability

## Public dataset

The complete COVER-Fish paper-data and reproducibility package is available from
the Hugging Face dataset repository
[`COVER-Fish/COVER-Fish`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish).

- DOI: [`10.57967/hf/9706`](https://doi.org/10.57967/hf/9706)
- project version: `REV023-RC2-20260714`
- immutable tag: `rev023-rc2-20260714`
- fixed revision: `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`

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
successful release-time fetch and exact-hash match; no pointer currently meets that
definition. `Pointer-fragile` retains metadata and an upstream locator without that
reconstruction receipt.

ANGFA contributes 451 pointer-only rows and no redistributed ANGFA image payload.
Pointers must not be described as hosted pixels.

## Repository roles

Hugging Face carries the image/query packs, frozen arrays and indices, manifests,
checksums, and package verifiers. GitHub carries lightweight documentation,
scientific and benchmark scope, rights information, citation metadata, and public
verification code.

GitHub Git and GitHub Releases do not mirror the large data archives. Row-level
source, attribution, URL, licence, and rights fields remain authoritative for
third-party material.
