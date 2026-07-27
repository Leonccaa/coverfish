# Version map

| Surface | Version |
| --- | --- |
| GitHub documentation/code baseline | REV024, `main`, 2026-07-24 |
| Companion manuscript | REV024 release-metadata-only successor to frozen REV023 |
| Dataset package | `REV023-RC2-20260714` |
| Hugging Face tag | `rev023-rc2-20260714` |
| Hugging Face revision | `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8` |
| Dataset DOI | `10.57967/hf/9706` |
| DataCite version | `0ee47b2` |
| Pointer-audit supplement tag | `pointer-audit-v04-20260726` |
| Pointer-audit supplement revision | `70284660ee40128ff1d34ccec12e5c3e78f83f25` |
| Public downloader and model-smoke CLI schema | `1.0.0` |
| Pointer scientific producer | `0.2.0` |
| Pointer fixed-shard runner | `0.4.0` |
| Pointer single-receipt wrapper | `0.4.2` |
| Pointer pair verifier | `0.1.2` |
| Pointer minimal-fixture verifier | `0.2.0` |

The companion manuscript's REV024 changes release and citation metadata only. It
retains the frozen REV023 queries, gallery membership, prototypes, embeddings,
predictions, scores, figures, benchmark rosters, and reported results.

The public CLIs are post-publication convenience tools bound to the existing fixed
dataset and model revisions. Their addition does not create a new scientific data
version or modify a frozen paper object.

Pointer health is a dated observation and can decay independently of this
version map. The pointer tools do not upgrade the frozen RC2 tier counts or
create another scientific dataset version. The published receipt supplement is
a later observation layer; it does not rewrite the RC2 tag or mint a new DOI.

The GitHub baseline is intentionally lightweight. The immutable data-version
identity is the Hugging Face tag and DOI; image archives and tensor arrays are not
stored in this repository.
