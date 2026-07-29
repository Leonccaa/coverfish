# Version map

| Surface | Version |
| --- | --- |
| GitHub documentation/code alignment | REV035, `agent/rev035-release-alignment`, 2026-07-28 |
| Companion manuscript | REV035 |
| Current paper-aligned dataset snapshot | `4e437b6a2bf5f9a12a200bbe3a93411fe713db1f` |
| Current dataset DOI | `10.57967/hf/9776` |
| Current DataCite version | `4e437b6` |
| Analysis-evidence archive SHA-256 | `a83cea63de116c6b895551401f55a97af9b38bcc750006063a217aae44022a01` |
| Historical base DOI | `10.57967/hf/9706` |
| Historical base tag | `rev023-rc2-20260714` |
| Historical base revision | `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8` |
| Pointer-audit supplement tag | `pointer-audit-v04-20260726` |
| Pointer-audit supplement revision | `70284660ee40128ff1d34ccec12e5c3e78f83f25` |
| Base-release downloader and model-smoke CLI schema | `1.0.0` |
| Analysis-evidence verifier | `1.0.0` |
| Pointer scientific producer | `0.2.0` |
| Pointer fixed-shard runner | `0.4.0` |
| Pointer single-receipt wrapper | `0.4.2` |
| Pointer pair verifier | `0.1.2` |
| Pointer minimal-fixture verifier | `0.2.0` |

REV035 changes the manuscript interpretation, compact evidence surface, and public
documentation. It does not alter the frozen query rosters, gallery membership,
embeddings, prototypes, canonical predictions, or base archive bytes. The current
DOI snapshot adds stable semantic files under `supplements/analysis-evidence/`; the
large archives retain their original filenames and hashes so the historical base
remains independently verifiable.

The base downloader remains intentionally pinned to the old immutable base commit.
The separate analysis-evidence verifier is bound to the current compact archive.
The dated pointer audit remains an observation layer and does not upgrade the frozen
release tiers.

The GitHub repository is intentionally lightweight. Hugging Face is the
authoritative data surface; GitHub Git and Releases do not mirror image archives or
tensor bundles.
