# Version map

| Surface | Version |
| --- | --- |
| GitHub documentation/code alignment | Zenodo-first publication baseline, 2026-08-10 |
| Companion manuscript | submission baseline |
| QT26-QC Zenodo DOI / version | `10.5281/zenodo.21734785` / `1.0.0` |
| Reference-Update Zenodo DOI / version | `10.5281/zenodo.21735011` / `1.0.0` |
| Commons Scale-Out Zenodo DOI / version | `10.5281/zenodo.21386770` / `1.0.0` |
| Claim-evidence wrapper SHA-256 | `30d6ae64149f7f51d404c3932d5b139e546d5c669b0515e3d2c781d517b00cea` |
| Frozen-result audit wrapper SHA-256 | `0d7db77cb9bd6bce6f07b38a9e02df591a7554e2f4c24acfbd30fd832bb6b8b1` |
| Pointer-audit receipt wrapper SHA-256 | `aa05519748fcf585abeb816e0960d95eaa3cb6093872c1a87e7125b0c62d3755` |
| Base-release downloader and model-smoke CLI schema | `1.0.0` |
| Analysis-evidence verifier | `1.0.0` |
| Pointer scientific producer | `0.2.0` |
| Pointer fixed-shard runner | `0.4.0` |
| Pointer single-receipt wrapper | `0.4.2` |
| Pointer pair verifier | `0.1.2` |
| Pointer minimal-fixture verifier | `0.2.0` |

The current publication layer does not change the scientific objects. QT26-QC
exposes the canonical 6,719-query E0 roster as byte-identical Parquet shards; the
Reference-Update Benchmark exposes manifests, states, ledgers, protocols, source
archives and frozen tensors; Commons exposes independently extractable licence-tier
archives with a 26,454-row equivalence crosswalk. Frozen query membership, gallery
membership, embeddings, prototypes, predictions and paper estimates are unchanged.

The base downloader remains intentionally pinned to the old immutable fallback.
The separate analysis-evidence verifier is bound to the frozen nested release in
the current wrapper. The dated pointer audit remains an observation layer and does
not upgrade the frozen release tiers.

The GitHub repository is intentionally lightweight. Zenodo is the canonical data
and DOI surface; GitHub Git and Releases do not mirror image archives or tensor
bundles. Earlier Hugging Face snapshots are documented separately in
[Archive history](docs/ARCHIVE_HISTORY.md).
