# COVER-Fish

**Auditable Reference-Gallery Updates and Cross-Domain Sign Reversal in Fish Recognition**

COVER-Fish asks whether a frozen vision foundation model can adapt fish recognition
through evidence updates rather than weight updates—and when the same operation can
fail. The study reconstructs four staged iNaturalist reference-gallery updates,
then tests later source layers while holding the encoder, preprocessing,
aggregation, queries, and scoring fixed.

[QT26-QC evaluation roster](docs/DATA_AVAILABILITY.md#qt26-qc-evaluation-roster) ·
[Reference-Update Benchmark](docs/DATA_AVAILABILITY.md#reference-update-benchmark) ·
[Commons Scale-Out Layer](docs/DATA_AVAILABILITY.md#commons-scale-out-layer) ·
[Data availability](docs/DATA_AVAILABILITY.md) ·
[Reproduction](docs/REPRODUCTION.md) ·
[Citation](CITATION.md)

## Study at a glance

The experiment starts from 49,140 FishBase references. Its core intervention adds
30,796 pre-cutoff iNaturalist Research Grade references over 615 deliberately
selected taxa in four preserved rounds. The frozen recogniser uses BioCLIP 2.5
ViT-H/14, its native full-frame processor, L2-normalised embeddings, one
image-count-weighted centroid per species, and exact prototype ranking.

The evidence sequence is deliberately bounded:

1. **Refinement inside Fishial's scope.** Every R0--R4 view is reranked within
   Fishial v0.10.2's 775 normalized scientific names. R1--R3 provide virtually all
   in-scope reinforcement; R4 is flat at the restricted endpoint.
2. **Candidate-competition stress.** Restoring COVER-Fish's actual candidate field
   at R4 tests how added breadth displaces answers established in the bounded task.
   Fishial is not compared outside its 775-name support.
3. **Post-cutoff validation.** Across all 6,719 QT26-QC queries, R0--R4 improves
   query-micro top-1 by 1.35 points (95% CI, 0.30 to 2.43) and species-macro by
   1.72 points (0.77 to 2.70).
4. **Cross-domain gate.** Updating existing mixed-source prototypes at the Commons
   step raises species-macro by 5.95 points on natural photos but lowers it by 3.52
   points on museum specimens under the same frozen centroid rule.

The supported deployment boundary is therefore **source layer × aggregation rule ×
query domain**. Gallery updates are inspectable and reversible, but they are
globally coupled interventions rather than risk-free additions.

## Fishial comparison boundary

The only system-to-system comparison uses 1,931 queries from 665 species and the
same sealed 775-name candidate universe. At R4, COVER-Fish is 67.89% query-micro
top-1 versus Fishial's 67.63%; species-macro is 65.20% versus 65.47%. The paired
intervals resolve no winner and are not equivalence tests.

Opening COVER-Fish to its native 18,839-prototype candidate field lowers its own
score on those queries to 56.34%; that value is a candidate-space sensitivity, not
a comparison with Fishial. Of the remaining 4,788 QT26-QC queries whose targets lie
outside Fishial's vocabulary, COVER-Fish has target support for 4,696 and identifies
1,853 correctly at top-1. These numbers establish expanded task support, not
Fishial accuracy outside its scope.

## Public data objects

COVER-Fish separates the reusable evaluation set, the reference-update research
environment, and its large Commons source layer into three canonical Zenodo
records:

| Object | Use it for | Stable identity |
| --- | --- | --- |
| QT26-QC | Evaluate a fish-recognition model on the fixed 6,719-query roster | Zenodo DOI `10.5281/zenodo.21734785` |
| COVER-Fish Reference-Update Benchmark | Replay gallery states, aggregation rules and transition ledgers | Zenodo DOI `10.5281/zenodo.21735011` |
| COVER-Fish Commons Scale-Out Layer | Download the S4 manifest or independently extractable licence-tier image archives | Zenodo DOI `10.5281/zenodo.21386770` |

QT26-QC is directly loadable and exposes one canonical scoring split:

```python
from datasets import load_dataset

test = load_dataset("COVER-Fish/QT26-QC", split="test")
```

Its 6,719 image byte payloads are byte-identical across the Parquet and E0
representations. The separate 6,734-row parent roster is construction lineage,
not a second scoring split. A Hugging Face access mirror supports the one-line
`load_dataset()` path; Zenodo remains the canonical citation surface.

The Reference-Update Benchmark publishes the source archives required for replay,
frozen CORE tensors, manifests, gallery-state membership, protocols, ledgers and
verification tools. Its final clean view contains 107,722 gallery rows represented
by 19,144 species prototypes. The Commons companion publishes three licence-tier archives;
its 26,454-row crosswalk verifies that their image byte sequences match the
canonical S4 row set.

The same Reference-Update record carries three compact verification packages. A
claim ledger maps 14 manuscript claims to 169 frozen artifacts across 13 modules;
a separately implemented frozen-result audit passed all 3,639 checks; and the
complete dated pointer-audit receipts remain available for direct inspection.

Earlier Hugging Face DOI snapshots remain available as immutable historical
fallbacks. They are documented in [Archive history](docs/ARCHIVE_HISTORY.md), not
presented as alternative current citations.

## Reproduce and verify

The base-release downloader remains pinned to the historical immutable base:

```bash
python3 scripts/download_release.py plan --profile smoke
```

To verify the compact claim-level evidence after downloading
`cover-fish-analysis-evidence-v1.tar.zst` from the Reference-Update record:

```bash
tar --zstd -xf cover-fish-analysis-evidence-v1.tar.zst
python3 scripts/verify_analysis_evidence.py \
  --root cover-fish-analysis-evidence-v1/release
```

The complete 83,253,466,397-byte base surface and every profile of 5 GB or more
require the downloader's explicit `--accept-large-download` flag. See
[Reproduction](docs/REPRODUCTION.md) for exact commands and the distinction between
ranking replay, image re-encoding, and pointer reconstruction.

The dated pointer audit observes 41,917/42,387 archive pointers as SHA-256 exact;
18 URLs returned HTTP 404/410, 451 ANGFA rows remain permission-policy pending, and
one pHash diagnostic candidate is not counted as byte-exact. It does not rewrite
the frozen release tiers. See [Pointer audit](docs/POINTER_AUDIT.md).

## Evidence boundaries

- The 615 intervention taxa were deliberately selected and are not a random or
  representative sample of FishBase.
- Q-INT selects the frozen method; QT26-QC is the fixed primary evaluation roster.
- Fishial comparison stops at the shared 775-name candidate set.
- FishNet and Fish-Vista are third-party query benchmarks and never enter the
  reference gallery.
- Query-gallery checks do not establish separation from BioCLIP pretraining.
- Closed-set ranking is not open-set abstention.
- Machine verification and rollback apply to deposited tensors, row maps, ledgers,
  manifests, and byte-complete objects; pixel re-encoding is conditional on source
  bytes or successful pointer reconstruction.

See [Scientific scope](docs/SCIENTIFIC_SCOPE.md) and
[Benchmark roles](docs/BENCHMARKS.md) for the full interpretation boundary.

## Repository contents

- [Scientific scope](docs/SCIENTIFIC_SCOPE.md)
- [Benchmark roles](docs/BENCHMARKS.md)
- [Data availability](docs/DATA_AVAILABILITY.md)
- [Reproduction](docs/REPRODUCTION.md)
- [Pointer reconstruction audit](docs/POINTER_AUDIT.md)
- [Rights and licensing](docs/RIGHTS_AND_LICENSING.md)
- [Archive history](docs/ARCHIVE_HISTORY.md)
- [Citation](CITATION.md)
- [Version map](VERSION.md)
- [`download_release.py`](scripts/download_release.py): pinned base-release downloader
- [`verify_analysis_evidence.py`](scripts/verify_analysis_evidence.py): paper evidence verifier
- [`verify_bioclip_pipeline.py`](scripts/verify_bioclip_pipeline.py): one-record encoder/index smoke test

Run the repository consistency checks with:

```bash
python3 scripts/check_public_docs.py
python3 -m unittest discover -s tests -p 'test_*.py'
```

## Licensing and citation

Project-authored software is Apache-2.0 and project-authored explanatory
documentation is CC BY 4.0. Third-party images, metadata, and database content
remain governed by their file-specific or row-level rights information; byte
completeness is an integrity class, not a blanket licence.

See [CITATION.md](CITATION.md) for the four citation branches: QT26-QC queries,
reference-update states and tensors, Commons source data, and the paper's
scientific claims.

Liang Li (ORCID [`0009-0004-0467-7032`](https://orcid.org/0009-0004-0467-7032))
is the manuscript author and contact. Dataset and manuscript citations remain
separate in [CITATION.md](CITATION.md).
