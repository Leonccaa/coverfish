# COVER-Fish

**Auditable Reference-Gallery Updates and Cross-Domain Sign Reversal in Fish Recognition**

COVER-Fish asks whether a frozen vision foundation model can adapt fish recognition
through evidence updates rather than weight updates—and when the same operation can
fail. The study reconstructs four staged iNaturalist reference-gallery updates,
then tests later source layers while holding the encoder, preprocessing,
aggregation, queries, and scoring fixed.

[Dataset](https://huggingface.co/datasets/COVER-Fish/COVER-Fish) ·
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

## Public release

The current paper-aligned release is hosted at
[`COVER-Fish/COVER-Fish`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish):

- DOI: assigned only after this GitHub alignment commit is recorded in the release card;
- paper-aligned evidence commit: `8d17ddb7209870111719e871f4fc947576f8b8d1`;
- manuscript alignment: `REV035`;
- clean active gallery: 107,722 rows / 19,144 species prototypes; and
- archive surface: 117,640 records (75,253 byte-bearing; 42,387 pointer rows).

The release preserves the earlier 83 GB archives byte-for-byte and adds compact
REV035 analysis evidence under
[`supplements/analysis-evidence/`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish/tree/8d17ddb7209870111719e871f4fc947576f8b8d1/supplements/analysis-evidence).
Its deterministic archive contains 169 artifacts across 13 analysis modules;
`CLAIM-ARTIFACT-LEDGER.tsv` maps 14 manuscript claims to those files. The archive's
SHA-256 is
`a83cea63de116c6b895551401f55a97af9b38bcc750006063a217aae44022a01`.

The earlier base release remains citable as historical provenance at DOI
[`10.57967/hf/9706`](https://doi.org/10.57967/hf/9706), tag
`rev023-rc2-20260714`, and commit
`0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`. The dated pointer-audit supplement
remains fixed at tag `pointer-audit-v04-20260726` and commit
`70284660ee40128ff1d34ccec12e5c3e78f83f25`.

## Reproduce and verify

The base-release downloader remains pinned to the historical immutable base:

```bash
python3 scripts/download_release.py plan --profile smoke
```

To verify the compact paper-aligned analysis layer after downloading its six files:

```bash
python3 scripts/verify_analysis_evidence.py \
  --root supplements/analysis-evidence
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
- [Citation](CITATION.md)
- [Version map](VERSION.md)
- [`download_release.py`](scripts/download_release.py): pinned base-release downloader
- [`verify_analysis_evidence.py`](scripts/verify_analysis_evidence.py): REV035 evidence verifier
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

The current DOI citation is inserted only after the paper-aligned Hugging Face
snapshot is final. See [CITATION.md](CITATION.md) for the historical base citation
and the release-state distinction.

Liang Li (ORCID [`0009-0004-0467-7032`](https://orcid.org/0009-0004-0467-7032))
is the manuscript author and contact. Dataset and manuscript citations remain
separate in [CITATION.md](CITATION.md).
