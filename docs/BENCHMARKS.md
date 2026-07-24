# Benchmark roles

Benchmark names in COVER-Fish identify different scientific roles. They are not
successive versions of one test set, and their numbers must not be pooled.

| Object | Frozen denominator | Role in the paper | Public availability now |
| --- | ---: | --- | --- |
| Fishial1300 | 1,300 = 1,200 positives + 100 distractors | Historical engineering initialization and spillover discovery | Not distributed here |
| Q-INT / D0 | 1,044 = 946 positives + 98 distractors; 562 positive target taxa | Development-only encoder and aggregation selection; open-set diagnostic | Included in the public tagged Hub package |
| QT26-QC / E0 | 6,719 queries / 3,121 taxa | Fixed primary current evaluation roster | Included in the public tagged Hub package |
| Broad QT26 | 133,041 queries / 4,264 taxa | Query-frequency-imbalanced sensitivity analysis | Not the current public-package object |
| Fishial matched subset | 1,931 queries / 665 species | Native-pipeline system comparison | Predictions are not distributed here |
| FishNet | 18,901 fixed natural-photo queries | Third-party transfer benchmark | Upstream benchmark; not repackaged here |
| Fish-Vista | 8,699 fixed museum queries / 1,517 labels | Third-party failure domain and post-freeze mechanism diagnostic | Upstream benchmark; not repackaged here |
| WildFish | 20,394 fixed queries | Supplementary legacy-web stress test | Upstream lineage; not repackaged here |

## Q-INT

Q-INT selects the frozen recognition pipeline. Its 946 positive fish queries include
376 Fishial-facing and 570 non-Fishial queries; 98 distractors support a
development-only open-set diagnostic. Five predeclared encoders were compared with
instance-max, after which the winning BioCLIP 2.5 engine was compared under
instance-max and species-centroid aggregation. The centroid was then frozen before
the current evaluation.

Q-INT is a development benchmark included in the public tagged package, not a
hidden headline test. Its public availability does not change its development-only
scientific role.

## QT26-QC

QT26-QC is the primary quality-controlled, stratified roster. It keeps no more than
five queries per represented taxon and retains unmapped or structurally unsupported
queries in the full denominator as wrong.

The temporal claim is narrow and image-specific. Active iNat-RG-Pre evidence must
pass three pre-1-March-2026 image clocks, while QT26-QC observations begin after the
cutoff. This does not date FishBase, later source layers, or BioCLIP pretraining, and
it does not claim a prospectively sealed 1 March experiment.

The separate broad QT26 roster is severely query-frequency imbalanced and is a
sensitivity analysis. Its larger query count does not provide a larger number of
independent species-level units, and its contrast with QT26-QC does not identify the
quota rule as a causal explanation.

## Fishial

Fishial is an offline specialist comparator. The paper fixes its v0.10.2 classifier
bundle and official detector/segmenter preprocessing. COVER-Fish and Fishial retain
their native pipelines, so the comparison is end-to-end rather than encoder-only.
Fishial is stronger overall at top-1; COVER-Fish is stronger at top-5 and top-20 on
the fixed matched subset.

## External rosters

FishNet and Fish-Vista test bounded transfer under both their cleaned native-gallery
protocols and COVER-Fish source-layer views. Neither benchmark contributes images to
the COVER-Fish source packs or embedding index. Fish-Vista's museum domain exposes
the Commons/single-centroid failure. WildFish is supplementary because its mixed
web-image lineage cannot support a clean directional migration claim and it did not
participate in method or pack selection.

## Metrics and denominators

- Query-micro accuracy is the fraction of correct queries over the complete fixed
  query roster.
- Species-macro accuracy first computes accuracy per target species and then gives
  each species equal weight, including species with zero gallery coverage.
- Top-1, top-5, top-10, and top-20 are reported where frozen.
- Structural coverage is reported separately and never used to shrink the accuracy
  denominator.
- Current intervals use 20,000 target-species-cluster bootstrap replicates. Marginal
  rounds, depth strata, and external-roster intervals are nominal; no family-wise
  multiplicity correction is claimed.

## Identity-audit boundary

Q-INT and QT26-QC have no exact overlap in available SHA-256, photo identity,
observation identity, exact pHash, or pHash pairs within Hamming distance six.
External benchmark views remove fixed query identities only from derived gallery
views, never from immutable source packs.

These guarantees are source-conditional. FishBase and several other sources do not
provide the complete observation/photo/contributor identity keys available for
iNaturalist. The audit must not be summarized as a universal five-key zero-overlap
claim, and it says nothing conclusive about encoder pretraining exposure.
