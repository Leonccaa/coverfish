# Scientific scope

## Paper-aligned question

The companion study asks whether a frozen vision foundation model can adapt fish
recognition through governed reference-gallery updates without retraining, and
whether the same update can reverse sign under a different source, aggregation
rule, or query domain.

It does not claim to invent embedding retrieval, nearest-neighbour classification,
or species prototypes. Its contribution is the longitudinal fish-domain evidence,
the measured cross-domain failure, and the versioned substrate needed to inspect,
accept, reject, and roll back gallery updates.

## Frozen data object

FishBase 25.04 is the only taxonomy namespace. The accepted taxonomy master contains
36,130 species. The paper's canonical source sequence is:

```text
FishBase R0
  -> + iNat-RG-Pre R1
  -> + iNat-RG-Pre R2
  -> + iNat-RG-Pre R3
  -> + iNat-RG-Pre R4
  -> + USFWS
  -> + ANGFA
  -> + Wikimedia Commons
```

| State or pack | Added rows | Clean rows after the state | Taxa where frozen |
| --- | ---: | ---: | ---: |
| FishBase R0 | 49,140 | 49,140 | 18,733 |
| iNat-RG-Pre R1 | 9,350 | 58,490 | 18,737 |
| iNat-RG-Pre R2 | 4,590 | 63,080 | 18,739 |
| iNat-RG-Pre R3 | 4,155 | 67,235 | 18,749 |
| iNat-RG-Pre R4 | 12,701 | 79,936 | 18,839 |
| USFWS | 1,044 source rows | 80,979 | 18,841 |
| ANGFA | 451 source rows | 81,429 | 18,890 |
| Wikimedia Commons | 26,586 source rows | 107,722 | 19,144 |

The immutable iNat-RG Archive contains 32,656 rows across 618 taxa. Its active
iNat-RG-Pre projection retains 30,796 rows across 615 taxa and retires 1,860 rows
without rewriting the archive. Each active iNaturalist row must have observation
date, observation creation, and exact-photo availability evidence before 1 March
2026. QT26-QC observations begin after that cutoff.

R4 is exactly `49,140 + 30,796 = 79,936`. The 295 lower-priority cross-source
duplicate removals occur only in later layers: one at USFWS, one at ANGFA, and 293
at Commons.

## Frozen recogniser

The method-selection step on Q-INT freezes:

- BioCLIP 2.5 ViT-H/14;
- the model's native full-frame processor;
- L2-normalised image embeddings;
- one image-count-weighted, renormalised centroid per species; and
- exact cosine-equivalent dot-product ranking over species prototypes.

For species `s`, the frozen prototype and score are:

```text
c_s = normalize(sum of L2-normalised reference embeddings for species s)
score(s | q) = q dot c_s
```

The final state has 19,144 species prototypes. The paper does not benchmark FAISS
or another approximate nearest-neighbour index, and it does not use instance-max as
the frozen primary aggregation. Historical repository descriptions that did so are
not descriptions of the current paper system.

## Current effects

On 6,719 QT26-QC queries across 3,121 taxa, R0 to R4 changes:

| Metric | R0 | R4 | Paired change, percentage points |
| --- | ---: | ---: | ---: |
| Query-micro top-1 | 42.42% | 43.77% | +1.35, 95% CI +0.30 to +2.43 |
| Species-macro top-1 | 37.17% | 38.89% | +1.72, 95% CI +0.77 to +2.70 |
| Query-micro top-5 | — | — | +3.33 |
| Species-macro top-5 | — | — | +3.90 |
| Query-micro top-20 | — | — | +2.84 |
| Species-macro top-20 | — | — | +3.16 |

A post-freeze explanatory partition closes the net top-1 effect as 362 additional
correct queries on represented intervention taxa and 271 lost correct queries on
untouched taxa, for `+362 - 271 = +91`. This is competitive-spillover accounting,
not a randomized estimate of targeted versus untargeted acquisition.

The same-final-pool order control does not support an early-budget advantage for the
historical R1-R3 ordering. Target-only removal shows sensitivity to retained
true-species evidence while competitor prototypes remain fixed; it does not prove
that every added image helps or identify an optimal image count.

## Fishial comparison boundary

Fishial v0.10.2's 866 raw classes resolve to 775 distinct scientific names. The
only system-to-system comparison fixes that candidate universe and contains 1,931
queries from 665 target species. Fishial and COVER-Fish retain their native
end-to-end preprocessing.

| Endpoint inside the shared 775-name universe | COVER-Fish R4 | Fishial | Paired difference, points |
| --- | ---: | ---: | ---: |
| Query-micro top-1 | 67.89% | 67.63% | +0.26, 95% CI -2.90 to +3.43 |
| Species-macro top-1 | 65.20% | 65.47% | -0.27, 95% CI -3.75 to +3.16 |

Neither interval resolves a winner, and neither is an equivalence test. In the
historical depth bands, 0--20 references contribute -78 net correct answers across
1,407 queries, while 21+ contributes +83 across 524. These bands describe effect
modification; `51+` is a historical label rather than a causal threshold.

Opening COVER-Fish to its actual 18,839-prototype R4 candidate field lowers its own
query-micro top-1 on the same 1,931 queries to 56.34%. This is a COVER-Fish
candidate-space sensitivity, not a system comparison: Fishial has no predictions
outside its sealed vocabulary. Correspondingly, the other 4,788 QT26-QC queries
have targets outside Fishial's vocabulary. COVER-Fish provides a target prototype
for 4,696 of them and identifies 1,853 correctly at top-1; those values quantify
expanded task support, not comparative accuracy.

The bounded comparison does not isolate encoder quality because Fishial uses its
official segment-bbox pipeline while COVER-Fish uses a BioCLIP full frame.

## Cross-domain failure and rollback

The Commons step adds +7.49 query-micro and +5.98 species-macro top-1 points on
QT26-QC, but the same frozen centroid transition reduces Fish-Vista museum-query
top-1 by -2.16 micro and -3.52 macro points. A frozen-embedding factorial assigns
nearly all of the natural-photo change to updating existing prototypes (+5.95
species-macro), while the same operation reproduces the complete -3.52-point
museum-domain loss; label expansion contributes almost none of either transition.
Commons lacks the iNaturalist
image-time wall and has weaker species-identity assurance, so its natural-photo gain
is a fixed-order scale/stress result, not a source-isolated causal estimate.

Source-separated and instance-level alternatives are diagnostics, not validated
defaults. Under the frozen centroid pipeline, a failed specimen-domain gate means
reject or roll back the Commons transition.

## Evidence roles

| Role | Evidence | Permitted interpretation |
| --- | --- | --- |
| Frozen current effects | QT26-QC R0-R4, sequential flips, order control, target-only removal, fixed source ladder, frozen Fish-Vista loss | Exact effects within the frozen data, method, and scoring contracts |
| System comparison | Fixed Fishial intersection with native pipelines | End-to-end top-k comparison, not encoder-only causality |
| Post-freeze diagnostic | Spillover partition, depth sensitivity, Fish-Vista factorial and alternative aggregators, broad QT26 | Bounded mechanism and sensitivity language |
| Historical development | Demand logs, Fishial400/1300, Q-INT, earlier crop/open-set/QC work | Design lineage and method selection only |
| Substrate | Manifests, projections, schemas, hashes, rights tiers, rollback | Reproducibility and reversibility, not accuracy by documentation |

## Known limits

- The intervention targets are development-coupled and cover only a small part of
  the FishBase breadth base.
- Query-gallery identity separation does not establish BioCLIP pretraining
  separation.
- Open-set rejection remains unsolved.
- Commons photo suitability is not taxonomic verification.
- Exact-ranking resource figures are not a complete deployment or concurrency
  benchmark.
- The paper-aligned dataset snapshot is publicly accessible at commit
  `8d17ddb7209870111719e871f4fc947576f8b8d1`; its current DOI is
  finalized after the release card records the GitHub alignment commit. The earlier
  base remains historical provenance at `10.57967/hf/9706`.
