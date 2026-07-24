# Reproduction

## Documentation check

Clone this repository and run:

```bash
python3 scripts/check_public_docs.py
```

The check verifies required files, repository-relative links, frozen headline
values, release identifiers, licence texts, and the absence of private paths or
service identifiers. It does not download data or recompute paper estimates.

## Data package

Use the immutable Hugging Face tag `rev023-rc2-20260714` or fixed revision
`0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`, rather than an unpinned moving branch.
The package README, file map, `SHA256SUMS`, and offline verifiers define the expected
inventory and integrity checks.

The public package separates:

| Layer | Contents |
| --- | --- |
| Metadata and views | Taxonomy, source/projection/view counts, rights tiers, fixed denominators, and hashes |
| Frozen arrays and row maps | Tensor identity, active selectors, embeddings, indices, and species prototypes |
| Compact replay evidence | Fixed query examples, top-k decisions, representative flips, and audit bindings |

The independent frozen-result audit reports 3,639/3,639 checks passing. It
reaggregates denominators, metrics, flips, and bootstrap point estimates from frozen
row-level outputs. It does not rerun image encoding or publish full dense
prediction/score matrices.

## Frozen computation contract

The paper's current recognition contract is:

1. map reference and mapped query targets to FishBase 25.04;
2. process full-frame images with the frozen native BioCLIP 2.5 ViT-H/14 processor;
3. L2-normalise image embeddings;
4. sum embeddings within each species and renormalise once to form an
   image-count-weighted species centroid;
5. rank every species centroid by exact query-centroid dot product; and
6. score the complete fixed query denominator at top-1, top-5, top-10, and top-20.

Approximate-nearest-neighbour search and image-level max pooling are not the frozen
paper method.

## Frozen checkpoints

- FishBase R0: 49,140 rows / 18,733 taxa;
- iNat-RG Archive: 32,656 / 618;
- iNat-RG-Pre: 30,796 / 615;
- retired temporal rows: 1,860;
- R0--R4 rows: 49,140 / 58,490 / 63,080 / 67,235 / 79,936;
- final clean gallery: 107,722 rows / 19,144 species centroids;
- D0 / Q-INT: 1,044 records;
- E0 / QT26-QC: 6,719 records; and
- archive tiers: 75,253 / 0 / 42,387.
