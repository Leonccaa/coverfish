# Rights and licensing

## Project-authored licences

Project-authored verification and reproduction software is available under
Apache-2.0. Project-authored explanatory documentation is available under CC BY
4.0. The exact
scope and official licence texts are in [LICENSE.md](../LICENSE.md).

These are scoped grants, not a repository-wide data licence. They apply only where
the COVER-Fish project owns the relevant rights, do not automatically cover mixed
schemas/manifests unless explicitly marked, and do not override third-party image
or metadata terms.

Project-authored pointer-audit software and tests are covered by Apache-2.0.
The checked-in minimal pointer fixture is wholly synthetic Apache-2.0 test
material. This does not grant the same licence to production bindings,
manifests, URLs, receipts, or third-party rows.

## Row-level data semantics

COVER-Fish uses row-level rights evidence rather than one blanket dataset licence.

- `bytes` means an exact payload was selected and locally bound to its size and
  SHA-256 under the recorded row-level evidence. It is not a blanket relicense.
- `pointer` means metadata, attribution fields where approved, and an upstream
  locator only. It is not a redistributed image.
- `byte-complete` is a local integrity class, not a public-availability or legal
  conclusion.
- `pointer-reconstructable` requires release-time fetch and exact-hash evidence;
  the current count is zero.
- `pointer-fragile` means no current successful reconstruction receipt exists.

ANGFA remains pointer-only unless separate permission is obtained. Other source
packs can contain a mixture of byte and pointer rows. Aggregate source names or
counts never replace the row-level terms in an authorized release manifest.

QT26-QC retains the source licence and attribution on every query row; its dataset
card uses `license: other` because no single blanket licence replaces those terms.
The Reference-Update Benchmark publishes `rows-by-licence/` as row-ID selection
lists. Those lists make the rights ledger filterable after a user obtains the
relevant source archive; they are not separately downloadable image packs.

## Current repository boundary

This GitHub documentation baseline contains no third-party image bytes, source-row
manifests, upstream contributor identity values, embedding arrays, or private
database records. The canonical Zenodo records are separate mixed-rights data
surfaces; this GitHub baseline neither mirrors their payloads nor relicenses them.

The pointer tools retrieve bounded response bodies only for in-memory integrity
and diagnostic checks and discard them. A receipt records dated observations;
it does not redistribute the response body or grant permission to reuse it.

Before any data-facing file is added here, the public field allowlist, attribution
surface, source terms, and exact immutable release binding must pass review. Rights
classification is an engineering aid and not a substitute for source terms or legal
advice.
