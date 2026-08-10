# COVER-Fish licence scope

COVER-Fish contains material with different rights. There is no single blanket
licence for every file, dataset row, image, name, attribution, or upstream record.

## Project-authored software

Unless a file carries a more specific notice, COVER-Fish verification and
reproduction software authored by the project, including code under `scripts/`
and `software/` and project-authored tests under `tests/`, is licensed under the
[Apache License 2.0](LICENSES/Apache-2.0.txt).

SPDX identifier: `Apache-2.0`.

The files under `fixtures/minimal-pointer-receipt-v04/` are fully synthetic,
contain no third-party row values, and are included as Apache-2.0 test material.

## Project-authored explanatory documentation

Unless a file carries a more specific notice, explanatory Markdown documentation
authored by the COVER-Fish project is licensed under
[Creative Commons Attribution 4.0 International](LICENSES/CC-BY-4.0.txt).

SPDX identifier: `CC-BY-4.0`.

When attribution is required, a reasonable form is:

> COVER-Fish documentation, Liang Li, 2026,
> <https://github.com/Leonccaa/coverfish>, CC BY 4.0.

## Data, media, manifests, and third-party material

The licences above apply only where the COVER-Fish project owns the relevant rights.
They do not relicense third-party images, metadata, database records, taxonomic
content, contributor names, attributions, URLs, or incorporated material.

The mixed-rights data records therefore use `license: other`. Row-level source,
provenance, creator/attribution, URL, licence, and rights fields are authoritative
for data records. `Byte-complete` describes integrity, not permission. `Pointer`
describes the absence of hosted image bytes, not a grant to fetch or reuse them.

Schemas, bindings, generated manifests, receipts, and mixed-content files are
covered only when their own notice or accompanying release documentation
explicitly assigns a licence. The synthetic fixture exception above does not
extend to production receipt rows. If a specific file conflicts with this
summary, the file-specific and row-level notices control.
