# Pointer reconstruction audit

This workflow measures the dated availability of the `42,387` pointer rows in
the immutable COVER-Fish archive. It verifies response bytes in bounded memory
and discards them; it does not create or retain a reconstructed image tree.

The audit is bound to:

- dataset revision `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`;
- tag `rev023-rc2-20260714`;
- DOI `10.57967/hf/9706`; and
- `41,945` active plus `442` retired pointer rows.

It does not modify any dataset row, query, gallery membership, prototype,
embedding, prediction, score, benchmark roster, or frozen reproducibility tier.

## Dated receipt result

The published 2026-07-26 receipt closes all 42,387 archive pointers and
all 41,945 active pointers. Integrity is `PASS`; scientific status is
`PENDING` because policy-bound observations remain pending rather than being
reported as retrieval success or failure.

| Outcome | Archive rows | Active rows |
|---|---:|---:|
| SHA-256 byte-exact | 41,917 | 41,476 |
| HTTP 404/410 | 18 | 17 |
| Permission-policy pending | 451 | 451 |
| pHash distance 0--2 diagnostic candidate | 1 | 1 |
| Total | 42,387 | 41,945 |

The strict byte-exact rates are 98.8912% for the archive and 98.8819% for the
active set. The pHash row is diagnostic and is not counted as byte-exact.
R0/S1 is 34,088/34,088 byte-exact. E0/QT26-QC remains independently
6,719/6,719 byte-complete with no pointer-dependent row. This dated observation
does not retroactively upgrade the frozen RC2 tier table.

A direct-only second observation ran from `2026-07-27T04:49:28Z` through
`2026-07-27T04:50:11Z`. It selected the same 18 HTTP 404/410 rows and made one
new direct-image request per row; all 18 again returned HTTP 404. It attempted
no resolver or landing page, retained no response bytes, and used no GPU. This
supplemental observation does not alter the frozen tier table or terminal
receipt pair.

## Published receipts

The complete receipt and the direct-only recheck are published on Hugging Face:

- supplement tag: [`pointer-audit-v04-20260726`](https://huggingface.co/datasets/COVER-Fish/COVER-Fish/tree/pointer-audit-v04-20260726/supplements/pointer-audit-v04-20260726);
- supplement revision: `70284660ee40128ff1d34ccec12e5c3e78f83f25`;
- complete receipt: `coverfish-pointer-audit-v04-receipts-rc1-20260726.tar`,
  81,162,240 bytes, SHA-256
  `d754587cfed6fd42277192d0887ad6413fd5343d10a926c42145df9bad665181`;
- direct recheck: `coverfish-pointer-404-direct-recheck-local-rc1-20260726.tar`,
  51,200 bytes, SHA-256
  `bfd7afccb335605e2d279f89abb574dc1b0fca4d1628e3aa5365c432739a0493`.

Download only this supplement with the Hugging Face CLI, then verify both
payloads locally:

```bash
hf download COVER-Fish/COVER-Fish \
  --repo-type dataset \
  --revision pointer-audit-v04-20260726 \
  --include "supplements/pointer-audit-v04-20260726/*" \
  --local-dir coverfish-pointer-audit-supplement

cd coverfish-pointer-audit-supplement/supplements/pointer-audit-v04-20260726
sha256sum -c SHA256SUMS
```

The historical DOI `10.57967/hf/9706` continues to identify the frozen base dataset.
No separate DOI was minted for this dated supplement; the later paper-aligned DOI
incorporates the receipt files without changing them.

## Offline smoke test

The checked-in two-row fixture is synthetic and requires no data download,
network access, optional package, or GPU:

```bash
python3 software/verify_pointer_audit_minimal_fixture_v04.py \
  --root fixtures/minimal-pointer-receipt-v04
```

Success exits `0` and emits one JSON object with `"status":"PASS"`. The
fixture checks the production-shaped attempt, completion, and health columns;
it is not a substitute for verifying a full receipt.

## Download the frozen CORE package

The pointer manifests are in the `core` profile. Image archives are not needed
to start the audit. From this repository:

```bash
python3 -m pip install "huggingface_hub>=0.34,<2"

python3 scripts/download_release.py plan \
  --profile core \
  --output coverfish-release

python3 scripts/download_release.py download \
  --profile core \
  --output coverfish-release

python3 scripts/download_release.py verify \
  --profile core \
  --output coverfish-release
```

`plan` and `verify` are network-free. `download` is pinned to the immutable
revision and verifies exact size and SHA-256. The `core` profile contains nine
files totalling `499,616,679` bytes.

Extract CORE into a new directory and run its own offline verifier:

```bash
mkdir -p artifacts/coverfish-core

tar --zstd -xf \
  coverfish-release/coverfish-rev023-core-metadata-index-code-rc2-20260714.tar.zst \
  -C artifacts/coverfish-core

python3 artifacts/coverfish-core/software/verify_full_surface_rc.py \
  --root artifacts/coverfish-core
```

## Prepare the exact environment

Use CPython `3.10.12` and the locked dependencies:

```bash
test "$(python3.10 --version)" = "Python 3.10.12"
python3.10 -m venv pointer-audit-work/venv
pointer-audit-work/venv/bin/python -m pip install \
  --requirement requirements-pointer-audit-parallel-v04.txt

export PY=pointer-audit-work/venv/bin/python
export SOURCE=artifacts/coverfish-core
export POLICY=config/host-policy-fishbase-2s-v2.json
export BINDINGS=inputs/input-bindings.json
```

Before permitting network access, validate the fixed inputs and counts:

```bash
"$PY" software/reconstruct_pointers.py plan \
  --source-root "$SOURCE" \
  --bindings "$BINDINGS"
```

The plan must close at `42,387` archive pointer rows, comprising `41,945`
active and `442` retired rows. E0/QT26-QC is separately `6,719/6,719`
byte-complete and has zero pointer-dependent rows.

The serial producer is the scientific core used for offline planning. Use the
v0.4 runner below for live archive work, with the policy path stated explicitly.

## Complete single-run mode

Use a new output directory. Network access remains disabled unless the command
contains `--accept-network`.

```bash
export RECEIPT=pointer-audit-work/archive-v04-single
export XDG_RUNTIME_DIR=pointer-audit-work/runtime-single
install -d -m 700 "$XDG_RUNTIME_DIR"
test ! -e "$RECEIPT"

"$PY" software/reconstruct_pointers_parallel_v04.py \
  --source-root "$SOURCE" \
  --bindings "$BINDINGS" \
  --policy "$POLICY" \
  --scope archive \
  --output-dir "$RECEIPT" \
  --window-id archive-single-w1 \
  --retry-mode none \
  --shard-count 1 \
  --shard-index 0 \
  --accept-network
```

Allow approximately **4--5 days** for an initial complete run. This is a
planning estimate; transient responses and later dated retry windows can extend
the calendar duration.

## Fixed two-shard mode

For a shorter wall-clock run, run the following command once with `SHARD=0` and
once with `SHARD=1`. Each shard needs the same verified inputs and byte-identical
audit files, plus its own receipt and runtime directory.

```bash
export SHARD=0  # repeat with SHARD=1 for the complementary shard
export RECEIPT="pointer-audit-work/archive-v04-shard-${SHARD}-of-2"
export XDG_RUNTIME_DIR="pointer-audit-work/runtime-shard-${SHARD}-of-2"
install -d -m 700 "$XDG_RUNTIME_DIR"
test ! -e "$RECEIPT"

"$PY" software/reconstruct_pointers_parallel_v04.py \
  --source-root "$SOURCE" \
  --bindings "$BINDINGS" \
  --policy "$POLICY" \
  --scope archive \
  --output-dir "$RECEIPT" \
  --window-id "archive-shard-${SHARD}-w1" \
  --retry-mode none \
  --shard-count 2 \
  --shard-index "$SHARD" \
  --accept-network
```

Do not add `--component` or `--max-rows` to a two-shard population run. The
fixed partition is `21,195 + 21,192 = 42,387`, is disjoint, and is computed
before retry eligibility.

Allow approximately **2--3 days** for the initial fixed two-shard workflow.
Transient responses and later dated retry windows can extend that estimate.

## Resume and dated retries

After an interruption, resume the same receipt with the same shard count and
index, `--retry-mode none`, and a new `--window-id`. Never reuse a window ID.

Rows that remain non-exact require later observation windows:

```text
window 1: --retry-mode none
window 2: --retry-mode nonexact
window 3: --retry-mode nonexact --final-window
```

The closing observation must occur on a third distinct UTC date and at least 48
hours after the first counted observation. Each invocation appends evidence; it
does not erase an earlier attempt.

## Verify one receipt offline

After an invocation closes cleanly, verify its receipt without network access:

```bash
"$PY" software/verify_pointer_receipt_parallel_v04.py \
  --source-root "$SOURCE" \
  --bindings "$BINDINGS" \
  --policy "$POLICY" \
  --scope archive \
  --audit-dir "$RECEIPT" \
  --output "$RECEIPT/independent-checks.tsv"
```

Run this separately for each shard in two-shard mode. Receipt integrity `PASS`
does not mean that every pointer was byte-exact; inspect scientific status
separately.

## Verify a two-shard union offline

After both receipts close, write the pair report outside both receipt trees:

```bash
"$PY" software/verify_pointer_receipt_pair_v04.py \
  --source-root "$SOURCE" \
  --bindings "$BINDINGS" \
  --policy "$POLICY" \
  --shard-0-dir pointer-audit-work/archive-v04-shard-0-of-2 \
  --shard-1-dir pointer-audit-work/archive-v04-shard-1-of-2 \
  --output pointer-audit-work/pointer-pair-verification.json
```

The pair verifier rebuilds the fixed schedule, verifies each receipt in a
private temporary copy, and checks disjoint union coverage. It does not open the
network or modify either source receipt.

## Machine contract

- Operational stdout is one JSON object; progress belongs on stderr.
- `PASS` and `PENDING` exit `0`; always inspect the JSON `status`.
- Integrity `FAIL` exits `6`.
- `scientific_status` is separate from structural and cryptographic integrity.
- Response bodies are never receipt members and every health row records
  `bytes_retained=false`.
- No pointer-audit command requires or uses a GPU.

See [Pointer-audit protocol](POINTER_AUDIT_PROTOCOL.md) for classification and
finality semantics and [Source policy](POINTER_SOURCE_POLICY.md) for request
boundaries.
