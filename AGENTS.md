# Agent operating contract

This repository exposes deterministic, non-interactive commands for automated
clients. Treat the release identity, scientific objects, and benchmark roles as
frozen.

## Safe order of operations

1. Run `python3 scripts/download_release.py plan --profile smoke`.
2. Parse the single JSON object from stdout. Logs and progress belong to stderr.
3. Download only the requested profile. Never add `--accept-large-download` unless
   the operator has approved the reported byte count and destination capacity.
4. Run `verify` and require `status == "PASS"` before extracting an archive.
5. Extract CORE and D0 into separate directories.
6. Run `verify_bioclip_pipeline.py plan` before allowing the approximately 3.95 GB
   pinned model transfer.
7. Treat the model result as a one-record pipeline smoke test, not as a full-paper
   reproduction.
8. Keep the default CPU execution. Never request `--device cuda` without explicit
   operator approval for accelerator use.
9. Treat `download_release.py` as a downloader for the historical immutable base;
   it does not fetch the later analysis-evidence supplement.
10. For REV035 claim evidence, download only
    `supplements/analysis-evidence/*` at revision
    `4e437b6a2bf5f9a12a200bbe3a93411fe713db1f`, then run
    `python3 scripts/verify_analysis_evidence.py --root supplements/analysis-evidence`.

## Pointer-audit order of operations

1. Run the synthetic fixture verifier before downloading data:
   `python3 software/verify_pointer_audit_minimal_fixture_v04.py --root fixtures/minimal-pointer-receipt-v04`.
2. Download and verify only the `core` profile, then run its offline package
   verifier after extraction.
3. Run `software/reconstruct_pointers.py plan` and require archive closure at
   `42,387 = 41,945 active + 442 retired` before permitting network access.
4. Use `software/reconstruct_pointers_parallel_v04.py` for live work. Always pass
   `--policy config/host-policy-fishbase-2s-v2.json` explicitly and require
   `--accept-network` from the operator.
5. Keep every receipt, runtime directory, and generated pilot manifest outside
   Git history. Never retain response bodies or build a reconstructed image tree.
6. For two-shard mode, use exactly shard count `2` with indices `0` and `1`.
   Preserve the same shard count and index across resumes and retries.
7. Never reuse a window ID. A final non-exact observation requires the dated
   retry protocol described in `docs/POINTER_AUDIT.md`.
8. Verify each closed receipt offline, then verify the disjoint pair. Treat
   receipt integrity status and `scientific_status` as separate fields.
9. Pointer-audit commands do not require a GPU; do not add accelerator use.
10. For the published reference receipts, resolve immutable tag
    `pointer-audit-v04-20260726`, download only
    `supplements/pointer-audit-v04-20260726/*`, and require
    `sha256sum -c SHA256SUMS` to pass.

## Invariants

- Current paper-aligned dataset snapshot:
  `4e437b6a2bf5f9a12a200bbe3a93411fe713db1f`.
- Current dataset DOI: `10.57967/hf/9776`.
- Analysis-evidence archive SHA-256:
  `a83cea63de116c6b895551401f55a97af9b38bcc750006063a217aae44022a01`.
- Historical base revision: `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`.
- Historical base tag: `rev023-rc2-20260714`.
- Historical base DOI: `10.57967/hf/9706`.
- Pointer-audit supplement tag: `pointer-audit-v04-20260726`.
- Pointer-audit supplement revision:
  `70284660ee40128ff1d34ccec12e5c3e78f83f25`.
- BioCLIP revision: `191d741545e4c741cdef4b22c6eb69c945c1e592`.
- Q-INT/D0 is the development benchmark; QT26-QC/E0 is the fixed primary
  evaluation benchmark. Do not pool them.
- Pointer rows are metadata and locators, not hosted image bytes.
- Archive pointer closure is `42,387`; active closure is `41,945`; retired
  closure is `442`; E0 has `6,719` byte-complete rows and zero pointer rows.
- A dated pointer result does not alter the frozen RC2 tier counts.
- Do not substitute a moving branch, alternate model snapshot, crop, approximate
  index, or image-level max-pooling rule.

## Machine interface

- Operational stdout is one compact JSON document with sorted keys.
- `plan` is network-free and does not create its destination.
- Commands never prompt and never discover or print credentials.
- Status is `PASS`, `PENDING`, `FAIL`, or `ERROR`.
- A successful `plan` may exit `0` with `PENDING`; always inspect `status` and the
  reported confirmation requirement before the next action.
- Pointer receipt and pair verifiers use exit `0` for `PASS` or honest
  `PENDING`, and exit `6` for integrity `FAIL`.
- Exit codes are stable: `0` success, `2` usage, `3` dependency, `4` safety gate,
  `5` transfer/runtime, `6` integrity, and `7` numerical comparison.
- `--help` and `--version` are human-readable exceptions to the JSON stdout rule.
- For S4, use `S4-PARTS.tsv` and `verify_s4_reassembly.sh`; never infer reassembly
  order from a JSON file list or transfer order.

See [Reproduction](docs/REPRODUCTION.md) and [Pointer
audit](docs/POINTER_AUDIT.md) for complete commands and scope.
