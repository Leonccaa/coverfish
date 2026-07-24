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

## Invariants

- Dataset revision: `0ee47b20fc0e767c8b3b9ef07ab55b37ac80b2f8`.
- Dataset tag: `rev023-rc2-20260714`.
- Dataset DOI: `10.57967/hf/9706`.
- BioCLIP revision: `191d741545e4c741cdef4b22c6eb69c945c1e592`.
- Q-INT/D0 is the development benchmark; QT26-QC/E0 is the fixed primary
  evaluation benchmark. Do not pool them.
- Pointer rows are metadata and locators, not hosted image bytes.
- Do not substitute a moving branch, alternate model snapshot, crop, approximate
  index, or image-level max-pooling rule.

## Machine interface

- Operational stdout is one compact JSON document with sorted keys.
- `plan` is network-free and does not create its destination.
- Commands never prompt and never discover or print credentials.
- Status is `PASS`, `PENDING`, `FAIL`, or `ERROR`.
- A successful `plan` may exit `0` with `PENDING`; always inspect `status` and the
  reported confirmation requirement before the next action.
- Exit codes are stable: `0` success, `2` usage, `3` dependency, `4` safety gate,
  `5` transfer/runtime, `6` integrity, and `7` numerical comparison.
- `--help` and `--version` are human-readable exceptions to the JSON stdout rule.
- For S4, use `S4-PARTS.tsv` and `verify_s4_reassembly.sh`; never infer reassembly
  order from a JSON file list or transfer order.

See [Reproduction](docs/REPRODUCTION.md) for complete commands and scope.
