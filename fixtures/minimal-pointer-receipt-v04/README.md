# Minimal offline pointer receipt fixture

This deterministic two-row fixture is a production-shaped schema-and-
aggregation smoke test. It uses the complete authoritative health, attempt, and
completion column layouts, contains no downloaded image bytes, and performs no
network access.

The two outcomes are `byte_exact` and `visual_near_candidate_d0_2`. The latter
is a pHash diagnostic only: it is not byte-exact, never enters the exact
numerator, and keeps the fixture's `scientific_status` at `PENDING`.

From the package root, run:

```bash
python3 software/verify_pointer_audit_minimal_fixture_v04.py \
  --root fixtures/minimal-pointer-receipt-v04
```

The one-line JSON output must match `EXPECTED-RESULT.json` semantically and
report `"status":"PASS"`. This smoke test does not replace the frozen
production single-receipt or pair verifiers.
