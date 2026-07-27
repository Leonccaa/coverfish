# Pointer-audit protocol v0.4

## Immutable scope

The audit measures dated availability for the `42,387` pointer rows in the
REV023-RC2 archive: `41,945` active and `442` retired. It does not alter the
DOI-bound dataset, query or gallery membership, prototypes, embeddings,
predictions, scores, benchmark rosters, or frozen reproducibility tiers.

The fixed two-shard schedule closes as `21,195 + 21,192 = 42,387` rows. The
shards are selected from the frozen schedule by lane ordinal modulo two; they
are disjoint and their union is the full archive. R0/S1 contributes
`17,044 + 17,044 = 34,088` pointer rows.

## Outcome classes

- `byte_exact`: retrieved SHA-256 equals the frozen expected SHA-256.
- `visual_near_candidate_d0_2`: SHA-256 differs, the response is a valid image,
  and canonical 64-bit pHash distance is zero through two.
- `visual_related_candidate_d3_6`: SHA-256 differs, the response is a valid
  image, and pHash distance is three through six.
- `content_changed_candidate_d_gt6`: SHA-256 differs, the response is a valid
  image, and pHash distance is greater than six.
- Other terminal classes preserve the exact transport, policy, decode, or
  protocol outcome; a failed observation is never promoted silently.

The three pHash classes are diagnostic candidates only. They never become
byte-exact, never enter the exact-recovery numerator, and never change a frozen
package tier.

## Requests and response bytes

Only configured HTTPS routes and roles are eligible. Redirects are bounded and
each target is rechecked against scheme, address, host role, policy, and robots
rules. Sensitive query values, non-public resolved addresses, and unallowlisted
routes fail closed.

Responses are bounded by request time, response size, decoded dimensions, and
decoded pixel count. Bytes are held in memory for magic, SHA-256, and optional
image/pHash validation, then discarded. Receipts record
`bytes_retained=false`; response bodies and reconstructed image trees are not
receipt members.

## Attempts, retries, and finality

Every physical request has a unique attempt ID. A completion row binds the
committed attempt range for one record, window, and invocation. Interrupted or
uncommitted attempts remain visible in disposition evidence and cannot be
selected as successful observations.

Transient and non-exact observations follow the dated retry protocol. Finality
requires the configured number of distinct windows and UTC dates, the minimum
elapsed interval, and a declared final-window observation. Exact matches use
`not_applicable_exact`; completed non-exact rows use `satisfied` only when the
retry contract is met. Until then, scientific status remains `PENDING`.
Physical or robots safety-review evidence takes precedence and produces
scientific `FAIL` even when other counters are internally consistent.

## Offline verification

The single-receipt wrapper verifies hashes, schemas, fixed roots, membership,
attempt/completion transactions, recovery evidence, derived health and summary
rebuilds, and output-tree closure in a private copy. The pair verifier checks
both wrapper results, fixed shard membership, disjoint completion IDs, full
union coverage, active/retired closure, and aggregate outcome and attempt
counters. Neither verifier accesses the network or writes to a source receipt.

If an image request finishes just after a cached robots decision expires, the
wrapper accepts that boundary only when the next receipt records the same
robots state within one configured request timeout. No changed state or other
base-verifier failure is adapted.

Top-level pair `status=PASS` means the evidence is structurally and
cryptographically consistent. `scientific_status` separately reports the dated
outcome. A scientifically `PENDING` or `FAIL` result must not be rewritten as
integrity failure or success.

## Expected duration

An initial complete audit generally takes about four to five days as one run,
or about two to three days in fixed two-shard mode. Transient failures and the
dated retry protocol can extend that range. Exact commands are in
[Pointer reconstruction audit](POINTER_AUDIT.md).
