# Pointer audit schema catalog

Schema family: `coverfish.pointer-audit.v2`

The authoritative machine-readable catalog is
`schema/pointer-audit-schema.json`. TSV files use UTF-8, one header row, tab
separators, and LF line endings. Boolean TSV values are lowercase strings
`true`/`false`; absent values are empty strings. JSON booleans and integers use
native JSON types.

The schema-family version is the shared scientific receipt vocabulary, not the
executor version. Two execution profiles are accepted:

| Executor | Output contract | Additional execution evidence | Verifier |
|---|---|---|---|
| frozen serial producer v0.2.0 | `coverfish.pointer-audit-contract.v1` | none | `verify_pointer_receipt.py` |
| process runner v0.3.0 | `coverfish.pointer-audit-contract.v2` | immutable profile plus one execution summary per invocation | `verify_pointer_receipt_parallel_v03.py` |

Existing v0.2 output directories remain immutable contract-v1 receipts. The
v0.3 runner may use only a new empty output directory; the two contracts must
never be pooled, converted in place, or appended to one another.

## Transaction invariant

The request layer is append-only and the record layer is derived. An attempt is
committed exactly when its `(record_id, window_id, attempt_index)` lies inside
one valid `record-completions.tsv` range from the same invocation. A disposition
and a completion are mutually exclusive for an attempt.

Only committed attempts may contribute to `pointer-health.tsv` and strict
rates. Physical attempts, including abandoned rows, remain visible in the
summary's physical-attempt totals.

The derived best image attempt is selected by identity priority and then by
numeric `attempt_index`. It never uses the second-resolution UTC timestamp as a
tie breaker.

In v0.2 each fully evaluated attempt row is append-fsynced to the canonical
ledger before the next request. In v0.3 a worker append-fsyncs evaluated receipt
rows to its private per-record spool; the coordinator alone canonicalizes
robots evidence, attempts, and then the completion, and replacement work is not
submitted for that worker until canonical fsync. No response bytes enter the
spool or IPC.

There is an unavoidable boundary between receipt of a response and completion
of its evaluation/fsync; a crash inside that boundary cannot produce a complete
attempt row. A completion is therefore the record-level commit, while an
uncommitted attempt remains physical history only.

The inclusive completion range is logically contiguous in numeric
`attempt_index` for its record/window/invocation; it does not make global file
position or global timestamp order a scientific invariant. V0.3 canonical
physical order follows worker-completion commit events. Deterministic IDs,
record-local attempt order, frozen scope binding, and completion/disposition
coverage define the logical receipt and derived output.

## `pointer-health-attempts.tsv`

Primary key: `attempt_id`. It is the first 24 hexadecimal characters of
SHA-256 over `record_id NUL window_id NUL attempt_index`.

One row records one direct image, resolver, or fallback image request. The
ordered header is specified exactly in the JSON catalog. Important groups are:

- identity: record, component, active flag, window, invocation, attempt index,
  and UTC timestamp;
- request: kind, resolution path, requested/final URL, host, redirect chain;
- authority: policy state and robots decision;
- transport: status, HTTP status, Retry-After, Content-Type, and byte count;
- image diagnostics: magic, decode status, dimensions, actual SHA-256, optional
  pHash/distance, SHA match, and identity class; and
- invariant: `bytes_retained=false`.

For `byte_exact`, `sha256_match=true`, `decode_status=skipped_byte_exact`, and
actual pHash/distance remain empty. A pHash class requires
`sha256_match=false`; it is diagnostic only. `empty_response` is retryable.
Non-exact image decode is capped at `25,000,000` pixels and `6,000` pixels on
either dimension, above the frozen known maxima of approximately `12.8` million
pixels / `4,384` pixels but below the earlier permissive engineering ceiling.
Resolver responses never count as image recovery.

This schema describes verification fetch-and-discard receipts, not a
rights-filtered image materializer. No artifact in this catalog is a
reconstructed image tree, and row-level rights remain outside the strict
byte-identity measurement.

## `record-completions.tsv`

Primary key: `completion_id`. A row atomically commits one record transaction by
binding a logically contiguous inclusive attempt-index range to the same
record, window, component, and invocation. Physical ledger adjacency and global
timestamp order are not required. `attempt_count` equals
`last_attempt_index - first_attempt_index + 1`. Every referenced attempt must
exist and belong to that invocation. The row also records
`resolution_protocol_complete`, `resolution_protocol_status`,
`resolver_candidate_count`, and `fallback_attempt_count`; all four fields are
bound into `completion_id`.

The route-attempt status uses safety-first precedence, then direct
policy/robots/local blockers, then resolver outcome. A later resolver result
cannot mask an earlier blocker. A complete route observation is not necessarily
a recovery success: resolver/fallback transient, invalid, HTTP-error, and
no-candidate observations still require the dated retry protocol. Policy,
local-cap, candidate-cap, missing-adapter, and safety statuses are incomplete.

## `attempt-dispositions.tsv`

Primary key: `attempt_id`. The only disposition is
`abandoned_incomplete_transaction`. It identifies an attempt appended by an
interrupted/error invocation but not covered by any completion. A disposed
attempt remains in the physical history but is excluded from health.

## `pointer-health.tsv`

Primary key: `record_id`; exactly one row per selected frozen scope record. The
producer rebuilds it from committed attempts only. Exact SHA has priority over
every diagnostic class. When no committed image attempt exists, `final_class`
is `not_audited`.

`retryable` is the operational transient-retry flag. The separate
`resolution_protocol_complete` and `resolution_protocol_status` fields describe
the latest committed route transaction. The separate
`multiwindow_protocol_applies` flag is true only for actual non-exact image
observations. Policy-pending, local bandwidth deferral, safety review, and
not-audited classes do not enter finality; `oversize` is likewise a local
response-cap non-observation. Per-row fields record distinct
observation windows and UTC dates, elapsed hours, whether a declared final
window observed the row, whether the latest observation is declared final, and
`finality_status`. Only attempts from route-protocol-complete windows enter
those counters; an incomplete route yields `pending_resolution_protocol` (or
the more specific policy/local/safety status).

The only strict-success class is `byte_exact`. The following are never strict
success: pHash candidates, decode/non-image/empty responses, HTTP/transport
failures, robots or policy outcomes, and `not_audited`.

## `tail-recoveries.tsv`

Primary key: `recovery_id`. This is a cumulative artifact rewritten atomically,
not an append ledger. Before truncating an unparseable non-newline crash tail,
the producer records the affected ledger name, UTC detection time, original and
retained sizes, discarded byte count, and discarded-fragment SHA-256. It never
stores the raw fragment.

Allowed ledger values are `pointer-health-attempts.tsv`,
`record-completions.tsv`, `attempt-dispositions.tsv`, `robots.tsv`, and
`atomic-temp-recoveries.tsv`. `original_size_bytes = retained_size_bytes +
discarded_fragment_bytes`, and the discarded count is positive. A recovery ID
deterministically binds the ledger, sizes, and fragment hash and must be unique.

## `atomic-temp-recoveries.tsv`

Primary key: `recovery_id`. On resume, a strictly recognized orphan from an
interrupted atomic rewrite is hashed and length-recorded here, the evidence row
is append-fsynced, and only then is the temporary removed. The row records UTC
detection time, temporary name, intended target, byte length, and SHA-256; raw
temporary bytes are not retained. Unrecognized `.tmp` names fail closed.

## `robots.tsv`

One robots observation per `(window_id, invocation_id, host)` cache fill. It
records the robots URL, transport result, parsed state, error code, redirect
count/chain, optional body SHA-256, and UTC observation time. Requests with a
robots decision must bind to an observation in the same
window/invocation/host. A robots transport `safety_block` is separately
aggregated and forces summary status `FAIL`.

V0.3 performs this cache fill as a cross-process singleflight. The fetching
worker fsyncs the robots receipt before publishing the parsed decision. Shared
cache state contains parsed rule structure and crawl delay, never the body; the
canonical receipt retains only the body SHA-256 when available.

## `audit-contract.json`

Created before the first output attempt and immutable thereafter. Contract v1
identifies a frozen v0.2 serial receipt. Contract v2 identifies a new v0.3
process receipt and adds `execution_profile_schema` plus
`execution_profile_sha256` and the exact `parallel_scheduler`; every other
scientific/input field remains the frozen producer contract. It binds the
scope IDs, dataset identity, bindings, optional pilot manifest, policy, tool,
requirements, dependency versions, pHash algorithm, public User-Agent, and the
no-byte/no-GPU invariants. It also binds the actual and required CPython runtime
(CPython 3.10.x), embeds the compact host-rate policy, and freezes the exact
transient-backoff contract. A mismatch fails closed.

The tool applies component-specific image and resolver/robots host maps to the
initial request and every redirect, in addition to the global policy. It rejects
credential-like query keys before network access and redacts such query values
from receipt URLs. Shared byte caps persist an in-flight reservation before each
body read; a crash may conservatively consume capacity until the 24-hour state
horizon but cannot silently overcommit a rate group.

## `parallel-execution-profile-v03.json`

Present only for contract-v2 receipts and immutable. Schema
`coverfish.pointer-parallel-execution-profile.v1` binds runner and frozen
producer hashes, CPython `spawn`, maximum/default inflight count of five,
deterministic primary-source-host lane scheduling, shared-rate request-start
scheduling, rate-state schema/update rules, robots singleflight, private fsynced
spools, coordinator-only canonical writes,
canonical-fsync worker reuse, physical-versus-logical ordering, mandatory spool
recovery, hash-bound private-TSV tail truncation, hash-bound promotion or
quarantine of nested atomic JSON temporaries, immutable non-recursive recovery-
ledger tail receipts, full-lifetime shared worker leases, exclusive recovery
leasing, durable fencing, a 900-second quiescence timeout, the pre-network epoch
gate, `bytes_retained=false`, and `gpu_used=false`.

The scheduler field is exactly schema
`coverfish.pointer-parallel-scheduler.v1`, algorithm
`stable_round_robin_primary_source_lane_v1`; it binds the rate-group-or-host
lane key, first-appearance lane order, frozen lane-internal order, and
post-full-schedule `max_rows` truncation semantics.

The contract binds the exact profile file SHA-256. A v0.3 terminal receipt is
invalid if `.parallel-work-v03` exists.

## `parallel-spool-recoveries.tsv`

Present only for contract-v2 receipts and append-only. One row binds the window,
invocation, record/spool identity, artifact, recovery action, original/retained/
discarded sizes, discarded-or-artifact SHA-256, UTC detection time, and
`requires_safety_review`.

Supported actions are bounded TSV-tail truncation, zero-byte prewrite removal,
promotion or deduplication of a complete nested atomic JSON temporary,
hash-receipted removal of an incomplete JSON temporary, and a pre-start
invocation missing its manifest. A removed TSV fragment has positive length and
satisfies `original = retained + discarded`; `discard_empty_tsv_prewrite`
instead requires all three sizes to be zero. An incomplete attempt or robots
tail requires safety review. Raw fragments are never retained.

`artifact_name` and its action/identity shape use an exact basename-only
allowlist. Row-bound TSV actions admit only `pointer-health-attempts.tsv`,
`record-completions.tsv`, or `robots.tsv`. The no-row pre-start action admits
only `invocation-work.json`. JSON-temporary actions admit only
`.invocation-work.json.<safe>.tmp`, `.robots-cache.json.<safe>.tmp`,
`.worker-result.json.<safe>.tmp`, or
`.disposition-stamp.json.<safe>.tmp`, where `<safe>` matches
`[A-Za-z0-9_-]+`; the first two require no row identity and the latter two
require it. Path separators and every other pairing fail closed.

## `parallel-recovery-ledger-tail-<recovery_id>.json`

Present only when the final row of `parallel-spool-recoveries.tsv` itself was
crash-truncated. Because that ledger cannot recursively append its own recovery
row, this immutable JSON receipt binds its schema, deterministic recovery ID,
ledger name, UTC detection time, original/retained/discarded integer sizes, and
discarded-fragment SHA-256. It is fsynced before truncation and must satisfy
`original = retained + discarded` with a positive fragment and at least the
complete TSV header retained. Raw fragment bytes are never retained.

Atomic-create temporaries for the spool-recovery ledger are handled only before
invocation metadata or a work tree exists: a complete header is promoted and an
incomplete header prefix is removed. This narrowly proven pre-network case does
not recursively create another receipt; all other temporary shapes fail closed.

## `run-metadata-<window>-<invocation>.json`

One file per process invocation. `RUNNING` is written before requests;
terminal status is `COMPLETE`, `ERROR`, `INTERRUPTED`, or
`ABANDONED_BY_RESUME`. The filename contains the validated window ID and
16-hexadecimal invocation ID. Counts bind the exact append segment and completed
record transactions produced by the invocation. Window ID and invocation ID are
strictly one-to-one; neither an empty nor a failed invocation permits reusing a
window ID. Run metadata repeats the actual/required CPython runtime report.
Contract-v2 metadata additionally includes `parallel_max_inflight` in `1..5`;
the wrapper requires it and binds it to the invocation execution summary. It
also includes the portable `parallel_worker_epoch`, derived from the contract,
invocation, window, and start time rather than the local path/device/inode.
Scheduler metadata binds the exact scheduler contract, eligible row count, and
SHA-256 of both the complete deterministic schedule and the post-`max_rows`
prefix. `max_rows` is applied only after building the full schedule.

## `parallel-execution-<window>-<invocation>.json`

Present once per v0.3 invocation. Schema
`coverfish.pointer-parallel-execution-summary.v1` binds the runner, execution
profile, original contract, run-metadata hash, maximum inflight value, exact
canonical attempt/completion/disposition/robots segment hashes and counts,
the spool-recovery segment hash/count, terminal status, and closed-spool
assertion. Its `worker_epoch` must equal the portable metadata epoch. It records
`bytes_retained=false` and `gpu_used=false`. Error and interrupted executions
also bind their `error_code`.

The execution summary repeats the scheduler contract, eligible-row count, and
both schedule hashes, so a verifier can bind performance-oriented dispatch
interleaving without treating it as a scientific row-order change.

Completed spools are canonicalized before resume. Durable attempts from an
incomplete spool are copied to the canonical physical ledger and receive
`abandoned_incomplete_transaction`; they remain excluded from health. The v0.3
wrapper verifier requires one exact execution summary for every run-metadata
file and rejects missing, extra, or regenerated execution evidence.

Worker lease names and private epoch payloads use the current output
path/device/inode only inside secure runtime/work-tree state. Those local
identifiers are not terminal receipt fields. A closed terminal receipt therefore
remains verifiable after copying or a fresh unpack at a new filesystem identity;
an active `.parallel-work-v03` tree is not a portable receipt.

## `pointer-health-summary.json`

Schema: `coverfish.pointer-health-summary.v2`. The summary is a deterministic
full aggregation over the frozen scope and transaction ledgers. It separates:

- physical versus committed attempts and bytes;
- completed versus uncompleted scope rows;
- exact versus diagnostic/failure/pending outcomes;
- all-row versus active-row counts;
- scope-wide rates versus completed-row interim rates; and
- scientific status versus receipt integrity.

The nested `receipt` object reports both `tail_recovery_rows` and
`atomic_temp_recovery_rows`, allowing an independent verifier to bind both
recovery ledgers into the summary.

`attempt_aggregations` reports physical attempts by host/transport, transport,
error code, and redirect count. `run_retry_modes` counts run-metadata retry
modes. `robots_aggregations` reports fetch/state/safety counts.
`by_resolution_protocol_status` and the `resolution_protocol` object report
route completion. `runtime`, `host_rate_policy`, and `transient_backoff` must
exactly match the immutable contract, so a receipt carries the implementation,
rate, redirect/retry, circuit, and byte-cap interpretation needed for comparison.

Scope-wide exact rates are JSON `null` until scope closure. `PASS` scientific
status requires scope closure, no incomplete route protocol, no policy-pending
rows, and every applicable non-exact row having at least three
protocol-complete windows on three distinct UTC dates spanning at least 48
hours, with its latest counted observation in a declared final window. This is
broader than the transient `retryable` flag.

Summary `status` is `FAIL` if any physical attempt (including an abandoned
attempt) or robots observation has transport `safety_block`; attempt rows use identity class
`safety_review_required`. Absent safety review, policy-pending classes
(`policy_pending_permission`, `robots_disallowed`, `robots_unavailable`), local
deferrals/caps, incomplete route protocol, incomplete scope, or incomplete
finality yield `PENDING`. The `pending` object separates policy, local,
route-protocol, record-safety, and robots-safety counts.

The summary must include all grouping maps even when a group has zero members:
`outcomes`, `active_outcomes`, `by_component`, `by_source_host`,
`by_resolved_via`, `by_resolution_protocol_status`, `by_final_host`, and
`by_http_status`.

No pointer-reconstruction rate is claimed by the current smoke artifacts. The
only already-closed quantities are E0 `6,719/6,719` byte-complete and the R0
redistributed baseline `15,052/49,140 = 30.63%`; neither is a measured pointer
rate. Active staging is `115,780 = 73,835` frozen byte-complete `+ 41,945`
pointer rows (`63.77%` frozen-byte baseline), also not a measured pointer rate.
R0 and active-staging composite exact-availability values are populated only
when their complete archive pointer populations are present; the final fields
and rates are JSON `null` for pilot or partial scopes, while explicit in-scope
fields (`exact.scope_rows`, `exact.active_scope_rows`, and their scope rates)
remain available. D0 includes `23` archive / `7` pilot rows whose frozen pointer
is `https://www.inaturalist.org/taxa/<taxon_id>` and whose exact photo resolver
is derived as `https://www.inaturalist.org/photos/<photo_id>` from the frozen
image URL. That blocked `www.inaturalist.org` route is currently
`pending_policy`, not measured health.

## Exit and stream contract

Every command prints exactly one JSON object to stdout. Progress, if any, is
stderr-only. Producer exit codes are `0` success/completed command, `2` usage,
`3` dependency, `4` safety/policy, `5` network/runtime, `6` integrity, and `130`
operator interruption. A process exit of zero does not imply that scientific
status is `PASS`; it may be `PENDING` or `FAIL`, so inspect the JSON `status`
field.

The v0.2 independent verifier is deterministic offline consistency verification
for contract-v1 receipts. Contract-v2 receipts require the v0.3 wrapper, which
first verifies the immutable original profile/contract bindings, closed work
spool, and exact per-invocation execution summaries before invoking the v0.2
scientific verifier in a private temporary adapted view. It does not alter the
original receipt or open the network.

The wrapper pins the base verifier SHA-256 and requires an exact ordered,
non-empty scope-specific check roster with exactly four fields per row. The only
dynamic choice is the declared `tail-recoveries.tsv` absent/present variant.
Empty, missing, duplicate, reordered, or extended rosters fail closed. The only
adaptable base failure is `attempt_full_semantics.global_time_order`, and only
when it is the sole semantic failure with the expected base integrity exit;
every logical record, transaction, binding, and safety check remains mandatory.

The archive producer/base-verifier record-order divergence is handled only in
the wrapper's disposable private base view. The producer's per-component
`record_id` order and the base verifier's physical source-TSV order must be
unique and set-identical; original health order must equal producer order and
its row content must equal a fresh producer-order rebuild; the original
contract-v2 reconstruction must also be exact. The wrapper records both order
hashes, a sorted health-content hash, and the adapter-required flag in
`parallel_private_base_order_adapter_bound`, then reorders only copied health
rows and derived private-view contract references.
`parallel_private_base_view_content_preserved` separately binds equal sorted
source/private-view content hashes. This is a set-bound verification adapter,
not a mutation of the original receipt or scientific object.

The wrapper's `parallel_receipt_fields_no_local_identity` check scans all
root-level JSON/TSV receipt keys and scalar values except the requested
`independent-checks.tsv`. It rejects absolute Unix, Windows, UNC, or `file://`
paths and embedded common local runtime paths. Public HTTP(S) pointers are
allowed. This independently enforces that local output path/device/inode
identity remains private work-tree/runtime state and not persistent evidence.

Neither verifier establishes that HTTP exchanges occurred and neither is a
third-party attestation, trusted timestamp, or tamper-proof mechanism against a
local writer. Since `robots.tsv` retains a body hash rather than the body, the
verifier also cannot cryptographically attest the remote robots text.
