# Pointer-audit source policy

This note explains the conservative controls in
`config/host-policy-fishbase-2s-v2.json`. A robots file or public API guideline
governs request conduct; it does not grant copyright, redistribution, reuse, or
model-training rights. Policies can change, so each live invocation observes
robots rules and records its dated result.

## Source routes

- FishBase media routes use the configured shared rate group. A current robots
  rule can raise the effective interval above the static minimum.
- iNaturalist observation HTML is not parsed. The tool uses a frozen media URL
  and, when needed and permitted, the official API for the exact frozen photo
  identifier.
- Wikimedia Commons fallback uses the official API, bounded serial requests,
  `maxlag`, and persistent `Retry-After` handling.
- ANGFA image retrieval remains `policy_pending_permission`; only permitted
  landing-page observation is attempted.
- USFWS uses configured HTTPS image and landing roles while retaining the
  frozen row-level provenance and licence evidence.

The policy file fixes allowed schemes, ports, host roles, response and decode
limits, request deadlines, rate groups, and byte ceilings. Every redirect is
checked against the same boundaries. Credential-like query keys fail closed,
and receipt URLs do not retain sensitive query values.

The bound policy SHA-256 is
`2987dffde63b8a0fa1e4a795142267d964d6644bd23d22c35b76b337112687a9`.
A policy change requires a new receipt directory and produces distinguishable
evidence.

## Interpretation

The input contract closes before network work:

- active staging: `115,780 = 73,835` byte-complete `+ 41,945` pointer;
- R0: `49,140 = 15,052` byte-complete `+ 34,088` pointer;
- archive pointer scope: `42,387 = 41,945` active `+ 442` retired; and
- E0: `6,719` byte-complete and `0` pointer.

These are population bindings, not a pointer-health result. The offline
verifiers establish local structure, hashes, bindings, and cross-file
consistency; they are not third-party network attestations or grants of rights.
