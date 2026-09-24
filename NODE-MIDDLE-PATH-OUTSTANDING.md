# Node middle-path — outstanding after Phase 1+2

Hanger for `feat/node-middle-path-phase12` (fork branch on `RFitz/flue`).
What already landed is Phase 1 fencing absorb + Phase 2 named owner (sticky
while hot / cold when idle). This file lists what is **still open** so review
does not depend on local `.flue-investigation/` notes.

## Done on this branch (context)

- Phase 1: per-claim producer acquire, fold-host admission, `renewLeases`
  returns renewed ids, stop-on-lease-loss, optional Postgres abort/append
  notify, dual-coordinator harness.
- Finding 2: shutdown classification is
  `(entry.leaseLost || stopping) && isAbortError(error)` — non-abort settle
  errors after lease loss are not stranded as shutdown.
- Phase 2: additive `owner_id` + `owner_lease_expires_at` on conversation
  streams (Postgres / MySQL / libSQL); default **30s** idle TTL, renew on
  **claim-shaped** work only; admit anywhere; only the named owner claims /
  reconciles while hot.
- Q3 interim choice: **layered** instance lease on top of submission leases
  (still open as a product call — see below).

## Outstanding (priority order)

### 1. Phase 0 — reserve-first settlement (highest)

`failInterruptedSubmission` / cross-process reconciler race is **not** closed.
Owner gating narrows who may reconcile while the lease is hot; it does not
replace reserve-first. The `TODO(multi-process)` in agent-submissions should
still be treated as urgent.

### 2. Ship-gate fencing items (1e / 1f / 1g)

Required by the middle-path decisions once per-claim acquire landed; not done
on this branch:

- **1e** — typed / coded fence errors on `ConversationStreamStoreError`
- **1f** — no retry on fence in `conversation-writer.ts` (still retries today)
- **1g** — churn observability: counter or log when claiming an instance last
  owned by another process (metrics should track lost process-local warmth /
  MCP-sandbox, not fold-cache drop)

### 3. Coverage gaps / validation

- MySQL owner-lease migration and claim path: written, **untested** (no MySQL
  in the implement environment)
- Real concurrent owner CAS: PGlite serializes transactions, so contention was
  never exercised under true parallel claims
- Missing race-contract tests still called out by the plan:
  - concurrent claim on an idle instance
  - dual expired-lease reconcilers
  - concurrent unborn birth
  - attach offset after a foreign unready-pass claim
  - heartbeat-failure self-stop
  - non-Postgres dual-coordinator (Redis, MySQL)
- Owner lease **not** implemented on Redis / Mongo / Node-default SQLite —
  those keep Phase 1 open-claim race (correct via fencing, no stickiness)

### 4. Product knobs still open (do not block merge of this slice)

| Id | Question | Status on this branch |
|---|---|---|
| **Q3** | Instance lease **subsume** vs **layer** on submission lease | Layered chosen as smaller additive; subsume deferred |
| **Q4** | Wake capability optional vs required | Still optional (attempt + owner wake) |
| **Q5** | Placement public API | Not started; only TTL knob (`ownership.idleTtlMs` / `FLUE_OWNER_IDLE_TTL_MS`) |
| **Q6** | Phase 0 vs 1h ordering | Phase 0 still needed; attempt largely superseded 1h |

### 5. Known operational risks (not blockers for review)

- Mixed old/new runtimes: processes without the owner capability ignore owners;
  fencing still holds, stickiness does not
- TTL above submission lease length delays crash failover by the same amount
- Owner lease compares wall clocks across hosts (same as submission leases)
- Postgres with `listen` uses additional LISTEN channels / NOTIFY serialization
- Non-owner claim pass does one no-op owner `UPDATE` per foreign-owned runnable
  row each pass

## Suggested next passes

1. Review / draft-PR this branch as the Phase 1+2 slice.
2. Phase 0 reserve-first (settlement race).
3. 1e / 1f / 1g ship-gate.
4. MySQL smoke + real Postgres contention + missing race contracts.
5. Only then: Q3 subsume and/or Q5 placement API.

## Pointers on this branch

- Guide copy softened in `apps/docs/src/content/docs/guide/node-target.md` and
  `durability.md` (replicas are *correct*; placement is not free).
- Dual-coordinator tests: `packages/postgres/src/multi-process.test.ts`.
- Owner lease store wiring: SQL conversation stream store + dialect opt-in
  `instanceOwnerLease`.
