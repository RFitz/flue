# API vs agent-loop decoupling (Node)

Hanger for Ryan Fitzpatrick / the Flue fork, on `feat/node-middle-path-phase12`.
Investigation only — no runtime change. Related open items stay in
`NODE-MIDDLE-PATH-OUTSTANDING.md`; this file is the topology answer.

## Answer

**Correctness already allows a two-pool deploy. Packaging does not.**

On this branch, admit / history / SSE / abort are durable store operations.
The claim loop, heartbeat, and `processSubmission` are a separate in-process
machine. Any replica can admit and read; only a named instance owner (Postgres
/ MySQL / libSQL) claims and runs while the conversation is hot. That is the
mailbox + owner-lease model Phase 1+2 already shipped.

The default Node packaging still puts both roles in **one process and one
event loop**:

- `node dist/server.mjs` → `startFlueNodeServer` → `loadFlueNodeApplication`
  → `assembleNodeAgentRuntime` (coordinator + claim loop) **and**
  `@hono/node-server` `serve()` (HTTP).
- `createNodeAgentCoordinator(...)` has **no** `role`, `apiOnly`,
  `workerOnly`, or `claim: false` option.
- Every successful admit calls `ensureClaimLoop()` + `wake()` in **that**
  process. There is no public "start the claim loop while idle" either.

So: a busy agent loop can starve HTTP on the container that is running it,
even though another process *could* have served that HTTP. The mailbox is
not the bug. Co-location on one Node event loop is.

**What to do today (no new flags):**

1. **Do not** run the same `dist/server.mjs` image as "API" (port open) and
   "worker" (port closed). API replicas will claim. Idle workers will never
   start their claim loop. That inverts the split.
2. **Do** run **two or more full replicas** on shared Postgres **with
   `listen`**, **without sticky sessions**. Admit and stream from any replica;
   the owner lease pins *execution* to one process. Traffic that lands on a
   non-owner replica stays responsive.
3. Split **readiness** from **liveness**. A `/health` that fails when the
   event loop is wedged should take the owner out of the load balancer, not
   SIGTERM it (it is still the named owner).
4. A clean API-only / worker-only pair needs a small runtime feature (below).
   It is not on this branch. Q5 ("placement public API") in the outstanding
   hanger is the product-shaped version of the same gap.

---

## 1. Two machines in one process

### HTTP (admit, attach, history, SSE, abort)

| Surface | Route | Node path |
|---|---|---|
| Route map | whatever `app.ts` mounts | `apps/docs/.../guide/routing.md`; `createAgentRouter` in `packages/runtime/src/runtime/registration.ts` |
| Prompt admit | `POST /:id` → `202` | `executeAgentPrompt` → `handleAgentRequest` → `rt.createAgentAdmission` |
| History | `GET /:id?view=history` | `executeAgentConversationRead` → `handleAgentConversationRead` |
| Updates / long-poll / SSE | `GET /:id?view=updates` | same file; SSE heartbeats every 15s |
| Abort | `POST /:id/abort` | `executeAgentAbort` → `coordinator.abortInstance` |
| Attachments | `GET /:id/attachments/:id` | `handleAgentAttachmentRead` |

Concrete files:

- `packages/runtime/src/runtime/agent-routes.ts` — Node vs Cloudflare fork.
  Node handles the request **in this process**. Cloudflare forwards to the
  agent Durable Object (`routeToAgent` → `stub.fetch`).
- `packages/runtime/src/runtime/handle-agent.ts` — admission is
  fire-and-forget. No `?wait=result`. Returns `streamUrl` / `offset` /
  `submissionId`. Does **not** run the model.
- `packages/runtime/src/runtime/handle-conversation-routes.ts` — history is a
  fold-at-head read; updates subscribe + long-poll (30s) or SSE.
- `packages/runtime/src/runtime/conversation-observer.ts` —
  `waitForConversationData` wakes on `store.subscribe` and also polls every
  250ms as a backstop.

Admission itself (still HTTP-path, still this process):

- `coordinator.admitDispatch` / `createAdmission` in
  `packages/runtime/src/node/agent-coordinator.ts`
- contact gate + persist row + materialize birth/attachments + mark
  canonical-ready + **`ensureClaimLoop(); wake()`**
- `activityGate.enter()` wraps **only the admission call**, not settlement
  and not GET/SSE. The gate is a drain seam (`pause()` / reload), not an
  isolation boundary.

### Claim / loop / heartbeat

Same file: `packages/runtime/src/node/agent-coordinator.ts`.

| Piece | What it does |
|---|---|
| `ensureClaimLoop()` | Starts `claimLoop()` once; also starts heartbeat, abort watch, owner-wake watch, 15s lease-scan timer |
| `claimLoop` / `runClaimPass` | `listRunnableSubmissions` → `holdInstanceOwnership` → `claimSubmission` → `spawnSubmissionTask` |
| `spawnSubmissionTask` | `processSubmission` (`packages/runtime/src/runtime/agent-submissions.ts`) — model, tools, mid-turn joins |
| Heartbeat | `renewLeases` every 10s (default); lost lease aborts the in-process attempt |
| Owner lease | `claimInstanceOwner` on the conversation stream; sticky while claim-shaped work flows; default 30s idle TTL (`FLUE_OWNER_IDLE_TTL_MS`) |
| Owner nudge | `wakeInstanceOwner` / `subscribeOwnerWakes` (Postgres `LISTEN`), else 1s poll while this process holds any owner lease |
| Reconcile | startup + periodic expired-lease scan; owner-gated while hot |

`createNodeDispatchQueue` is a thin `admitDispatch` wrapper. `dispatch()`
from application code hits the same coordinator.

Mid-turn join / mailbox (queued input absorbed at a turn boundary) lives in
the submission store (`joining` / `joined` in
`packages/runtime/src/sql-agent-execution-store.ts`). That is conversation
queueing, not HTTP isolation. It does not give the listen process its own
event loop.

### Who starts both

| Entrypoint | HTTP | Coordinator / claim loop |
|---|---|---|
| Generated server `dist/server.mjs` | yes — `startFlueNodeServer` | yes — `assembleNodeAgentRuntime` |
| `packages/vite/src/bootstrap/node-entry.ts` | `PORT` (default 3000) | via `virtual:flue/server` |
| `packages/vite/src/bootstrap/node-server.ts` | `@hono/node-server` `serve({ fetch, requestTimeout: 0 })` | `assembleNodeAgentRuntime` then `userApp.fetch` |
| `vite dev` (`packages/vite/src/node-dev.ts`) | Vite middleware → same assembly | same; observation GETs skip the activity lease |
| `start()` (`packages/runtime/src/node/start.ts`) | **none** | yes (transport-free) |
| `flue run` (`packages/cli/src/lib/run-bootstrap.ts`) | **none** | yes; one-shot admit + observe |
| Dual-coordinator tests | none | two `createNodeAgentCoordinator` over one PGlite (`packages/postgres/src/multi-process.test.ts`) |

`assembleNodeAgentRuntime` (`packages/runtime/src/node/assemble.ts`) is the
shared boot: register agents → `createNodeAgentCoordinator` →
`createNodeDispatchQueue` → `configureFlueRuntime` →
`coordinator.reconcileSubmissions()`. Comment in that file: *"without HTTP:
no Hono app, no listener, no channels."* HTTP is layered on afterwards by
the Vite Node server, never instead of the coordinator.

`createNodeAgentCoordinator` options today:

```
submissions, agents, createContext,
conversationStreamStore?, attachmentStore?,
env?, activityGate?,
timings?: { heartbeat, leaseScan, abortPoll, ownerPoll },
ownership?: { idleTtlMs }
```

No role. No claim switch. The function is exported from
`@flue/runtime/internal`, not from `@flue/runtime/node`.

---

## 2. Can you run API-only vs worker-only today?

**No first-class mode.** A deploy that uses the generated server always
shares one process.

| Want | Supported? | Why |
|---|---|---|
| Many full replicas, shared durable DB | **Yes** | Documented. Admit anywhere; owner claims while hot. |
| API process that admits/reads and **never** claims | **No** | `createAdmission` / `admitDispatch` always `ensureClaimLoop()` + `wake()`. |
| Worker process that claims and **never** listens | **Partial** | `start()` / `assembleNodeAgentRuntime` have no HTTP. But the claim loop starts only if reconcile sees unsettled work, or something in-process admits/aborts. An idle worker that never receives HTTP **never starts the loop.** |
| Separate binaries | **No** | One `dist/server.mjs`. `start()` is a script helper, not a worker image. |
| Env flag (`FLUE_ROLE=api\|worker`) | **No** | Only owner TTL is env-wired: `FLUE_OWNER_IDLE_TTL_MS`. |

### Idle-worker trap (important)

`reconcileSubmissions()`:

```ts
if (!(await submissions.hasUnsettledSubmissions())) return;
ensureClaimLoop();
```

Wake sources after the loop is up: in-process admit, owner `LISTEN` / 1s
owner poll, abort `LISTEN` / poll, 15s lease-scan timer. There is **no**
global "new submission" notify. `LISTEN` channels (Postgres) are:

1. conversation append (stream readers)
2. abort
3. owner wake (nudge a process that **already** owns)

A worker that boots against an empty queue never subscribes to those, never
scans, and never claims work that API replicas later admit.

A naive "API pool + worker pool, same image, workers have no port" therefore
does the **opposite** of a split: API replicas start the loop on first
admit; workers stay asleep.

### What *is* already split (store-level)

From `apps/docs/src/content/docs/guide/durability.md` and
`apps/docs/src/content/docs/guide/node-target.md` (this branch):

- Replicas over a shared DB are **correct** (producer fence + submission
  lease + stop-on-lease-loss).
- Placement is **not free** (MCP / sandbox warmth is process-local).
- Only claiming/running keeps the owner; **reading history or streaming
  does not**.
- Any process may admit; only the named owner claims while the owner lease
  is hot.
- Owner lease is **not** implemented on Redis / Mongo / default Node
  SQLite — those stay an open claim race (still fenced).

That is why two full replicas already help API latency for conversations
this process does **not** own. It is also why the owner replica is the one
that goes non-responsive.

---

## 3. Why a busy container stops answering HTTP

Evidence, not guesswork. Node is one thread. The listen socket and the
agent loop share it.

### The bind

`packages/vite/src/bootstrap/node-server.ts` `startFlueNodeServer`:

- `loadFlueNodeApplication()` assembles the coordinator (and may already be
  running a claim loop from startup reconcile).
- `serve({ fetch: application.fetch, port, serverOptions: { requestTimeout: 0 } })`
  attaches HTTP to the same process.
- `requestTimeout: 0` means hung or slow requests are not evicted by the
  HTTP server.

There is no `worker_threads`, no `cluster`, no sidecar listen process in
`packages/runtime` or `packages/vite`.

### What actually blocks the loop

1. **Synchronous SQLite (default / `vite dev`).**
   `packages/runtime/src/node/agent-execution-store.ts` wraps
   `node:sqlite` `DatabaseSync`. Every claim, lease renew, stream append,
   and fold read is a **synchronous** `prepare`/`all`/`run` on the event
   loop. A hot conversation writing many stream records plus a 10s
   heartbeat plus a claim pass will stall `fetch`. This is the strongest
   in-tree explanation for "health/API dies when the agent is busy" on the
   default adapter.

2. **Synchronous JS CPU in the same isolate.**
   History and admit both call `getConversationFoldHost(...).getStateAtHead()`
   (`handle-conversation-routes.ts`, `readConversationState` in the
   coordinator). A large conversation fold is CPU on the request path.
   Agent hooks / tools that do tight JS, big JSON, or sync crypto do the
   same during `processSubmission`.

3. **The owner lease concentrates work on the process that is already busy.**
   Once this process claims, it keeps claiming that instance until the idle
   TTL. Heartbeat renews the instance owner for every live attempt. The
   replica that is running the model is the replica whose HTTP is most
   likely to starve — and Phase 2 makes that stickier, not less.

4. **Async work does *not* by itself block HTTP.**
   Model provider fetches, Postgres `pg` queries, `local()` file ops
   (`node:fs/promises`), and `child_process.spawn` for shell yield the
   event loop. A container that is "busy" only awaiting Anthropic/OpenAI
   should still accept admits and GET history **unless** (1) or (2) is
   also happening, or the process is CPU/memory-saturated (GC, too many
   concurrent submissions, OOM).

5. **SSE / long-poll need the loop to flush.**
   `sseResponse` heartbeats every 15s
   (`handle-conversation-routes.ts`). If the event loop is stuck in
   `DatabaseSync` or sync CPU, heartbeats do not flush; proxies drop the
   stream; the API looks dead. Long-poll waits up to 30s and polls every
   250ms — same dependency.

6. **Health is your route, same loop.**
   Flue generates **no** `/health`
   (`apps/docs/src/content/docs/ecosystem/deploy/docker.md`, `node.md`).
   A user `/health` in `app.ts` is ordinary Hono on the same `fetch`. A
   wedged event loop fails the probe. Platforms that treat that as
   liveness then kill the **owner**.

7. **Activity gate is not a shield.**
   `packages/runtime/src/runtime/runtime-activity-gate.ts` counts in-flight
   admission leases so reload/shutdown can drain. Production
   `startFlueNodeServer` does not even wrap GET in it. It cannot move work
   off the thread.

8. **Not a missing mailbox.**
   POST returns `202` after durable persist + materialize. The model runs
   later in `spawnSubmissionTask`. If the symptom is "POST/GET/health hang
   *while* a turn is running," that is event-loop / resource contention in
   the owner process, not "admission waits for the LLM."

---

## 4. Deploy recipe

### What works on this branch (recommended)

**Two or more full Node replicas, one Postgres, `listen` enabled, no
sticky sessions.**

Each replica is `node dist/server.mjs` (HTTP + coordinator). That is the
supported packaging.

```
            +----------------+
   clients ->|  load balancer |   round-robin / least-conn; NOT instance-sticky
            +--------+-------+
                     |
          +----------+----------+
          |                     |
          v                     v
    replica A             replica B
    HTTP + loop           HTTP + loop
          |                     |
          +----------+----------+
                     |
                     v
                 Postgres
        submissions + streams
        + owner lease + LISTEN
```

**Each replica must:**

- Use the same `db.ts` Postgres adapter (`@flue/postgres`) with `query`,
  `transaction`, **and** `listen` (three channels; up to three checked-out
  clients — see `apps/docs/src/content/docs/ecosystem/databases/postgres.md`).
- Register the same agent set (generated server does this from the scan).
- Have model keys / sandbox credentials (a replica that claims must be
  able to run).
- Define `/health` (or `/healthz`) in `app.ts` yourself.

**How owner lease + admit interact across the pool:**

1. Client `POST`s to whichever replica the LB chose. That replica admits
   (contact gate, persist, materialize). Admission does **not** require
   ownership.
2. That replica's claim pass runs. If the instance is cold, this replica
   becomes owner and `spawnSubmissionTask`. If another replica already
   owns it, this replica **skips the claim**, calls `wakeInstanceOwner`,
   and the row waits in the durable queue.
3. The owner is nudged immediately with `LISTEN`, or within ~1s by owner
   poll, and claims.
4. Client `GET` history / SSE may hit **any** replica. Stream subscribe
   uses append notify when `listen` is on; otherwise 250ms poll / 30s
   long-poll. Reads do not renew the owner lease.
5. After `idleTtlMs` (default 30s) without claim-shaped work, the owner
   goes cold. The next claim is an open race again.

**Health checks:**

| Probe | Should | Should not |
|---|---|---|
| Readiness | Cheap `GET /health` (200 if event loop + DB reachable). Fail when the loop is wedged so the LB stops sending **new** HTTP here. | SIGTERM the process. This replica may still be the named owner. |
| Liveness | Much looser, or a side-channel that does not require a timely `/health` (e.g. process exists). | Match readiness. Killing the owner delays failover by the owner TTL (and the submission lease if the process cannot release). |

Flue will not emit the health route. `vite dev` treats `GET/HEAD`
`/health`, `/healthz`, `/readyz`, `/livez` as observation (no activity
lease). Production does not special-case them.

**In-process schedules** fire in **every** replica
(`apps/docs/.../guide/schedules.md`). Gate them or move them to a
platform cron that `dispatch()`s once.

### What each role *would* start (target split)

Not packaged. If you add a role flag later, this is the cut.

**API container**

- Import / start: persistence `connect` + Hono `app.ts` + the admission
  and conversation-read seams (`createAgentAdmission`,
  `conversationStreamStore`, `abortAgentInstance`).
- Must: shared Postgres with `listen` (SSE/abort/owner-wake). Same agent
  identities for the contact gate (the real `Agent` functions, not
  stubs).
- Must not: `ensureClaimLoop`, heartbeat, `spawnSubmissionTask`,
  `processSubmission`.
- Health: this pool's readiness **is** the public API health.

**Worker container**

- Import / start: `assembleNodeAgentRuntime` (or `start()` plus a way to
  hold the process open). Same agents, same DB, model keys, sandboxes.
- Must: start the claim loop **even when the queue is empty** (missing
  today — see gaps).
- Must not: bind a public port, or if it does, keep it off the LB.
- Health: "can I reach Postgres and is the claim loop alive," not
  "can I serve `app.ts`."

**Do not** try this split with the current `dist/server.mjs` by only
closing a port. See the idle-worker trap.

### Workaround if you must run a worker-only process *now*

This uses `@flue/runtime/internal` (not a public API) and is a hanger,
not a blessed entry:

1. Custom entry: `connectPersistenceAdapter` + `assembleNodeAgentRuntime`
   (no `serve()`).
2. After assemble, poll `coordinator.reconcileSubmissions()` on an
   interval (e.g. 2s). When API admits a row, `hasUnsettledSubmissions()`
   becomes true, `ensureClaimLoop()` runs, and the 15s lease scan plus
   owner `LISTEN` keep it alive.
3. Keep the process open (`setInterval` is enough).
4. API side still has no way to **disable** claiming without forking
   `createAdmission`. So this only adds extra workers; it does not stop
   API replicas from becoming owners. To keep API off the hot path you
   still need the flag below — or accept that API replicas will claim
   cold conversations.

### Adapter requirements for a split

| Adapter | Owner lease | Cross-process wake | Shared state | Split-ready? |
|---|---|---|---|---|
| Postgres + `listen` | yes | append / abort / owner-wake | yes | **best** |
| Postgres, no `listen` | yes | 1s poll / 15s scan / 250ms stream poll | yes | correct, slower |
| MySQL / libSQL | yes (MySQL untested on this branch) | poll only | yes | possible |
| Default `sqlite()` / `node:sqlite` | no | n/a (process-local) | **no** | **no** — memory/file is one process; also `DatabaseSync` |
| Redis / Mongo | no owner | poll | yes | claim race; no stickiness |

---

## 5. What is still missing for a clean two-pool deploy

These are branch gaps, not review-blockers for Phase 1+2 correctness.

1. **Role / claim switch on the coordinator** — e.g.
   `createNodeAgentCoordinator({ role: 'admit' | 'claim' | 'both' })`
   (default `'both'`). Admit-only must persist + materialize + **not**
   call `ensureClaimLoop`. Claim-only must call `ensureClaimLoop()` from
   assemble even when `hasUnsettledSubmissions()` is false, and subscribe
   to owner wakes / lease scan immediately.
2. **A worker entry** — `assembleNodeAgentRuntime` without HTTP is the
   core; there is no `dist/worker.mjs`, no `FLUE_ROLE`, no Dockerfile
   example. `start()` is close but does not expose the coordinator or
   keep a daemon alive.
3. **Q5 placement public API** — listed as not started in
   `NODE-MIDDLE-PATH-OUTSTANDING.md`. A role flag is the minimum;
   placement (pin this instance to this worker) is the product form.
4. **Global "work available" notify** — owner-wake only helps a process
   that already owns. Cold work depends on some process already running
   a claim loop. A submission-queued `NOTIFY` would let idle workers
   wake without polling `reconcileSubmissions`.
5. **Owner lease on Redis / Mongo / Node SQLite** — without it, every
   replica with a claim loop races every claim (still fenced). API-only
   still needs the claim switch or those API processes will win races.
6. **Health / event-loop isolation** — no generated probe; no way to
   serve `/health` if `DatabaseSync` or sync CPU is mid-statement. A
   two-pool deploy is the operational fix; a worker thread or a tiny
   separate listen process would be the in-process fix (not designed).
7. **Older deploy docs still say "route each instance to one live
   owner"** (`docker.md`, `fly.md`, `render.md`). On this branch that is
   stale: admit/read anywhere, owner is automatic. Sticky HTTP routing
   to the owner is what you **do not** want if the goal is API
   responsiveness.
8. Phase 0 reserve-first settlement, 1e/1f/1g fencing, MySQL soak — see
   the outstanding hanger. They affect multi-process *correctness*, not
   the listen/claim cut.

---

## 6. Cloudflare comparison (as this repo documents it)

Flue targets both. The shapes are not the same split.

| | Node (this branch) | Cloudflare |
|---|---|---|
| HTTP entry | One long-lived process: `app.ts` + `@hono/node-server` | Worker isolate: `app.ts` `fetch` (`packages/vite/src/cloudflare-entry.ts`) |
| Agent execution | Same process, `NodeAgentCoordinator` claim loop | Per-conversation Durable Object (`createCloudflareAgentRuntime`, `onRequest`) |
| Isolation unit | Process (all agents, all conversations, all HTTP) | DO isolate per conversation; Worker isolate for routing |
| Admit vs run | Same coordinator, same event loop | Same DO: `onRequest` admits or reads; attempt runs as a **fiber** (`runFiber`) detached from the wake that started it |
| Cross-conversation starvation | One busy conversation + sync CPU/SQLite can stall **all** HTTP on that container | Worker can still route to **other** DOs; one busy DO does not own the Worker's event loop |
| Same-conversation HTTP vs loop | Co-located | Also co-located **inside that DO** (SSE/history/`onRequest` share the DO isolate with the attempt fiber) |
| Ownership | Lease + named owner (SQL adapters) | Structural: platform guarantees one live DO per name |
| Wake | Process must be up; lease scan / LISTEN | Platform wake + self-renewing 30s drain schedule while unsettled work exists (`durability.md`) |
| Persistence | `db.ts` (or in-memory sqlite) | DO SQLite; `db.ts` is a build error |

Cloudflare therefore already has the isolation Ryan wants **across
conversations** (Worker vs DO, and DO vs DO). It does **not** put
conversation HTTP in a different isolate from that conversation's agent
loop — `executeAgentPrompt` / conversation read on Cloudflare always
`routeToAgent`. Node's analogue of the Worker/DO cut is exactly
"API pool vs worker pool over shared Postgres," which this branch's
**protocol** allows and its **packaging** does not.

---

## Follow-up Q&A stubs

**Q. Is the mailbox enough?**
A. For not losing input, yes. For keeping `/health` and `POST` fast on the
owner container, no. Mailbox + owner lease *moves* work to one process;
it does not give that process a second event loop.

**Q. Will adding replicas fix a single-container deploy?**
A. Yes for conversations this replica does not own, if Postgres+`listen`
and the LB is not sticky. No for the owner replica's own HTTP. No if
there is only one replica.

**Q. Should we disable the owner lease to spread CPU?**
A. That spreads claims (and drops warmth) but every claiming process
still co-locates HTTP and `processSubmission`. Prefer a role flag over
turning stickiness off.

**Q. Can we serve API from `start()`?**
A. No. `start()` is coordinator-only. HTTP is the generated server.
The missing piece is the inverse: coordinator-only **as a deployable
worker**, plus admit-only on the server.

**Q. Where should the flag live?**
A. `createNodeAgentCoordinator` + `assembleNodeAgentRuntime` + env
(`FLUE_ROLE` or similar) so `dist/server.mjs` can be API-only and a
new worker entry can be claim-only. That is Q5-adjacent; not started.
