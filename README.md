# Cross-Channel Journey Stitching

A cross-channel identity resolution and event-stitching platform for a credit card company:
assembling one unified timeline per card member across app, web, call-center, and branch
touchpoints, so a dispute filed in the app and a follow-up call two days later are visibly the
same case instead of two disconnected fragments.

Full architecture, design rationale, and measured results: **[DESIGN.md](DESIGN.md)**.
This README covers what's implemented, how to run it, and what the numbers mean.

## The problem

Card member data is siloed across four systems. A card member who files a dispute in the app
and calls support two days later gets an agent with no idea the dispute exists. The case sits
invisible between channels, and if the member gives up without following up, there's no way to
trace where the experience broke down. This project builds the platform that makes that journey
visible end to end.

## Original tasks

1. Design an identity resolution algorithm linking one card member's interactions across app,
   web, call-center, and in-person channels.
2. Build a data pipeline to ingest, normalize, and stitch events from all four channels into a
   unified timeline per card member.
3. Develop an analyst-facing interface to visualize stitched journeys and highlight drop-off
   points, escalations, and unresolved issues.
4. Implement journey analytics logic to surface patterns correlating with churn, repeat
   contacts, or poor customer experience.
5. Test and optimize for identity resolution accuracy, end-to-end data latency, and
   actionability of insights.

## What's here

`DESIGN.md` is the full architecture doc (Kafka/Spark/Snowflake/Amplitude at production scale).
`prototype/` is a **working, runnable implementation** of the same architecture's core logic,
built with Python stdlib only — no containerization, no external services, so it runs anywhere
with Python 3.10+ and nothing else installed. It's sized to prove the design decisions actually
hold up on real (synthetic) data, not to be production infrastructure.

### Identity resolution — `prototype/models.py`, `prototype/identity_graph.py`

The core algorithm (Task 1). Every event carries a common envelope (`models.Event`). Candidate
identity matches are stored as **typed, weighted edges** in a graph — `deterministic` (shared
Membership Rewards ID, ANI, card ID, or dispute reference; confidence ~0.95–1.0),
`probabilistic` (shared device fingerprint or email hash, with a time-proximity confidence
boost), or `tiebreaker` (shared IP only — recorded for audit, but structurally excluded from
ever merging two identities on its own, since shared households/NAT/VPN make IP alone
unreliable).

This is a deliberate departure from plain union-find: union-find only ever merges, so a bad
probabilistic match can never be cleanly retracted. Here, connected components are **derived on
demand** from the current valid edge set, so `invalidate_edge()` can retract a bad merge and
`resolve_all()` re-derives components from what's left — proven end to end in
`eval_identity.py` and `run_all.py` (Stage 5).

Two IDs come out of resolution, not one — this is the load-bearing compliance decision:

- **`confirmed_id`** — deterministic edges only. Gates any agent-facing data exposure (financial
  data, case history). High precision, deliberately lower recall.
- **`analytics_id`** — deterministic + probabilistic edges. Used for aggregate journey
  analytics and sequence mining only, never surfaced as PII to an agent or another session.

### Pipeline — `prototype/pipeline.py`

Task 2. `run_pipeline(events)` does batch ingest → normalize → resolve → stitch into two
timeline groupings (`confirmed_timelines`, `analytics_timelines`) keyed by the two IDs above.

`run_streaming_pipeline(events)` simulates the streaming architecture in-process: a producer
thread feeding a `queue.Queue` (stand-in for Kafka), a consumer resolving identity in
micro-batches (stand-in for Spark Structured Streaming — resolving per-event instead of
per-batch was tried and measured 15x slower for no benefit), and a **dual sink**: SQLite
(stand-in for the Snowflake warehouse) and an in-memory dict (stand-in for a Redis/DynamoDB
real-time serving layer, keyed by `confirmed_id` for sub-millisecond lookups). A simulated
arrival-time model (CALL/BRANCH events can lag their own event time, as real batch exports do)
feeds a 24-hour watermark; events arriving later than that route to
`reconcile_late_events()`, standing in for the nightly reconciliation job.

### Analyst interface — `prototype/export_snapshot.py` → `prototype/analyst_dashboard.html`

Task 3. `export_snapshot.py` runs the full pipeline and writes a single self-contained HTML
file — no server, no build step, opens directly in a browser. Three views:

- **SLA / Escalation Queue** — sortable/filterable table of members in the queue or alert tier.
- **Per-Member Timeline** — the actual "one view across channels" deliverable: every event for
  one member, in order, color-coded by channel.
- **Identity Merge Audit** — for any event, every edge linking it to others, with type, signal,
  confidence, and the human-readable reason, so an analyst can answer "why does the system
  think these are the same person" (deterministic edges shown distinctly from probabilistic
  from tiebreaker, with tiebreaker edges explicitly labeled "never merges identities").

### Journey analytics — `prototype/analytics.py`

Task 4. Two pieces, both explained in more depth in DESIGN.md §7:

- **Escalation scoring**: `S = 0.30·C + 0.25·T + 0.25·R + 0.15·(1−res) + 0.05·P` (channel
  switches, SLA elapsed time, repeat contacts, unresolved status, human-agent involvement).
  `classify_escalation()` buckets into normal / queue (>65) / alert (>85).
- **Sequence mining**: a simplified PrefixSpan-style miner over channel-transition sequences
  (or `(channel, event_type)` sequences at finer granularity), filtered by **outcome lift**
  (does this pattern correlate with bad outcomes) rather than raw frequency — deliberately, so
  it doesn't just flag any novel pattern.

### Data generation — `prototype/generate_data.py`

Synthetic ground-truth data (10,000 members by default) across four journey archetypes, with
stress cases built in: a tunable fraction of events anonymized post-generation (simulating
unauthenticated sessions), intra-member IP drift, shared-household IP collisions, and two
**adversarial test pairs** used throughout eval — `household_twin` (two different people
sharing a device/wifi — must never merge under `confirmed_id`) and `hard_drift` (one person
whose every probabilistic signal changes mid-journey, but a deterministic anchor should still
hold them together). `inject_chaos_cohort()` and `inject_alert_demo()` add manufactured
cohorts used only by the actionability eval (see below).

### Evaluation — `prototype/eval_identity.py`, `prototype/eval_actionability.py`

Task 5. The two highest-stakes modules — a wrong number here looks plausible instead of
obviously broken, so these were hand-verified rather than taken on trust:

- **`eval_identity.py`**: pairwise precision/recall via the **contingency-table formulation**
  (linear in event count, not the ~4×10⁸-pair naive approach that scale would otherwise
  require) — proven correct with a hand-computed toy case before running at scale. Reports
  aggregate P/R for both ID groupings, segment-stratified P/R (IP-drift vs. stable members),
  adversarial-case pass/fail, and the edge-retraction demo.
- **`eval_actionability.py`**: single-node latency percentiles, the chaos-cohort-vs-control
  pattern-mining discrimination check, escalation-score separation between cohort and control,
  the alert-tier reachability demo, and an auditability cost metric (edges inspected per event
  to explain a merge).

### `prototype/run_all.py`

Runs every stage in sequence and prints one consolidated report, regenerating the dashboard at
the end. This is the single command that exercises the whole system.

## Running it

Python 3.10+, standard library only — no `pip install` required.

```bash
cd prototype
python run_all.py                 # full run: accuracy, latency, actionability, dashboard
python eval_identity.py           # identity resolution accuracy only (10k members)
python eval_actionability.py      # latency + chaos injection + auditability only
python export_snapshot.py         # just regenerate analyst_dashboard.html
```

Then open `prototype/analyst_dashboard.html` directly in a browser (works via `file://`, no
server needed).

## Results

Full results and interpretation: **[DESIGN.md §8.5](DESIGN.md#85-results-from-the-working-prototype-prototyperun_allpy)**.
Headline numbers from the 10,000-member synthetic run:

| Metric | Value |
|---|---|
| `confirmed_id` precision / recall | 1.000 / 0.222 |
| `analytics_id` precision / recall | 0.991 / 1.000 |
| Household-twin pairs staying separate under `confirmed_id` | 50/50 |
| Household-twin pairs merging under `analytics_id` | 50/50 |
| Hard-drift members correctly linked (both ID types) | 50/50 |
| Streaming latency (enqueue → queryable), single-node | p50 ~70ms, p95 ~250ms, p99 ~280ms |
| Chaos-cohort vs. control escalation score separation | avg 76 (99% queued) vs. avg 13 (0% queued) |
| Alert tier (>85) reachable | Yes, confirmed by a deliberately extreme journey |

The `confirmed_id` recall of 0.22 looks low in isolation — it's the direct, intended cost of
gating agent-facing data exposure on deterministic-only identity under an aggressive 40%
anonymization stress test, not a defect. The `analytics_id` numbers show the two-tier design's
actual payoff: recall recovers to 1.00, at the cost of exactly the false-positive risk the
design exists to contain (every household-twin decoy pair merges) — which is precisely why
`analytics_id` is never surfaced as PII.

## Honesty notes (also in DESIGN.md §8.3/8.5)

- **This is a synthetic-data prototype.** All accuracy numbers are measured against ground
  truth this project generated, under failure modes this project also designed. They prove the
  algorithm behaves correctly against the specific stress cases constructed for it — not that
  it will perform this well against real, messier production data.
- **Latency numbers are single-node**, not a distributed-system result (no real Kafka/Spark
  cluster involved).
- **The escalation score and the "bad outcome" label used to validate pattern mining share
  underlying fields** — the chaos-injection test demonstrates the measurement methodology can
  detect a manufactured effect, not that a real intervention reduces real churn.
