# Cross-Channel Journey Stitching — Problem & Design Doc

## 1. The Problem

A card member's journey today is scattered across four disconnected systems: the mobile app,
amex.com, the call center, and branch/ATM/in-person touchpoints. Each channel captures its own
slice of the interaction with no shared view across them.

Concretely, this means:

- A card member files a dispute in the app, then calls support two days later — the agent has
  no idea the dispute exists, so the card member has to explain everything from scratch.
- A case can sit invisible between channels with nobody owning the handoff.
- If a card member gives up on their issue entirely (does not call back, does not follow up),
  there is no way to trace where in the journey the experience broke down.

Without a unified, cross-channel view, teams can't answer basic questions: where do journeys
fail, which patterns predict a churn or an escalation, and which in-flight cases are at risk
right now.

## 2. Tasks Given

1. **Identity resolution algorithm** — link a single card member's interactions across app,
   web, call-center, and in-person channels.
2. **Data pipeline** — ingest, normalize, and stitch events from all four channels into a
   unified timeline per card member.
3. **Analyst-facing interface** — visualize stitched journeys and highlight drop-off points,
   escalations, and unresolved issues.
4. **Journey analytics logic** — surface patterns that correlate with churn, repeat contacts,
   or poor customer experience.
5. **Test and optimize** — for identity resolution accuracy, end-to-end data latency, and
   actionability of insights.

## 3. Solution Overview

The platform assembles one stitched timeline per card member across all four channels, built
on: two-tier identity resolution → a streaming pipeline that normalizes and stitches events →
a dual analytics layer (off-the-shelf product analytics + a custom ops interface) → continuous
journey intelligence that scores and flags at-risk journeys in real time.

## 4. Identity Resolution

**Why two tiers:** deterministic signals are certain but don't cover every interaction
(e.g., an anonymous web session before login); probabilistic signals cover the gaps but carry
real risk of misattribution, so they must be used narrowly and never trusted blindly.

### Tier 1 — Deterministic (primary, high-confidence)
- Logged-in app/web sessions → Membership Rewards ID
- Inbound calls → ANI matched against CRM
- Branch visits → card swipe or associate lookup
- Dispute reference IDs → link a call back to an app-originated case

### Tier 2 — Probabilistic (fallback, for genuinely anonymous interactions only)
- Device fingerprint
- IP / geo
- Email hash
- Time-proximity boost (e.g., an anonymous web session followed quickly by a matched call)

### Merge mechanism — typed edge graph, not plain union-find
Every candidate pair (deterministic or probabilistic) is stored as a **typed, weighted edge**
— signal type, confidence score, and reason — in a graph, rather than being collapsed
immediately through plain union-find. Plain union-find only ever merges: once two nodes are
joined there is no clean way to split them if a merge turns out wrong. Instead:

- Connected components (and therefore canonical IDs) are **derived on demand** from the
  current edge set.
- A bad edge (failed step-up confirmation, an analyst-flagged false positive) can be
  **invalidated** without touching the rest of the edge set; components are then re-derived
  from the remaining valid edges (a full O(V+E) recompute at our scale — cheap enough that
  incremental, affected-component-only recomputation isn't worth the complexity yet).
- Every merge keeps its confidence score and reason so analysts can audit *why* two
  interactions were linked, and can retract that reasoning later.

### Refinement: false-positive blast radius (compliance-driven)
Incorrectly merging two identities in a FinTech context is not just a UX bug — it's a
potential data-exposure incident (e.g., exposing one card member's dispute data to another
via a bad IP/time-proximity match). Plain "one canonical ID for everything" can't support this
requirement — if a probabilistic edge is part of the graph that produces the canonical ID, the
data behind it is already merged. Instead, identity resolution produces **two IDs per member**:

- **Confirmed ID** — derived from deterministic edges only. This is the ID that gates
  agent-facing data exposure (financial/case data, timeline detail).
- **Analytics ID** — derived from deterministic + probabilistic edges. Used for aggregate
  journey analytics and sequence mining only; never surfaced as PII to an agent or another
  session.

The **edge type is the actual security boundary**, not a policy note layered on afterward:

- **Deterministic merges** unlock full data visibility in agent-facing views (they feed the
  Confirmed ID).
- **Probabilistic merges** feed the Analytics ID only — sufficient for aggregate mining, never
  sufficient on their own to surface one member's data to an agent or another session.
- Where a probabilistic link needs to become actionable (e.g., an agent wants to reference an
  anonymous web session on a call), require a step-up confirmation (last 4 digits, DOB) that
  promotes the edge to deterministic before it can affect the Confirmed ID.
- IP address is downweighted / used only as a tiebreaker alongside device fingerprint, since
  shared households, NAT'd corporate networks, and VPNs make it an unreliable standalone
  signal.

## 5. Data Pipeline

- **Kafka** as the streaming backbone, with a **schema registry** so events from all four
  channels conform to a common envelope.
- **Spark Structured Streaming** for processing: schema normalization and identity resolution
  run in parallel.
- **24-hour watermark** so late-arriving call-center and branch batch exports still join
  correctly into the timeline.
- **Nightly Airflow reconciliation job** to handle events that fall outside the watermark
  window.
- Resolved events write to:
  - **S3 raw lake** — for replay
  - **Snowflake** — for querying / historical analytics

### Refinement: real-time serving layer (latency-driven)
Snowflake is an OLAP warehouse — well suited to the analyst queue and historical analysis, but
not to the sub-second point lookups a live call-center application needs (e.g., "what's this
member's current escalation score and open issues, right now"). To close this gap:

- Add a **low-latency serving store** (Redis or DynamoDB) holding current-state per canonical
  member ID: latest escalation score, open case flags, last N events.
- The same Spark streaming job writes to this store as a second sink, alongside Snowflake/S3.
- Snowflake/S3 remain system of record for analytics and replay; Redis/DynamoDB is purely the
  hot path for live alerts and agent-facing lookups.

## 6. Analyst-Facing Interface

Two complementary layers:

- **Amplitude** — every stitched event is POSTed via a Spark `foreachBatch` job, using the
  canonical member ID as `user_id`. This gives Pathfinder journey views, drop-off funnels, and
  cohort analysis with no additional custom code.
- **Custom React interface** — covers what Amplitude doesn't do natively:
  - A per-member chronological timeline across all four channels.
  - An SLA / unresolved-case queue for ops teams.

## 7. Journey Analytics Logic

Two continuous, complementary analyses run on the event stream:

### Sequence mining (pattern discovery)
**PrefixSpan** finds which channel-transition patterns (e.g.
`APP_DISPUTE → CALL → CALL` within 48 hours) correlate most strongly with churn or repeat
contact, using time-bounded windows per journey type. PrefixSpan is a batch algorithm, not a
streaming one — it runs periodically over windowed snapshots of the event stream, not
continuously per-event.

### Real-time escalation scoring (per in-progress journey)
Each in-progress journey gets a live score:

```
S = 0.30·C + 0.25·T + 0.25·R + 0.15·(1 − res) + 0.05·P
```

Where:
- **C** — channel switches
- **T** — SLA elapsed time
- **R** — repeat contact count
- **(1 − res)** — absence of a resolution tag
- **P** — human-agent involvement

Weights are a starting hypothesis based on Amex's ops context, not yet tuned against real
outcome data — they should be recalibrated once enough labeled journeys (churned/repeat-
contact/resolved) are available. Thresholds:
- **Score > 65** → enters the analyst queue
- **Score > 85** → triggers a live alert to the agent or an outbound nudge to the card member

(This is the path that depends on the real-time serving layer above — the score must be
readable with sub-second latency to be useful as a live alert.)

## 8. Testing & Optimization Plan

Three pillars — identity resolution accuracy, end-to-end latency, actionability of insights —
each with a mechanics-level test (does the pipe work) and an outcome-level test (does it
matter). Mechanics-only tests can all pass on a system that's mediocre in production; the
outcome-level tests are what should distinguish this build.

### 8.1 Identity Resolution Accuracy

**Ground-truth test.** Generate 10,000 synthetic card member journeys with known true IDs for
every event. Strip identifiers from 40% of events (simulating anonymous web browsing /
unauthenticated calls) via an explicit, tunable post-generation pass — not baked into journey
archetypes, so the anonymity rate can be dialed for different test runs. Add realistic noise:
changed IPs, shared Wi-Fi/household IPs, and **intra-member IP drift** (the same member's IP
changing over time, e.g. new home, mobile network) — not just inter-member IP collisions —
so IP is stress-tested on both failure modes it actually has in production.

**Metric — pairwise precision/recall**, computed via the **contingency-table formulation**
(group events by predicted-cluster ∩ true-cluster intersection, sum `C(n,2)` over
intersections) rather than naive all-pairs comparison. Naive O(n²) pairwise comparison is fine
at prototype scale (dozens of members) but is ~10⁹ comparisons at 10,000 journeys —
minutes-to-OOM in a straightforward implementation. The contingency-table form gives identical
numbers in linear time.
- **Recall** — did fragmented/anonymous events get stitched back to the right member?
- **Precision** — did the system avoid merging two different members? This is the metric that
  matters most in FinTech: the system should be tuned to **fail gracefully by keeping IDs
  separate** rather than merging two card members' financial data. This is precisely the
  reasoning behind the Confirmed ID / Analytics ID split in §4 — precision is enforced
  structurally (deterministic-only gates data exposure), not just hoped for statistically.

**Segment-stratified precision/recall, not just aggregate.** Aggregate P/R over 10k random
journeys can look excellent while hiding the failure mode that matters most. Report P/R broken
out by segment — joint/authorized-user cards, shared-household devices, low-digital-footprint
members — and surface the worst-performing segment explicitly rather than only the headline
number.

**Adversarial "twin" test.** Beyond random noise, deliberately construct decoy pairs that
probe both failure directions at once:
- Two genuinely different members who legitimately share a signal (spouses on the same wifi /
  device) — the system must **not** merge them into one Confirmed ID.
- A single member whose signals drift hard within their own journey (new phone, new IP, new
  city) — the system must **still** recall them as one member via the deterministic anchors
  that persist (Membership Rewards ID, ANI-to-CRM) even as probabilistic signals change.

### 8.2 End-to-End Data Latency

**Stress test.** Load-test the Kafka ingest layer (e.g. Locust/JMeter) with concurrent events
simulating peak-volume conditions (holiday shopping day).

**Metric — P95 and P99 latency** (not mean — tail latency is what determines whether an agent
actually has context when the phone rings), measured end-to-end: milliseconds from an event
(e.g. `APP_ERROR`) hitting Kafka to that event being queryable in the real-time KV store
described in §5. The scenario to prove out: if a card member calls 10 seconds after an app
crash, the agent already has the context on screen.

**Honesty note on prototype-scale testing.** A P95/P99 number is only meaningful if it comes
off real infrastructure. At prototype scale (no live Kafka/Spark cluster), the closer analog
is a single-node freshness/ordering harness — producer → queue → consumer resolving identity
and writing to an in-memory KV store — with real enqueue-to-queryable percentiles measured and
explicitly labeled as **single-node prototype numbers**, not a distributed system's numbers.
Full Kafka/Docker load testing is a separate, larger scope step if the evaluation requires
running infrastructure rather than a design + prototype.

### 8.3 Actionability of Insights

**Chaos injection test.** Manually inject an anomaly into the synthetic stream — e.g. force a
cohort of members through an `APP_DISPUTE_SUBMIT_FAIL → CALL` sequence — and check whether the
PrefixSpan sequence miner flags it as a high-risk pattern and whether the escalation score
pushes those accounts into the analyst queue.

**Validity requirement — control group, not just novelty.** If the injected sequence appears
nowhere else in the data, "does the detector flag it" is trivially yes — that only proves the
detector notices frequent novel patterns, not that it's correlated with bad outcomes. To make
this a real test: inject the same sequence into both a cohort that goes on to churn/repeat-
contact and a control cohort that hits the identical sequence but resolves normally. The
detector should only flag the pattern via **outcome lift** (churn/repeat-contact rate
difference between cohorts), not raw frequency — and the escalation score should independently
push the at-risk cohort over threshold while leaving the control cohort below it.

**Outcome-based actionability — the metric that should carry the most weight.** Detection
tests only prove the pipes work. The metric that ties back to the actual business problem is:
for accounts that were flagged and received an intervention (agent nudge, outbound contact),
did repeat-contact/churn rate actually improve versus a matched **holdout group** that hit the
same risk pattern but wasn't flagged/acted on. This requires a holdout split in the synthetic
(or eventually real) data and is the difference between "the algorithm fires" and "the
algorithm is worth building."

**Auditability as a measured property.** The identity graph already stores confidence and
reason per merge (§4). Turn that into a metric rather than an architectural claim: time (or
click count) for an analyst to trace, from the interface, why two events were merged into one
member. A fast, legible audit trail is itself a differentiator for a FinTech identity system
and should be tested, not just designed for.

### 8.4 Summary Table

| Pillar | Mechanics-level test | Outcome-level test |
|---|---|---|
| Identity resolution | Aggregate pairwise P/R at 10k journeys, 40% stripped | Segment-stratified P/R + adversarial twin test |
| Latency | P95/P99 event-to-queryable time under load | Real scenario check: is context present before the 10s-later call lands |
| Actionability | Chaos injection flagged by PrefixSpan + escalation score | Outcome lift vs. control cohort + holdout-group intervention effect on churn/repeat-contact |

### 8.5 Results (from the working prototype, `prototype/run_all.py`)

Measured on the synthetic 10,000-member dataset (`prototype/generate_data.py`), single-node,
no containerization -- see §8.1-8.3 for what each number means and its honesty caveats.

**Identity resolution accuracy** (contingency-table pairwise P/R, verified O(events) via a
hand-checked toy case in `eval_identity.py`):

| Grouping | Precision | Recall | Household-twin (50 pairs) | Hard-drift (50 members) |
|---|---|---|---|---|
| `confirmed_id` (deterministic-only) | 1.0000 | 0.222 | 50/50 stay separate | 50/50 stay linked |
| `analytics_id` (+ probabilistic) | 0.991 | 1.000 | 0/50 stay separate (all merge) | 50/50 stay linked |

This is the two-tier design's thesis made concrete: confirmed_id trades recall for a hard
precision guarantee (safe to gate data exposure on); analytics_id recovers the recall lost to
anonymization but at the cost of exactly the false-positive risk the design flags -- every one
of the 50 household-twin decoy pairs (two different people sharing a device/wifi) merges under
analytics_id. That merge is why analytics_id is never surfaced as PII (§4). Segment-stratified
by IP-drift status shows the same pattern holds regardless of whether a member's IP changed
mid-journey (precision 1.00 either way).

**Edge retraction** (`eval_identity.py`, `run_all.py` Stage 5): invalidating the
device_fingerprint edge behind a household-twin merge and re-deriving components splits the
merged `analytics_id` back apart, with no change to any unrelated event's identity --
demonstrating the capability plain union-find structurally cannot provide (§4).

**Latency** (single-node producer/consumer over `queue.Queue` + SQLite/dict dual sink,
`pipeline.py`): p50 40-70ms, p95 160-290ms, p99 180-310ms enqueue-to-queryable, worst single
event under 350ms -- comfortably inside the "agent has context 10s after the call" scenario
this number exists to support. Not a distributed-system number; see §8.2.

**Actionability**: a chaos cohort (`APP_DISPUTE_SUBMIT_FAIL -> CALL -> CALL -> BRANCH -> CALL`,
unresolved) mined against a control cohort sharing the same leading 2-gram but resolving
normally. The shared 2-gram scores below the lift threshold (correctly not flagged); the
cohort-only 3-gram extension scores ~2300x lift (bad_support 23%, good_support 0%) -- the miner
distinguishes a merely-shared prefix from an outcome-specific pattern rather than flagging any
novel sequence. Escalation scores separate cleanly: cohort avg 76-77 (99% enter the analyst
queue), control avg ~13 (0% queued). A deliberately extreme journey (`inject_alert_demo`, 4+
channel switches and 3+ repeat calls) confirms the >85 alert tier is reachable, not merely
illustrative -- no default archetype or the chaos cohort itself ever reaches it on their own.

**Circularity and scope caveats still apply** (§8.3): the escalation score and the "bad
outcome" label share underlying fields, so this validates the measurement machinery, not an
independently-discovered predictor; and the outcome-lift result demonstrates the method can
detect a manufactured effect, not that a real intervention reduces real churn.
| (cross-cutting) | — | Time-to-audit-a-merge (analyst-facing legibility) |
