"""Actionability eval: latency + chaos-injection pattern mining + escalation separation.

See DESIGN.md Sec.8.2/8.3. Two honesty notes baked into this script's own
framing, not just documentation, so they can't be silently dropped:

  - Circularity: the escalation score (C/T/R/res/P) and the "bad outcome"
    label used by mine_risk_patterns are both functions of the same
    underlying journey fields. A high score correlating with bad outcome is
    partly by construction, not an independently discovered predictor.
  - Baked-in holdout: this script demonstrates the MEASUREMENT METHOD can
    detect a manufactured lift when one exists (chaos cohort vs control).
    It does not demonstrate that a real intervention reduces real churn --
    that requires production outcome data this prototype doesn't have.
"""

import time

from analytics import escalation_score, classify_escalation, mine_risk_patterns
from generate_data import generate, inject_chaos_cohort, inject_alert_demo
from pipeline import run_pipeline, run_streaming_pipeline


def _percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(int(len(s) * p / 100.0), len(s) - 1)
    return s[idx]


def run_latency_eval(num_members=500):
    print("=== Latency (single-node prototype, NOT a distributed-system number) ===")
    events, members, adversarial_cases, n_stripped = generate(num_members=num_members)
    result = run_streaming_pipeline(events)
    latencies_ms = [(sunk - enq) * 1000.0 for _, enq, sunk in result["event_latencies"]]
    p50, p95, p99 = (_percentile(latencies_ms, p) for p in (50, 95, 99))
    print(f"  events={len(events)} on_time={result['on_time_count']} late={result['late_count']}")
    print(f"  p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms")

    # Scenario check: does an event become queryable well within the "call 10s later" window?
    worst_ms = max(latencies_ms) if latencies_ms else 0.0
    ok = worst_ms < 10_000
    print(f"  worst single-event latency={worst_ms:.1f}ms -- "
          f"{'OK' if ok else 'FAIL'}: agent would have context if member calls 10s after an event")
    return {"p50": p50, "p95": p95, "p99": p99, "worst_ms": worst_ms}


def run_chaos_eval(n_cohort=500, n_control=500, background_members=3000):
    print("\n=== Chaos injection: pattern mining + escalation separation ===")
    base_events, _, _, _ = generate(num_members=background_members)
    chaos_events, chaos_cases = inject_chaos_cohort(n_cohort=n_cohort, n_control=n_control)
    events = base_events + chaos_events

    graph, confirmed_timelines, analytics_timelines = run_pipeline(events)

    # --- pattern mining check (event_type granularity, per DESIGN.md Sec.7 fix) ---
    patterns = mine_risk_patterns(analytics_timelines, min_bad_support=0.01, min_lift=1.2,
                                   granularity="event_type")
    shared_prefix = (("APP", "APP_DISPUTE_SUBMIT_FAIL"), ("CALL", "DISPUTE_FOLLOWUP"))
    by_pattern = {p["pattern"]: p for p in patterns}

    print(f"  {len(patterns)} patterns passed min_bad_support/min_lift thresholds")
    prefix_hit = by_pattern.get(shared_prefix)
    print(f"  shared 2-gram prefix {shared_prefix}: "
          f"{'lift=%.2fx' % prefix_hit['lift'] if prefix_hit else 'below threshold (correctly not flagged as risk-specific)'}")

    cohort_specific = [p for p in patterns if len(p["pattern"]) >= 3
                        and p["pattern"][:2] == shared_prefix]
    if cohort_specific:
        top = max(cohort_specific, key=lambda p: p["lift"])
        print(f"  cohort-specific extension {top['pattern']}: lift={top['lift']:.2f}x "
              f"(bad_support={top['bad_support']:.1%}, good_support={top['good_support']:.1%})")
        discriminates = (not prefix_hit or prefix_hit["lift"] < 2.0) and top["lift"] > 2.0
        print(f"  discrimination check: {'PASS' if discriminates else 'FAIL'} -- "
              f"miner should score the shared prefix near 1x and the cohort-only extension high")
    else:
        print("  cohort-specific extension NOT found above threshold -- check thresholds/injection")

    # --- escalation score separation ---
    cohort_ids = {c["member_ids"][0] for c in chaos_cases if c["type"] == "cohort"}
    control_ids = {c["member_ids"][0] for c in chaos_cases if c["type"] == "control"}
    event_by_id = {e.event_id: e for e in events}

    def score_for(true_member_ids):
        scores = []
        for cid, journey in confirmed_timelines.items():
            if journey and journey[0].true_member_id in true_member_ids:
                scores.append(escalation_score(journey))
        return scores

    cohort_scores = score_for(cohort_ids)
    control_scores = score_for(control_ids)
    cohort_avg = sum(cohort_scores) / len(cohort_scores) if cohort_scores else 0
    control_avg = sum(control_scores) / len(control_scores) if control_scores else 0
    cohort_queued = sum(1 for s in cohort_scores if classify_escalation(s) != "normal")
    control_queued = sum(1 for s in control_scores if classify_escalation(s) != "normal")

    print(f"  cohort avg score={cohort_avg:.1f}  ({cohort_queued}/{len(cohort_scores)} entered queue/alert)")
    print(f"  control avg score={control_avg:.1f}  ({control_queued}/{len(control_scores)} entered queue/alert)")
    print(f"  separation check: {'PASS' if cohort_avg > control_avg + 20 else 'FAIL'}")

    top = max(cohort_specific, key=lambda p: p["lift"]) if cohort_specific else None
    return {
        "patterns_found": len(patterns),
        "prefix_lift": prefix_hit["lift"] if prefix_hit else None,
        "top_pattern": top["pattern"] if top else None,
        "top_lift": top["lift"] if top else None,
        "top_bad_support": top["bad_support"] if top else None,
        "top_good_support": top["good_support"] if top else None,
        "cohort_avg": cohort_avg,
        "control_avg": control_avg,
        "cohort_queued": cohort_queued,
        "cohort_total": len(cohort_scores),
        "control_queued": control_queued,
        "control_total": len(control_scores),
    }


def run_alert_tier_demo(n=20):
    """Neither archetype journeys nor the chaos cohort (avg 77, queue tier)
    ever cross the >85 alert threshold in a normal run. Prove the alert path
    is reachable at all, not just illustrative, with a deliberately extreme journey."""
    print("\n=== Alert tier (>85) demo ===")
    events, member_ids = inject_alert_demo(n=n)
    _, confirmed_timelines, _ = run_pipeline(events)
    by_member = {m: [] for m in member_ids}
    for journey in confirmed_timelines.values():
        if journey and journey[0].true_member_id in by_member:
            by_member[journey[0].true_member_id] = journey

    scores = [escalation_score(j) for j in by_member.values() if j]
    alerts = sum(1 for s in scores if classify_escalation(s) == "alert")
    print(f"  {len(scores)} demo journeys, avg score={sum(scores)/len(scores):.1f}, "
          f"{alerts}/{len(scores)} classified 'alert'")
    print(f"  {'PASS' if alerts > 0 else 'FAIL'}: alert tier is reachable, not just illustrative")


def run_auditability_eval(num_members=300):
    print("\n=== Auditability: cost to explain a merge ===")
    events, members, adversarial_cases, n_stripped = generate(num_members=num_members)
    graph, confirmed_timelines, analytics_timelines = run_pipeline(events)

    # proxy for "clicks to explain": # of edge lookups + edges returned for a
    # multi-event confirmed_id -- this is what export_snapshot.py's audit view
    # renders directly, so it's the same cost the analyst dashboard pays.
    multi_event = [tl for tl in confirmed_timelines.values() if len(tl) > 1]
    sample = multi_event[:50]
    edge_counts = []
    t0 = time.time()
    for journey in sample:
        for event in journey:
            edge_counts.append(len(graph.edges_for(event.event_id)))
    elapsed = time.time() - t0

    avg_edges = sum(edge_counts) / len(edge_counts) if edge_counts else 0
    print(f"  sampled {len(sample)} multi-event journeys ({sum(len(j) for j in sample)} events)")
    print(f"  avg edges to inspect per event: {avg_edges:.1f}")
    print(f"  wall time for {len(edge_counts)} edge lookups: {elapsed*1000:.1f}ms "
          f"({elapsed*1000/max(len(edge_counts),1):.3f}ms/lookup)")
    return {"avg_edges_per_event": avg_edges}


if __name__ == "__main__":
    print("NOTE: see module docstring -- circularity and baked-in-holdout caveats apply.\n")
    run_latency_eval()
    run_chaos_eval()
    run_alert_tier_demo()
    run_auditability_eval()
