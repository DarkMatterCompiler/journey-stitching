#!/usr/bin/env python3
"""End-to-end orchestrator: runs every stage and prints one consolidated report.

Stages run at different scales on purpose, not for uniformity:
  - identity accuracy: 10,000 members (batch resolution is O(events), 0.6s -- cheap at scale)
  - streaming/latency: ~2,000 events (sink_event commits per row; 10k events here
    buys no extra information, just a slower demo)
  - actionability/chaos: sized to its own cohort, independent of the above

See DESIGN.md Sec.8 for what each number means and why.
"""

import eval_actionability
import eval_identity
import export_snapshot


def main():
    print("#" * 70)
    print("# Cross-Channel Journey Stitching -- full prototype run")
    print("#" * 70)

    print("\n" + "=" * 70)
    print("STAGE 1: Identity resolution accuracy (10,000 members)")
    print("=" * 70)
    eval_identity._toy_self_check()
    import time
    from generate_data import generate
    from pipeline import run_pipeline

    t0 = time.time()
    events, members, adversarial_cases, n_stripped = generate()
    graph, confirmed_timelines, analytics_timelines = run_pipeline(events)
    print(f"{len(events)} events, {n_stripped} stripped, resolved in {time.time()-t0:.1f}s")

    for label, attr in [("confirmed_id", "confirmed_id"), ("analytics_id", "analytics_id")]:
        p, r = eval_identity.pairwise_precision_recall(events, attr)
        print(f"  {label:14s} precision={p:.4f} recall={r:.4f}")
    for attr in ("confirmed_id", "analytics_id"):
        results = eval_identity.adversarial_checks(events, adversarial_cases, attr)
        for case_type, r in results.items():
            print(f"  [{attr}] {case_type:16s} pass={r['pass']:3d} fail={r['fail']:3d}")

    print("\n" + "=" * 70)
    print("STAGE 2: Journey analytics (escalation + pattern mining, same 10k dataset)")
    print("=" * 70)
    from analytics import escalation_score, classify_escalation, mine_risk_patterns
    classifications = {"alert": 0, "queue": 0, "normal": 0}
    for journey in confirmed_timelines.values():
        classifications[classify_escalation(escalation_score(journey))] += 1
    total = sum(classifications.values())
    for cls in ("alert", "queue", "normal"):
        print(f"  {cls:8s}: {classifications[cls]:5d} ({100*classifications[cls]/total:.1f}%)")
    patterns = mine_risk_patterns(analytics_timelines, granularity="event_type")
    print(f"  {len(patterns)} risk patterns found (event_type granularity)")

    print("\n" + "=" * 70)
    print("STAGE 3: Streaming pipeline latency (~2,000 events, single-node)")
    print("=" * 70)
    eval_actionability.run_latency_eval(num_members=650)

    print("\n" + "=" * 70)
    print("STAGE 4: Actionability -- chaos injection, alert tier, auditability")
    print("=" * 70)
    eval_actionability.run_chaos_eval()
    eval_actionability.run_alert_tier_demo()
    eval_actionability.run_auditability_eval()

    print("\n" + "=" * 70)
    print("STAGE 5: Edge retraction (typed-edge-graph vs union-find)")
    print("=" * 70)
    twin_case = next(c for c in adversarial_cases if c["type"] == "household_twin")
    by_event = {e.event_id: e for e in events}
    twin_events = [by_event[eid] for eid in twin_case["event_ids"]]
    before = {e.analytics_id for e in twin_events}
    for e in twin_events:
        for edge in graph.edges_for(e.event_id):
            # cross-member only -- keep each person's own intra-member edges intact
            if edge.signal == "device_fingerprint" and \
                    by_event[edge.u].true_member_id != by_event[edge.v].true_member_id:
                graph.invalidate_edge(edge.id)
    graph.resolve_all()
    after = {e.analytics_id for e in twin_events}
    print(f"  household-twin analytics_id: {len(before)} group(s) -> {len(after)} group(s) after retraction")

    print("\n" + "=" * 70)
    print("STAGE 6: Regenerating analyst dashboard")
    print("=" * 70)
    export_snapshot.main()

    print("\n" + "#" * 70)
    print("# Full run complete. See DESIGN.md Sec.8 for result interpretation.")
    print("#" * 70)


if __name__ == "__main__":
    main()
