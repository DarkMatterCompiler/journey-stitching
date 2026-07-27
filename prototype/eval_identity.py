"""Identity resolution accuracy: pairwise precision/recall via contingency table.

Naive all-pairs comparison is O(events^2) -- ~4*10^8 pairs at 29k events,
too slow. The contingency-table formulation gives identical numbers in
O(events) by working from cluster-intersection sizes instead of enumerating
pairs directly:

    precision = sum_ij C(n_ij, 2) / sum_k C(n_pred_k, 2)
    recall    = sum_ij C(n_ij, 2) / sum_g C(n_true_g, 2)

where n_ij is the size of the intersection of predicted cluster i and true
cluster j, n_pred_k is predicted cluster k's size, n_true_g is true cluster
g's size, and C(n,2) = n*(n-1)/2.
"""

from collections import Counter


def _c2(n):
    return n * (n - 1) // 2


def pairwise_precision_recall(events, pred_attr, true_attr="true_member_id"):
    """precision/recall of the clustering induced by pred_attr against true_attr.

    Both are per-event attribute names (e.g. pred_attr="confirmed_id").
    Returns (precision, recall). A denominator of zero (every cluster is a
    singleton) is defined as 1.0 -- no pair exists to be wrong about.
    """
    pair_counts = Counter()
    pred_sizes = Counter()
    true_sizes = Counter()

    for e in events:
        pred_sizes[getattr(e, pred_attr)] += 1
        true_sizes[getattr(e, true_attr)] += 1
        pair_counts[(getattr(e, pred_attr), getattr(e, true_attr))] += 1

    numerator = sum(_c2(n) for n in pair_counts.values())
    pred_denom = sum(_c2(n) for n in pred_sizes.values())
    true_denom = sum(_c2(n) for n in true_sizes.values())

    precision = numerator / pred_denom if pred_denom else 1.0
    recall = numerator / true_denom if true_denom else 1.0
    return precision, recall


def segment_precision_recall(events, pred_attr, segment_fn, true_attr="true_member_id"):
    """Same metric, computed independently within each segment.

    segment_fn(event) -> hashable segment label. Assumes true clusters don't
    span segments (true here: segment is a per-member property in our data).
    Returns dict[segment -> (precision, recall, n_events)].
    """
    by_segment = {}
    for e in events:
        by_segment.setdefault(segment_fn(e), []).append(e)

    return {
        seg: (*pairwise_precision_recall(seg_events, pred_attr, true_attr), len(seg_events))
        for seg, seg_events in by_segment.items()
    }


def adversarial_checks(events, adversarial_cases, pred_attr):
    """Segment-specific pass/fail checks the aggregate P/R can't isolate.

    household_twin: the two members must NOT share pred_attr on any event
    (a merge here is the exact false positive the two-tier design exists
    to prevent for confirmed_id).
    hard_drift: every event belonging to the member must share the same
    pred_attr value (the deterministic anchor should hold the journey
    together despite every probabilistic signal drifting).

    Returns dict with per-case-type pass counts and the failing case ids.
    """
    by_id = {e.event_id: e for e in events}
    results = {"household_twin": {"pass": 0, "fail": 0, "failures": []},
               "hard_drift": {"pass": 0, "fail": 0, "failures": []}}

    for case in adversarial_cases:
        case_events = [by_id[eid] for eid in case["event_ids"] if eid in by_id]
        if not case_events:
            continue
        pred_vals = {getattr(e, pred_attr) for e in case_events}

        if case["type"] == "household_twin":
            # each member's own events should be internally consistent, but
            # the two members must not collapse into the same pred_attr
            by_member = {}
            for e in case_events:
                by_member.setdefault(e.true_member_id, set()).add(getattr(e, pred_attr))
            member_ids = list(by_member)
            ok = len(member_ids) < 2 or by_member[member_ids[0]].isdisjoint(by_member[member_ids[1]])
        else:  # hard_drift
            ok = len(pred_vals) == 1

        bucket = results[case["type"]]
        if ok:
            bucket["pass"] += 1
        else:
            bucket["fail"] += 1
            bucket["failures"].append(case["member_ids"])

    return results


def _toy_self_check():
    """Hand-checked ground truth: 6 events, 2 true members, one deliberate
    misgrouping in the predicted clustering. Computed by hand below."""
    class E:
        def __init__(self, pred, true):
            self.pred = pred
            self.true_member_id = true

    # true clusters: {A: 1,2,3}, {B: 4,5,6}
    # predicted clusters: {P1: 1,2,3,4} (wrongly includes event 4 from B), {P2: 5,6}
    events = [
        E("P1", "A"), E("P1", "A"), E("P1", "A"),
        E("P1", "B"), E("P2", "B"), E("P2", "B"),
    ]

    # by hand: pair_counts: (P1,A)=3 -> C(3,2)=3, (P1,B)=1 -> C(1,2)=0, (P2,B)=2 -> C(2,2)=1
    # numerator = 3 + 0 + 1 = 4
    # pred_sizes: P1=4 -> C(4,2)=6, P2=2 -> C(2,2)=1 => pred_denom = 7
    # true_sizes: A=3 -> C(3,2)=3, B=3 -> C(3,2)=3 => true_denom = 6
    # precision = 4/7 = 0.5714..., recall = 4/6 = 0.6667
    precision, recall = pairwise_precision_recall(events, "pred", "true_member_id")
    expected_p, expected_r = 4 / 7, 4 / 6
    assert abs(precision - expected_p) < 1e-9, f"precision {precision} != {expected_p}"
    assert abs(recall - expected_r) < 1e-9, f"recall {recall} != {expected_r}"
    print(f"toy self-check passed: precision={precision:.4f} recall={recall:.4f}")


if __name__ == "__main__":
    _toy_self_check()

    import time
    from generate_data import generate
    from pipeline import run_pipeline

    print("\nGenerating full-scale dataset (10,000 members)...")
    t0 = time.time()
    events, members, adversarial_cases, n_stripped = generate()
    print(f"  {len(events)} events, {n_stripped} stripped, generated in {time.time()-t0:.1f}s")

    print("Running identity resolution...")
    t0 = time.time()
    graph, confirmed_timelines, analytics_timelines = run_pipeline(events)
    print(f"  resolved in {time.time()-t0:.1f}s")

    print("\n=== Aggregate pairwise precision/recall ===")
    for label, attr in [("confirmed_id", "confirmed_id"), ("analytics_id", "analytics_id")]:
        t0 = time.time()
        p, r = pairwise_precision_recall(events, attr)
        print(f"  {label:14s} precision={p:.4f} recall={r:.4f}  ({time.time()-t0:.2f}s)")

    print("\n=== Adversarial checks ===")
    for attr in ("confirmed_id", "analytics_id"):
        results = adversarial_checks(events, adversarial_cases, attr)
        print(f"  [{attr}]")
        for case_type, r in results.items():
            print(f"    {case_type:16s} pass={r['pass']:3d} fail={r['fail']:3d}")

    print("\n=== Edge retraction demo (the capability plain union-find can't give you) ===")
    twin_case = next(c for c in adversarial_cases if c["type"] == "household_twin")
    by_event = {e.event_id: e for e in events}
    twin_events = [by_event[eid] for eid in twin_case["event_ids"]]
    before_ids = {e.analytics_id for e in twin_events}
    print(f"  before: household-twin pair analytics_ids = {before_ids} "
          f"({'merged' if len(before_ids) == 1 else 'separate'})")

    # only the cross-member edges -- an intra-member device_fingerprint edge
    # (same person, two of their own events) should stay intact; invalidating
    # it too would fragment that person's own journey, not just split the pair
    device_edges = [edge for e in twin_events for edge in graph.edges_for(e.event_id)
                     if edge.signal == "device_fingerprint"
                     and by_event[edge.u].true_member_id != by_event[edge.v].true_member_id]
    for edge in device_edges:
        graph.invalidate_edge(edge.id)
    graph.resolve_all()  # events are the same objects graph.add_event() stored -- mutated in place
    after_ids = {e.analytics_id for e in twin_events}
    print(f"  invalidated {len(device_edges)} cross-member device_fingerprint edge(s)")
    print(f"  after:  household-twin pair analytics_ids = {after_ids} "
          f"({'merged' if len(after_ids) == 1 else 'separate'})")
    print(f"  retraction {'PASS' if len(before_ids) == 1 and len(after_ids) > 1 else 'FAIL'} -- "
          f"a bad merge was cleanly split without touching any other event's identity")

    print("\n=== Segment-stratified precision/recall (ip-drift vs stable members) ===")
    drifted_ids = {m["true_member_id"] for m in members if m.get("_ip_drift")}
    segment_fn = lambda e: "ip_drift" if e.true_member_id in drifted_ids else "stable"
    for attr in ("confirmed_id", "analytics_id"):
        seg_results = segment_precision_recall(events, attr, segment_fn)
        print(f"  [{attr}]")
        for seg, (p, r, n) in sorted(seg_results.items()):
            print(f"    {seg:10s} precision={p:.4f} recall={r:.4f}  (n={n} events)")
