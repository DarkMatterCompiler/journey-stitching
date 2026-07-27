"""Real-time journey analytics for identity resolution system.

Provides two core analytics functions:
1. Escalation scoring: real-time risk assessment for journeys
2. Risk pattern mining: simplified PrefixSpan-style sequential pattern discovery
"""

def escalation_score(journey):
    """Compute escalation score [0, 100] for a given journey. See escalation_breakdown for detail."""
    return escalation_breakdown(journey)["score"]


def escalation_breakdown(journey):
    """
    Return dict with individual component values for audit/explainability.

    Analyst can see why a journey scored high by inspecting each component.
    """
    if not journey:
        return {
            'C': 0.0, 'T': 0.0, 'R': 0.0, 'res': 0.0, 'P': 0.0,
            'switches': 0, 'hours': 0.0, 'repeat_calls': 0, 'score': 0.0
        }

    # Channel switches
    switches = 0
    for i in range(1, len(journey)):
        if journey[i].channel != journey[i-1].channel:
            switches += 1
    C = min(switches / 4.0, 1.0)

    # SLA elapsed time
    time_diff = journey[-1].timestamp - journey[0].timestamp
    hours = time_diff / 3600.0
    T = min(hours / 72.0, 1.0)

    # Repeat contact
    call_events = [e for e in journey if e.channel == "CALL"]
    repeat_calls = max(0, len(call_events) - 1)
    R = min(repeat_calls / 3.0, 1.0)

    # Resolved
    res = 1.0 if any(e.resolved for e in journey) else 0.0

    # Human agent
    P = 1.0 if any(e.human_agent for e in journey) else 0.0

    # Compute score
    score = 0.30*C + 0.25*T + 0.25*R + 0.15*(1.0 - res) + 0.05*P

    return {
        'C': C,
        'T': T,
        'R': R,
        'res': res,
        'P': P,
        'switches': switches,
        'hours': hours,
        'repeat_calls': repeat_calls,
        'score': score * 100.0
    }


def classify_escalation(score):
    """
    Classify escalation level based on score.

    - "alert": score > 85 (triggers live alert)
    - "queue": 65 < score <= 85 (enters analyst queue)
    - "normal": score <= 65 (routine processing)
    """
    if score > 85:
        return "alert"
    elif score > 65:
        return "queue"
    else:
        return "normal"


def journey_bad_outcome(journey):
    """
    Determine if a journey has a bad outcome.

    Bad outcome if:
    - NO resolved=True event in the journey, OR
    - repeat_calls >= 2 (2+ CALL events beyond the first one)
    """
    has_resolved = any(e.resolved for e in journey)
    call_events = [e for e in journey if e.channel == "CALL"]
    repeat_calls = max(0, len(call_events) - 1)

    return (not has_resolved) or (repeat_calls >= 2)


def mine_risk_patterns(timelines_dict, min_bad_support=0.05, min_lift=1.5, granularity="channel"):
    """
    Simplified PrefixSpan-style frequent sequential pattern mining.

    Mines 2-grams and 3-grams of channel sequences that correlate with bad
    outcomes. Patterns are filtered by BOTH:
    - min_bad_support: minimum support in bad outcome journeys (default 5%)
    - min_lift: minimum lift ratio (default 1.5x)

    Design note: lift-based filtering avoids the "flags any novel pattern" trap.
    A pattern is only flagged if it correlates with bad outcomes (lift > 1.5),
    not merely because it's novel or frequent. This focuses actionability on
    patterns that actually predict negative customer outcomes.

    Args:
        timelines_dict: dict[confirmed_id -> list[Event]] (from pipeline)
        min_bad_support: minimum fraction of bad journeys containing pattern
        min_lift: minimum (bad_support / good_support) ratio
        granularity: "channel" (default -- e.g. APP,CALL,CALL) or "event_type"
            (e.g. (APP,DISPUTE_FILED),(CALL,RESOLVED) -- distinguishes patterns
            that share a channel sequence but differ in what actually happened,
            e.g. a dispute-fail-then-call vs a routine login-then-call are both
            APP->CALL at channel granularity but very different signals)

    Returns:
        list[dict] with keys: pattern, bad_support, good_support, lift,
        bad_count, good_count -- sorted by lift descending
    """

    # Classify journeys into bad and good outcomes
    bad_journeys = []
    good_journeys = []
    for journey in timelines_dict.values():
        if journey_bad_outcome(journey):
            bad_journeys.append(journey)
        else:
            good_journeys.append(journey)

    if not bad_journeys:
        return []

    def extract_sequence(journey):
        """Extract (channel or channel,event_type) sequence, collapsing consecutive dupes."""
        if not journey:
            return []
        key = (lambda e: e.channel) if granularity == "channel" else (lambda e: (e.channel, e.event_type))
        seq = [key(journey[0])]
        for event in journey[1:]:
            k = key(event)
            if k != seq[-1]:
                seq.append(k)
        return seq

    bad_seqs = [extract_sequence(j) for j in bad_journeys]
    good_seqs = [extract_sequence(j) for j in good_journeys]

    # Collect all distinct n-gram patterns (2-grams and 3-grams)
    all_patterns = set()
    for seq in bad_seqs + good_seqs:
        for ngram_size in [2, 3]:
            for i in range(len(seq) - ngram_size + 1):
                pattern = tuple(seq[i:i+ngram_size])
                all_patterns.add(pattern)

    # Compute support and lift for each pattern
    results = []
    for pattern in all_patterns:
        pattern_len = len(pattern)

        # Count journeys containing this pattern in bad group
        bad_count = 0
        for seq in bad_seqs:
            for i in range(len(seq) - pattern_len + 1):
                if tuple(seq[i:i+pattern_len]) == pattern:
                    bad_count += 1
                    break  # Count each journey once

        # Count journeys containing this pattern in good group
        good_count = 0
        for seq in good_seqs:
            for i in range(len(seq) - pattern_len + 1):
                if tuple(seq[i:i+pattern_len]) == pattern:
                    good_count += 1
                    break  # Count each journey once

        bad_support = bad_count / len(bad_seqs) if bad_seqs else 0.0
        good_support = good_count / len(good_seqs) if good_seqs else 0.0

        # Lift: bad_support / max(good_support, 0.0001) to guard divide-by-zero
        lift = bad_support / max(good_support, 0.0001)

        # Filter by both thresholds
        if bad_support >= min_bad_support and lift >= min_lift:
            results.append({
                'pattern': pattern,
                'bad_support': bad_support,
                'good_support': good_support,
                'lift': lift,
                'bad_count': bad_count,
                'good_count': good_count
            })

    # Sort by lift descending (highest correlation with bad outcomes first)
    results.sort(key=lambda x: x['lift'], reverse=True)
    return results


if __name__ == "__main__":
    import sys

    # Attempt to import and run the full pipeline
    try:
        from generate_data import generate
        from pipeline import run_pipeline

        # Generate synthetic dataset
        events, members = generate(num_members=40)[:2]

        print(f"Generated {len(events)} events for {len(members)} members")

        # Run identity resolution and stitching pipeline
        graph, confirmed_timelines, analytics_timelines = run_pipeline(events)
        print(f"Resolved into {len(confirmed_timelines)} confirmed timelines, "
              f"{len(analytics_timelines)} analytics timelines\n")

        # Escalation scoring runs on confirmed_id timelines -- this gates
        # agent-facing exposure, so it must stay on the deterministic-only grouping.
        scores = {}
        classifications = {"alert": 0, "queue": 0, "normal": 0}
        for confirmed_id, journey in confirmed_timelines.items():
            score = escalation_score(journey)
            scores[confirmed_id] = score
            classification = classify_escalation(score)
            classifications[classification] += 1

        print("Escalation Classification Distribution:")
        for cls in ["alert", "queue", "normal"]:
            count = classifications[cls]
            pct = (count / len(confirmed_timelines)) * 100 if confirmed_timelines else 0
            print(f"  {cls:8s}: {count:3d} ({pct:5.1f}%)")

        # Sequence mining runs on analytics_id timelines -- aggregate-only,
        # so it can include probabilistically-linked anonymous pre-login activity.
        patterns = mine_risk_patterns(analytics_timelines, min_bad_support=0.05, min_lift=1.5)

        print(f"\nRisk Patterns Found: {len(patterns)}")
        print("\nTop 5 Risk Patterns (by lift):")
        for i, pattern_data in enumerate(patterns[:5], 1):
            pattern = pattern_data['pattern']
            lift = pattern_data['lift']
            bad_support = pattern_data['bad_support']
            good_support = pattern_data['good_support']
            print(f"  {i}. Pattern {pattern}:")
            print(f"     Lift={lift:.2f}, Bad Support={bad_support:.1%}, Good Support={good_support:.1%}")

    except ImportError as e:
        print(f"Import error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
