"""Synthetic multi-channel event generator with known ground-truth identities.

Produces events across APP/WEB/CALL/BRANCH for a configurable population of fake card
members, using journey archetypes (clean resolution, escalation, silent drop-off, routine).
Includes explicit stress cases:

  1. Anonymous web sessions with only probabilistic signals until login
  2. Household/shared-IP collisions across different members
  3. Intra-member IP drift (member changes networks during their journey)
  4. Adversarial test pairs:
     a. Shared household: two different members sharing IP/device (precision trap)
     b. Hard drift: one member where ALL probabilistic signals change between
        early/late events, but deterministic anchors bridge the gap (recall trap)
"""

import random
import string

random.seed(7)

from models import Event

CHANNELS_EVENTS = {
    "APP": ["LOGIN", "DISPUTE_FILED", "VIEW_STATEMENT", "PAYMENT", "APP_DISPUTE_SUBMIT_FAIL"],
    "WEB": ["ANON_BROWSE", "LOGIN", "DISPUTE_FILED", "VIEW_STATEMENT"],
    "CALL": ["INBOUND_CALL", "DISPUTE_FOLLOWUP", "ESCALATION_REQUEST", "RESOLVED"],
    "BRANCH": ["BRANCH_VISIT", "CARD_REPLACEMENT", "DISPUTE_FOLLOWUP"],
}

ARCHETYPES = [
    ("clean_resolution", 0.35),
    ("escalation_path", 0.25),
    ("silent_dropoff", 0.20),
    ("routine", 0.20),
]


def _rand_id(prefix, n=8):
    return prefix + "-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def _gen_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"


def _make_member(idx):
    return {
        "true_member_id": f"MEMBER-{idx:04d}",
        "membership_rewards_id": _rand_id("MR"),
        "ani": "1" + "".join(random.choices(string.digits, k=10)),
        "card_id": _rand_id("CARD", 6),
        "email_hash": _rand_id("EMAILHASH", 12),
        "device_fingerprint": _rand_id("DEV", 10),
        "ip": _gen_ip(),
    }


def _pick_archetype():
    r = random.random()
    acc = 0.0
    for name, weight in ARCHETYPES:
        acc += weight
        if r <= acc:
            return name
    return ARCHETYPES[-1][0]


_eid_counter = 0


def _new_event_id():
    global _eid_counter
    _eid_counter += 1
    return f"EVT-{_eid_counter:06d}"


def _emit(events, member, channel, event_type, t, anonymous=False, dispute_ref=None,
          resolved=False, human_agent=False):
    e = Event(
        event_id=_new_event_id(),
        channel=channel,
        event_type=event_type,
        timestamp=t,
        true_member_id=member["true_member_id"],
        device_fingerprint=member["device_fingerprint"],
        ip=member["ip"],
        resolved=resolved,
        human_agent=human_agent,
        dispute_ref=dispute_ref,
    )
    if not anonymous:
        e.membership_rewards_id = member["membership_rewards_id"] if channel in ("APP", "WEB") else None
        e.ani = member["ani"] if channel == "CALL" else None
        e.card_id = member["card_id"] if channel == "BRANCH" else None
        e.email_hash = member["email_hash"]
    events.append(e)
    return e


def _gen_journey(events, member, archetype, t0):
    t = t0
    dispute_ref = _rand_id("DSP", 6)

    if archetype == "clean_resolution":
        _emit(events, member, "APP", "DISPUTE_FILED", t, dispute_ref=dispute_ref)
        t += random.uniform(1800, 7200)
        _emit(events, member, "CALL", "RESOLVED", t, dispute_ref=dispute_ref,
              resolved=True, human_agent=True)

    elif archetype == "escalation_path":
        _emit(events, member, "APP", "DISPUTE_FILED", t, dispute_ref=dispute_ref)
        t += random.uniform(3600, 2 * 86400)
        _emit(events, member, "CALL", "DISPUTE_FOLLOWUP", t, dispute_ref=dispute_ref, human_agent=True)
        t += random.uniform(3600, 2 * 86400)
        _emit(events, member, "CALL", "ESCALATION_REQUEST", t, dispute_ref=dispute_ref, human_agent=True)
        t += random.uniform(3600, 3 * 86400)
        resolved = random.random() < 0.5
        _emit(events, member, "BRANCH", "DISPUTE_FOLLOWUP", t, dispute_ref=dispute_ref,
              resolved=resolved, human_agent=True)

    elif archetype == "silent_dropoff":
        _emit(events, member, "WEB", "ANON_BROWSE", t, anonymous=True)
        t += random.uniform(60, 900)
        _emit(events, member, "WEB", "DISPUTE_FILED", t, anonymous=True, dispute_ref=dispute_ref)

    else:  # routine
        channel = random.choice(["APP", "WEB", "BRANCH"])
        event_type = random.choice(CHANNELS_EVENTS[channel])
        _emit(events, member, channel, event_type, t)

    return dispute_ref


def _gen_probabilistic_login_boost(events, member, t0):
    """Anonymous web session immediately followed by a logged-in session."""
    _emit(events, member, "WEB", "ANON_BROWSE", t0, anonymous=True)
    t = t0 + random.uniform(30, 600)
    _emit(events, member, "WEB", "LOGIN", t)


def _apply_ip_drift(events, members):
    """For members marked with _ip_drift, change their IP for events after the midpoint."""
    member_by_id = {m["true_member_id"]: m for m in members}
    member_events = {}

    for event in events:
        if event.true_member_id not in member_events:
            member_events[event.true_member_id] = []
        member_events[event.true_member_id].append(event)

    for mid, member in member_by_id.items():
        if member.get("_ip_drift"):
            member_evt = member_events.get(mid, [])
            if len(member_evt) > 1:
                midpoint = len(member_evt) // 2
                for i in range(midpoint, len(member_evt)):
                    member_evt[i].ip = member["_second_ip"]


def _strip_identifiers(events, strip_fraction, protected_event_ids=frozenset()):
    """Randomly remove deterministic identifiers from a fraction of events.

    protected_event_ids is excluded from the sample -- adversarial hard-drift
    cases rely on specific events keeping their deterministic anchor, and a
    random strip would otherwise silently break that guarantee.
    """
    eligible = [i for i, e in enumerate(events) if e.event_id not in protected_event_ids]
    n_to_strip = int(len(events) * strip_fraction)
    indices = random.sample(eligible, min(n_to_strip, len(eligible)))
    for i in indices:
        events[i].membership_rewards_id = None
        events[i].ani = None
        events[i].card_id = None
        events[i].dispute_ref = None
    return len(indices)


def _inject_hard_drift(events, hard_drift_member_ids):
    """For hard_drift members, change all probabilistic signals in later events."""
    member_events = {}

    for event in events:
        if event.true_member_id not in member_events:
            member_events[event.true_member_id] = []
        member_events[event.true_member_id].append(event)

    for mid in hard_drift_member_ids:
        member_evt = member_events.get(mid, [])
        if len(member_evt) > 1:
            new_device = _rand_id("DEV", 10)
            new_ip = _gen_ip()
            new_email = _rand_id("EMAILHASH", 12)

            midpoint = len(member_evt) // 2
            for i in range(midpoint, len(member_evt)):
                member_evt[i].device_fingerprint = new_device
                member_evt[i].ip = new_ip
                member_evt[i].email_hash = new_email


def generate(num_members=10000, strip_fraction=0.4, ip_drift_fraction=0.15,
             household_ip_fraction=0.1, n_adversarial_pairs=50):
    """Generate synthetic multi-channel events with known ground truth.

    Args:
        num_members: Total regular members to generate.
        strip_fraction: Fraction of events to anonymize (strip deterministic IDs).
        ip_drift_fraction: Fraction of regular members whose IP changes mid-journey.
        household_ip_fraction: Fraction of regular members to place in shared-IP households.
        n_adversarial_pairs: Number of adversarial test pairs (shared household + hard drift).

    Returns:
        (events, members, adversarial_cases, n_stripped) where:
          - events: list of Event objects sorted by timestamp
          - members: list of member dicts (includes adversarial members)
          - adversarial_cases: list of {"type": str, "member_ids": [...], "event_ids": [...]}
          - n_stripped: actual count of events stripped by the anonymization pass
            (may be less than len(events)*strip_fraction if adversarial hard-drift
            anchor events had to be excluded from eligibility)
    """
    events = []
    members = []
    adversarial_cases = []
    adversarial_member_map = {}
    hard_drift_ids = []

    # Create adversarial member pairs (separate from regular population)
    for i in range(n_adversarial_pairs):
        # Household twin pair: two different members sharing IP and device
        idx_a = len(members)
        idx_b = idx_a + 1
        member_a = _make_member(idx_a)
        member_b = _make_member(idx_b)

        shared_ip = member_a["ip"]
        shared_dev = member_a["device_fingerprint"]
        member_b["ip"] = shared_ip
        member_b["device_fingerprint"] = shared_dev

        members.append(member_a)
        members.append(member_b)

        case_idx = len(adversarial_cases)
        adversarial_cases.append({
            "type": "household_twin",
            "member_ids": [member_a["true_member_id"], member_b["true_member_id"]],
            "event_ids": []
        })
        adversarial_member_map[member_a["true_member_id"]] = case_idx
        adversarial_member_map[member_b["true_member_id"]] = case_idx

        # Hard drift member: all probabilistic signals change, but deterministic anchors bridge
        idx_c = len(members)
        member_c = _make_member(idx_c)
        members.append(member_c)

        case_idx = len(adversarial_cases)
        adversarial_cases.append({
            "type": "hard_drift",
            "member_ids": [member_c["true_member_id"]],
            "event_ids": []
        })
        adversarial_member_map[member_c["true_member_id"]] = case_idx
        hard_drift_ids.append(member_c["true_member_id"])

    # Create regular members
    for i in range(len(members), len(members) + num_members):
        members.append(_make_member(i))

    # Inject shared-IP households among regular members (not adversarial)
    non_adv_members = [m for m in members if m["true_member_id"] not in adversarial_member_map]
    if non_adv_members:
        shared = random.sample(non_adv_members, max(2, int(len(non_adv_members) * household_ip_fraction)))
        for i in range(0, len(shared) - 1, 2):
            shared[i + 1]["ip"] = shared[i]["ip"]

    # Mark regular members for intra-journey IP drift
    drift_candidates = [m for m in non_adv_members]
    if drift_candidates:
        ip_drift_members = random.sample(drift_candidates, max(1, int(len(drift_candidates) * ip_drift_fraction)))
        for member in ip_drift_members:
            member["_ip_drift"] = True
            member["_second_ip"] = _gen_ip()

    # Generate journeys for all members (regular + adversarial)
    hard_drift_id_set = set(hard_drift_ids)
    base_t = 1_700_000_000.0
    for member in members:
        t0 = base_t + random.uniform(0, 20 * 86400)
        if member["true_member_id"] in hard_drift_id_set:
            # recall trap requires a deterministic field (dispute_ref) persisting
            # across the journey; silent_dropoff/routine don't carry one, which
            # would make the drift unlinkable by construction, not a real test
            archetype = "escalation_path"
        else:
            archetype = _pick_archetype()
        _gen_journey(events, member, archetype, t0)
        # skip for hard_drift: an anonymous boost event could land earliest/latest
        # by timestamp with no deterministic field at all, invalidating the anchor check
        if member["true_member_id"] not in hard_drift_id_set and random.random() < 0.3:
            _gen_probabilistic_login_boost(events, member, t0 + random.uniform(-3600, 3600))

    events.sort(key=lambda e: e.timestamp)

    # Track event_ids for adversarial members
    for event in events:
        if event.true_member_id in adversarial_member_map:
            case_idx = adversarial_member_map[event.true_member_id]
            adversarial_cases[case_idx]["event_ids"].append(event.event_id)

    # Apply post-generation transformations
    _apply_ip_drift(events, members)
    _inject_hard_drift(events, hard_drift_ids)
    protected_ids = {eid for c in adversarial_cases for eid in c["event_ids"]}
    n_stripped = _strip_identifiers(events, strip_fraction, protected_ids)

    events.sort(key=lambda e: e.timestamp)
    return events, members, adversarial_cases, n_stripped


def inject_chaos_cohort(n_cohort=500, n_control=500, base_t=1_700_000_000.0):
    """Manually inject an anomaly cohort for the actionability chaos test.

    Cohort: APP_DISPUTE_SUBMIT_FAIL -> CALL -> CALL -> BRANCH -> CALL, unresolved,
    spread over ~4 days -- guaranteed bad outcome (unresolved + repeat calls).
    Control: APP_DISPUTE_SUBMIT_FAIL -> CALL, resolved quickly -- guaranteed good
    outcome, but shares the same leading (APP,APP_DISPUTE_SUBMIT_FAIL)->(CALL,...)
    2-gram as the cohort.

    This design deliberately makes the 2-gram prefix NOT diagnostic (it appears
    equally in both outcome groups, so a correct lift-based miner should score
    it near 1.0x) while the cohort's full 3+-event extension is genuinely
    exclusive to the bad-outcome group. A miner that flags the shared prefix as
    high-risk is measuring novelty/frequency, not outcome correlation; a miner
    that correctly scores the 2-gram low and the extended pattern high is doing
    what it claims. See DESIGN.md Sec.8.3.

    Returns (events, chaos_cases) where chaos_cases is
    [{"type": "cohort"|"control", "member_ids": [...], "event_ids": [...]}, ...]
    """
    events = []
    chaos_cases = []

    for i in range(n_cohort):
        member = _make_member(i)
        member["true_member_id"] = f"CHAOS-COHORT-{i:04d}"
        dispute_ref = _rand_id("CHAOS", 6)  # persistent deterministic anchor across channels,
        t = base_t + random.uniform(0, 20 * 86400)  # same as real archetypes in _gen_journey
        e1 = _emit(events, member, "APP", "APP_DISPUTE_SUBMIT_FAIL", t, dispute_ref=dispute_ref)
        t += random.uniform(1800, 7200)
        e2 = _emit(events, member, "CALL", "DISPUTE_FOLLOWUP", t, dispute_ref=dispute_ref, human_agent=True)
        t += random.uniform(3600, 2 * 86400)
        e3 = _emit(events, member, "CALL", "ESCALATION_REQUEST", t, dispute_ref=dispute_ref, human_agent=True)
        t += random.uniform(3600, 86400)
        e4 = _emit(events, member, "BRANCH", "DISPUTE_FOLLOWUP", t, dispute_ref=dispute_ref, human_agent=True)
        t += random.uniform(3600, 86400)
        e5 = _emit(events, member, "CALL", "ESCALATION_REQUEST", t, dispute_ref=dispute_ref,
                    human_agent=True, resolved=False)
        chaos_cases.append({
            "type": "cohort",
            "member_ids": [member["true_member_id"]],
            "event_ids": [e.event_id for e in (e1, e2, e3, e4, e5)],
        })

    for i in range(n_control):
        member = _make_member(i)
        member["true_member_id"] = f"CHAOS-CONTROL-{i:04d}"
        dispute_ref = _rand_id("CHAOS", 6)
        t = base_t + random.uniform(0, 20 * 86400)
        e1 = _emit(events, member, "APP", "APP_DISPUTE_SUBMIT_FAIL", t, dispute_ref=dispute_ref)
        t += random.uniform(1800, 7200)
        # same (channel,event_type) as the cohort's first call -- genuinely shared 2-gram,
        # differs only in that it resolves here instead of escalating further
        e2 = _emit(events, member, "CALL", "DISPUTE_FOLLOWUP", t, dispute_ref=dispute_ref,
                    human_agent=True, resolved=True)
        chaos_cases.append({
            "type": "control",
            "member_ids": [member["true_member_id"]],
            "event_ids": [e.event_id for e in (e1, e2)],
        })

    events.sort(key=lambda e: e.timestamp)
    return events, chaos_cases


def inject_alert_demo(n=20, base_t=1_700_000_000.0):
    """Journeys crafted to cross the >85 alert threshold (design doc Sec.7),
    since neither the chaos cohort (avg 77, queue tier) nor any archetype
    journey ever reaches it in a normal run: needs C=1.0 (4+ channel
    switches) AND R=1.0 (3+ repeat calls) simultaneously, unresolved,
    spread past 72h so T also saturates. Returns (events, member_ids).
    """
    events = []
    member_ids = []
    for i in range(n):
        member = _make_member(i)
        member["true_member_id"] = f"ALERT-DEMO-{i:04d}"
        member_ids.append(member["true_member_id"])
        dispute_ref = _rand_id("ALERT", 6)
        t = base_t + random.uniform(0, 20 * 86400)
        channels = ["APP", "CALL", "BRANCH", "CALL", "BRANCH", "CALL", "CALL"]
        for j, channel in enumerate(channels):
            event_type = CHANNELS_EVENTS[channel][0] if j == 0 else "DISPUTE_FOLLOWUP"
            _emit(events, member, channel, event_type, t, dispute_ref=dispute_ref,
                  human_agent=(channel != "APP"), resolved=False)
            t += random.uniform(18 * 3600, 30 * 3600)  # ensures >72h total elapsed
    events.sort(key=lambda e: e.timestamp)
    return events, member_ids


if __name__ == "__main__":
    events, members, adversarial_cases, n_stripped = generate()

    # Summary stats
    n_ip_drift = sum(1 for m in members if m.get("_ip_drift"))
    n_household_members = sum(len(c["member_ids"]) for c in adversarial_cases if c["type"] == "household_twin")
    n_hard_drift_members = sum(len(c["member_ids"]) for c in adversarial_cases if c["type"] == "hard_drift")
    n_household_pairs = sum(1 for c in adversarial_cases if c["type"] == "household_twin")
    n_hard_drift_pairs = sum(1 for c in adversarial_cases if c["type"] == "hard_drift")

    print(f"Generated {len(events)} events for {len(members)} members")
    print(f"  Anonymized by strip pass: {n_stripped} events ({100*n_stripped/len(events):.1f}%)")
    print(f"  IP drift: {n_ip_drift} members")
    print(f"  Adversarial pairs: {n_household_pairs} household twins ({n_household_members} members), {n_hard_drift_pairs} hard drift ({n_hard_drift_members} members)")
    print(f"  Total adversarial members: {n_household_members + n_hard_drift_members}")
