#!/usr/bin/env python3
"""Demo narrative: Sarah's story, told with real numbers from the actual system.

Scene 1 (micro) -- one card member, resolved on first contact because the
agent has her context instantly.
Scene 2 (macro) -- the aggregate pattern behind cases like Sarah's that
DON'T get resolved on first contact, surfaced by the same pipeline.

Every number printed here is computed by the real pipeline/analytics/eval
code, not scripted -- if the underlying logic changes, this demo's output
changes with it. See DESIGN.md "Demo narrative" for the write-up this
script exists to justify.
"""

from analytics import escalation_breakdown, classify_escalation
from eval_actionability import run_chaos_eval
from export_snapshot import explain_score
from generate_data import generate
from models import Event
from pipeline import run_pipeline, run_streaming_pipeline


def build_sarah_events(base_t):
    """Sarah: mid-dispute in the app, her connection drops, she calls ~90s later.

    The two events share a dispute_ref, not a device/session id -- modeling
    the real mechanism DESIGN.md Sec.4 describes (dispute reference IDs link
    a call back to an app-originated case), the same convention every other
    journey in this prototype's synthetic data already uses.
    """
    dispute_ref = "DSP-SARAH1"
    app_fail = Event(
        event_id="EVT-SARAH-1", channel="APP", event_type="APP_DISPUTE_SUBMIT_FAIL",
        timestamp=base_t, true_member_id="MEMBER-SARAH",
        membership_rewards_id="MR-SARAH01", dispute_ref=dispute_ref,
        device_fingerprint="DEV-SARAH-PHONE", ip="203.0.113.42", email_hash="EMAILHASH-SARAH",
    )
    call_in = Event(
        event_id="EVT-SARAH-2", channel="CALL", event_type="INBOUND_CALL",
        timestamp=base_t + 90, true_member_id="MEMBER-SARAH",
        ani="14085550142", dispute_ref=dispute_ref,
        device_fingerprint="DEV-SARAH-PHONE", ip="203.0.113.42", email_hash="EMAILHASH-SARAH",
        human_agent=True, resolved=False,
    )
    return [app_fail, call_in]


def scene_1(background_events, base_t):
    print("=" * 70)
    print("SCENE 1: Sarah calls in")
    print("=" * 70)
    print("""
Sarah is traveling. She opens the Amex app to dispute a $200 charge she
doesn't recognize -- her connection drops mid-submission. A minute and a
half later, she calls support.
""")

    sarah_events = build_sarah_events(base_t)
    all_events = background_events + sarah_events

    # identity resolution: do her app attempt and her call resolve to one case?
    graph, confirmed_timelines, analytics_timelines = run_pipeline(all_events)
    sarah_confirmed_id = sarah_events[0].confirmed_id
    linked = sarah_events[1].confirmed_id == sarah_confirmed_id
    print(f"Identity resolution: app event and call event -> "
          f"{'SAME case (' + sarah_confirmed_id + ')' if linked else 'NOT linked -- see below'}")
    if not linked:
        print("  (unexpected -- would mean the dispute_ref match failed; not the intended demo state)")
        return

    edges = [e for e in graph.edges_for("EVT-SARAH-1") if e.v == "EVT-SARAH-2" or e.u == "EVT-SARAH-2"]
    for edge in edges:
        print(f"  linked via: {edge.type} / {edge.signal} (confidence {edge.confidence:.0%}) -- {edge.reason}")

    # real-time serving latency: is her app event queryable before she finishes dialing?
    print("\nReal-time serving layer check:")
    result = run_streaming_pipeline(background_events + [sarah_events[0]])
    sarah_latency = next(((sunk - enq) * 1000 for eid, enq, sunk in result["event_latencies"]
                           if eid == "EVT-SARAH-1"), None)
    if sarah_latency is not None:
        print(f"  her app-fail event became queryable in the KV store {sarah_latency:.1f}ms after it happened")
        print(f"  she calls {90}s later -- her context has been sitting there "
              f"for {90 - sarah_latency / 1000:.1f}s by the time the call connects")

    # escalation score at the moment the call connects -- BEFORE resolution
    sarah_journey = sorted([e for e in all_events if e.confirmed_id == sarah_confirmed_id],
                            key=lambda e: e.timestamp)
    breakdown = escalation_breakdown(sarah_journey)
    score = breakdown["score"]
    classification = classify_escalation(score)
    print(f"\nEscalation score at the moment she calls: {score:.1f} ({classification.upper()})")
    print(f"  {explain_score(breakdown, classification)}")
    print("""
The score is NOT dramatic here -- and that's correct, not a shortfall. The
formula rewards accumulated risk (repeat contacts, elapsed unresolved time);
a case that's one channel-switch old hasn't accumulated any yet. The thing
that actually changes this call is the identity-linked timeline itself: the
agent's screen shows her app attempt the instant the call connects, so the
conversation starts as
  "I see you were just trying to dispute a $200 charge on the app a few
   seconds ago -- I have the details right here. Are you safe?"
instead of asking her to repeat everything from scratch. That's the
real-time KV lookup + deterministic dispute_ref linking working exactly as
designed (DESIGN.md Sec.4-5) -- not the escalation score.
""")
    return sarah_confirmed_id


def scene_2(background_events):
    print("=" * 70)
    print("SCENE 2: the pattern behind the Sarahs who AREN'T saved on the first call")
    print("=" * 70)
    print("""
Sarah's own two-event journey is too short and too quickly staffed by a
human agent to register as a risk pattern on its own. But she isn't the
only card member whose app dispute fails and who then calls -- some of
those calls DON'T get resolved on the first contact. That's the aggregate
pattern the same pipeline surfaces automatically, using the actual
chaos-injection eval already built for this system (eval_actionability.py):
""")
    run_chaos_eval(background_members=len(background_events) // 3)
    print("""
Translated: every card member who hits APP_DISPUTE_SUBMIT_FAIL and then
calls shares Sarah's opening two steps. The ones who get a Sarah-style
first-contact resolution never accumulate risk. The ones who don't --
repeat calls, no resolution -- are the exact cohort this pattern isolates,
with escalation scores that push them into the analyst queue automatically.
That's the handoff from Scene 1 to Scene 2: the same event that made
Sarah's call effortless is the event a product team can now see, at scale,
is failing customers before it reaches 10,000 of them.
""")


if __name__ == "__main__":
    t0 = 1_700_500_000.0  # fixed, not wall-clock -- keeps output reproducible
    print("Generating background population for realistic context...\n")
    background_events, _, _, _ = generate(num_members=1500)

    scene_1(background_events, t0)
    scene_2(background_events)
