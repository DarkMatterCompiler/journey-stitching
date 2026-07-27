#!/usr/bin/env python3
"""Export identity resolution and escalation data to a self-contained HTML dashboard.

Runs the pipeline on synthetic data and embeds the result as JSON in a standalone
HTML file that requires no server or external dependencies.
"""

import json
import os
from datetime import datetime, timezone

from generate_data import generate
from pipeline import run_pipeline
from analytics import escalation_score, escalation_breakdown, classify_escalation, explain_score
from demo_sarah import compute_demo_data


def format_timestamp(epoch_seconds):
    """Convert epoch seconds to ISO 8601 string for display."""
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_export_dict(graph, confirmed_timelines, events):
    """Build the data dict for embedding in HTML."""
    members_data = []
    counts = {"alert": 0, "queue": 0, "normal": 0}
    event_by_id = {e.event_id: e for e in events}

    for case_number, (confirmed_id, journey) in enumerate(sorted(confirmed_timelines.items()), start=1):
        if not journey:
            continue

        breakdown = escalation_breakdown(journey)
        score = breakdown["score"]
        classification = classify_escalation(score)
        counts[classification] += 1

        timeline = [{
            "event_id": event.event_id,
            "channel": event.channel,
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "timestamp_str": format_timestamp(event.timestamp),
            "resolved": event.resolved,
            "human_agent": event.human_agent,
        } for event in journey]  # already sorted by timestamp

        # Audit edges: for each event in this member's journey, every edge
        # (deterministic/probabilistic/tiebreaker) linking it to another event.
        audit = []
        for event in journey:
            linked_to = []
            for edge in graph.edges_for(event.event_id):
                other_id = edge.v if edge.u == event.event_id else edge.u
                other_event = event_by_id.get(other_id)
                if other_event:
                    linked_to.append({
                        "other_event_id": other_id,
                        "other_channel": other_event.channel,
                        "other_event_type": other_event.event_type,
                        "edge_type": edge.type,
                        "signal": edge.signal,
                        "confidence": edge.confidence,
                        "reason": edge.reason,
                    })
            audit.append({"event_id": event.event_id, "linked_to": linked_to})

        members_data.append({
            "confirmed_id": confirmed_id,
            "case_number": case_number,
            "escalation": {
                "score": score,
                "classification": classification,
                "reason_text": explain_score(breakdown, classification),
                "breakdown": {
                    "switches": breakdown["switches"],
                    "hours": breakdown["hours"],
                    "repeat_calls": breakdown["repeat_calls"],
                },
            },
            "timeline": timeline,
            "audit": audit,
        })

    return {
        "members": members_data,
        "summary": {
            "total_members": len(confirmed_timelines),
            "alerts": counts["alert"],
            "queue": counts["queue"],
            "normal": counts["normal"],
        },
    }


def main():
    print("Generating synthetic data...")
    events, members, adversarial_cases, n_stripped = generate(num_members=300)
    print(f"  Generated {len(events)} events for {len(members)} members")

    print("Running identity resolution pipeline...")
    graph, confirmed_timelines, analytics_timelines = run_pipeline(events)
    print(f"  Resolved into {len(confirmed_timelines)} confirmed member journeys")

    print("Building export data structure...")
    export_data = build_export_dict(graph, confirmed_timelines, events)

    summary = export_data["summary"]
    print(f"  Total members: {summary['total_members']}")
    print(f"  Alerts (score > 85): {summary['alerts']}")
    print(f"  Queue (65 < score <= 85): {summary['queue']}")
    print(f"  Normal (score <= 65): {summary['normal']}")

    print("Computing demo narrative (Sarah's story)...")
    demo_data = compute_demo_data(events, base_t=1_700_500_000.0)

    output_path = "analyst_dashboard.html"
    html_content = generate_html(export_data, demo_data)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    abs_path = os.path.abspath(output_path)
    print(f"\nDashboard written to: {abs_path}")
    print(f"Open in browser: file://{abs_path.replace(chr(92), '/')}")


def render_demo_html(demo):
    """Server-render Sarah's story tab. Not JSON+JS like the other tabs --
    this is one fixed case, not filterable/sortable data, so there's nothing
    JS needs to do here that Python can't do once at export time.
    """
    if not demo or not demo.get("scene1"):
        return '<div class="no-data">Demo data unavailable -- see demo_sarah.py.</div>'

    s1, s2 = demo["scene1"], demo["scene2"]
    early_by = s1["wait_seconds"] - s1["latency_ms"] / 1000

    edge_rows = "".join(f"""
        <div class="demo-edge">
            <span class="edge-type {e['type']}">{e['type'].upper()}</span>
            <span class="demo-edge-signal">{e['signal'].replace('_', ' ')}</span>
            <span class="demo-edge-conf">{e['confidence']:.0%} confidence</span>
        </div>""" for e in s1["edges"])

    timeline_rows = "".join(f"""
        <div class="timeline-event {e['channel']}">
            <div>
                <div class="timeline-header">
                    <span class="channel-pill {e['channel']}">{e['channel']}</span>
                    <span class="event-type">{e['event_type'].replace('_', ' ')}</span>
                    {'<span class="flag human">HUMAN AGENT</span>' if e['human_agent'] else ''}
                </div>
                <div class="event-meta"><code>{e['event_id']}</code></div>
            </div>
        </div>""" for e in s1["timeline"])

    cls = s1["classification"]
    prefix_lift_text = (f"{s2['prefix_lift']:.1f}x" if s2.get("prefix_lift") else "below threshold")
    top_pattern_text = " → ".join(f"{c}:{t}" for c, t in s2["top_pattern"]) if s2.get("top_pattern") else "n/a"

    cohort_pct = min(100, s2["cohort_avg"])
    control_pct = min(100, s2["control_avg"])

    return f"""
    <article class="demo">
        <div class="demo-hero">
            <div class="demo-hero-num">{s1['latency_ms']:.0f}<span class="demo-hero-unit">ms</span></div>
            <p class="demo-hero-caption">
                is how long it took Sarah's failed app dispute to become queryable in the
                real-time store &mdash; <strong>{early_by:.0f} seconds before she even finished dialing.</strong>
            </p>
        </div>

        <section class="demo-scene">
            <div class="demo-scene-label">Scene 1 &mdash; the agent already knows</div>
            <p class="demo-copy">
                Sarah is traveling. She opens the Amex app to dispute a $200 charge she doesn't
                recognize &mdash; her connection drops mid-submission. Ninety seconds later, she
                calls support.
            </p>

            <blockquote class="demo-quote">
                &ldquo;Hi Sarah &mdash; I see you were just trying to dispute a $200 charge on the
                app a few seconds ago. I have the details right here. Are you safe?&rdquo;
            </blockquote>

            <div class="demo-grid">
                <div class="demo-panel">
                    <h4>Why the agent already knew</h4>
                    <div class="demo-edges">{edge_rows}</div>
                </div>
                <div class="demo-panel">
                    <h4>Her case, in order</h4>
                    <div class="timeline">{timeline_rows}</div>
                </div>
            </div>

            <div class="demo-callout">
                <span class="badge {cls}">{cls.upper()} &middot; {s1['score']:.1f} / 100</span>
                <p>{s1['reason_text']} The score isn't dramatic here &mdash; correctly. It rewards
                <em>accumulated</em> risk (repeat contacts, elapsed unresolved time), and a case
                that's one channel-switch old hasn't built any up yet. What actually changed this
                call wasn't an alert &mdash; it was the agent having her timeline already open.</p>
            </div>
        </section>

        <section class="demo-scene">
            <div class="demo-scene-label">Scene 2 &mdash; the pattern behind the Sarahs who aren't saved</div>
            <p class="demo-copy">
                Sarah's own two-event case is too short to register as a risk pattern on its own
                &mdash; and it shouldn't, she was helped immediately. But she isn't the only card
                member whose app dispute fails and who then calls. Some of those calls
                <em>don't</em> get resolved on the first try. That's the same pattern, surfacing
                automatically, in the aggregate:
            </p>

            <div class="demo-grid">
                <div class="demo-panel">
                    <h4>The shared opening step</h4>
                    <p class="demo-stat-small">{prefix_lift_text}</p>
                    <p class="demo-stat-caption">lift for the step Sarah also took &mdash;
                    correctly ignored, since customers who get saved share it too.</p>
                </div>
                <div class="demo-panel">
                    <h4>The pattern that predicts trouble</h4>
                    <p class="demo-stat-small alert-num">{s2['top_lift']:.0f}x</p>
                    <p class="demo-stat-caption">lift for <code>{top_pattern_text}</code> &mdash;
                    found in {s2['top_bad_support']:.0%} of unresolved cases, 0% of resolved ones.</p>
                </div>
            </div>

            <div class="demo-compare">
                <div class="demo-compare-row">
                    <span class="demo-compare-label">Resolved on first contact</span>
                    <div class="demo-compare-track"><div class="demo-compare-fill resolved" style="width:{control_pct}%"></div></div>
                    <span class="demo-compare-value">{s2['control_avg']:.1f} avg &middot; {s2['control_queued']}/{s2['control_total']} queued</span>
                </div>
                <div class="demo-compare-row">
                    <span class="demo-compare-label">Not resolved, repeat contact</span>
                    <div class="demo-compare-track"><div class="demo-compare-fill alert" style="width:{cohort_pct}%"></div></div>
                    <span class="demo-compare-value">{s2['cohort_avg']:.1f} avg &middot; {s2['cohort_queued']}/{s2['cohort_total']} queued</span>
                </div>
            </div>

            <p class="demo-copy">
                The same event that made Sarah's call effortless is, at scale, an event a product
                team can now see is failing customers &mdash; before it reaches the next 10,000 of them.
            </p>
        </section>
    </article>
    """


def generate_html(data, demo=None):
    """Generate self-contained HTML with embedded JSON data.

    Design: a "case file" reading room, not a generic admin table. Serif for
    the narrative (case headers, the plain-language "why"), sans for UI
    chrome (tabs, controls, labels), monospace for raw system identifiers
    (event/case IDs) -- so the typeface itself tells you whether you're
    reading the story or reading the machine's bookkeeping. No external
    fonts/CDNs: the whole thing has to work offline from a double-clicked file.
    """
    json_data = json.dumps(data, indent=2)
    demo_html = render_demo_html(demo)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Case Review -- Cross-Channel Journey Dashboard</title>
    <style>
        :root {{
            --paper: #f7f4ee;
            --paper-raised: #ffffff;
            --ink: #24262b;
            --ink-soft: #6b6558;
            --rule: #e1dacb;
            --brand: #1f4e79;
            --brand-soft: #d9e6f0;
            --alert: #a53326;
            --alert-soft: #f6ded9;
            --queue: #93600c;
            --queue-soft: #f3e4c8;
            --normal: #726c5e;
            --normal-soft: #edeae0;
            --resolved: #3f6144;
            --resolved-soft: #dfe9de;
            --ch-app: #2f5d8a;
            --ch-web: #3f7a5c;
            --ch-call: #96501a;
            --ch-branch: #5c4d82;
            --font-display: Georgia, "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
            --font-body: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            --font-mono: "SF Mono", "Cascadia Code", Consolas, "Liberation Mono", monospace;
        }}

        @media (prefers-color-scheme: dark) {{
            :root {{
                --paper: #1a1b1e;
                --paper-raised: #232427;
                --ink: #ece8de;
                --ink-soft: #a29c8d;
                --rule: #38393d;
                --brand: #7fb0dd;
                --brand-soft: #1e3244;
                --alert: #e3897c;
                --alert-soft: #3b241f;
                --queue: #dcb267;
                --queue-soft: #3a2e17;
                --normal: #a39d8c;
                --normal-soft: #2a2924;
                --resolved: #8ec293;
                --resolved-soft: #21301f;
                --ch-app: #7fb0dd;
                --ch-web: #86c9a1;
                --ch-call: #dd9862;
                --ch-branch: #b09fdb;
            }}
        }}

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        html {{ background: var(--paper); }}

        body {{
            font-family: var(--font-body);
            color: var(--ink);
            line-height: 1.55;
            padding: 32px 20px 80px;
        }}

        .page {{ max-width: 1180px; margin: 0 auto; }}

        header.masthead {{
            border-bottom: 3px solid var(--ink);
            padding-bottom: 18px;
            margin-bottom: 22px;
        }}

        .eyebrow {{
            font-size: 11px;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: var(--ink-soft);
            font-weight: 600;
            margin-bottom: 6px;
        }}

        h1 {{
            font-family: var(--font-display);
            font-size: 30px;
            font-weight: 700;
            letter-spacing: -0.01em;
        }}

        .mission {{
            font-size: 14.5px;
            color: var(--ink-soft);
            margin-top: 8px;
            max-width: 62ch;
        }}

        details.legend {{
            margin-top: 16px;
            background: var(--paper-raised);
            border: 1px solid var(--rule);
            border-radius: 6px;
            padding: 12px 16px;
        }}

        details.legend summary {{
            cursor: pointer;
            font-size: 13px;
            font-weight: 600;
            color: var(--brand);
            list-style: none;
        }}

        details.legend summary::-webkit-details-marker {{ display: none; }}
        details.legend summary::after {{ content: " \\2192"; }}
        details.legend[open] summary::after {{ content: " \\2193"; }}

        .legend-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 14px;
            margin-top: 14px;
            font-size: 13px;
            color: var(--ink-soft);
        }}

        .legend-grid dt {{ font-weight: 700; color: var(--ink); margin-bottom: 2px; }}
        .legend-grid dd {{ margin: 0 0 10px; }}

        nav.tabs {{
            display: flex;
            gap: 4px;
            margin: 26px 0 20px;
            border-bottom: 1px solid var(--rule);
        }}

        .tab-button {{
            font-family: var(--font-body);
            padding: 10px 18px;
            border: 1px solid var(--rule);
            border-bottom: none;
            background: var(--paper);
            color: var(--ink-soft);
            cursor: pointer;
            font-size: 13.5px;
            font-weight: 600;
            border-radius: 6px 6px 0 0;
            position: relative;
            top: 1px;
        }}

        .tab-button.active {{
            background: var(--paper-raised);
            color: var(--brand);
            border-color: var(--rule);
            border-bottom: 1px solid var(--paper-raised);
        }}

        .view {{ display: none; }}
        .view.active {{ display: block; }}

        .view-intro {{
            font-size: 13.5px;
            color: var(--ink-soft);
            margin-bottom: 16px;
            max-width: 70ch;
        }}

        .controls {{
            margin-bottom: 16px;
            display: flex;
            gap: 12px;
            align-items: center;
            flex-wrap: wrap;
        }}

        input[type="text"], select {{
            font-family: var(--font-body);
            padding: 7px 11px;
            border: 1px solid var(--rule);
            border-radius: 5px;
            font-size: 13.5px;
            background: var(--paper-raised);
            color: var(--ink);
        }}

        label.check {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 13px;
            color: var(--ink-soft);
        }}

        table {{ width: 100%; border-collapse: collapse; font-size: 13.5px; background: var(--paper-raised); }}

        th {{
            text-align: left;
            padding: 10px 12px;
            border-bottom: 2px solid var(--ink);
            font-size: 11px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--ink-soft);
            font-weight: 700;
            cursor: pointer;
            user-select: none;
        }}

        th:hover {{ color: var(--brand); }}

        td {{ padding: 11px 12px; border-bottom: 1px solid var(--rule); vertical-align: middle; }}
        tbody tr:hover {{ background: var(--brand-soft); cursor: pointer; }}

        .case-id {{ font-weight: 700; color: var(--brand); }}
        .raw-id {{ font-family: var(--font-mono); font-size: 11px; color: var(--ink-soft); display: block; margin-top: 1px; }}

        .badge {{
            display: inline-block;
            padding: 3px 9px;
            border-radius: 3px;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 0.03em;
        }}

        .badge.alert {{ background: var(--alert-soft); color: var(--alert); }}
        .badge.queue {{ background: var(--queue-soft); color: var(--queue); }}
        .badge.normal {{ background: var(--normal-soft); color: var(--normal); }}

        .gauge-cell {{ min-width: 140px; }}
        .gauge-track {{
            position: relative;
            height: 6px;
            border-radius: 3px;
            background: linear-gradient(to right,
                var(--normal-soft) 0%, var(--normal-soft) 65%,
                var(--queue-soft) 65%, var(--queue-soft) 85%,
                var(--alert-soft) 85%, var(--alert-soft) 100%);
            margin-bottom: 4px;
        }}

        .gauge-marker {{
            position: absolute;
            top: -3px;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            border: 2px solid var(--paper-raised);
            transform: translateX(-50%);
        }}

        .gauge-marker.alert {{ background: var(--alert); }}
        .gauge-marker.queue {{ background: var(--queue); }}
        .gauge-marker.normal {{ background: var(--normal); }}

        .gauge-score {{ font-size: 11px; color: var(--ink-soft); font-family: var(--font-mono); }}

        .channel-pill {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 999px;
            font-size: 10.5px;
            font-weight: 700;
            color: white;
            margin-right: 3px;
        }}

        .channel-pill.APP {{ background: var(--ch-app); }}
        .channel-pill.WEB {{ background: var(--ch-web); }}
        .channel-pill.CALL {{ background: var(--ch-call); }}
        .channel-pill.BRANCH {{ background: var(--ch-branch); }}

        .case-header {{
            background: var(--paper-raised);
            border: 1px solid var(--rule);
            border-left: 4px solid var(--brand);
            border-radius: 6px;
            padding: 16px 20px;
            margin-bottom: 20px;
        }}

        .case-header .case-title {{
            font-family: var(--font-display);
            font-size: 19px;
            font-weight: 700;
            margin-bottom: 2px;
        }}

        .case-header .raw-id {{ margin-bottom: 10px; }}

        .case-header .reason {{
            font-size: 14.5px;
            line-height: 1.5;
            margin-top: 10px;
        }}

        .timeline {{ position: relative; padding: 10px 0; }}

        .timeline-event {{
            display: flex;
            gap: 16px;
            margin-bottom: 4px;
            position: relative;
            padding: 12px 0 12px 26px;
            border-left: 2px solid var(--rule);
        }}

        .timeline-event:last-child {{ border-left: 2px solid transparent; }}

        .timeline-event::before {{
            content: '';
            position: absolute;
            left: -7px;
            top: 16px;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            border: 2px solid var(--paper);
        }}

        .timeline-event.APP::before {{ background: var(--ch-app); }}
        .timeline-event.WEB::before {{ background: var(--ch-web); }}
        .timeline-event.CALL::before {{ background: var(--ch-call); }}
        .timeline-event.BRANCH::before {{ background: var(--ch-branch); }}

        .timeline-header {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 3px; }}

        .event-type {{ font-weight: 700; font-size: 14px; }}

        .event-meta {{ font-size: 12px; color: var(--ink-soft); }}
        .event-meta code {{ font-family: var(--font-mono); font-size: 11px; }}

        .flag {{
            display: inline-block;
            padding: 2px 7px;
            border-radius: 3px;
            font-size: 10.5px;
            font-weight: 700;
        }}

        .flag.resolved {{ background: var(--resolved-soft); color: var(--resolved); }}
        .flag.human {{ background: var(--brand-soft); color: var(--brand); }}

        .audit-event {{
            margin-bottom: 22px;
            padding: 16px 18px;
            border: 1px solid var(--rule);
            border-radius: 6px;
            background: var(--paper-raised);
        }}

        .audit-header {{ font-weight: 700; margin-bottom: 4px; }}
        .audit-header code {{ font-family: var(--font-mono); font-size: 12px; font-weight: 400; color: var(--ink-soft); }}

        .edge {{ padding: 10px 0; border-top: 1px solid var(--rule); font-size: 13px; }}
        .edge:first-of-type {{ border-top: none; }}

        .edge-type {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 10.5px;
            font-weight: 700;
            margin-right: 8px;
        }}

        .edge-type.deterministic {{ background: var(--resolved-soft); color: var(--resolved); }}
        .edge-type.probabilistic {{ background: var(--queue-soft); color: var(--queue); }}
        .edge-type.tiebreaker {{ background: var(--normal-soft); color: var(--normal); }}

        .edge-plain {{ margin-top: 5px; color: var(--ink); }}
        .edge-tech {{ margin-top: 4px; font-size: 11.5px; color: var(--ink-soft); font-family: var(--font-mono); }}

        .no-data {{ color: var(--ink-soft); text-align: center; padding: 30px; font-size: 13.5px; }}

        /* Case study demo tab */
        .demo-hero {{
            text-align: center;
            padding: 36px 20px 32px;
            border-bottom: 1px solid var(--rule);
            margin-bottom: 34px;
        }}

        .demo-hero-num {{
            font-family: var(--font-display);
            font-size: 84px;
            font-weight: 700;
            line-height: 1;
            color: var(--brand);
            letter-spacing: -0.02em;
        }}

        .demo-hero-unit {{ font-size: 30px; font-weight: 400; margin-left: 3px; color: var(--ink-soft); }}

        .demo-hero-caption {{
            font-size: 15.5px;
            color: var(--ink-soft);
            max-width: 46ch;
            margin: 14px auto 0;
            line-height: 1.55;
        }}

        .demo-hero-caption strong {{ color: var(--ink); font-weight: 700; }}

        .demo-scene {{ margin-bottom: 44px; }}

        .demo-scene-label {{
            font-size: 11px;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--brand);
            font-weight: 700;
            margin-bottom: 10px;
        }}

        .demo-copy {{ font-size: 14.5px; line-height: 1.65; max-width: 74ch; margin-bottom: 18px; }}
        .demo-copy em {{ font-style: italic; }}

        .demo-quote {{
            font-family: var(--font-display);
            font-style: italic;
            font-size: 18.5px;
            line-height: 1.5;
            padding: 20px 26px;
            border-left: 3px solid var(--brand);
            background: var(--brand-soft);
            border-radius: 0 6px 6px 0;
            margin: 20px 0 26px;
        }}

        .demo-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 22px; }}

        .demo-panel {{
            background: var(--paper-raised);
            border: 1px solid var(--rule);
            border-radius: 8px;
            padding: 18px 20px;
        }}

        .demo-panel h4 {{
            font-size: 11.5px;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: var(--ink-soft);
            font-weight: 700;
            margin-bottom: 12px;
        }}

        .demo-edges {{ display: flex; flex-direction: column; gap: 10px; }}
        .demo-edge {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; font-size: 12.5px; }}
        .demo-edge-signal {{ text-transform: capitalize; }}
        .demo-edge-conf {{ color: var(--ink-soft); font-size: 11.5px; margin-left: auto; }}

        .demo-callout {{
            background: var(--paper-raised);
            border: 1px solid var(--rule);
            border-left: 4px solid var(--queue);
            border-radius: 6px;
            padding: 16px 20px;
        }}

        .demo-callout p {{ font-size: 13.5px; line-height: 1.6; margin-top: 10px; }}

        .demo-stat-small {{ font-family: var(--font-display); font-size: 32px; font-weight: 700; color: var(--resolved); }}
        .demo-stat-small.alert-num {{ color: var(--alert); }}
        .demo-stat-caption {{ font-size: 12.5px; color: var(--ink-soft); margin-top: 4px; line-height: 1.5; }}
        .demo-stat-caption code {{ font-family: var(--font-mono); font-size: 11px; }}

        .demo-compare {{ display: flex; flex-direction: column; gap: 14px; margin: 22px 0; }}

        .demo-compare-row {{
            display: grid;
            grid-template-columns: 190px 1fr 190px;
            gap: 14px;
            align-items: center;
            font-size: 12.5px;
        }}

        .demo-compare-label {{ color: var(--ink-soft); font-weight: 600; }}
        .demo-compare-track {{ height: 10px; border-radius: 5px; background: var(--normal-soft); overflow: hidden; }}
        .demo-compare-fill {{ height: 100%; border-radius: 5px; }}
        .demo-compare-fill.resolved {{ background: var(--resolved); }}
        .demo-compare-fill.alert {{ background: var(--alert); }}
        .demo-compare-value {{ font-family: var(--font-mono); font-size: 11.5px; color: var(--ink-soft); }}

        @media (max-width: 780px) {{
            .demo-grid {{ grid-template-columns: 1fr; }}
            .demo-compare-row {{ grid-template-columns: 1fr; }}
            .demo-hero-num {{ font-size: 60px; }}
        }}
    </style>
</head>
<body>
    <div class="page">
        <header class="masthead">
            <div class="eyebrow">Cross-Channel Journey Stitching &mdash; Internal Case Review</div>
            <h1>Case Review Dashboard</h1>
            <p class="mission">
                Every card member's activity across app, web, phone, and branch, stitched into one
                story. Use this to find where an experience broke down, see a customer's full
                history before you call them back, and check why the system believes two
                interactions are the same person.
            </p>
            <details class="legend">
                <summary>How to read this</summary>
                <dl class="legend-grid">
                    <div>
                        <dt>Risk score</dt>
                        <dd>0&ndash;100, combining how many times a customer switched channels,
                        how long the issue has been open, repeat phone contact, and whether it's
                        resolved. Above 65 enters the review queue; above 85 triggers an alert.</dd>
                    </div>
                    <div>
                        <dt>Case ID vs. raw ID</dt>
                        <dd>"Case #12" is a friendly label for this dashboard. The
                        <code style="font-family:var(--font-mono)">CONFIRMED-EVT-&hellip;</code> code
                        beneath it is the system's actual identifier &mdash; use it if you need to
                        cross-reference elsewhere.</dd>
                    </div>
                    <div>
                        <dt>Identity Merge Audit</dt>
                        <dd>Shows why the system believes two interactions belong to the same
                        person. "Deterministic" means certain (same account login, phone number
                        on file, etc). "Probabilistic" means likely, not certain. "Tiebreaker"
                        (e.g. shared IP address) is recorded for reference only &mdash; it never
                        merges two people on its own, since a shared home network isn't proof of
                        identity.</dd>
                    </div>
                </dl>
            </details>
        </header>

        <nav class="tabs">
            <button class="tab-button active" onclick="switchTab(event, 'demo')">Case Study: Sarah</button>
            <button class="tab-button" onclick="switchTab(event, 'queue')">Review Queue</button>
            <button class="tab-button" onclick="switchTab(event, 'timeline')">Customer Story</button>
            <button class="tab-button" onclick="switchTab(event, 'audit')">Identity Merge Audit</button>
        </nav>

        <div id="demo" class="view active">{demo_html}</div>

        <div id="queue" class="view">
            <p class="view-intro" id="queueIntro">Loading&hellip;</p>
            <div class="controls">
                <input type="text" id="queueSearch" placeholder="Search case or ID&hellip;" onkeyup="filterQueue()">
                <label class="check">
                    <input type="checkbox" id="showAll" onchange="filterQueue()">
                    Show everyone, not just flagged cases
                </label>
            </div>
            <table id="queueTable">
                <thead>
                    <tr>
                        <th onclick="sortQueue('case_number')">Case</th>
                        <th onclick="sortQueue('score')">Risk</th>
                        <th>Status</th>
                        <th>Channels touched</th>
                        <th>Last activity</th>
                        <th>Resolved</th>
                    </tr>
                </thead>
                <tbody id="queueBody"></tbody>
            </table>
        </div>

        <div id="timeline" class="view">
            <p class="view-intro">Every event for one customer, in order, across all four channels &mdash; the single story a case is scattered pieces of everywhere else.</p>
            <div class="controls">
                <label>Select a case:
                    <select id="memberSelect" onchange="showTimeline()">
                        <option value="">&mdash; choose a case &mdash;</option>
                    </select>
                </label>
            </div>
            <div id="timelineContent"></div>
        </div>

        <div id="audit" class="view">
            <p class="view-intro">For each interaction in a case, every reason the system linked it to another &mdash; so you can verify the match, not just trust it.</p>
            <div class="controls">
                <label>Select a case:
                    <select id="auditMemberSelect" onchange="showAudit()">
                        <option value="">&mdash; choose a case &mdash;</option>
                    </select>
                </label>
            </div>
            <div id="auditContent"></div>
        </div>
    </div>

    <script>
        const DATA = {json_data};

        let queueSortField = 'score';
        let queueSortAscending = false;

        function switchTab(event, tabName) {{
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');
            if (tabName === 'queue') initQueue();
            else if (tabName === 'timeline') initTimeline();
            else if (tabName === 'audit') initAudit();
        }}

        function initQueue() {{
            const flagged = DATA.summary.alerts + DATA.summary.queue;
            document.getElementById('queueIntro').textContent =
                `${{DATA.summary.total_members}} cases total. ${{flagged}} need review right now `+
                `(${{DATA.summary.alerts}} at alert level, ${{DATA.summary.queue}} in the queue).`;
            filterQueue();
        }}

        function scoreOf(member) {{ return member.escalation.score; }}

        function filterQueue() {{
            const search = document.getElementById('queueSearch').value.toLowerCase();
            const showAll = document.getElementById('showAll').checked;
            const tbody = document.getElementById('queueBody');
            tbody.innerHTML = '';

            let rows = DATA.members.filter(m => {{
                if (!showAll && m.escalation.classification === 'normal') return false;
                if (search) {{
                    const hay = (`case ${{m.case_number}} ${{m.confirmed_id}}`).toLowerCase();
                    if (!hay.includes(search)) return false;
                }}
                return true;
            }});

            rows.sort((a, b) => {{
                let aVal = queueSortField === 'score' ? scoreOf(a) : a[queueSortField];
                let bVal = queueSortField === 'score' ? scoreOf(b) : b[queueSortField];
                return queueSortAscending ? aVal - bVal : bVal - aVal;
            }});

            if (rows.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="6" class="no-data">No cases match. Try "Show everyone."</td></tr>';
                return;
            }}

            rows.forEach(member => {{
                const cls = member.escalation.classification;
                const score = member.escalation.score;
                const channels = [...new Set(member.timeline.map(e => e.channel))];
                const lastEvent = member.timeline[member.timeline.length - 1];
                const resolved = member.timeline.some(e => e.resolved) ? 'Yes' : 'No';

                const row = document.createElement('tr');
                row.onclick = () => clickMember(member.confirmed_id);
                row.innerHTML = `
                    <td>
                        <span class="case-id">Case #${{member.case_number}}</span>
                        <span class="raw-id">${{member.confirmed_id}}</span>
                    </td>
                    <td class="gauge-cell">
                        <div class="gauge-track">
                            <div class="gauge-marker ${{cls}}" style="left:${{score}}%"></div>
                        </div>
                        <span class="gauge-score">${{score.toFixed(1)}} / 100</span>
                    </td>
                    <td><span class="badge ${{cls}}">${{cls.toUpperCase()}}</span></td>
                    <td>${{channels.map(c => `<span class="channel-pill ${{c}}">${{c}}</span>`).join('')}}</td>
                    <td>${{new Date(lastEvent.timestamp_str).toLocaleString()}}</td>
                    <td>${{resolved}}</td>
                `;
                tbody.appendChild(row);
            }});
        }}

        function sortQueue(field) {{
            if (queueSortField === field) {{ queueSortAscending = !queueSortAscending; }}
            else {{ queueSortField = field; queueSortAscending = false; }}
            filterQueue();
        }}

        function clickMember(confirmedId) {{
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
            document.getElementById('timeline').classList.add('active');
            document.querySelectorAll('.tab-button')[1].classList.add('active');
            initTimeline();  // dropdown only populates on tab-switch; this bypasses that, so do it first
            document.getElementById('memberSelect').value = confirmedId;
            showTimeline();
        }}

        function populateSelect(select) {{
            select.innerHTML = '<option value="">&mdash; choose a case &mdash;</option>';
            DATA.members.forEach(m => {{
                const opt = document.createElement('option');
                opt.value = m.confirmed_id;
                opt.text = `Case #${{m.case_number}} (${{m.escalation.classification}}, score ${{m.escalation.score.toFixed(0)}})`;
                select.appendChild(opt);
            }});
        }}

        function initTimeline() {{ populateSelect(document.getElementById('memberSelect')); }}
        function initAudit() {{ populateSelect(document.getElementById('auditMemberSelect')); }}

        function showTimeline() {{
            const confirmedId = document.getElementById('memberSelect').value;
            const content = document.getElementById('timelineContent');
            if (!confirmedId) {{ content.innerHTML = '<div class="no-data">Choose a case above to see its story.</div>'; return; }}

            const member = DATA.members.find(m => m.confirmed_id === confirmedId);
            const cls = member.escalation.classification;

            let html = `<div class="case-header">
                <div class="case-title">Case #${{member.case_number}} <span class="badge ${{cls}}">${{cls.toUpperCase()}}</span></div>
                <span class="raw-id">${{member.confirmed_id}}</span>
                <p class="reason">${{member.escalation.reason_text}}</p>
            </div>`;

            html += '<div class="timeline">';
            member.timeline.forEach(event => {{
                const resolved = event.resolved ? '<span class="flag resolved">RESOLVED</span>' : '';
                const human = event.human_agent ? '<span class="flag human">HUMAN AGENT</span>' : '';
                html += `
                    <div class="timeline-event ${{event.channel}}">
                        <div>
                            <div class="timeline-header">
                                <span class="channel-pill ${{event.channel}}">${{event.channel}}</span>
                                <span class="event-type">${{event.event_type.replaceAll('_', ' ')}}</span>
                                ${{resolved}}${{human}}
                            </div>
                            <div class="event-meta">
                                ${{new Date(event.timestamp_str).toLocaleString()}} &middot; <code>${{event.event_id}}</code>
                            </div>
                        </div>
                    </div>
                `;
            }});
            html += '</div>';
            content.innerHTML = html;
        }}

        const EDGE_PLAIN = {{
            deterministic: 'Certain match',
            probabilistic: 'Likely match',
            tiebreaker: 'Reference only \\u2014 never merges identities on its own',
        }};

        const SIGNAL_PLAIN = {{
            membership_rewards_id: 'same account login',
            ani: 'same phone number on file',
            card_id: 'same card presented',
            dispute_ref: 'same case reference number',
            device_fingerprint: 'same device',
            email_hash: 'same email on file',
            ip: 'same network/IP address',
        }};

        function showAudit() {{
            const confirmedId = document.getElementById('auditMemberSelect').value;
            const content = document.getElementById('auditContent');
            if (!confirmedId) {{ content.innerHTML = '<div class="no-data">Choose a case above to see its identity links.</div>'; return; }}

            const member = DATA.members.find(m => m.confirmed_id === confirmedId);
            let html = `<div class="case-header" style="margin-bottom:18px;">
                <div class="case-title">Case #${{member.case_number}}</div>
                <span class="raw-id">${{member.confirmed_id}} &middot; ${{member.audit.length}} interactions</span>
            </div>`;

            member.audit.forEach(auditEvent => {{
                html += `<div class="audit-event"><div class="audit-header">Interaction <code>${{auditEvent.event_id}}</code></div>`;
                if (auditEvent.linked_to.length === 0) {{
                    html += '<div class="no-data" style="padding:10px 0;">No identity links found for this interaction.</div>';
                }} else {{
                    auditEvent.linked_to.forEach(link => {{
                        const typeClass = link.edge_type.toLowerCase();
                        const plain = EDGE_PLAIN[typeClass] || link.edge_type;
                        const signalPlain = SIGNAL_PLAIN[link.signal] || link.signal;
                        html += `
                            <div class="edge">
                                <span class="edge-type ${{typeClass}}">${{link.edge_type.toUpperCase()}}</span>
                                <span class="channel-pill ${{link.other_channel}}">${{link.other_channel}}</span>
                                linked to <code>${{link.other_event_id}}</code>
                                <div class="edge-plain">${{plain}} &mdash; ${{signalPlain}} (${{(link.confidence * 100).toFixed(0)}}% confidence)</div>
                                <div class="edge-tech">${{link.reason}}</div>
                            </div>
                        `;
                    }});
                }}
                html += '</div>';
            }});
            content.innerHTML = html;
        }}

        document.addEventListener('DOMContentLoaded', initQueue);
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
