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
from analytics import escalation_score, escalation_breakdown, classify_escalation


def format_timestamp(epoch_seconds):
    """Convert epoch seconds to ISO 8601 string for display."""
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_export_dict(graph, confirmed_timelines, events):
    """Build the data dict for embedding in HTML.

    Returns:
        {
            "members": [
                {
                    "confirmed_id": str,
                    "escalation": {"score": float, "classification": str, "breakdown": {...}},
                    "timeline": [{"event_id", "channel", "event_type", "timestamp_str",
                                  "resolved", "human_agent"}, ...],
                    "audit": [{"event_id", "linked_to": [{"other_id", "type", "signal",
                              "confidence", "reason"}, ...]}, ...]
                },
                ...
            ],
            "summary": {"total_members": int, "alerts": int, "queue": int, "normal": int}
        }
    """
    members_data = []
    counts = {"alert": 0, "queue": 0, "normal": 0}

    # Index all events by event_id for fast lookup
    event_by_id = {e.event_id: e for e in events}

    for confirmed_id, journey in sorted(confirmed_timelines.items()):
        if not journey:
            continue

        # Compute escalation metrics
        breakdown = escalation_breakdown(journey)
        score = breakdown["score"]
        classification = classify_escalation(score)
        counts[classification] += 1

        # Build timeline with readable timestamps
        timeline = []
        for event in journey:  # already sorted by timestamp
            timeline.append({
                "event_id": event.event_id,
                "channel": event.channel,
                "event_type": event.event_type,
                "timestamp": event.timestamp,
                "timestamp_str": format_timestamp(event.timestamp),
                "resolved": event.resolved,
                "human_agent": event.human_agent,
            })

        # Build audit edges: for each event in this member's journey,
        # show all edges (deterministic/probabilistic/tiebreaker) to other events
        audit = []
        for event in journey:
            edges = graph.edges_for(event.event_id)
            linked_to = []
            for edge in edges:
                # Determine which endpoint is the "other" event
                other_id = edge.v if edge.u == event.event_id else edge.u
                other_event = event_by_id.get(other_id)

                # Only include if other event is in a different journey (cross-identity)
                # or same journey (for showing intra-member links)
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

            audit.append({
                "event_id": event.event_id,
                "linked_to": linked_to,
            })

        members_data.append({
            "confirmed_id": confirmed_id,
            "escalation": {
                "score": score,
                "classification": classification,
                "breakdown": {
                    "C": breakdown["C"],  # channel switches component
                    "T": breakdown["T"],  # time component
                    "R": breakdown["R"],  # repeat calls component
                    "res": breakdown["res"],  # resolved component
                    "P": breakdown["P"],  # human agent component
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

    # Generate HTML dashboard
    output_path = "analyst_dashboard.html"
    html_content = generate_html(export_data)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    abs_path = os.path.abspath(output_path)
    print(f"\nDashboard written to: {abs_path}")
    print(f"Open in browser: file://{abs_path.replace(chr(92), '/')}")


def generate_html(data):
    """Generate self-contained HTML with embedded JSON data."""
    json_data = json.dumps(data, indent=2)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Amex Identity Resolution Analyst Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        html {{
            background-color: #f5f5f5;
            color: #333;
        }}

        @media (prefers-color-scheme: dark) {{
            html {{
                background-color: #1a1a1a;
                color: #e0e0e0;
            }}
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            line-height: 1.5;
            padding: 20px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 20px;
        }}

        @media (prefers-color-scheme: dark) {{
            .container {{
                background: #2a2a2a;
                box-shadow: 0 2px 4px rgba(0,0,0,0.3);
            }}
        }}

        h1 {{
            font-size: 28px;
            margin-bottom: 10px;
            color: #003d82;
        }}

        @media (prefers-color-scheme: dark) {{
            h1 {{
                color: #66b3ff;
            }}
        }}

        .header-info {{
            font-size: 14px;
            color: #666;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 1px solid #ddd;
        }}

        @media (prefers-color-scheme: dark) {{
            .header-info {{
                color: #999;
                border-bottom-color: #444;
            }}
        }}

        .tabs {{
            display: flex;
            gap: 0;
            margin-bottom: 20px;
            border-bottom: 2px solid #ddd;
        }}

        @media (prefers-color-scheme: dark) {{
            .tabs {{
                border-bottom-color: #444;
            }}
        }}

        .tab-button {{
            padding: 12px 20px;
            border: none;
            background: none;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            color: #666;
            border-bottom: 3px solid transparent;
            transition: all 0.2s;
        }}

        @media (prefers-color-scheme: dark) {{
            .tab-button {{
                color: #999;
            }}
        }}

        .tab-button.active {{
            color: #003d82;
            border-bottom-color: #003d82;
        }}

        @media (prefers-color-scheme: dark) {{
            .tab-button.active {{
                color: #66b3ff;
                border-bottom-color: #66b3ff;
            }}
        }}

        .tab-button:hover {{
            color: #003d82;
        }}

        @media (prefers-color-scheme: dark) {{
            .tab-button:hover {{
                color: #66b3ff;
            }}
        }}

        .view {{
            display: none;
        }}

        .view.active {{
            display: block;
        }}

        /* Queue view */
        .queue-controls {{
            margin-bottom: 15px;
            display: flex;
            gap: 10px;
            align-items: center;
        }}

        .queue-controls input {{
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }}

        @media (prefers-color-scheme: dark) {{
            .queue-controls input {{
                background: #333;
                border-color: #555;
                color: #e0e0e0;
            }}
        }}

        .queue-controls label {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 14px;
        }}

        .queue-controls input[type="checkbox"] {{
            width: auto;
            padding: 0;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}

        th {{
            text-align: left;
            padding: 12px;
            background: #f5f5f5;
            border-bottom: 2px solid #ddd;
            font-weight: 600;
            color: #333;
            cursor: pointer;
            user-select: none;
        }}

        @media (prefers-color-scheme: dark) {{
            th {{
                background: #333;
                border-bottom-color: #555;
                color: #e0e0e0;
            }}
        }}

        th:hover {{
            background: #eee;
        }}

        @media (prefers-color-scheme: dark) {{
            th:hover {{
                background: #3a3a3a;
            }}
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid #eee;
        }}

        @media (prefers-color-scheme: dark) {{
            td {{
                border-bottom-color: #444;
            }}
        }}

        tbody tr:hover {{
            background: #f9f9f9;
            cursor: pointer;
        }}

        @media (prefers-color-scheme: dark) {{
            tbody tr:hover {{
                background: #333;
            }}
        }}

        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: 600;
        }}

        .badge.alert {{
            background: #fee;
            color: #c33;
        }}

        @media (prefers-color-scheme: dark) {{
            .badge.alert {{
                background: #5c2e2e;
                color: #ff6b6b;
            }}
        }}

        .badge.queue {{
            background: #ffeaa7;
            color: #d97706;
        }}

        @media (prefers-color-scheme: dark) {{
            .badge.queue {{
                background: #5c4e2e;
                color: #fbbf24;
            }}
        }}

        .badge.normal {{
            background: #e0e0e0;
            color: #666;
        }}

        @media (prefers-color-scheme: dark) {{
            .badge.normal {{
                background: #444;
                color: #999;
            }}
        }}

        /* Timeline */
        .timeline-controls {{
            margin-bottom: 15px;
        }}

        .timeline-controls select {{
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }}

        @media (prefers-color-scheme: dark) {{
            .timeline-controls select {{
                background: #333;
                border-color: #555;
                color: #e0e0e0;
            }}
        }}

        .timeline {{
            position: relative;
            padding: 20px 0;
        }}

        .timeline-event {{
            display: flex;
            gap: 20px;
            margin-bottom: 20px;
            position: relative;
            padding-left: 30px;
        }}

        .timeline-event::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 8px;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #ddd;
            border: 2px solid white;
        }}

        @media (prefers-color-scheme: dark) {{
            .timeline-event::before {{
                background: #666;
                border-color: #2a2a2a;
            }}
        }}

        .timeline-event.APP::before {{
            background: #4285f4;
        }}

        .timeline-event.WEB::before {{
            background: #34a853;
        }}

        .timeline-event.CALL::before {{
            background: #ea4335;
        }}

        .timeline-event.BRANCH::before {{
            background: #fbbc04;
        }}

        .timeline-content {{
            flex: 1;
        }}

        .timeline-header {{
            display: flex;
            gap: 10px;
            align-items: center;
            margin-bottom: 4px;
            flex-wrap: wrap;
        }}

        .channel-badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 11px;
            font-weight: 700;
            color: white;
        }}

        .channel-badge.APP {{
            background: #4285f4;
        }}

        .channel-badge.WEB {{
            background: #34a853;
        }}

        .channel-badge.CALL {{
            background: #ea4335;
        }}

        .channel-badge.BRANCH {{
            background: #fbbc04;
            color: #333;
        }}

        .event-type {{
            font-weight: 500;
            color: #333;
        }}

        @media (prefers-color-scheme: dark) {{
            .event-type {{
                color: #e0e0e0;
            }}
        }}

        .event-meta {{
            font-size: 12px;
            color: #666;
            margin-top: 4px;
        }}

        @media (prefers-color-scheme: dark) {{
            .event-meta {{
                color: #999;
            }}
        }}

        .flag {{
            display: inline-block;
            padding: 2px 6px;
            margin-left: 4px;
            border-radius: 2px;
            font-size: 11px;
            font-weight: 600;
        }}

        .flag.resolved {{
            background: #d4edda;
            color: #155724;
        }}

        @media (prefers-color-scheme: dark) {{
            .flag.resolved {{
                background: #2d5a3a;
                color: #5dd98b;
            }}
        }}

        .flag.human {{
            background: #cfe2ff;
            color: #084298;
        }}

        @media (prefers-color-scheme: dark) {{
            .flag.human {{
                background: #2d4563;
                color: #5d9eff;
            }}
        }}

        /* Audit */
        .audit-event {{
            margin-bottom: 30px;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: #fafafa;
        }}

        @media (prefers-color-scheme: dark) {{
            .audit-event {{
                background: #2a2a2a;
                border-color: #444;
            }}
        }}

        .audit-header {{
            font-weight: 600;
            margin-bottom: 10px;
            color: #333;
        }}

        @media (prefers-color-scheme: dark) {{
            .audit-header {{
                color: #e0e0e0;
            }}
        }}

        .edge-list {{
            padding-left: 20px;
        }}

        .edge {{
            padding: 8px 0;
            border-bottom: 1px solid #e0e0e0;
            font-size: 13px;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge {{
                border-bottom-color: #444;
            }}
        }}

        .edge:last-child {{
            border-bottom: none;
        }}

        .edge-type {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: 2px;
            font-size: 11px;
            font-weight: 600;
            margin-right: 8px;
        }}

        .edge-type.deterministic {{
            background: #d4edda;
            color: #155724;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge-type.deterministic {{
                background: #2d5a3a;
                color: #5dd98b;
            }}
        }}

        .edge-type.probabilistic {{
            background: #fff3cd;
            color: #856404;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge-type.probabilistic {{
                background: #5c4e2e;
                color: #fbbf24;
            }}
        }}

        .edge-type.tiebreaker {{
            background: #e0e0e0;
            color: #666;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge-type.tiebreaker {{
                background: #444;
                color: #999;
            }}
        }}

        .edge-signal {{
            display: inline-block;
            color: #0066cc;
            font-weight: 500;
            margin: 0 4px;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge-signal {{
                color: #66b3ff;
            }}
        }}

        .edge-confidence {{
            font-size: 12px;
            color: #666;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge-confidence {{
                color: #999;
            }}
        }}

        .edge-reason {{
            font-size: 12px;
            color: #666;
            margin-top: 4px;
            padding-left: 6px;
            border-left: 2px solid #ddd;
        }}

        @media (prefers-color-scheme: dark) {{
            .edge-reason {{
                color: #999;
                border-left-color: #555;
            }}
        }}

        .no-data {{
            color: #999;
            text-align: center;
            padding: 20px;
        }}

        @media (prefers-color-scheme: dark) {{
            .no-data {{
                color: #666;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Amex Identity Resolution Analyst Dashboard</h1>
        <div class="header-info" id="headerInfo">
            Loading...
        </div>

        <div class="tabs">
            <button class="tab-button active" onclick="switchTab(event, 'queue')">
                SLA / Escalation Queue
            </button>
            <button class="tab-button" onclick="switchTab(event, 'timeline')">
                Per-Member Timeline
            </button>
            <button class="tab-button" onclick="switchTab(event, 'audit')">
                Identity Merge Audit
            </button>
        </div>

        <!-- Queue View -->
        <div id="queue" class="view active">
            <div class="queue-controls">
                <input type="text" id="queueSearch" placeholder="Search confirmed ID..."
                       onkeyup="filterQueue()">
                <label>
                    <input type="checkbox" id="showAll" onchange="filterQueue()">
                    Show all (including normal)
                </label>
            </div>
            <table id="queueTable">
                <thead>
                    <tr>
                        <th onclick="sortQueue('confirmed_id')">Confirmed ID</th>
                        <th onclick="sortQueue('score')" style="text-align: right;">Score</th>
                        <th onclick="sortQueue('classification')">Classification</th>
                        <th>Channels</th>
                        <th>Last Event</th>
                        <th>Resolved</th>
                    </tr>
                </thead>
                <tbody id="queueBody">
                </tbody>
            </table>
        </div>

        <!-- Timeline View -->
        <div id="timeline" class="view">
            <div class="timeline-controls">
                <label>Select member:
                    <select id="memberSelect" onchange="showTimeline()">
                        <option value="">-- Select a member --</option>
                    </select>
                </label>
            </div>
            <div id="timelineContent"></div>
        </div>

        <!-- Audit View -->
        <div id="audit" class="view">
            <div class="timeline-controls">
                <label>Select member:
                    <select id="auditMemberSelect" onchange="showAudit()">
                        <option value="">-- Select a member --</option>
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
            // Hide all views
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));

            // Show selected view and mark button active
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');

            // Initialize view data
            if (tabName === 'queue') {{
                initQueue();
            }} else if (tabName === 'timeline') {{
                initTimeline();
            }} else if (tabName === 'audit') {{
                initAudit();
            }}
        }}

        function initQueue() {{
            const header = document.getElementById('headerInfo');
            header.innerHTML = `Total members: ${{DATA.summary.total_members}} |
                Alerts: ${{DATA.summary.alerts}} |
                Queue: ${{DATA.summary.queue}} |
                Normal: ${{DATA.summary.normal}}`;

            filterQueue();
        }}

        function filterQueue() {{
            const search = document.getElementById('queueSearch').value.toLowerCase();
            const showAll = document.getElementById('showAll').checked;
            const tbody = document.getElementById('queueBody');
            tbody.innerHTML = '';

            let rows = [];
            DATA.members.forEach(member => {{
                const classification = member.escalation.classification;
                if (!showAll && classification === 'normal') return;

                if (search && !member.confirmed_id.toLowerCase().includes(search)) return;

                rows.push(member);
            }});

            // Apply current sort
            rows.sort((a, b) => {{
                let aVal = a.escalation[queueSortField] ?? a[queueSortField];
                let bVal = b.escalation[queueSortField] ?? b[queueSortField];

                if (typeof aVal === 'string') {{
                    return queueSortAscending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }} else {{
                    return queueSortAscending ? aVal - bVal : bVal - aVal;
                }}
            }});

            rows.forEach(member => {{
                const row = document.createElement('tr');
                const score = member.escalation.score.toFixed(1);
                const classification = member.escalation.classification;
                const channels = new Set(member.timeline.map(e => e.channel));
                const lastEvent = member.timeline[member.timeline.length - 1];
                const resolved = member.timeline.some(e => e.resolved) ? 'Yes' : 'No';

                row.innerHTML = `
                    <td style="cursor: pointer; color: #0066cc;"
                        onclick="clickMember('${{member.confirmed_id}}')">
                        ${{member.confirmed_id}}
                    </td>
                    <td style="text-align: right; font-weight: 600;">${{score}}</td>
                    <td><span class="badge ${{classification}}">${{classification.toUpperCase()}}</span></td>
                    <td>${{Array.from(channels).join(', ')}}</td>
                    <td>${{new Date(lastEvent.timestamp_str).toLocaleString()}}</td>
                    <td>${{resolved}}</td>
                `;
                tbody.appendChild(row);
            }});
        }}

        function sortQueue(field) {{
            if (queueSortField === field) {{
                queueSortAscending = !queueSortAscending;
            }} else {{
                queueSortField = field;
                queueSortAscending = false;
            }}
            filterQueue();
        }}

        function clickMember(confirmedId) {{
            // Switch to timeline view and select member
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
            document.getElementById('timeline').classList.add('active');
            document.querySelectorAll('.tab-button')[1].classList.add('active');

            initTimeline();  // dropdown is only populated on tab-switch via switchTab(); this
                              // bypasses switchTab(), so populate it explicitly before selecting
            document.getElementById('memberSelect').value = confirmedId;
            showTimeline();
        }}

        function initTimeline() {{
            const select = document.getElementById('memberSelect');
            select.innerHTML = '<option value="">-- Select a member --</option>';
            DATA.members.forEach(member => {{
                const opt = document.createElement('option');
                opt.value = member.confirmed_id;
                opt.text = member.confirmed_id;
                select.appendChild(opt);
            }});
        }}

        function showTimeline() {{
            const confirmedId = document.getElementById('memberSelect').value;
            const content = document.getElementById('timelineContent');

            if (!confirmedId) {{
                content.innerHTML = '<div class="no-data">Select a member to view timeline</div>';
                return;
            }}

            const member = DATA.members.find(m => m.confirmed_id === confirmedId);
            if (!member) {{
                content.innerHTML = '<div class="no-data">Member not found</div>';
                return;
            }}

            let html = `<div style="margin-bottom: 15px; padding: 12px; background: #f5f5f5; border-radius: 4px;">
                <strong>${{member.confirmed_id}}</strong> |
                Score: <span style="font-weight: 600;">${{member.escalation.score.toFixed(1)}}</span> |
                Classification: <span class="badge ${{member.escalation.classification}}">${{member.escalation.classification.toUpperCase()}}</span>
            </div>`;

            html += '<div class="timeline">';
            member.timeline.forEach(event => {{
                const resolved = event.resolved ? '<span class="flag resolved">RESOLVED</span>' : '';
                const human = event.human_agent ? '<span class="flag human">HUMAN AGENT</span>' : '';

                html += `
                    <div class="timeline-event ${{event.channel}}">
                        <div class="timeline-content">
                            <div class="timeline-header">
                                <span class="channel-badge ${{event.channel}}">${{event.channel}}</span>
                                <span class="event-type">${{event.event_type}}</span>
                                ${{resolved}}${{human}}
                            </div>
                            <div class="event-meta">
                                ${{new Date(event.timestamp_str).toLocaleString()}}
                                <br>Event ID: <code>${{event.event_id}}</code>
                            </div>
                        </div>
                    </div>
                `;
            }});
            html += '</div>';

            content.innerHTML = html;
        }}

        function initAudit() {{
            const select = document.getElementById('auditMemberSelect');
            select.innerHTML = '<option value="">-- Select a member --</option>';
            DATA.members.forEach(member => {{
                const opt = document.createElement('option');
                opt.value = member.confirmed_id;
                opt.text = member.confirmed_id;
                select.appendChild(opt);
            }});
        }}

        function showAudit() {{
            const confirmedId = document.getElementById('auditMemberSelect').value;
            const content = document.getElementById('auditContent');

            if (!confirmedId) {{
                content.innerHTML = '<div class="no-data">Select a member to view audit</div>';
                return;
            }}

            const member = DATA.members.find(m => m.confirmed_id === confirmedId);
            if (!member) {{
                content.innerHTML = '<div class="no-data">Member not found</div>';
                return;
            }}

            let html = `<div style="margin-bottom: 15px; padding: 12px; background: #f5f5f5; border-radius: 4px;">
                <strong>${{member.confirmed_id}}</strong> |
                ${{member.audit.length}} events in this member's journey
            </div>`;

            html += '<div class="audit-list">';
            member.audit.forEach(auditEvent => {{
                html += `<div class="audit-event">
                    <div class="audit-header">Event: ${{auditEvent.event_id}}</div>`;

                if (auditEvent.linked_to.length === 0) {{
                    html += '<div class="edge-list"><div class="no-data">No identity links</div></div>';
                }} else {{
                    html += '<div class="edge-list">';
                    auditEvent.linked_to.forEach(link => {{
                        const typeClass = link.edge_type.toLowerCase();
                        html += `
                            <div class="edge">
                                <span class="edge-type ${{typeClass}}">${{link.edge_type.toUpperCase()}}</span>
                                ${{typeClass === 'tiebreaker' ? '<span class="edge-confidence">(audit only -- never merges identities)</span> ' : ''}}
                                <strong>${{link.other_event_id}}</strong>
                                (<span class="channel-badge ${{link.other_channel}}">${{link.other_channel}}</span>
                                ${{link.other_event_type}})
                                <div class="edge-confidence">
                                    Signal: <span class="edge-signal">${{link.signal}}</span> |
                                    Confidence: ${{(link.confidence * 100).toFixed(0)}}%
                                </div>
                                <div class="edge-reason">
                                    ${{link.reason}}
                                </div>
                            </div>
                        `;
                    }});
                    html += '</div>';
                }}

                html += '</div>';
            }});
            html += '</div>';

            content.innerHTML = html;
        }}

        // Initialize on load
        document.addEventListener('DOMContentLoaded', initQueue);
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
