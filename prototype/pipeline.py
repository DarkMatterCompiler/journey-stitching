"""Ingest -> identity resolve -> stitch into per-member timelines.

Stands in for the Kafka/Spark streaming pipeline described in DESIGN.md:
same conceptual stages (normalize, resolve identity, stitch), executed
in-process over a batch of events instead of a live stream. Watermarking /
late-arriving batch reconciliation is not simulated here -- events are
already timestamped and sorted; a real deployment adds that as a separate
concern on top of this resolution+stitching core.
"""

import random
import sqlite3
import threading
import time
from collections import defaultdict
from queue import Queue

from identity_graph import IdentityGraph

_stream_rng = random.Random(42)  # separate from generate_data's global seed


def _group_and_sort(events, key_attr):
    grouped = defaultdict(list)
    for event in events:
        grouped[getattr(event, key_attr)].append(event)
    for tl in grouped.values():
        tl.sort(key=lambda e: e.timestamp)
    return dict(grouped)


def run_pipeline(events):
    """Returns (graph, confirmed_timelines, analytics_timelines).

    Two groupings, not one -- per DESIGN.md Sec.4/7: confirmed_timelines
    (deterministic-only identity) is what agent-facing views and the
    escalation queue use, since it's the data-exposure gate. analytics_timelines
    (deterministic + probabilistic identity) is what sequence mining and
    aggregate analytics use, since it also captures anonymous pre-login
    activity (e.g. an ANON_BROWSE session before a login) that confirmed_id
    alone would leave as an unlinked singleton.
    """
    graph = IdentityGraph()
    for event in events:
        graph.add_event(event)  # normalization is a no-op here; events already share the envelope
    graph.resolve_all()

    confirmed_timelines = _group_and_sort(events, "confirmed_id")
    analytics_timelines = _group_and_sort(events, "analytics_id")

    return graph, confirmed_timelines, analytics_timelines


# ---- Streaming pipeline simulation (new) -----

class StreamingSink:
    """Wraps warehouse (SQLite) and KV (dict) sinks."""

    def __init__(self, db_path=":memory:"):
        self.db_path = db_path
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()
        self.kv = {}  # confirmed_id -> latest member state
        self.lock = threading.Lock()

    def _init_schema(self):
        """Create warehouse table if not exists."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                channel TEXT,
                event_type TEXT,
                timestamp REAL,
                true_member_id TEXT,
                membership_rewards_id TEXT,
                ani TEXT,
                card_id TEXT,
                dispute_ref TEXT,
                device_fingerprint TEXT,
                ip TEXT,
                email_hash TEXT,
                resolved BOOLEAN,
                human_agent BOOLEAN,
                confirmed_id TEXT,
                analytics_id TEXT
            )
        """)
        self.db.commit()

    def sink_event(self, event):
        """Write event to both warehouse (SQLite) and KV (dict)."""
        with self.lock:
            # Warehouse write
            self.db.execute("""
                INSERT INTO events (event_id, channel, event_type, timestamp,
                    true_member_id, membership_rewards_id, ani, card_id,
                    dispute_ref, device_fingerprint, ip, email_hash,
                    resolved, human_agent, confirmed_id, analytics_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.event_id, event.channel, event.event_type, event.timestamp,
                event.true_member_id, event.membership_rewards_id, event.ani,
                event.card_id, event.dispute_ref, event.device_fingerprint,
                event.ip, event.email_hash, event.resolved, event.human_agent,
                event.confirmed_id, event.analytics_id
            ))
            self.db.commit()

            # KV store write (latest state per confirmed_id)
            if event.confirmed_id:
                self.kv[event.confirmed_id] = {
                    "latest_event_id": event.event_id,
                    "latest_timestamp": event.timestamp,
                    "latest_channel": event.channel,
                    "event_count": self.kv.get(event.confirmed_id, {}).get("event_count", 0) + 1,
                    "all_event_ids": self.kv.get(event.confirmed_id, {}).get("all_event_ids", []) + [event.event_id],
                    "resolved": event.resolved,
                    "human_agent": event.human_agent,
                }

    def close(self):
        self.db.close()


def _producer(events, queue_obj, event_timestamps):
    """Push events onto queue, recording enqueue timestamp."""
    for event in events:
        event_timestamps[event.event_id]["enqueued_at"] = time.time()
        queue_obj.put(event)


_MICROBATCH_SIZE = 25  # Spark Structured Streaming resolves per micro-batch, not per event --
                        # resolve_all() is O(V+E) over everything seen so far, so calling it
                        # per-event makes the whole run O(n^2) and inflates measured latency
                        # for reasons that have nothing to do with the architecture being tested.


def _consumer(queue_obj, graph, sink, event_timestamps):
    """Pull events from queue, resolve identity in micro-batches, write to sinks."""
    pending = []
    while True:
        event = queue_obj.get()
        done = event is None  # sentinel
        if not done:
            graph.add_event(event)
            pending.append(event)

        if pending and (done or len(pending) >= _MICROBATCH_SIZE):
            graph.resolve_all()
            now = time.time()
            for e in pending:
                sink.sink_event(e)
                event_timestamps[e.event_id]["sunk_at"] = now
            pending = []

        if done:
            break


def _simulate_arrival_time(event):
    """Event time != arrival time: CALL/BRANCH are batch exports that can lag
    their own event time by up to ~30h (usually under the 24h watermark, a
    minority over it); APP/WEB arrive near-instantly. Without this split,
    events already arrive pre-sorted by event time and a "late" watermark
    has nothing to measure -- everything would trivially be on-time."""
    if event.channel in ("CALL", "BRANCH"):
        return event.timestamp + _stream_rng.uniform(0, 30 * 3600)
    return event.timestamp + _stream_rng.uniform(0, 5)


def _split_on_time_late(events, late_threshold_seconds=86400):
    """Order events by simulated arrival time, then apply a rolling watermark:
    an event is late if, by the time it arrives, the max event-time already
    seen has moved more than late_threshold_seconds past this event's own
    event-time (i.e. a live stream processor would already have closed its window).
    """
    if not events:
        return [], []

    arrivals = sorted(events, key=_simulate_arrival_time)

    on_time = []
    late = []
    watermark = float("-inf")
    for event in arrivals:
        if event.timestamp < watermark - late_threshold_seconds:
            late.append(event)
        else:
            on_time.append(event)
        watermark = max(watermark, event.timestamp)

    return on_time, late


def reconcile_late_events(late_events, graph, sink, event_timestamps):
    """Batch reconciliation for late-arriving events.

    Add them to the graph, re-resolve all, and sink.
    """
    for event in late_events:
        event_timestamps[event.event_id]["enqueued_at"] = time.time()
        graph.add_event(event)

    graph.resolve_all()

    # Update all events (late + previous) to reflect new resolved identities
    for event in late_events:
        sink.sink_event(event)
        event_timestamps[event.event_id]["sunk_at"] = time.time()


def run_streaming_pipeline(events, late_threshold_seconds=86400):
    """Run streaming pipeline simulation with dual sinks and watermark handling.

    Returns a dict with:
        - db_path: path to SQLite database
        - kv: dict of current member state (confirmed_id -> latest state)
        - event_latencies: list of (event_id, enqueued_at, sunk_at) tuples
        - on_time_count: number of on-time events processed
        - late_count: number of late events processed
        - graph: final IdentityGraph after all resolution
    """
    # Split events into on-time and late
    on_time_events, late_events = _split_on_time_late(events, late_threshold_seconds)

    # Initialize sinks and graph
    sink = StreamingSink()
    graph = IdentityGraph()
    event_timestamps = {e.event_id: {} for e in events}

    # Run streaming pipeline on on-time events
    queue_obj = Queue()

    producer_thread = threading.Thread(
        target=_producer,
        args=(on_time_events, queue_obj, event_timestamps)
    )
    consumer_thread = threading.Thread(
        target=_consumer,
        args=(queue_obj, graph, sink, event_timestamps)
    )

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    queue_obj.put(None)  # Sentinel
    consumer_thread.join()

    # Reconcile late events
    if late_events:
        reconcile_late_events(late_events, graph, sink, event_timestamps)

    # Collect latency records
    event_latencies = []
    for event_id, timestamps in event_timestamps.items():
        if "enqueued_at" in timestamps and "sunk_at" in timestamps:
            event_latencies.append((
                event_id,
                timestamps["enqueued_at"],
                timestamps["sunk_at"]
            ))

    return {
        "db_path": sink.db_path,
        "kv": sink.kv,
        "event_latencies": event_latencies,
        "on_time_count": len(on_time_events),
        "late_count": len(late_events),
        "graph": graph,
        "sink": sink,
    }


def _percentile(values, p):
    """Compute pth percentile of a list of values."""
    if not values:
        return 0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * p / 100.0)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


if __name__ == "__main__":
    from generate_data import generate

    # Generate test data
    print("Generating synthetic data...")
    events, members, adversarial_cases, n_stripped = generate(num_members=500, strip_fraction=0.4)
    print(f"Generated {len(events)} events for {len(members)} members\n")

    # Run streaming pipeline
    print("Running streaming pipeline...")
    result = run_streaming_pipeline(events, late_threshold_seconds=86400)

    # Extract results
    kv = result["kv"]
    event_latencies = result["event_latencies"]
    on_time_count = result["on_time_count"]
    late_count = result["late_count"]

    # Compute latencies in milliseconds
    latencies_ms = [
        (t_sunk - t_enqueued) * 1000.0
        for _, t_enqueued, t_sunk in event_latencies
    ]

    # Print summary
    print(f"\nPipeline Summary:")
    print(f"  Total events processed: {len(events)}")
    print(f"  On-time events: {on_time_count}")
    print(f"  Late events: {late_count}")
    print(f"  Events with latency data: {len(latencies_ms)}")

    if latencies_ms:
        p50 = _percentile(latencies_ms, 50)
        p95 = _percentile(latencies_ms, 95)
        p99 = _percentile(latencies_ms, 99)
        print(f"\nEnd-to-end latency (enqueue to sink):")
        print(f"  p50: {p50:.2f} ms")
        print(f"  p95: {p95:.2f} ms")
        print(f"  p99: {p99:.2f} ms")

    print(f"\nKV Store (real-time member state):")
    print(f"  Total members tracked: {len(kv)}")

    # Sample a few KV lookups
    if kv:
        sample_ids = list(kv.keys())[:3]
        for cid in sample_ids:
            state = kv[cid]
            print(f"  {cid}: latest_event={state['latest_event_id']}, "
                  f"count={state['event_count']}, "
                  f"channel={state['latest_channel']}")

    # Verify warehouse
    graph = result["graph"]
    print(f"\nWarehouse (SQLite):")
    sink = result["sink"]
    cursor = sink.db.execute("SELECT COUNT(*) FROM events")
    db_event_count = cursor.fetchone()[0]
    print(f"  Events in warehouse: {db_event_count}")

    print("\nStreaming pipeline complete.")
