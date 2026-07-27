"""Identity resolution: typed edge graph -> two canonical IDs per event.

Design constraint (from DESIGN.md): plain union-find only ever merges, so a
bad probabilistic match can't be cleanly retracted. Here every candidate
match is kept as an explicit, typed edge (signal, confidence, reason) and
connected components are derived on demand from the *current* valid edge
set. Invalidating an edge (failed step-up confirmation, analyst override)
just removes it from the set that feeds recomputation -- no graph rebuild.

Two components are derived per event:
  - confirmed_id  : component over DETERMINISTIC edges only.
                    Gates agent-facing data exposure.
  - analytics_id  : component over DETERMINISTIC + PROBABILISTIC edges.
                    Aggregate journey analytics only, never surfaced as PII.
"""

from collections import defaultdict

DETERMINISTIC = "deterministic"
PROBABILISTIC = "probabilistic"
TIEBREAKER = "tiebreaker"  # recorded for audit, never merges a component on its own

TIME_PROXIMITY_WINDOW = 3600  # seconds; used to boost weak probabilistic signals


class Edge:
    __slots__ = ("id", "u", "v", "type", "signal", "confidence", "reason", "valid")

    def __init__(self, edge_id, u, v, etype, signal, confidence, reason):
        self.id = edge_id
        self.u = u
        self.v = v
        self.type = etype
        self.signal = signal
        self.confidence = confidence
        self.reason = reason
        self.valid = True


class IdentityGraph:
    def __init__(self):
        self.events = {}          # event_id -> Event
        self.edges = []           # list[Edge]
        self._edge_seq = 0
        # signal value -> list of event_ids sharing it, built incrementally
        self._by_signal = defaultdict(lambda: defaultdict(list))

    # ---- ingestion -------------------------------------------------
    def add_event(self, event):
        self.events[event.event_id] = event
        self._link_deterministic(event)
        self._link_probabilistic(event)
        self._index(event)

    def _index(self, event):
        for field in ("membership_rewards_id", "ani", "card_id", "dispute_ref",
                      "device_fingerprint", "ip", "email_hash"):
            val = getattr(event, field)
            if val:
                self._by_signal[field][val].append(event.event_id)

    def _add_edge(self, u, v, etype, signal, confidence, reason):
        self._edge_seq += 1
        self.edges.append(Edge(self._edge_seq, u, v, etype, signal, confidence, reason))

    DETERMINISTIC_FIELDS = {
        "membership_rewards_id": 1.0,
        "ani": 0.95,
        "card_id": 1.0,
        "dispute_ref": 1.0,
    }

    def _link_deterministic(self, event):
        # ponytail: quadratic edges per shared-signal group (each new event
        # links to every prior event sharing a value) -- fine through ~10k
        # events/run; switch to sampling or a union-find pre-pass on the
        # signal index if a group ever gets huge (e.g. a shared support-line ANI).
        for field, conf in self.DETERMINISTIC_FIELDS.items():
            val = getattr(event, field)
            if not val:
                continue
            for other_id in self._by_signal[field].get(val, []):
                self._add_edge(event.event_id, other_id, DETERMINISTIC, field, conf,
                                f"shared {field}={val}")

    def _link_probabilistic(self, event):
        # device fingerprint match
        if event.device_fingerprint:
            for other_id in self._by_signal["device_fingerprint"].get(event.device_fingerprint, []):
                other = self.events[other_id]
                conf = 0.6
                reason = f"shared device_fingerprint={event.device_fingerprint}"
                if abs(event.timestamp - other.timestamp) <= TIME_PROXIMITY_WINDOW:
                    conf = 0.85
                    reason += " + time-proximity boost"
                self._add_edge(event.event_id, other_id, PROBABILISTIC, "device_fingerprint", conf, reason)

        # email hash match
        if event.email_hash:
            for other_id in self._by_signal["email_hash"].get(event.email_hash, []):
                self._add_edge(event.event_id, other_id, PROBABILISTIC, "email_hash", 0.7,
                                f"shared email_hash={event.email_hash}")

        # ip match -- weak signal, tiebreaker only. Shared households/NAT/VPN
        # make it unreliable standalone (DESIGN.md false-positive-blast-radius
        # refinement), so it is typed TIEBREAKER: recorded for analyst audit
        # but excluded from both confirmed_id and analytics_id component
        # derivation -- it can never merge two events on its own.
        if event.ip:
            for other_id in self._by_signal["ip"].get(event.ip, []):
                self._add_edge(event.event_id, other_id, TIEBREAKER, "ip", 0.3,
                                f"shared ip={event.ip}")

    # ---- component derivation --------------------------------------
    def _adjacency(self, allowed_types):
        adj = defaultdict(set)
        for e in self.edges:
            if e.valid and e.type in allowed_types:
                adj[e.u].add(e.v)
                adj[e.v].add(e.u)
        return adj

    def _components(self, allowed_types):
        adj = self._adjacency(allowed_types)
        seen = set()
        comp_of = {}
        for eid in self.events:
            if eid in seen:
                continue
            stack = [eid]
            seen.add(eid)
            members = [eid]
            while stack:
                cur = stack.pop()
                for nxt in adj[cur]:
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
                        members.append(nxt)
            canonical = min(members)  # stable id derived from event ids
            for m in members:
                comp_of[m] = canonical
        return comp_of

    def resolve_all(self):
        confirmed = self._components({DETERMINISTIC})
        analytics = self._components({DETERMINISTIC, PROBABILISTIC})
        for eid, event in self.events.items():
            event.confirmed_id = "CONFIRMED-" + confirmed[eid]
            event.analytics_id = "ANALYTICS-" + analytics[eid]

    # ---- audit / retraction ------------------------------------------
    def invalidate_edge(self, edge_id):
        for e in self.edges:
            if e.id == edge_id:
                e.valid = False
                return True
        return False

    def edges_for(self, event_id):
        return [e for e in self.edges if e.valid and (e.u == event_id or e.v == event_id)]
