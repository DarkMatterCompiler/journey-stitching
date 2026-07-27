"""Common event envelope shared by all four channels."""

from dataclasses import dataclass, field


@dataclass
class Event:
    event_id: str
    channel: str          # APP | WEB | CALL | BRANCH
    event_type: str
    timestamp: float      # seconds, synthetic clock
    true_member_id: str   # ground truth, NEVER visible to resolution logic

    # deterministic identity signals (present only when the channel can supply them)
    membership_rewards_id: str = None
    ani: str = None
    card_id: str = None
    dispute_ref: str = None

    # probabilistic identity signals
    device_fingerprint: str = None
    ip: str = None
    email_hash: str = None

    # journey/outcome metadata used by analytics, not identity resolution
    resolved: bool = False
    human_agent: bool = False

    # filled in by the pipeline
    confirmed_id: str = None
    analytics_id: str = None
