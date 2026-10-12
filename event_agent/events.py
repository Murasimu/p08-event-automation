"""Event envelope + idempotency key computation.

An Event is (source, type, payload, event_id). The dedupe key is derived
from source+type+payload hash, so natural duplicates (webhook retries,
at-least-once queue redeliveries) collapse to one execution even when
the producer generated different event_ids.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


def _stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def compute_dedupe_key(source: str, type_: str, payload: dict[str, Any]) -> str:
    h = hashlib.sha256(
        f"{source}|{type_}|{_stable_json(payload)}".encode()
    ).hexdigest()[:16]
    return h


@dataclass
class Event:
    source: str                 # e.g. "webhook:stripe", "queue:orders"
    type: str                   # e.g. "payment.captured"
    payload: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    ts: float = field(default_factory=time.time)

    @property
    def dedupe_key(self) -> str:
        return compute_dedupe_key(self.source, self.type, self.payload)
