"""Event queue + dead-letter queue.

Plain FIFO in-memory queue (swap for Redis/SQS in production -- the
engine only needs `dequeue()`). The DLQ keeps the full context of every
failed delivery: the event, the workflow name, the error, and every
attempt with timestamps -- enough to replay `dlq.requeue(name)` after a
fix ships.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from .events import Event


@dataclass
class EventQueue:
    _items: deque[Event] = field(default_factory=deque)

    def enqueue(self, event: Event) -> None:
        self._items.append(event)

    def dequeue(self) -> Event | None:
        return self._items.popleft() if self._items else None

    def __len__(self) -> int:
        return len(self._items)


@dataclass
class DeadLetter:
    event: Event
    workflow: str
    error: str
    attempts: list[float] = field(default_factory=list)  # ts per attempt
    dead_ts: float = field(default_factory=time.time)


class DeadLetterQueue:
    """Named DLQ with inspection + selective requeue."""

    def __init__(self) -> None:
        self._items: deque[DeadLetter] = deque()

    def add(self, dead: DeadLetter) -> None:
        self._items.append(dead)

    def __len__(self) -> int:
        return len(self._items)

    def by_workflow(self, workflow: str) -> list[DeadLetter]:
        return [d for d in self._items if d.workflow == workflow]

    def requeue(self, queue: EventQueue, workflow: str | None = None,
                on_requeue=None) -> int:
        """Move dead letters back to the main queue (optionally only one
        workflow's). Returns the number requeued. `on_requeue(event,
        workflow)` is called per moved item so the engine can clear its
        dead-mark (allowing the retry)."""
        keep: deque[DeadLetter] = deque()
        moved = 0
        while self._items:
            d = self._items.popleft()
            if workflow is None or d.workflow == workflow:
                queue.enqueue(d.event)
                if on_requeue is not None:
                    on_requeue(d.event, d.workflow)
                moved += 1
            else:
                keep.append(d)
        self._items = keep
        return moved

    def dump(self) -> list[dict]:
        return [
            {
                "event_id": d.event.event_id,
                "type": d.event.type,
                "workflow": d.workflow,
                "error": d.error,
                "attempts": len(d.attempts),
            }
            for d in self._items
        ]
