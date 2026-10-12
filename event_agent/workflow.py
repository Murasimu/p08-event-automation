"""Workflow registry.

A workflow = (name, trigger types it handles, handler, retry policy).
Handlers are `payload -> result` callables. Retry policy: max attempts
with exponential backoff (base * 2^n, capped), applied by the engine.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

Handler = Callable[[dict[str, Any]], Any]


class UnknownWorkflowError(KeyError):
    pass


@dataclass(frozen=True)
class Workflow:
    name: str
    triggers: tuple[str, ...]      # event types this workflow handles
    handler: Handler
    max_attempts: int = 3
    backoff_base: float = 0.1       # seconds; engine sleeps base * 2^n
    backoff_cap: float = 5.0

    def handles(self, event_type: str) -> bool:
        return event_type in self.triggers

    def backoff_for(self, attempt: int) -> float:
        return min(self.backoff_cap, self.backoff_base * (2 ** attempt))


@dataclass
class WorkflowRegistry:
    _workflows: dict[str, Workflow] = field(default_factory=dict)
    _by_trigger: dict[str, list[str]] = field(default_factory=dict)

    def register(self, wf: Workflow) -> None:
        if wf.name in self._workflows:
            raise ValueError(f"workflow {wf.name!r} already registered")
        self._workflows[wf.name] = wf
        for t in wf.triggers:
            self._by_trigger.setdefault(t, []).append(wf.name)

    def for_event(self, event_type: str) -> list[Workflow]:
        return [self._workflows[n] for n in self._by_trigger.get(event_type, [])]

    def get(self, name: str) -> Workflow:
        if name not in self._workflows:
            raise UnknownWorkflowError(name)
        return self._workflows[name]
