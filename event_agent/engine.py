"""The automation engine: consume events, dedupe, execute workflows with
retries and backoff, dead-letter on exhaustion.

Exactly-once *effects* (not exactly-once delivery): the processed-set
stores dedupe keys of completed executions, so redelivered duplicates are
skipped. The set is bounded (max_processed) with simple FIFO eviction --
swap for Redis/DB in production.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .events import Event
from .queue import DeadLetter, DeadLetterQueue, EventQueue
from .workflow import Workflow, WorkflowRegistry


@dataclass
class EngineStats:
    processed: int = 0
    duplicates_skipped: int = 0
    retried: int = 0
    dead_lettered: int = 0
    no_workflow: int = 0

    def snapshot(self) -> dict:
        return dict(processed=self.processed, duplicates=self.duplicates_skipped,
                    retried=self.retried, dead_lettered=self.dead_lettered,
                    no_workflow=self.no_workflow)


@dataclass
class AutomationEngine:
    registry: WorkflowRegistry
    queue: EventQueue = field(default_factory=EventQueue)
    dlq: DeadLetterQueue = field(default_factory=DeadLetterQueue)
    max_processed: int = 10_000          # dedupe set bound
    stats: EngineStats = field(default_factory=EngineStats)
    _processed: dict[str, str] = field(default_factory=dict)  # dedupe_key -> workflow
    _order: list[str] = field(default_factory=list)           # FIFO for eviction
    _effects_log: list[tuple[str, str, float]] = field(default_factory=list)
    _dead_marked: set[str] = field(default_factory=set)  # dead-lettered keys

    # ------------------------------------------------------------------
    def receive(self, event: Event) -> None:
        """Webhook/queue ingestion point."""
        self.queue.enqueue(event)

    def _mark_processed(self, key: str, workflow: str) -> None:
        if key in self._processed:
            return
        self._processed[key] = workflow
        self._order.append(key)
        while len(self._order) > self.max_processed:
            old = self._order.pop(0)
            self._processed.pop(old, None)

    def _already_processed(self, key: str) -> bool:
        return key in self._processed

    # ------------------------------------------------------------------
    def run_once(self) -> int:
        """Process every event currently in the queue. Returns the number
        of events handled (including duplicates skipped)."""
        handled = 0
        while True:
            event = self.queue.dequeue()
            if event is None:
                break
            self._dispatch(event)
            handled += 1
        return handled

    def _dispatch(self, event: Event) -> None:
        workflows = self.registry.for_event(event.type)
        if not workflows:
            self.stats.no_workflow += 1
            return

        key = event.dedupe_key
        # A duplicate delivery is skipped only if at least one target
        # workflow already completed for it; per-workflow completion is
        # checked again inside _execute (fan-out must not double-run).
        all_done = all(
            self._already_processed(f"{key}|{wf.name}") for wf in workflows
        )
        if workflows and all_done:
            self.stats.duplicates_skipped += 1
            return

        for wf in workflows:
            wkey = f"{key}|{wf.name}"
            if self._already_processed(wkey):
                continue
            if wkey in self._dead_marked:
                # already exhausted retries and dead-lettered: a redelivery
                # must not restart the poison-message loop. Only an explicit
                # dlq.requeue() (which clears the mark) may retry it.
                self.stats.duplicates_skipped += 1
                continue
            self._execute(event, wf, key)

    def _execute(self, event: Event, wf: Workflow, dedupe_key: str) -> None:
        attempts_ts: list[float] = []
        last_error = ""
        for attempt in range(wf.max_attempts):
            attempts_ts.append(time.time())
            try:
                result = wf.handler(event.payload)
                self._mark_processed(f"{dedupe_key}|{wf.name}", wf.name)
                self.stats.processed += 1
                self._effects_log.append((wf.name, event.event_id, time.time()))
                return
            except Exception as exc:
                last_error = str(exc)
                if attempt > 0:  # first failure is not a retry
                    self.stats.retried += 1
                if attempt < wf.max_attempts - 1:
                    time.sleep(wf.backoff_for(attempt))

        # exhausted -> dead letter with full context; mark terminal so
        # duplicate deliveries don't retrigger the poison loop
        wkey = f"{dedupe_key}|{wf.name}"
        self._dead_marked.add(wkey)
        self.dlq.add(DeadLetter(
            event=event, workflow=wf.name, error=last_error,
            attempts=attempts_ts,
        ))
        self.stats.dead_lettered += 1

    # ------------------------------------------------------------------
    def requeue_dlq(self, workflow: str | None = None) -> int:
        """Requeue dead letters into the engine queue, clearing dead-marks
        so the retried executions are allowed to run again."""
        def clear_mark(event: Event, wf_name: str) -> None:
            self._dead_marked.discard(f"{event.dedupe_key}|{wf_name}")
        moved = self.dlq.requeue(self.queue, workflow=workflow,
                                 on_requeue=clear_mark)
        return moved

    def effects(self) -> list[tuple[str, str, float]]:
        """The audit of side effects actually performed (workflow, event, ts)."""
        return list(self._effects_log)
