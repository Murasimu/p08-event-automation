"""Tests for the event-triggered automation agent. Offline, deterministic."""

import time

import pytest

from event_agent import (
    AutomationEngine, Event, EventQueue, Workflow, WorkflowRegistry,
    compute_dedupe_key,
)
from event_agent.queue import DeadLetter, DeadLetterQueue


# ---------------------------------------------------------------- events
class TestEvents:
    def test_dedupe_key_stable_across_event_ids(self):
        a = Event("webhook:stripe", "payment.captured", {"order": "A-1"})
        b = Event("webhook:stripe", "payment.captured", {"order": "A-1"})
        assert a.event_id != b.event_id  # producer retried with new id
        assert a.dedupe_key == b.dedupe_key

    def test_dedupe_key_payload_order_insensitive(self):
        k1 = compute_dedupe_key("s", "t", {"a": 1, "b": 2})
        k2 = compute_dedupe_key("s", "t", {"b": 2, "a": 1})
        assert k1 == k2

    def test_dedupe_key_differs_for_different_payloads(self):
        k1 = compute_dedupe_key("s", "t", {"a": 1})
        k2 = compute_dedupe_key("s", "t", {"a": 2})
        assert k1 != k2

    def test_dedupe_key_differs_for_different_type(self):
        assert compute_dedupe_key("s", "t1", {}) != compute_dedupe_key("s", "t2", {})


# ---------------------------------------------------------------- queue
class TestQueues:
    def test_fifo(self):
        q = EventQueue()
        e1, e2 = Event("s", "t1"), Event("s", "t2")
        q.enqueue(e1)
        q.enqueue(e2)
        assert q.dequeue() is e1
        assert q.dequeue() is e2
        assert q.dequeue() is None

    def test_dlq_add_inspect_requeue(self):
        q = EventQueue()
        dlq = DeadLetterQueue()
        ev = Event("s", "boom.t")
        dlq.add(DeadLetter(ev, "wf-a", "kaput", attempts=[1.0, 2.0]))
        dlq.add(DeadLetter(Event("s", "other.t"), "wf-b", "nope"))
        assert len(dlq) == 2
        assert len(dlq.by_workflow("wf-a")) == 1
        moved = dlq.requeue(q, workflow="wf-a")
        assert moved == 1 and len(q) == 1
        assert len(dlq) == 1  # wf-b stays

    def test_dlq_dump(self):
        dlq = DeadLetterQueue()
        dlq.add(DeadLetter(Event("s", "x.t", {"k": 1}), "wf", "err",
                           attempts=[1.0, 2.0, 3.0]))
        d = dlq.dump()[0]
        assert d["workflow"] == "wf" and d["attempts"] == 3 and d["error"] == "err"


# ---------------------------------------------------------------- registry
class TestRegistry:
    def test_register_and_route(self):
        r = WorkflowRegistry()
        r.register(Workflow("a", ("t1",), lambda p: 1))
        r.register(Workflow("b", ("t1", "t2"), lambda p: 2))
        assert [w.name for w in r.for_event("t1")] == ["a", "b"]
        assert [w.name for w in r.for_event("t2")] == ["b"]
        assert r.for_event("t9") == []

    def test_duplicate_registration(self):
        r = WorkflowRegistry()
        r.register(Workflow("a", ("t",), lambda p: 1))
        with pytest.raises(ValueError):
            r.register(Workflow("a", ("t",), lambda p: 2))

    def test_backoff_growth(self):
        wf = Workflow("a", ("t",), lambda p: 1, backoff_base=0.01)
        assert wf.backoff_for(0) == 0.01
        assert wf.backoff_for(2) == 0.04
        assert wf.backoff_for(10) == 5.0  # capped


# ---------------------------------------------------------------- engine
class TestEngine:
    def _engine(self, *wfs, **kw):
        r = WorkflowRegistry()
        for w in wfs:
            r.register(w)
        return AutomationEngine(registry=r, **kw)

    def test_event_triggers_workflow(self):
        calls = []
        engine = self._engine(Workflow("w", ("order.created",),
                                       lambda p: calls.append(p) or "ok"))
        engine.receive(Event("queue:orders", "order.created", {"id": "O-9"}))
        engine.run_once()
        assert calls == [{"id": "O-9"}]
        assert engine.stats.processed == 1

    def test_idempotent_duplicate_skipped(self):
        calls = []
        engine = self._engine(Workflow("w", ("t",), lambda p: calls.append(1)))
        for _ in range(3):  # webhook retried 3x, same payload
            engine.receive(Event("webhook:stripe", "t", {"order": "A-1"}))
        engine.run_once()
        assert len(calls) == 1
        assert engine.stats.duplicates_skipped == 2

    def test_same_payload_different_type_runs(self):
        calls = []
        engine = self._engine(
            Workflow("a", ("t1",), lambda p: calls.append("a")),
            Workflow("b", ("t2",), lambda p: calls.append("b")),
        )
        engine.receive(Event("s", "t1", {"x": 1}))
        engine.receive(Event("s", "t2", {"x": 1}))
        engine.run_once()
        assert calls == ["a", "b"]

    def test_multiple_workflows_per_event(self):
        calls = []
        engine = self._engine(
            Workflow("a", ("pay",), lambda p: calls.append("a")),
            Workflow("b", ("pay",), lambda p: calls.append("b")),
        )
        engine.receive(Event("s", "pay", {"id": 1}))
        engine.run_once()
        assert calls == ["a", "b"]  # fan-out, each deduped independently

    def test_retry_then_success(self):
        attempts = []

        def flaky(p):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return "ok"

        engine = self._engine(Workflow("w", ("t",), flaky, max_attempts=3,
                                       backoff_base=0.0))
        engine.receive(Event("s", "t"))
        engine.run_once()
        assert len(attempts) == 3
        assert engine.stats.processed == 1 and engine.stats.retried == 1  # attempts 2&3; only the failed one after the first counts
        assert len(engine.dlq) == 0

    def test_exhausted_retries_dead_letter(self):
        def always_fails(p):
            raise ValueError("permanent failure")

        engine = self._engine(Workflow("w", ("t",), always_fails,
                                       max_attempts=3, backoff_base=0.0))
        ev = Event("s", "t", {"k": 1})
        engine.receive(ev)
        engine.run_once()
        assert len(engine.dlq) == 1
        assert engine.stats.dead_lettered == 1
        d = engine.dlq.dump()[0]
        assert d["attempts"] == 3 and "permanent failure" in d["error"]

    def test_dlq_requeue_retries_after_fix(self):
        state = {"fixed": False}

        def broken_then_fixed(p):
            if not state["fixed"]:
                raise RuntimeError("bug")
            return "ok"

        engine = self._engine(Workflow("w", ("t",), broken_then_fixed,
                                       max_attempts=1, backoff_base=0.0))
        engine.receive(Event("s", "t", {"k": 1}))
        engine.run_once()
        assert len(engine.dlq) == 1

        state["fixed"] = True  # deploy the fix
        moved = engine.requeue_dlq(workflow="w")
        engine.run_once()
        assert moved == 1
        assert engine.stats.processed == 1
        assert len(engine.dlq) == 0

    def test_redelivery_does_not_retrigger_dead_workflows(self):
        def always_fails(p):
            raise RuntimeError("permanent")

        engine = self._engine(Workflow("w", ("t",), always_fails,
                                       max_attempts=2, backoff_base=0.0))
        for _ in range(5):  # 5 duplicate deliveries of a poison message
            engine.receive(Event("s", "t", {"k": 1}))
        engine.run_once()
        # executed the retry policy once, then dead-lettered terminally;
        # the 4 duplicate deliveries were skipped, not re-executed
        assert engine.stats.dead_lettered == 1
        assert engine.stats.retried == 1  # 2 attempts = 1 retry, once only
        assert engine.stats.duplicates_skipped == 4
        assert len(engine.dlq) == 1

    def test_no_workflow_handlers(self):
        engine = self._engine()
        engine.receive(Event("s", "unhandled.t"))
        engine.run_once()
        assert engine.stats.no_workflow == 1

    def test_effects_audit(self):
        engine = self._engine(Workflow("w", ("t",), lambda p: "ok"))
        engine.receive(Event("s", "t", {"k": 1}))
        engine.run_once()
        assert len(engine.effects()) == 1
        wf, eid, ts = engine.effects()[0]
        assert wf == "w" and ts > 0

    def test_dedupe_set_bounded(self):
        engine = self._engine(Workflow("w", ("t",), lambda p: "ok"),
                              max_processed=3)
        for i in range(5):
            engine.receive(Event("s", "t", {"i": i}))
        engine.run_once()
        # 5 distinct payloads processed; set keeps only last 3 keys
        assert engine.stats.processed == 5
        assert len(engine._processed) <= 3


# ---------------------------------------------------------------- webhook
class TestWebhookAPI:
    def test_endpoints(self):
        from fastapi.testclient import TestClient
        from event_agent.webhook import app

        client = TestClient(app)
        r = client.post("/webhook/payment", json={
            "event_type": "payment.captured",
            "payload": {"order_id": "A-1", "amount": 4200}})
        assert r.status_code == 200
        assert r.json()["handled"] == 1

        # duplicate webhook delivery -> deduped
        r2 = client.post("/webhook/payment", json={
            "event_type": "payment.captured",
            "payload": {"order_id": "A-1", "amount": 4200}})
        assert r2.json()["stats"]["duplicates"] == 1

        s = client.get("/stats")
        assert s.status_code == 200 and "dlq" in s.json()
