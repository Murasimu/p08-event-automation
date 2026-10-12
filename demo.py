"""Demo: order lifecycle automation over webhooks/queues.

A fictional store fires webhook events (payment captured, order
created, shipment delivered). The engine:
- dedupes webhook retries (exactly-once effects),
- fans out one event to multiple workflows,
- retries a flaky handler with backoff,
- dead-letters a permanently failing one, then requeues after a 'fix'.

Usage:
  python demo.py
"""

from __future__ import annotations

import time

from event_agent import AutomationEngine, Event, Workflow, WorkflowRegistry


def main():
    calls: list[str] = []

    def notify_fulfillment(p):
        calls.append(f"notify-fulfillment({p['order_id']})")
        return "ok"

    def backup(p):
        calls.append(f"backup({p['order_id']})")
        return "ok"

    state = {"attempts": 0}

    def flaky_analytics(p):
        state["attempts"] += 1
        if state["attempts"] < 3:
            raise RuntimeError("analytics db temporarily unavailable")
        calls.append(f"analytics({p['order_id']})")
        return "ok"

    def broken_receipts(p):
        raise RuntimeError("receipt template missing")

    registry = WorkflowRegistry()
    registry.register(Workflow("notify-fulfillment", ("payment.captured",),
                               notify_fulfillment))
    registry.register(Workflow("backup", ("payment.captured", "order.created"),
                               backup))
    registry.register(Workflow("analytics", ("payment.captured",),
                               flaky_analytics, max_attempts=3, backoff_base=0.01))
    registry.register(Workflow("receipts", ("payment.captured",),
                               broken_receipts, max_attempts=2, backoff_base=0.01))

    engine = AutomationEngine(registry=registry)

    print("=" * 62)
    print("1) payment.captured -> fan-out to 4 workflows")
    print("=" * 62)
    engine.receive(Event("webhook:stripe", "payment.captured",
                         {"order_id": "A-1001", "amount": 4200}))
    engine.run_once()
    for c in calls:
        print(f"  effect: {c}")
    print(f"  stats: {engine.stats.snapshot()}")
    print(f"  dlq: {engine.dlq.dump()}")

    print()
    print("=" * 62)
    print("2) webhook retries the same payment -> deduped")
    print("=" * 62)
    before = len(calls)
    for _ in range(3):
        engine.receive(Event("webhook:stripe", "payment.captured",
                             {"order_id": "A-1001", "amount": 4200}))
    engine.run_once()
    print(f"  new effects after 3 duplicate deliveries: {len(calls) - before}")
    print(f"  stats: {engine.stats.snapshot()}")

    print()
    print("=" * 62)
    print("3) order.created -> only the backup workflow cares")
    print("=" * 62)
    before = len(calls)
    engine.receive(Event("queue:orders", "order.created", {"order_id": "A-1002"}))
    engine.run_once()
    print(f"  new effects: {calls[before:]}")

    print()
    print("=" * 62)
    print("4) fix ships -> DLQ requeue -> receipts now succeed")
    print("=" * 62)
    # swap the broken handler for a fixed one
    registry._workflows["receipts"] = Workflow(
        "receipts", ("payment.captured",),
        lambda p: calls.append(f"receipts({p['order_id']})") or "ok")
    moved = engine.requeue_dlq(workflow="receipts")
    engine.run_once()
    print(f"  requeued: {moved}, dlq now: {len(engine.dlq)}")
    print(f"  stats: {engine.stats.snapshot()}")
    print(f"  final effects: {[c for c in calls if c.startswith('receipts')]}")


if __name__ == "__main__":
    main()
