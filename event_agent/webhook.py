"""FastAPI webhook receiver exposing the engine over HTTP.

Production wiring example: Stripe/GitHub-style webhooks POST here, the
engine dedupes and processes them with retry + DLQ semantics. Run with
`uvicorn event_agent.webhook:app`, then:

  curl -X POST localhost:8000/webhook/payment \\
       -H 'Content-Type: application/json' \\
       -d '{"event_type":"payment.captured","order_id":"A-1","amount":4200}'

Requires: pip install fastapi uvicorn (already in [demo] extras).
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .engine import AutomationEngine
from .events import Event
from .workflow import Workflow, WorkflowRegistry

app = FastAPI(title="Event Automation Agent", version="1.0.0")

registry = WorkflowRegistry()
registry.register(Workflow(
    name="notify-fulfillment",
    triggers=("payment.captured",),
    handler=lambda p: {"notified": True, "order": p.get("order_id")},
))
registry.register(Workflow(
    name="backup-order",
    triggers=("payment.captured", "order.created"),
    handler=lambda p: {"backed_up": True},
))

engine = AutomationEngine(registry=registry)


class WebhookBody(BaseModel):
    event_type: str
    payload: dict = {}


@app.post("/webhook/{source}")
def webhook(source: str, body: WebhookBody):
    event = Event(source=f"webhook:{source}", type=body.event_type,
                  payload=body.payload)
    engine.receive(event)
    n = engine.run_once()
    return {"received": True, "handled": n,
            "stats": engine.stats.snapshot(),
            "dlq": len(engine.dlq)}


@app.get("/stats")
def stats():
    return {"stats": engine.stats.snapshot(), "dlq": engine.dlq.dump()}


@app.post("/dlq/requeue")
def requeue(workflow: str | None = None):
    moved = engine.dlq.requeue(engine.queue, workflow)
    engine.run_once()
    return {"requeued": moved}
