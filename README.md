# Event-Triggered Automation Agent

**Project 8 / 12 — Agentic AI portfolio.**
*Shows: you can build production automation, not demos.*

Webhook/queue-driven workflow execution with the semantics production
requires: idempotent (exactly-once) effects, retries with exponential
backoff, dead-lettering with full context, and replay after fixes.

## Why this matters

Event systems deliver **at least once** -- webhooks retry on timeout,
queues redeliver on crash. Without idempotency you double-charge
customers; without dead-lettering a single poison message wedges the
whole pipeline; without replay your DLQ is a black hole. This project
implements the four mechanics that make event automation trustworthy:

1. **Idempotent effects** — dedupe keys are computed from
   `source|type|canonical(payload)`, so redeliveries (even with fresh
   event ids) collapse. Per-workflow keys mean one event fans out to N
   workflows exactly once *each*.
2. **Retry with backoff** — per-workflow policy (`max_attempts`,
   `base * 2^n`, capped) turns transient failures into eventual
   success.
3. **Dead-letter with context** — exhausted retries land in the DLQ with
   the event, the error, and every attempt timestamp. Dead is
   *terminal*: duplicate deliveries of a poison message do not restart
   the retry loop (a bug this project's own tests caught).
4. **Replay** — `requeue_dlq(workflow=...)` moves dead letters back and
   clears the terminal mark, so a fixed handler retries only what
   failed.

## Architecture

```
webhook/queue ──> Event(source, type, payload)
                       │ dedupe_key = sha256(source|type|canonical(payload))
                       ▼
                 EventQueue (FIFO)
                       │  run_once()
                       ▼
                AutomationEngine._dispatch
                       │ skip if all target workflows already completed
                       │ skip dead-marked (poison) items
                       ▼
              for each Workflow matching event.type:
                       │
              _execute: attempt 1..N (backoff between)
                       │ success ──> mark processed + effects audit log
                       │ exhausted ──> DLQ (terminal mark, full context)
                                        │
                          requeue_dlq() after fix ──> retry allowed once
```

- `event_agent/events.py` — envelope + payload-order-insensitive dedupe keys
- `event_agent/queue.py` — FIFO queue + DLQ (inspection, selective requeue)
- `event_agent/workflow.py` — registry: triggers, handler, retry policy
- `event_agent/engine.py` — dispatch, dedupe, retries, terminal dead-marking
- `event_agent/webhook.py` — FastAPI receiver (`POST /webhook/{source}`,
  `GET /stats`, `POST /dlq/requeue`) as the production wiring example

## Quickstart

```bash
uv pip install -e ".[demo]"
python demo.py      # full lifecycle: fan-out, dedupe, retry, DLQ, replay
```

Optional HTTP wiring:

```bash
uvicorn event_agent.webhook:app --port 8000
curl -X POST localhost:8000/webhook/payment \
     -H 'Content-Type: application/json' \
     -d '{"event_type":"payment.captured","payload":{"order_id":"A-1","amount":4200}}'
# repeat the same curl: {"stats":{"duplicates":1,...}}  <- deduped
```

The demo runs a fictional store's order lifecycle: one
`payment.captured` fans out to 4 workflows (notify, backup, flaky
analytics that succeeds on retry 3, broken receipts that dead-letters),
three duplicate webhook deliveries produce zero extra effects, and a
post-fix `requeue_dlq` completes the receipts.

## Tests

```bash
pytest -q
```

22 tests cover: dedupe key stability across event ids, payload-order
insensitivity, payload/type discrimination; FIFO semantics, DLQ
inspection/selective requeue/dump; registry routing, duplicate
registration, backoff growth and cap; engine: trigger execution,
idempotent duplicate skipping, same-payload-different-type, fan-out,
retry-then-success with correct counters, dead-letter on exhaustion,
post-fix replay, poison-message terminality (5 redeliveries → 1
execution of the retry policy), no-handler events, effects audit,
bounded dedupe set; plus FastAPI endpoint tests (receive, dedupe, stats,
requeue).

## Design decisions

- **Exactly-once effects, not delivery** — you can't control redelivery;
  you can control whether effects repeat. The dedupe set is per
  (event, workflow), so fan-out stays independent.
- **Dead is terminal until explicitly requeued** — the obvious
  implementation (dedupe-on-success only) lets every redelivery of a
  failing message re-trigger the full retry policy: a poison-message
  storm. Terminal dead-marks close that hole; only `requeue_dlq()`
  clears them.
- **Canonical payloads** — key hashing uses sorted-key JSON, so `{"a":1,
  "b":2}` and `{"b":2,"a":1}` dedupe together, matching how real
  producers serialize.
- **Bounded dedupe set** — memory discipline: FIFO eviction with a cap,
  swappable for Redis/DB in production.
- **In-memory queue, real semantics** — the engine depends only on
  `dequeue()`; the queue and DLQ are deliberately simple so the hard
  parts (idempotency, retry, DLQ, replay) are the code you read.

## Portfolio context

Project 8 of a 12-project agentic-AI portfolio (structured output, RAG
grounding, ReAct planning, tool orchestration, memory, HITL approval,
cost routing, event automation, debate, self-eval, observability, OSS
contribution). See the profile README for the full map.
