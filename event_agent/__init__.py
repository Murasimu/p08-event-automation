"""Event-triggered automation agent.

Listen to webhooks/queues, execute workflows on triggers, with
idempotent execution, dead-letter handling, and retry logic.

Modules:
- events.py    Event envelope + dedupe key computation
- queue.py     in-memory queue + dead-letter queue (DLQ) with inspection
- workflow.py  workflow registry: handler + config (retries, backoff)
- engine.py    the automation engine: consume, dedupe, execute, retry,
               dead-letter on exhaustion, exactly-once effects
"""

from .engine import AutomationEngine, EngineStats
from .events import Event, compute_dedupe_key
from .queue import DeadLetterQueue, EventQueue
from .workflow import Workflow, WorkflowRegistry

__all__ = [
    "AutomationEngine", "EngineStats",
    "Event", "compute_dedupe_key",
    "DeadLetterQueue", "EventQueue",
    "Workflow", "WorkflowRegistry",
]
__version__ = "1.0.0"
