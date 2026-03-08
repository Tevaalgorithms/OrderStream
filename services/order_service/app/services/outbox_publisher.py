"""
Outbox Publisher — Background task that polls outbox table and publishes to SQS.

CONCEPT: Outbox Publisher / Transactional Outbox
─────────────────────────────────────────────────────────────────────────────
This background task is the second half of the Outbox Pattern:

1. Order is created + OutboxEvent is written → same DB transaction (order_service.py)
2. THIS task polls the outbox for PENDING events and publishes them to SQS

Why not publish directly to SQS in the same request?
  - If we publish to SQS and the DB commit fails, the event is sent but order doesn't exist → ghost event
  - If we commit to DB and then the process crashes before SQS publish → order exists but no event
  - The outbox solves both: DB is the source of truth, SQS is eventually consistent

Guarantees:
  - At-least-once delivery to SQS (we retry on failure)
  - SKIP_LOCKED prevents two instances of this task from publishing the same event
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import Settings
from ..database import async_session_factory
from ..models.order import OutboxEvent, OutboxStatus
from shared.messaging import sqs_client

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 2
BATCH_SIZE = 10
MAX_RETRIES = 5


async def run_outbox_publisher(settings: Settings) -> None:
    """
    Long-running background task. Runs for the lifetime of the service process.

    CONCEPT: Polling with SKIP_LOCKED
    SELECT ... FOR UPDATE SKIP LOCKED is key for horizontal scaling:
    If you run 2 instances of Order Service, both will run this publisher.
    SKIP LOCKED ensures they each grab different rows — no double-publishing.
    """
    logger.info("Outbox publisher started — polling every %ds", POLL_INTERVAL_SECONDS)

    while True:
        try:
            await _publish_pending_events(settings)
        except Exception:
            logger.exception("Outbox publisher cycle failed — will retry")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _publish_pending_events(settings: Settings) -> None:
    async with async_session_factory() as db:
        async with db.begin():
            result = await db.execute(
                select(OutboxEvent)
                .where(OutboxEvent.status == OutboxStatus.PENDING)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)  # CONCEPT: Prevents double-publish in multi-instance
            )
            events = result.scalars().all()

            if not events:
                return

            logger.info("Outbox publisher: found %d pending event(s)", len(events))

            for event in events:
                try:
                    payload = json.loads(event.payload)
                    await sqs_client.publish(
                        queue_url=settings.SQS_ORDER_CREATED_QUEUE_URL,
                        payload=payload,
                        endpoint_url=settings.AWS_ENDPOINT_URL,
                    )
                    event.status = OutboxStatus.PUBLISHED
                    event.published_at = datetime.utcnow()
                    logger.info("Outbox event published: %s (%s)", event.event_type, event.id)

                except Exception as exc:
                    event.retry_count += 1
                    if event.retry_count >= MAX_RETRIES:
                        event.status = OutboxStatus.FAILED
                        logger.error(
                            "Outbox event permanently failed after %d retries: %s",
                            MAX_RETRIES, event.id,
                        )
                    else:
                        logger.warning(
                            "Outbox event publish failed (attempt %d/%d): %s — %s",
                            event.retry_count, MAX_RETRIES, event.id, exc,
                        )
            # All status updates committed atomically
