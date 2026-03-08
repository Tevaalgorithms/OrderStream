"""
Inventory Service — SQS Consumer

CONCEPT: Async Consumer Pattern
This service consumes TWO queues:
  1. order-created-queue     → reserve stock for new orders
  2. payment-failed-queue    → release stock when payment fails (saga compensation)

CONCEPT: Saga Choreography (vs Orchestration)
In orchestration, a central controller tells each service what to do.
In choreography (what we use here), each service listens for events
and reacts independently. There is no central brain.

Pros of choreography: loose coupling, services can evolve independently
Cons: harder to visualize the full flow (the README architecture diagram helps)
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..database import async_session_factory
from .inventory_service import reserve_stock, release_stock
from shared.messaging import sqs_client
from shared.messaging.schemas import OrderCreatedPayload, PaymentFailedPayload, EventEnvelope

logger = logging.getLogger(__name__)


async def handle_order_created(message: dict) -> None:
    """
    Triggered when Order Service publishes to order-created-queue.
    Reserve stock for each item in the order.
    """
    envelope = EventEnvelope(**message)
    payload = OrderCreatedPayload(**envelope.payload)

    logger.info("Processing OrderCreated for order %s", payload.order_id)

    async with async_session_factory() as db:
        result = await reserve_stock(db, payload)
        logger.info(
            "Stock reservation result for order %s: %s",
            payload.order_id, "success" if result.success else f"failed: {result.reason}"
        )


async def handle_payment_failed(message: dict) -> None:
    """
    CONCEPT: Saga Compensation
    Payment failed → we must release the stock reservation we made earlier.
    This is the "compensating transaction" that reverses the side effect.
    """
    envelope = EventEnvelope(**message)
    payload = PaymentFailedPayload(**envelope.payload)

    logger.info("Releasing stock for failed payment on order %s", payload.order_id)
    async with async_session_factory() as db:
        await release_stock(db, payload.order_id)


QUEUE_HANDLERS = {
    "order_created": handle_order_created,
    "payment_failed": handle_payment_failed,
}


async def run_consumers(settings) -> None:
    """
    Run both SQS consumers concurrently as separate asyncio tasks.

    CONCEPT: Concurrent Consumers
    We poll two different queues at the same time using asyncio.gather.
    Both run in the same event loop — no threads needed.
    """
    logger.info("Starting SQS consumers for Inventory Service")
    while True:
        await asyncio.gather(
            sqs_client.consume(
                queue_url=settings.SQS_ORDER_CREATED_QUEUE_URL,
                handler=handle_order_created,
                endpoint_url=settings.AWS_ENDPOINT_URL,
            ),
            sqs_client.consume(
                queue_url=settings.SQS_PAYMENT_FAILED_QUEUE_URL,
                handler=handle_payment_failed,
                endpoint_url=settings.AWS_ENDPOINT_URL,
            ),
            return_exceptions=True,
        )
