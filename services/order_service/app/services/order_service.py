"""
Order Service — Business Logic

Key patterns demonstrated:
  - Synchronous pre-check (inventory availability) before accepting order
  - Outbox Pattern: atomic write of order + event in one DB transaction
  - Idempotency via idempotency_key
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from uuid import uuid4

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.order import Order, OrderItem, OrderStatus, OutboxEvent, OutboxStatus
from ..schemas.order import CreateOrderRequest, OrderItemRequest
from shared.messaging.schemas import (
    OrderCreatedPayload, OrderItemPayload, build_envelope,
)

logger = logging.getLogger(__name__)


async def check_inventory_sync(
    inventory_url: str,
    items: list[OrderItemRequest],
) -> tuple[bool, str]:
    """
    CONCEPT: Synchronous Communication
    We call Inventory Service synchronously (blocking HTTP request) BEFORE
    creating the order, because we need the answer to decide whether to accept.

    This is a deliberate sync call — the client is waiting for a 202 or 400.
    We don't want to accept an order we already know is unfulfillable.
    """
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            response = await client.post(
                f"{inventory_url}/inventory/check",
                json={"items": [{"product_id": i.product_id, "quantity": i.quantity} for i in items]},
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("available"):
                    return True, ""
                return False, data.get("reason", "Insufficient stock")
            return False, f"Inventory service returned {response.status_code}"
        except httpx.RequestError as exc:
            logger.warning("Inventory service unreachable: %s", exc)
            # CONCEPT: Fail open vs Fail closed
            # We fail open here (allow order) because inventory will double-check
            # when it processes the SQS event. In production you'd use a circuit breaker.
            return True, ""


async def create_order(
    db: AsyncSession,
    payload: CreateOrderRequest,
    inventory_url: str,
    order_created_queue_url: str,
) -> Order:
    """
    Create an order atomically with its outbox event.

    CONCEPT: Idempotency
    We store the idempotency_key (provided by the client) in the DB with a
    UNIQUE constraint. If the client retries the same request (e.g. network timeout),
    we return the existing order instead of creating a duplicate.
    """
    # Check for duplicate (idempotency)
    existing = await db.execute(
        select(Order).where(Order.idempotency_key == payload.idempotency_key)
    )
    existing_order = existing.scalar_one_or_none()
    if existing_order:
        logger.info("Duplicate order request — returning existing order %s", existing_order.id)
        return existing_order

    # Synchronous stock check
    available, reason = await check_inventory_sync(inventory_url, payload.items)
    if not available:
        raise ValueError(f"Stock unavailable: {reason}")

    total = sum(
        Decimal(str(item.unit_price)) * item.quantity
        for item in payload.items
    )

    # CONCEPT: Outbox Pattern
    # Write Order + OutboxEvent in ONE database transaction.
    # If this transaction succeeds, the event WILL eventually be published.
    # If it fails, neither the order nor the event is written — clean rollback.
    async with db.begin():
        order = Order(
            customer_id=payload.customer_id,
            total_amount=total,
            currency=payload.currency,
            idempotency_key=payload.idempotency_key,
            status=OrderStatus.PENDING,
        )
        db.add(order)
        await db.flush()  # Get the order.id before commit

        for item in payload.items:
            db.add(OrderItem(
                order_id=order.id,
                product_id=item.product_id,
                sku=item.sku,
                quantity=item.quantity,
                unit_price=item.unit_price,
            ))

        # Build the event payload
        event_payload = OrderCreatedPayload(
            order_id=str(order.id),
            customer_id=str(payload.customer_id),
            items=[
                OrderItemPayload(
                    product_id=str(i.product_id),
                    sku=i.sku,
                    quantity=i.quantity,
                    unit_price=i.unit_price,
                )
                for i in payload.items
            ],
            total_amount=total,
            currency=payload.currency,
            idempotency_key=payload.idempotency_key,
        )
        envelope = build_envelope("OrderCreated", "order-service", event_payload)

        # Write outbox event — SAME transaction as the order
        outbox = OutboxEvent(
            aggregate_type="order",
            aggregate_id=order.id,
            event_type="OrderCreated",
            payload=json.dumps(envelope.model_dump(mode="json"), default=str),
            status=OutboxStatus.PENDING,
        )
        db.add(outbox)
        # Transaction commits here — both order and outbox event are persisted atomically

    logger.info("Order created: %s, outbox event queued", order.id)
    return order


async def update_order_status(
    db: AsyncSession,
    order_id: str,
    new_status: OrderStatus,
) -> Order | None:
    """Called by Payment Service and Inventory Service via REST callback."""
    async with db.begin():
        result = await db.execute(select(Order).where(Order.id == order_id))
        order = result.scalar_one_or_none()
        if not order:
            return None
        order.status = new_status
    return order
