"""
Inventory Service — Business Logic

CONCEPT: Idempotency in Consumers
SQS delivers messages at-least-once. The same OrderCreated message might
arrive twice (e.g., network retry, visibility timeout expiry).

To handle this safely, we check for an existing reservation with the same
order_id before creating a new one. If one exists → return it, do not reserve twice.
This makes the operation idempotent: processing it N times == processing it once.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models.inventory import Product, Stock, StockReservation, ReservationStatus
from shared.messaging.schemas import (
    OrderCreatedPayload, InventoryReservedPayload, InventoryFailedPayload,
    ReservationItem, build_envelope,
)
from shared.messaging import sqs_client, kinesis_client

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ReservationResult:
    success: bool
    reason: str = ""


async def check_availability(db: AsyncSession, items: list[dict]) -> tuple[bool, list[str]]:
    """Synchronous stock check — called by Order Service before order creation."""
    unavailable = []
    for item in items:
        result = await db.execute(
            select(Stock).where(Stock.product_id == item["product_id"])
        )
        stock = result.scalar_one_or_none()
        available_qty = (stock.quantity_total - stock.quantity_reserved) if stock else 0
        if available_qty < item["quantity"]:
            unavailable.append(item["product_id"])
    return len(unavailable) == 0, unavailable


async def reserve_stock(db: AsyncSession, payload: OrderCreatedPayload) -> ReservationResult:
    """
    Reserve stock for an order.

    CONCEPT: Idempotency
    Check if a reservation for this order already exists.
    If yes: skip — this is a duplicate message from SQS.
    """
    settings = get_settings()

    async with db.begin():
        # Idempotency check — has this order already been reserved?
        existing = await db.execute(
            select(StockReservation).where(StockReservation.order_id == payload.order_id)
        )
        if existing.scalar_one_or_none():
            logger.info("Reservation already exists for order %s — skipping (idempotent)", payload.order_id)
            return ReservationResult(success=True)

        reservations = []
        unavailable_items = []

        for item in payload.items:
            stock_result = await db.execute(
                select(Stock).where(Stock.product_id == item.product_id).with_for_update()
            )
            stock = stock_result.scalar_one_or_none()
            available = (stock.quantity_total - stock.quantity_reserved) if stock else 0

            if available < item.quantity:
                unavailable_items.append(str(item.product_id))
                continue

            # Reserve the stock
            stock.quantity_reserved += item.quantity
            reservation = StockReservation(
                order_id=payload.order_id,
                product_id=str(item.product_id),
                quantity=item.quantity,
                status=ReservationStatus.RESERVED,
                expires_at=datetime.utcnow() + timedelta(minutes=15),
            )
            db.add(reservation)
            reservations.append(reservation)

        if unavailable_items:
            # Rollback reservations we just made (will happen via transaction rollback if we raise)
            # Publish failure event
            await db.rollback()

            fail_payload = InventoryFailedPayload(
                order_id=payload.order_id,
                customer_id=payload.customer_id,
                reason="Insufficient stock",
                unavailable_items=unavailable_items,
            )
            envelope = build_envelope("InventoryFailed", "inventory-service", fail_payload)
            await sqs_client.publish(
                queue_url=settings.SQS_INVENTORY_FAILED_QUEUE_URL,
                payload=envelope.model_dump(mode="json"),
                endpoint_url=settings.AWS_ENDPOINT_URL,
            )
            # Notify Order Service to cancel the order (sync callback)
            await _update_order_status(settings.ORDER_SERVICE_URL, payload.order_id, "OUT_OF_STOCK")
            # Publish to Kinesis for analytics
            await kinesis_client.put_event(
                stream_name=settings.KINESIS_STREAM_NAME,
                event_type="StockUnavailable",
                source_service="inventory-service",
                aggregate_id=payload.order_id,
                payload=fail_payload.model_dump(mode="json"),
                endpoint_url=settings.AWS_ENDPOINT_URL,
            )
            return ReservationResult(success=False, reason="Insufficient stock")

    # All reserved — publish success event
    reserved_payload = InventoryReservedPayload(
        order_id=payload.order_id,
        customer_id=payload.customer_id,
        total_amount=payload.total_amount,
        currency=payload.currency,
        idempotency_key=payload.idempotency_key,
        reservations=[ReservationItem(product_id=str(i.product_id), quantity=i.quantity) for i in payload.items],
    )
    envelope = build_envelope("InventoryReserved", "inventory-service", reserved_payload)
    await sqs_client.publish(
        queue_url=settings.SQS_INVENTORY_RESERVED_QUEUE_URL,
        payload=envelope.model_dump(mode="json"),
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )
    await kinesis_client.put_event(
        stream_name=settings.KINESIS_STREAM_NAME,
        event_type="StockReserved",
        source_service="inventory-service",
        aggregate_id=payload.order_id,
        payload=reserved_payload.model_dump(mode="json"),
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )
    return ReservationResult(success=True)


async def release_stock(db: AsyncSession, order_id: str) -> None:
    """CONCEPT: Saga Compensation — undo the reservation when payment fails."""
    async with db.begin():
        result = await db.execute(
            select(StockReservation)
            .where(StockReservation.order_id == order_id)
            .with_for_update()
        )
        reservation = result.scalar_one_or_none()
        if not reservation or reservation.status != ReservationStatus.RESERVED:
            logger.info("No active reservation to release for order %s", order_id)
            return

        stock_result = await db.execute(
            select(Stock).where(Stock.product_id == reservation.product_id).with_for_update()
        )
        stock = stock_result.scalar_one_or_none()
        if stock:
            stock.quantity_reserved -= reservation.quantity

        reservation.status = ReservationStatus.RELEASED
        reservation.released_at = datetime.utcnow()
    logger.info("Stock released for order %s", order_id)


async def _update_order_status(order_url: str, order_id: str, status: str) -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.patch(
                f"{order_url}/orders/{order_id}/status",
                json={"status": status},
            )
        except Exception as exc:
            logger.warning("Failed to update order status for %s: %s", order_id, exc)
