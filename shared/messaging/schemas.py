"""
Shared SQS and Kinesis message schemas.

CONCEPT: Message Contracts
All services share these Pydantic models as the "contract" for events.
Changing these schemas must be backward-compatible — consumers must not break.
In production, use a Schema Registry (AWS Glue) to enforce versioning.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from uuid import uuid4


# ─────────────────────────────────────────────
# Envelope — wraps every SQS and Kinesis message
# CONCEPT: Envelope Pattern
# Every message carries metadata (id, type, source, time) separate from the payload.
# This allows consumers to route, filter, and deduplicate without deserializing payload.
# ─────────────────────────────────────────────

class EventEnvelope(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    source_service: str
    occurred_at: datetime = Field(default_factory=datetime.utcnow)
    schema_version: str = "1.0"
    payload: dict[str, Any]


# ─────────────────────────────────────────────
# Order Events
# ─────────────────────────────────────────────

class OrderItemPayload(BaseModel):
    product_id: str
    sku: str
    quantity: int
    unit_price: Decimal


class OrderCreatedPayload(BaseModel):
    """
    Published by: Order Service → SQS order-created-queue
    Consumed by:  Inventory Service
    Purpose:      Trigger stock reservation for the order items
    """
    order_id: str
    customer_id: str
    items: list[OrderItemPayload]
    total_amount: Decimal
    currency: str
    idempotency_key: str  # Used by Inventory to deduplicate reservations


class OrderStatusChangedPayload(BaseModel):
    """Published to Kinesis only — no SQS consumer, analytics purposes."""
    order_id: str
    customer_id: str
    old_status: str
    new_status: str


# ─────────────────────────────────────────────
# Inventory Events
# ─────────────────────────────────────────────

class ReservationItem(BaseModel):
    product_id: str
    quantity: int


class InventoryReservedPayload(BaseModel):
    """
    Published by: Inventory Service → SQS inventory-reserved-queue
    Consumed by:  Payment Service
    Purpose:      Stock confirmed — trigger payment processing
    """
    order_id: str
    customer_id: str
    total_amount: Decimal
    currency: str
    idempotency_key: str
    reservations: list[ReservationItem]


class InventoryFailedPayload(BaseModel):
    """
    Published by: Inventory Service → SQS inventory-failed-queue
    Consumed by:  Notification Service
    Purpose:      Out of stock — notify customer and cancel order
    """
    order_id: str
    customer_id: str
    reason: str
    unavailable_items: list[str]  # product_ids that were unavailable


# ─────────────────────────────────────────────
# Payment Events
# ─────────────────────────────────────────────

class PaymentProcessedPayload(BaseModel):
    """
    Published by: Payment Service → SQS payment-processed-queue
    Consumed by:  Notification Service
    Purpose:      Payment succeeded — send confirmation to customer
    """
    order_id: str
    customer_id: str
    payment_id: str
    amount: Decimal
    currency: str
    gateway_ref: str


class PaymentFailedPayload(BaseModel):
    """
    Published by: Payment Service → SQS payment-failed-queue
    Consumed by:  Inventory Service (to release reservation) + Notification Service
    Purpose:      Payment declined — release stock, notify customer
    CONCEPT: Saga Compensation
    This is the "compensating transaction" in the saga —
    undoing the stock reservation because payment failed.
    """
    order_id: str
    customer_id: str
    payment_id: str
    amount: Decimal
    failure_reason: str


# ─────────────────────────────────────────────
# Notification Events (Kinesis only — no SQS consumer)
# ─────────────────────────────────────────────

class NotificationSentPayload(BaseModel):
    order_id: str
    customer_id: str
    channel: str
    template_name: str
    recipient: str


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def build_envelope(
    event_type: str,
    source_service: str,
    payload: BaseModel,
) -> EventEnvelope:
    """Wrap any payload in a standard EventEnvelope."""
    return EventEnvelope(
        event_type=event_type,
        source_service=source_service,
        payload=payload.model_dump(mode="json"),
    )
