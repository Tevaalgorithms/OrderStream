"""
Order Service — Database Models

CONCEPT: Database-per-Service
This PostgreSQL database belongs exclusively to the Order Service.
No other service can query this database directly.
If Inventory Service needs order data, it must call the Order Service REST API.
This enforces loose coupling — services can evolve their schemas independently.
"""

import enum
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Column, DateTime, Enum, ForeignKey, Integer,
    Numeric, String, Text, func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .base import Base


class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    CANCELLED = "CANCELLED"
    SHIPPED = "SHIPPED"


class OutboxStatus(str, enum.Enum):
    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"


class Order(Base):
    __tablename__ = "orders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.PENDING)
    total_amount = Column(Numeric(12, 2), nullable=False)
    currency = Column(String(3), default="USD")
    idempotency_key = Column(String(255), unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False)
    product_id = Column(UUID(as_uuid=True), nullable=False)
    sku = Column(String(100), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)

    order = relationship("Order", back_populates="items")


class OutboxEvent(Base):
    """
    CONCEPT: Outbox Pattern
    ──────────────────────────────────────────────────────────────────
    Problem: We need to create an Order AND publish an event to SQS.
    If we write to DB then publish to SQS, and the service crashes between
    the two, we get an order with no event → silent data inconsistency.

    Solution: Write both the Order and the OutboxEvent in ONE database
    transaction. A background worker (OutboxPublisher) then reads
    PENDING events and publishes them to SQS, marking them PUBLISHED.

    This gives us "exactly-once write to DB" + "at-least-once delivery to SQS"
    which is the best we can do without a distributed transaction.
    ──────────────────────────────────────────────────────────────────
    """
    __tablename__ = "outbox_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_type = Column(String(50), nullable=False)   # e.g. "order"
    aggregate_id = Column(UUID(as_uuid=True), nullable=False)
    event_type = Column(String(100), nullable=False)      # e.g. "OrderCreated"
    payload = Column(Text, nullable=False)                # JSON string
    status = Column(Enum(OutboxStatus), nullable=False, default=OutboxStatus.PENDING, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    published_at = Column(DateTime(timezone=True), nullable=True)
    retry_count = Column(Integer, default=0)
