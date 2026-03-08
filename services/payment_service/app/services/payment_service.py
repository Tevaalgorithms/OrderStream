"""
Payment Service — Business Logic

CONCEPT: Idempotency with Idempotency Key
The payment service is the most critical service for idempotency.
If SQS delivers the InventoryReserved message twice, we must NOT charge
the customer twice. We use the order_id as a unique key — one payment per order.

CONCEPT: Mock Payment Gateway
We simulate a payment gateway here. In production this would call
Stripe, Braintree, or AWS Payment Cryptography.
The PAYMENT_GATEWAY_FAIL_RATE env var lets you control the failure rate
to test the saga compensation (stock release + notification) path.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models.payment import Payment, PaymentStatus, PaymentAttempt
from shared.messaging.schemas import (
    InventoryReservedPayload, PaymentProcessedPayload,
    PaymentFailedPayload, build_envelope,
)
from shared.messaging import sqs_client, kinesis_client

logger = logging.getLogger(__name__)


async def process_payment(db: AsyncSession, payload: InventoryReservedPayload) -> None:
    """
    Process payment for a confirmed inventory reservation.

    CONCEPT: Idempotency Check
    Check if payment already exists for this order_id.
    If it does, this is a duplicate SQS delivery — skip safely.
    """
    settings = get_settings()

    async with db.begin():
        # Idempotency check
        existing = await db.execute(
            select(Payment).where(Payment.order_id == payload.order_id)
        )
        if existing.scalar_one_or_none():
            logger.info("Payment already processed for order %s — skipping (idempotent)", payload.order_id)
            return

        payment = Payment(
            order_id=payload.order_id,
            customer_id=payload.customer_id,
            amount=payload.total_amount,
            currency=payload.currency,
            idempotency_key=payload.idempotency_key,
            status=PaymentStatus.PROCESSING,
        )
        db.add(payment)
        await db.flush()

        # Call mock payment gateway
        gateway_result = await _call_mock_gateway(
            order_id=payload.order_id,
            amount=float(payload.total_amount),
            currency=payload.currency,
            fail_rate=float(settings.PAYMENT_GATEWAY_FAIL_RATE),
        )

        attempt = PaymentAttempt(
            payment_id=payment.id,
            attempt_number=1,
            status="SUCCEEDED" if gateway_result["success"] else "FAILED",
            gateway_request={"order_id": payload.order_id, "amount": str(payload.total_amount)},
            gateway_response=gateway_result,
        )
        db.add(attempt)

        if gateway_result["success"]:
            payment.status = PaymentStatus.SUCCEEDED
            payment.gateway_ref = gateway_result["transaction_id"]
        else:
            payment.status = PaymentStatus.FAILED
            payment.failure_reason = gateway_result["reason"]

    # Publish result events AFTER transaction commits
    if payment.status == PaymentStatus.SUCCEEDED:
        await _publish_payment_success(payment, payload, settings)
    else:
        await _publish_payment_failure(payment, payload, settings)


async def _call_mock_gateway(
    order_id: str,
    amount: float,
    currency: str,
    fail_rate: float = 0.2,
) -> dict:
    """
    Simulates a payment gateway.
    fail_rate=0.2 means 20% of payments will fail — useful for testing error paths.

    In production: replace with Stripe/Braintree SDK call.
    """
    await __import__("asyncio").sleep(0.1)  # Simulate network latency
    if random.random() < fail_rate:
        return {"success": False, "reason": "Card declined by issuing bank"}
    return {
        "success": True,
        "transaction_id": f"gw-txn-{order_id[:8]}",
        "processed_at": datetime.utcnow().isoformat(),
    }


async def _publish_payment_success(payment, payload: InventoryReservedPayload, settings) -> None:
    success_payload = PaymentProcessedPayload(
        order_id=payload.order_id,
        customer_id=payload.customer_id,
        payment_id=str(payment.id),
        amount=payload.total_amount,
        currency=payload.currency,
        gateway_ref=payment.gateway_ref,
    )
    envelope = build_envelope("PaymentProcessed", "payment-service", success_payload)

    await sqs_client.publish(
        queue_url=settings.SQS_PAYMENT_PROCESSED_QUEUE_URL,
        payload=envelope.model_dump(mode="json"),
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )
    await kinesis_client.put_event(
        stream_name=settings.KINESIS_STREAM_NAME,
        event_type="PaymentSucceeded",
        source_service="payment-service",
        aggregate_id=payload.order_id,
        payload=success_payload.model_dump(mode="json"),
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )
    # CONCEPT: Sync callback to Order Service
    # We use a synchronous REST call here because Order Service needs an
    # authoritative status update immediately — not eventually.
    await _update_order_status(settings.ORDER_SERVICE_URL, payload.order_id, "CONFIRMED")
    logger.info("Payment succeeded for order %s", payload.order_id)


async def _publish_payment_failure(payment, payload: InventoryReservedPayload, settings) -> None:
    fail_payload = PaymentFailedPayload(
        order_id=payload.order_id,
        customer_id=payload.customer_id,
        payment_id=str(payment.id),
        amount=payload.total_amount,
        failure_reason=payment.failure_reason or "Unknown",
    )
    envelope = build_envelope("PaymentFailed", "payment-service", fail_payload)

    await sqs_client.publish(
        queue_url=settings.SQS_PAYMENT_FAILED_QUEUE_URL,
        payload=envelope.model_dump(mode="json"),
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )
    await kinesis_client.put_event(
        stream_name=settings.KINESIS_STREAM_NAME,
        event_type="PaymentFailed",
        source_service="payment-service",
        aggregate_id=payload.order_id,
        payload=fail_payload.model_dump(mode="json"),
        endpoint_url=settings.AWS_ENDPOINT_URL,
    )
    await _update_order_status(settings.ORDER_SERVICE_URL, payload.order_id, "PAYMENT_FAILED")
    logger.info("Payment failed for order %s: %s", payload.order_id, payment.failure_reason)


async def _update_order_status(order_url: str, order_id: str, status: str) -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.patch(
                f"{order_url}/orders/{order_id}/status",
                json={"status": status},
            )
        except Exception as exc:
            logger.warning("Failed to update order status: %s", exc)
