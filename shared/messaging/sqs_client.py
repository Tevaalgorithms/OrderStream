"""
Shared SQS client — publish and consume helpers.

CONCEPT: At-Least-Once Delivery
SQS guarantees at-least-once delivery, meaning a message MAY be delivered more than once.
This is why every consumer MUST be idempotent — processing the same message twice
should produce the same result as processing it once.

CONCEPT: Long Polling (WaitTimeSeconds=20)
Without long polling, SQS returns immediately even if the queue is empty,
costing you money and burning CPU. With long polling, SQS waits up to 20 seconds
for a message to arrive before returning. This reduces empty API calls by ~95%.

CONCEPT: Visibility Timeout
When a consumer receives a message, SQS hides it from other consumers
for VisibilityTimeout seconds (30s here). If the consumer crashes or takes
too long, the message reappears and another consumer can pick it up.
This is how SQS enables fault tolerance without distributed locks.
"""

from __future__ import annotations

import json
import logging
from typing import AsyncGenerator, Callable

import aioboto3
from botocore.config import Config

logger = logging.getLogger(__name__)

_session = aioboto3.Session()

BOTO_CONFIG = Config(
    region_name="us-east-1",
    retries={"max_attempts": 3, "mode": "standard"},
)


def _get_client_kwargs(endpoint_url: str | None) -> dict:
    kwargs: dict = {"config": BOTO_CONFIG}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return kwargs


async def publish(
    queue_url: str,
    payload: dict,
    endpoint_url: str | None = None,
) -> str:
    """
    Publish a single message to an SQS queue.
    Returns the SQS MessageId.
    """
    body = json.dumps(payload, default=str)
    async with _session.client("sqs", **_get_client_kwargs(endpoint_url)) as sqs:
        response = await sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=body,
        )
    message_id = response["MessageId"]
    logger.info("Published to SQS", extra={"queue_url": queue_url, "message_id": message_id})
    return message_id


async def consume(
    queue_url: str,
    handler: Callable[[dict], None],
    endpoint_url: str | None = None,
    max_messages: int = 10,
    wait_seconds: int = 20,  # CONCEPT: Long polling
) -> None:
    """
    Single poll cycle: fetch up to max_messages, call handler, delete on success.

    CONCEPT: Consumer Pattern
    1. Receive message (becomes invisible to other consumers)
    2. Process message (call handler)
    3. Delete message on SUCCESS only
    4. On failure: leave message in queue → SQS retries → eventually DLQ

    This ensures messages are never lost even if the consumer crashes mid-processing.
    """
    async with _session.client("sqs", **_get_client_kwargs(endpoint_url)) as sqs:
        response = await sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,    # Long polling
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = response.get("Messages", [])

        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", 1))

            try:
                body = json.loads(message["Body"])
                await handler(body)

                # CONCEPT: Explicit Acknowledgement
                # Only delete the message AFTER successful processing.
                # If handler raises an exception, we do NOT delete,
                # so SQS will redeliver after visibility timeout expires.
                await sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                logger.info(
                    "Message processed and deleted",
                    extra={"queue_url": queue_url, "receive_count": receive_count},
                )

            except Exception as exc:
                # CONCEPT: Dead Letter Queue
                # We log but do NOT re-raise — the message stays in the queue.
                # After maxReceiveCount (3) failures, SQS moves it to the DLQ automatically.
                logger.exception(
                    "Failed to process SQS message — leaving for retry/DLQ",
                    extra={
                        "queue_url": queue_url,
                        "receive_count": receive_count,
                        "error": str(exc),
                    },
                )
