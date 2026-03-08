"""
Shared Kinesis client — put_record wrapper.

CONCEPT: Kinesis vs SQS — When to Use Which
─────────────────────────────────────────────
SQS:
  - Point-to-point: one producer, one consumer
  - Message deleted after consumption (no replay)
  - Good for task queues (order processing, payment, notification)
  - At-least-once delivery

Kinesis:
  - Fan-out: one stream, multiple independent consumers
  - Records retained for 24h-365d (can replay history)
  - Good for event streaming, analytics, audit logs
  - Ordered within a shard (by partition key)
  - Higher throughput than SQS

CONCEPT: Partition Key → Shard Assignment
Kinesis uses MD5(partition_key) % num_shards to route records.
By using order_id as the partition key, all events for the same order
land on the same shard → guaranteed ordering per order timeline.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from uuid import uuid4

import aioboto3
from botocore.config import Config

logger = logging.getLogger(__name__)

_session = aioboto3.Session()

BOTO_CONFIG = Config(
    region_name="us-east-1",
    retries={"max_attempts": 3, "mode": "standard"},
)


async def put_event(
    stream_name: str,
    event_type: str,
    source_service: str,
    aggregate_id: str,
    payload: dict,
    endpoint_url: str | None = None,
) -> str:
    """
    Publish a single event record to Kinesis.

    partition_key = aggregate_id (order_id) → same order events → same shard → ordered.

    Returns the Kinesis SequenceNumber.
    """
    record = {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "source_service": source_service,
        "aggregate_id": aggregate_id,
        "occurred_at": datetime.utcnow().isoformat() + "Z",
        "schema_version": "1.0",
        "payload": payload,
    }
    data = json.dumps(record, default=str).encode("utf-8")

    kwargs: dict = {"config": BOTO_CONFIG}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with _session.client("kinesis", **kwargs) as kinesis:
        response = await kinesis.put_record(
            StreamName=stream_name,
            Data=data,
            PartitionKey=aggregate_id,  # CONCEPT: Same order_id → same shard → ordering
        )

    seq_no = response["SequenceNumber"]
    shard_id = response["ShardId"]
    logger.info(
        "Published to Kinesis",
        extra={
            "stream": stream_name,
            "event_type": event_type,
            "shard": shard_id,
            "seq_no": seq_no,
        },
    )
    return seq_no


async def get_shard_iterator(
    stream_name: str,
    shard_id: str,
    iterator_type: str = "TRIM_HORIZON",  # Start from oldest record
    sequence_number: str | None = None,
    endpoint_url: str | None = None,
) -> str:
    """
    Get a shard iterator for polling.

    iterator_type options:
      TRIM_HORIZON    — start from the oldest record in the shard
      LATEST          — start from the newest record (skip history)
      AFTER_SEQUENCE_NUMBER — resume from a checkpoint (production pattern)

    CONCEPT: Checkpointing
    In production, the Analytics Service saves the last processed SequenceNumber
    to the database. On restart, it uses AFTER_SEQUENCE_NUMBER to resume exactly
    where it left off — no missed events, no duplicate processing.
    """
    kwargs: dict = {"config": BOTO_CONFIG}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    shard_iter_kwargs: dict = {
        "StreamName": stream_name,
        "ShardId": shard_id,
        "ShardIteratorType": iterator_type,
    }
    if sequence_number and iterator_type == "AFTER_SEQUENCE_NUMBER":
        shard_iter_kwargs["StartingSequenceNumber"] = sequence_number

    async with _session.client("kinesis", **kwargs) as kinesis:
        response = await kinesis.get_shard_iterator(**shard_iter_kwargs)
    return response["ShardIterator"]


async def get_records(
    shard_iterator: str,
    limit: int = 100,
    endpoint_url: str | None = None,
) -> tuple[list[dict], str | None]:
    """
    Fetch records from a shard iterator.
    Returns (records, next_shard_iterator).

    CONCEPT: Polling-Based Consumer
    Kinesis does not push records to consumers (unlike SQS long-polling).
    Consumers must actively poll each shard at up to 5 calls/sec/shard.
    In production, use Enhanced Fan-Out (HTTP/2 push) for lower latency.
    """
    kwargs: dict = {"config": BOTO_CONFIG}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with _session.client("kinesis", **kwargs) as kinesis:
        response = await kinesis.get_records(
            ShardIterator=shard_iterator,
            Limit=limit,
        )

    records = [
        json.loads(r["Data"].decode("utf-8"))
        for r in response.get("Records", [])
    ]
    next_iterator = response.get("NextShardIterator")
    return records, next_iterator
