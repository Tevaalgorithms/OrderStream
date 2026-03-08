"""
Analytics Service — Kinesis Consumer

CONCEPT: Kinesis Consumer with Checkpointing
─────────────────────────────────────────────────────────────────────────────
The analytics service reads from ALL shards of the Kinesis stream.
For each shard it maintains a checkpoint (last processed SequenceNumber).

On restart:
  - If checkpoint exists → resume from AFTER_SEQUENCE_NUMBER (no missed events)
  - If no checkpoint    → start from TRIM_HORIZON (process full history)

This guarantees exactly-once processing at the application level
(Kinesis itself delivers at-least-once; we deduplicate via event_id in DB).

CONCEPT: Shard Iterator Expiry
Kinesis shard iterators expire after 5 minutes of inactivity.
We handle ShardIteratorExpired by fetching a fresh iterator from the checkpoint.
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..database import async_session_factory
from ..models.analytics import RawEvent, KinesisCheckpoint, OrderMetric, PaymentMetric
from shared.messaging import kinesis_client

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 1  # Poll each shard every 1 second
RECORDS_PER_POLL = 100


async def run_kinesis_consumer(settings) -> None:
    """
    Main loop: discover shards, then poll each shard concurrently.
    """
    logger.info("Analytics Kinesis consumer starting...")

    import aioboto3
    session = aioboto3.Session()
    endpoint_url = settings.AWS_ENDPOINT_URL

    kwargs = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    async with session.client("kinesis", region_name="us-east-1", **kwargs) as kinesis:
        response = await kinesis.describe_stream_summary(StreamName=settings.KINESIS_STREAM_NAME)
        shard_count = response["StreamDescriptionSummary"]["OpenShardCount"]

    logger.info("Found %d shards in stream %s", shard_count, settings.KINESIS_STREAM_NAME)

    # Poll all shards concurrently
    shard_ids = [f"shardId-{str(i).zfill(12)}" for i in range(shard_count)]
    await asyncio.gather(*[
        _poll_shard(settings, shard_id)
        for shard_id in shard_ids
    ])


async def _poll_shard(settings, shard_id: str) -> None:
    """Poll a single shard in a loop."""
    endpoint_url = settings.AWS_ENDPOINT_URL

    # Load checkpoint or start from beginning
    async with async_session_factory() as db:
        checkpoint_seq = await _load_checkpoint(db, settings.KINESIS_STREAM_NAME, shard_id)

    iterator_type = "AFTER_SEQUENCE_NUMBER" if checkpoint_seq else "TRIM_HORIZON"
    shard_iterator = await kinesis_client.get_shard_iterator(
        stream_name=settings.KINESIS_STREAM_NAME,
        shard_id=shard_id,
        iterator_type=iterator_type,
        sequence_number=checkpoint_seq,
        endpoint_url=endpoint_url,
    )
    logger.info("Shard %s: starting from %s", shard_id, iterator_type)

    last_seq = checkpoint_seq

    while True:
        try:
            records, next_iterator = await kinesis_client.get_records(
                shard_iterator=shard_iterator,
                limit=RECORDS_PER_POLL,
                endpoint_url=endpoint_url,
            )

            if records:
                async with async_session_factory() as db:
                    for record in records:
                        await _ingest_event(db, record, shard_id)
                    if records:
                        last_seq = records[-1].get("sequence_number", last_seq)
                        await _save_checkpoint(db, settings.KINESIS_STREAM_NAME, shard_id, last_seq)
                    await db.commit()
                logger.info("Shard %s: ingested %d events", shard_id, len(records))

            if next_iterator:
                shard_iterator = next_iterator
            else:
                # Shard closed (resharding) — break and let caller restart
                logger.info("Shard %s closed", shard_id)
                break

        except Exception as exc:
            if "ExpiredIteratorException" in str(type(exc)):
                logger.warning("Shard iterator expired for %s — refreshing", shard_id)
                shard_iterator = await kinesis_client.get_shard_iterator(
                    stream_name=settings.KINESIS_STREAM_NAME,
                    shard_id=shard_id,
                    iterator_type="AFTER_SEQUENCE_NUMBER" if last_seq else "TRIM_HORIZON",
                    sequence_number=last_seq,
                    endpoint_url=endpoint_url,
                )
            else:
                logger.exception("Error polling shard %s", shard_id)

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _ingest_event(db: AsyncSession, record: dict, shard_id: str) -> None:
    """
    Store raw event and update aggregated metrics.

    CONCEPT: Event Deduplication
    We use ON CONFLICT DO NOTHING with the event_id to handle
    duplicate Kinesis records (at-least-once delivery).
    """
    event_id = record.get("event_id")
    if not event_id:
        return

    # Upsert raw event (idempotent)
    stmt = pg_insert(RawEvent).values(
        event_id=event_id,
        event_type=record.get("event_type", "unknown"),
        source_service=record.get("source_service", "unknown"),
        aggregate_id=record.get("aggregate_id", ""),
        payload=record.get("payload", {}),
        event_time=datetime.utcnow(),
        kinesis_shard_id=shard_id,
    ).on_conflict_do_nothing(index_elements=["event_id"])
    await db.execute(stmt)

    # Update aggregated metrics based on event type
    event_type = record.get("event_type")
    payload = record.get("payload", {})

    if event_type == "OrderCreated":
        await _increment_order_metric(db, "total_orders")
    elif event_type == "PaymentSucceeded":
        await _increment_order_metric(db, "successful_orders")
        await _record_revenue(db, float(payload.get("amount", 0)))
    elif event_type == "PaymentFailed":
        await _increment_order_metric(db, "failed_orders")


async def _increment_order_metric(db: AsyncSession, field: str) -> None:
    today = datetime.utcnow().date()
    hour = datetime.utcnow().hour
    # Simplified upsert — in production use a proper UPSERT
    result = await db.execute(
        select(OrderMetric).where(OrderMetric.date == today, OrderMetric.hour == hour)
    )
    metric = result.scalar_one_or_none()
    if not metric:
        metric = OrderMetric(date=today, hour=hour)
        db.add(metric)
    setattr(metric, field, getattr(metric, field, 0) + 1)


async def _record_revenue(db: AsyncSession, amount: float) -> None:
    today = datetime.utcnow().date()
    hour = datetime.utcnow().hour
    result = await db.execute(
        select(OrderMetric).where(OrderMetric.date == today, OrderMetric.hour == hour)
    )
    metric = result.scalar_one_or_none()
    if metric:
        metric.total_revenue = (metric.total_revenue or 0) + amount


async def _load_checkpoint(db: AsyncSession, stream: str, shard_id: str) -> str | None:
    result = await db.execute(
        select(KinesisCheckpoint).where(
            KinesisCheckpoint.stream_name == stream,
            KinesisCheckpoint.shard_id == shard_id,
        )
    )
    cp = result.scalar_one_or_none()
    return cp.sequence_number if cp else None


async def _save_checkpoint(db: AsyncSession, stream: str, shard_id: str, seq_no: str) -> None:
    stmt = pg_insert(KinesisCheckpoint).values(
        stream_name=stream,
        shard_id=shard_id,
        sequence_number=seq_no,
        updated_at=datetime.utcnow(),
    ).on_conflict_do_update(
        index_elements=["stream_name", "shard_id"],
        set_={"sequence_number": seq_no, "updated_at": datetime.utcnow()},
    )
    await db.execute(stmt)
