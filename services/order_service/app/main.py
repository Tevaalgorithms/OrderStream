"""
Order Service — FastAPI Application Entry Point

CONCEPT: Service Lifespan
FastAPI's lifespan context manager runs startup/shutdown logic.
We use it to launch the OutboxPublisher background task on startup
and cancel it gracefully on shutdown.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .config import get_settings
from .database import init_db
from .routers import orders
from .services.outbox_publisher import run_outbox_publisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    settings = get_settings()
    logger.info("Order Service starting up...")
    await init_db()

    # Launch outbox publisher as a background asyncio task
    # CONCEPT: Background Task
    # This runs concurrently with request handling using Python's asyncio event loop.
    # It's not a separate thread or process — it's cooperative multitasking.
    outbox_task = asyncio.create_task(run_outbox_publisher(settings))
    logger.info("Outbox publisher background task started")

    yield  # Service is running — handle requests

    # ── Shutdown ──
    logger.info("Order Service shutting down...")
    outbox_task.cancel()
    try:
        await outbox_task
    except asyncio.CancelledError:
        logger.info("Outbox publisher stopped gracefully")


app = FastAPI(
    title="Order Service",
    description="Handles order creation and status management. Part of the OrderStream microservices platform.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(orders.router, prefix="/orders", tags=["Orders"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "order-service"}
