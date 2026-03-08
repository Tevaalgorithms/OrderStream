from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..config import get_settings
from ..database import get_db
from ..models.order import Order, OrderStatus
from ..schemas.order import CreateOrderRequest, OrderResponse, UpdateOrderStatusRequest
from ..services import order_service

router = APIRouter()


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=OrderResponse)
async def create_order(
    payload: CreateOrderRequest,
    db: AsyncSession = Depends(get_db),
    settings=Depends(get_settings),
):
    """
    Create a new order.

    Returns 202 Accepted immediately — the order is PENDING while
    inventory reservation and payment are processed asynchronously.
    Poll GET /orders/{order_id} to track the final status.
    """
    try:
        order = await order_service.create_order(
            db=db,
            payload=payload,
            inventory_url=settings.INVENTORY_SERVICE_URL,
            order_created_queue_url=settings.SQS_ORDER_CREATED_QUEUE_URL,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return order


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(order_id: UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Order)
        .where(Order.id == order_id)
        .options(selectinload(Order.items))
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@router.patch("/{order_id}/status", response_model=OrderResponse)
async def update_order_status(
    order_id: UUID,
    payload: UpdateOrderStatusRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Internal endpoint — called by Payment Service and Inventory Service
    to update order status after async processing completes.
    """
    try:
        new_status = OrderStatus(payload.status)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid status: {payload.status}")

    order = await order_service.update_order_status(db, str(order_id), new_status)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
