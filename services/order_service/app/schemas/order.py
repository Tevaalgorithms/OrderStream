from decimal import Decimal
from uuid import UUID
from pydantic import BaseModel, Field
from uuid import uuid4


class OrderItemRequest(BaseModel):
    product_id: UUID
    sku: str
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_id: UUID
    items: list[OrderItemRequest] = Field(min_length=1)
    currency: str = "USD"
    idempotency_key: str = Field(default_factory=lambda: str(uuid4()))


class OrderItemResponse(BaseModel):
    product_id: UUID
    sku: str
    quantity: int
    unit_price: Decimal

    class Config:
        from_attributes = True


class OrderResponse(BaseModel):
    id: UUID
    customer_id: UUID
    status: str
    total_amount: Decimal
    currency: str
    idempotency_key: str
    items: list[OrderItemResponse] = []

    class Config:
        from_attributes = True


class UpdateOrderStatusRequest(BaseModel):
    status: str
