from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    AWS_ENDPOINT_URL: str | None = None
    AWS_DEFAULT_REGION: str = "us-east-1"
    SQS_ORDER_CREATED_QUEUE_URL: str
    KINESIS_STREAM_NAME: str = "ecommerce-events-stream"
    INVENTORY_SERVICE_URL: str = "http://inventory-service:8002"
    SERVICE_NAME: str = "order-service"

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
