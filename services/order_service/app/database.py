from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings

class Base(DeclarativeBase):
    pass

_engine = None
async_session_factory = None


async def init_db():
    global _engine, async_session_factory
    settings = get_settings()
    _engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    async with _engine.begin() as conn:
        from .models import order  # noqa — import models to register with Base
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncSession:
    async with async_session_factory() as session:
        yield session
