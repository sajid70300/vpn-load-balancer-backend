from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import settings

# Async for API
async_engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True, pool_size=20)
AsyncSessionLocal = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

# Sync for Celery
sync_engine = create_engine(settings.SYNC_DATABASE_URL, pool_pre_ping=True, pool_size=10)
SyncSessionLocal = sessionmaker(bind=sync_engine)

Base = declarative_base()


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise