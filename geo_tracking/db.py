from asyncpg import PostgresError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from geo_tracking.settings import Settings

DATABASE_ERRORS = (SQLAlchemyError, OSError, PostgresError)


class Database:
    def __init__(self, settings: Settings, pool_size: int | None = None):
        self.engine: AsyncEngine = create_async_engine(
            settings.database_url,
            pool_size=pool_size or settings.database_pool,
            max_overflow=0,
            pool_timeout=1,
            connect_args={
                "command_timeout": settings.database_timeout_ms / 1000 * 2,
                "server_settings": {"statement_timeout": str(settings.database_timeout_ms)},
            },
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def close(self) -> None:
        await self.engine.dispose()
