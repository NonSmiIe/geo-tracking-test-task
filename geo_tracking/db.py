from contextlib import asynccontextmanager

from asyncpg import PostgresError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from geo_tracking.settings import Settings

DATABASE_ERRORS = (SQLAlchemyError, OSError, PostgresError)


class Database:
    def __init__(self, settings: Settings):
        self.engine = create_async_engine(
            settings.database_url,
            pool_size=5,
            max_overflow=0,
            pool_timeout=1,
            pool_pre_ping=True,
            connect_args={
                "server_settings": {"statement_timeout": str(settings.database_timeout_ms)}
            },
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def processing_session(self):
        async with self.engine.connect() as connection:
            await connection.execution_options(isolation_level="REPEATABLE READ")
            async with AsyncSession(connection) as session, session.begin():
                yield session
