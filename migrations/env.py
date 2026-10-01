import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine

from geo_tracking.models import Base
from geo_tracking.settings import Settings


def migrate(connection):
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run():
    engine = create_async_engine(Settings().database_url)
    async with engine.connect() as connection:
        await connection.run_sync(migrate)
    await engine.dispose()


asyncio.run(run())
