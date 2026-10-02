import asyncio

import asyncpg
from sqlalchemy.engine import make_url

from geo_tracking.settings import Settings


async def main():
    url = make_url(Settings().database_url)
    if url.database != "geo_test":
        raise RuntimeError("test database must be named geo_test")
    connection = await asyncpg.connect(
        url.set(drivername="postgresql", database="postgres").render_as_string(hide_password=False)
    )
    exists = await connection.fetchval("SELECT 1 FROM pg_database WHERE datname = 'geo_test'")
    if not exists:
        await connection.execute("CREATE DATABASE geo_test")
    await connection.close()


asyncio.run(main())
