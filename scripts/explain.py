import argparse
import asyncio
from pathlib import Path

import orjson
from sqlalchemy import text

from geo_tracking.db import Database
from geo_tracking.settings import Settings
from geo_tracking.spatial import MATCH_SQL


async def run(args):
    db = Database(Settings())
    plans = {}
    try:
        async with db.engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.execute(text("SET LOCAL statement_timeout = '30s'"))
                await connection.execute(
                    text("""
                    INSERT INTO geozones (id,user_id,name,center,radius_m,active,version)
                    SELECT gen_random_uuid(), 'explain-fixture', 'spatial-' || i,
                           ST_SetSRID(ST_MakePoint(
                               -179 + (i*7)%358, -75 + (i*11)%150),4326)::geography,
                           50 + i%9000, true, 1
                    FROM generate_series(1,10000) i
                """)
                )
                await connection.execute(text("ANALYZE geozones"))
                for scenario in ("varied_radii", "global_radius_outlier"):
                    if scenario == "global_radius_outlier":
                        await connection.execute(
                            text("""
                            INSERT INTO geozones (id,user_id,name,center,radius_m,active,version)
                            VALUES (gen_random_uuid(),'explain-fixture','global',
                                    'SRID=4326;POINT(24.1052 56.9496)'::geography,30000000,true,1)
                        """)
                        )
                    result = await connection.execute(
                        text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + MATCH_SQL),
                        {
                            "longitudes": [24.1052] * 200,
                            "latitudes": [56.9496] * 200,
                            "match_limit": 4001,
                        },
                    )
                    plans[scenario] = result.scalar_one()
            finally:
                await transaction.rollback()
    finally:
        await db.engine.dispose()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(orjson.dumps(plans, option=orjson.OPT_INDENT_2) + b"\n")
    print({key: value[0]["Execution Time"] for key, value in plans.items()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("evidence/spatial-query.json"))
    asyncio.run(run(parser.parse_args()))
