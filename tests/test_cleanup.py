from buildpg import V, render

from dbami.db import DB
from dbami.util import syncrun


def test_verify_all_test_databases_are_cleaned_up(test_db_name_stem: str) -> None:
    query, params = render(
        "select exists(select 1 from pg_database where :where)",
        where=V("datname").like(test_db_name_stem),
    )

    async def verify():
        async with DB.get_db_connection(database="") as conn:
            return not await conn.fetchval(query, *params)

    assert syncrun(verify())
