import logging
import time
from typing import Literal, Optional, Sequence

import asyncpg
from buildpg import V, render

from dbami.db import DB

logger = logging.getLogger(__name__)


async def grant_connect_privileges(conn: asyncpg.Connection, *role_names: str) -> None:
    for role_name in role_names:
        logger.warning("Granting connect privileges for role '%s'", role_name)
        q, p = render(
            """
                DO $_$
                    BEGIN
                        EXECUTE FORMAT('GRANT CONNECT on database %s TO :role',
                        CURRENT_DATABASE());
                    END
                $_$;
            """,
            role=V(role_name),
        )
        await conn.execute(q, *p)


async def revoke_connect_privileges(conn: asyncpg.Connection, *role_names: str) -> None:
    for role_name in role_names:
        logger.warning("Revoking connect privileges for role '%s'", role_name)
        q, p = render(
            """
                DO $_$
                    BEGIN
                        EXECUTE FORMAT('REVOKE CONNECT on database %s FROM :role',
                        CURRENT_DATABASE());
                    END
                $_$;
            """,
            role=V(role_name),
        )
        await conn.execute(q, *p)


async def active_connections_exist(conn: asyncpg.Connection) -> bool:
    return bool(
        await conn.fetchval(
            """
        SELECT EXISTS(
            SELECT * FROM pg_stat_activity
            WHERE
                datname = current_database()
                AND pid != pg_backend_pid()
        )
        """,
        )
    )


async def wait_for_other_connections_to_close(
    conn: asyncpg.Connection,
    revoke_role_names: Sequence[str],
    poll_interval_ms: int = 2000,
) -> None:
    if poll_interval_ms < 100:
        logger.warning("Cowardly refusing to wait less than 100ms.")
        poll_interval_ms = 100

    await revoke_connect_privileges(conn, *revoke_role_names)

    while await active_connections_exist(conn):
        time.sleep(poll_interval_ms)


async def apply_migrations(
    database: DB,
    revoke_role_names: Sequence[str],
    do_rollback: bool = False,
    target_version: Optional[int] = None,
    connection_wait_poll_interval_ms: int = 2000,
    **connect_kwargs,
) -> None:
    async with database.get_db_connection(**connect_kwargs) as conn:
        try:
            current_version = await database.get_current_version(conn=conn)
            if current_version == target_version:
                logging.info("No migration required, already running target version")
                return

            if connection_wait_poll_interval_ms > 0:
                logging.info("Waiting on all connections to close before migrating...")
                await wait_for_other_connections_to_close(
                    conn,
                    revoke_role_names,
                    poll_interval_ms=connection_wait_poll_interval_ms,
                )

            direction: Literal["up", "down"]
            if do_rollback:
                logging.warning("Rolling back database to version %s", target_version)
                direction = "down"
            else:
                logging.warning("Migrating database to version %s", target_version)
                direction = "up"

            await database.migrate(
                target=target_version,
                direction=direction,
                conn=conn,
            )
            logging.info("Migration apply complete.")
        finally:
            logging.info("Ensuring connect privileges are granted correctly")
            await grant_connect_privileges(conn, *revoke_role_names)
            logging.info("End migration tasks.")
