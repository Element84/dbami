import logging
import time
from dataclasses import dataclass, field
from typing import Literal, Optional, Sequence

import asyncpg
from buildpg import V, render

from dbami.db import DB

logger = logging.getLogger(__name__)


@dataclass
class MigrationHelperConfig:
    # Role names to revoke connect on before running migrations.
    # Prevents other users/services from re-connecting during migrations.
    revoke_connect_on_roles_names: Sequence[str] = field(default_factory=list)

    # Migration version to target.
    # If unset then the highest-known migration version will be targeted.
    target_migration_version: Optional[int] = None

    # When the target migration version is older than the current
    # migration version this must be True to roll back.
    do_rollback: bool = False

    # Wait for other connections to close before running migrations.
    #
    #   connection_wait_poll_interval_ms (int)
    #     The polling interval on the connection check query, in miliseconds.
    #
    #     This setting has no function if connection_wait_max_attempts is set
    #     to a value less than or equal to 0.
    #
    #     Values less than 100 will be coerced to 100.
    #
    #   connection_wait_max_attempts (int | None)
    #     Limit the number of polling attempts before aborting migration,
    #     or attempting to force close existing connections (if enabled).

    #     Set to a value less than or equal to 0 to disable waiting.
    #     Set to None to not limit waiting.
    #     Any other value must be a positive integer.
    #
    #   force_close_connections_after_wait (bool)
    #     Enable to attempt to force existing connections to close.
    #
    #   force_close_connections_timeout_ms (int)
    #     A positive integer value limits the wait time for connections to
    #     close to the provided value, in miliseconds. If any connections do
    #     not close before the timeout the script will error and exit.
    #
    #     A value less than or equal to 0 will disable the wait and the force
    #     close will fire and proceed without any guarantee of success.
    #
    #     This setting only applies if force_close_connections_after_wait is
    #     set to True.
    connection_wait_poll_interval_ms: int = 2000
    connection_wait_max_attempts: Optional[int] = None
    force_close_connections_after_wait: bool = False
    force_close_connections_timeout_ms: int = 10000

    def __post_init__(self) -> None:
        if self.connection_wait_poll_interval_ms < 100:
            self.connection_wait_poll_interval_ms = 100

    @property
    def wait_for_other_connections_to_close(self) -> bool:
        return (
            self.connection_wait_max_attempts is None
            or self.connection_wait_max_attempts > 0
        )


class MigrationHelper:
    def __init__(
        self,
        database: DB,
        helper_config: Optional[MigrationHelperConfig] = None,
        logger: logging.Logger = logger,
        **connect_kwargs,
    ) -> None:
        self.database = database
        self.logger = logger
        self.connect_kwargs = connect_kwargs

        if helper_config is None:
            helper_config = MigrationHelperConfig()

        self.config = helper_config


    async def grant_connect_privileges(
        self,
        conn: asyncpg.Connection,
    ) -> None:
        for role_name in self.config.revoke_connect_on_roles_names:
            self.logger.warning("Granting connect privileges for role '%s'", role_name)
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


    async def revoke_connect_privileges(
        self,
        conn: asyncpg.Connection,
    ) -> None:
        for role_name in self.config.revoke_connect_on_roles_names:
            self.logger.warning("Revoking connect privileges for role '%s'", role_name)
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


    async def active_connections_exist(self, conn: asyncpg.Connection) -> bool:
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
        self,
        conn: asyncpg.Connection,
        poll_interval_ms: int = 2000,
    ) -> None:
        if poll_interval_ms < 100:
            logger.warning("Cowardly refusing to wait less than 100ms.")
            poll_interval_ms = 100

        await self.revoke_connect_privileges(conn)

        while await self.active_connections_exist(conn):
            time.sleep(poll_interval_ms)


    async def apply_migrations(self) -> None:
        async with self.database.get_db_connection(**self.connect_kwargs) as conn:
            ## NEED TO LOCK ON THE MIGRATION TABLE
            try:
                current_version = await self.database.get_current_version(conn=conn)

                ## TODO: need some way to validate target version if target version is None
                ## probably just need to set it here if it is none, well, before getting
                ## the current version and error out if it is does not resolve a value
                if current_version is None:


                if (
                    current_version is not None
                    and current_version == self.config.target_migration_version
                ):
                    self.logger.info("No migration required, already running target version")
                    return

                if self.config.wait_for_other_connections_to_close:
                    self.logger.info("Waiting on all connections to close before migrating...")
                    await self.wait_for_other_connections_to_close(
                        conn,
                        poll_interval_ms=self.config.connection_wait_poll_interval_ms,
                    )

                direction: Literal["up", "down"]
                if self.config.do_rollback:
                    self.logger.warning(
                        "Rolling back database to version %s",
                        self.config.target_migration_version,
                    )
                    direction = "down"
                else:
                    self.logger.warning(
                        "Migrating database to version %s",
                        self.config.target_migration_version,
                    )
                    direction = "up"

                await self.database.migrate(
                    target=self.config.target_migration_version,
                    direction=direction,
                    conn=conn,
                )
                self.logger.info("Migration apply complete.")
            finally:
                self.logger.info("Ensuring connect privileges are granted correctly")
                await self.grant_connect_privileges(conn)
                self.logger.info("End migration tasks.")
