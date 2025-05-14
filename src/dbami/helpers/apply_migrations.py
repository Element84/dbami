import asyncio
import logging
from dataclasses import dataclass, field
from typing import Literal, Optional, Sequence

import asyncpg
from buildpg import V, Values, render

from dbami.exceptions import MigrationError
from dbami.helpers.helper import Helper

logger = logging.getLogger(__name__)


@dataclass
class MigrationHelperConfig:
    """
    Configuration for managing database migrations.

    Attributes:
        target_migration_version (Optional[int]):
            The migration version to target. If unset, the highest-known migration
            version will be targeted.

        do_rollback (bool):
            Whether to roll back to an older migration version. Must be `True` if the
            target migration version is older than the current version.

        use_migration_lock (bool):
            Whether to use an advisory lock to prevent concurrent migrations.

        migration_lock_acquisition_timeout_ms (int):
            Timeout (in milliseconds) to wait for an advisory lock to be successfully
            acquired. A value of 0 disables the timeout, meaning the wait for a lock
            will continue indefinitely. Has no effect unless `use_migration_lock` is
            `True`.

        revoke_connect_on_role_names (Sequence[str]):
            List of role names to revoke `CONNECT` privileges on before running
            migrations. This prevents other users/services from reconnecting during
            migrations.

        connection_wait_max_polls (Optional[int]):
            Maximum number of times to check (poll) if revoked role connections are
            closed before aborting or attempting to force-close the connections. Set to
            `None` to wait (poll) indefinitely or <= 0 to disable waiting (polling).

        connection_wait_poll_interval_ms (int):
            Polling interval (in milliseconds) for checking if revoked role connections
            are closed. Values less than 100 will be coerced to 100. Has no effect if
            `connection_wait_max_polls` is set to <= 0.

        force_close_connections_timeout_ms (Optional[int]):
            Timeout (in milliseconds) to wait for force-closed connections to
            successfully close. Set to `None` to disable force-closing or <= 0 to
            proceed without waiting.
    """

    target_migration_version: Optional[int] = None
    do_rollback: bool = False
    use_migration_lock: bool = True
    migration_lock_acquisition_timeout_ms: int = 30000
    revoke_connect_on_role_names: Sequence[str] = field(default_factory=list)
    connection_wait_max_polls: Optional[int] = 10
    connection_wait_poll_interval_ms: int = 1000
    force_close_connections_timeout_ms: Optional[int] = 10000

    def __post_init__(self) -> None:
        if self.connection_wait_poll_interval_ms < 100:
            self.connection_wait_poll_interval_ms = 100

    @property
    def wait_for_other_connections_to_close(self) -> bool:
        return (
            self.connection_wait_max_polls is None or self.connection_wait_max_polls > 0
        )


class MigrationHelper(Helper[MigrationHelperConfig]):
    @classmethod
    def get_config_class(cls) -> MigrationHelperConfig:
        return MigrationHelperConfig()

    async def other_connections_closed(
        self,
        conn: asyncpg.Connection,
        poll_interval_ms: int = 2000,
        max_attempts: Optional[int] = 60,
    ) -> bool:
        if poll_interval_ms < 100:
            raise ValueError("Polling interval must be greater than or equal to 100ms")

        attempts = 0
        while await active_connections_exist(
            conn, self.config.revoke_connect_on_role_names
        ):
            if max_attempts is not None and attempts >= max_attempts:
                self.logger.warning(
                    "Max polling attempts reached, existing connections still open."
                )
                return False
            await asyncio.sleep(poll_interval_ms / 1000)
            attempts += 1

        return True

    async def run(self) -> None:
        async with self.database.migration_lock(
            use_lock=self.config.use_migration_lock,
            timeout_ms=self.config.migration_lock_acquisition_timeout_ms,
            **self.connect_kwargs,
        ) as conn:
            if self.config.target_migration_version is None:
                if not len(self.database.migrations.keys()):
                    raise MigrationError(
                        "No migrations exist, there is nothing to apply"
                    )
                self.config.target_migration_version = max(
                    self.database.migrations.keys()
                )

            current_version = await self.database.get_current_version(conn=conn)

            if (
                current_version is not None
                and current_version == self.config.target_migration_version
            ):
                self.logger.info(
                    "No migration required, already running target version"
                )
                return

            try:
                if self.config.revoke_connect_on_role_names:
                    self.logger.info(
                        "Revoking connect privileges for roles %s",
                        self.config.revoke_connect_on_role_names,
                    )
                    await revoke_connect_privileges(
                        conn, self.config.revoke_connect_on_role_names
                    )

                    conns_closed = False
                    if self.config.wait_for_other_connections_to_close:
                        self.logger.info(
                            "Waiting on all connections to close before migrating..."
                        )
                        conns_closed = await self.other_connections_closed(
                            conn,
                            self.config.connection_wait_poll_interval_ms,
                            self.config.connection_wait_max_polls,
                        )

                    if (
                        not conns_closed
                        and self.config.force_close_connections_timeout_ms is not None
                    ):
                        self.logger.info(
                            "Forcing all connections to close before migrating..."
                        )
                        await force_close_connections(
                            conn,
                            self.config.revoke_connect_on_role_names,
                            self.config.force_close_connections_timeout_ms,
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
                if self.config.revoke_connect_on_role_names:
                    self.logger.info(
                        "Granting connect privileges for roles %s",
                        self.config.revoke_connect_on_role_names,
                    )
                    await grant_connect_privileges(
                        conn, self.config.revoke_connect_on_role_names
                    )
                self.logger.info("End migration tasks.")


async def revoke_connect_privileges(
    conn: asyncpg.Connection,
    roles: Sequence[str],
) -> None:
    if roles:
        for role in roles:
            q, _ = render(
                """
                DO $_$
                    BEGIN
                        EXECUTE FORMAT('REVOKE CONNECT on database "%s" FROM :role',
                        CURRENT_DATABASE());
                    END
                $_$;
                """,
                role=V(role),
            )
            await conn.execute(q)


async def grant_connect_privileges(
    conn: asyncpg.Connection,
    roles: Sequence[str],
) -> None:
    for role in roles:
        q, _ = render(
            """
            DO $_$
                BEGIN
                    EXECUTE FORMAT('GRANT CONNECT on database "%s" TO :role',
                    CURRENT_DATABASE());
                END
            $_$;
            """,
            role=V(role),
        )
        await conn.execute(q)


async def active_connections_exist(
    conn: asyncpg.Connection, roles: Sequence[str]
) -> bool:
    """NOTE: This is for individual login roles, not group roles"""
    if roles:
        q, p = render(
            """
            SELECT EXISTS(
                SELECT * FROM pg_stat_activity
                WHERE usename IN :roles AND datname = current_database()
            )
            """,
            roles=Values(*roles),
        )
        return bool(await conn.fetchval(q, *p))

    return False


async def force_close_connections(
    conn: asyncpg.Connection,
    roles: Sequence[str],
    timeout_ms: int = 10000,
) -> None:
    """NOTE: This is for individual login roles, not group roles"""
    if roles:
        q, p = render(
            """
            SELECT pg_terminate_backend(pid, :timeout_ms)
            FROM pg_stat_activity
            WHERE usename IN :roles AND datname = current_database()
            """,
            timeout_ms=timeout_ms,
            roles=Values(*roles),
        )
        await conn.execute(q, *p)
