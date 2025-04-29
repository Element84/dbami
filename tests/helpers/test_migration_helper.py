import asyncpg
import pytest

from dbami.db import DB
from dbami.helpers.apply_migrations import (
    MigrationHelper,
    MigrationHelperConfig,
    active_connections_exist,
    force_close_connections,
    grant_connect_privileges,
    revoke_connect_privileges,
)


@pytest.mark.asyncio
async def test_revoke_grant_connect_privileges(
    db_initialization,
    tmp_db_name,
    app_role_names,
    app_role_pswds,
    owner_role_name,
    owner_role_pswd,
    rw_role_name,
):
    # check that a read/write user can connect
    await asyncpg.connect(
        database=tmp_db_name, user=app_role_names[0], password=app_role_pswds[0]
    )

    # revoke connect privileges for the read/write group
    async with DB.get_db_connection(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as conn:
        await revoke_connect_privileges(conn, [rw_role_name])

    # check that a read/write user can no longer connect
    with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
        await asyncpg.connect(
            database=tmp_db_name, user=app_role_names[0], password=app_role_pswds[0]
        )

    # grant connect privileges back to the read/write group
    async with DB.get_db_connection(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as conn:
        await grant_connect_privileges(conn, [rw_role_name])

    # check that a read/write user can once again connect
    await asyncpg.connect(
        database=tmp_db_name, user=app_role_names[0], password=app_role_pswds[0]
    )


@pytest.mark.asyncio
async def test_active_connections_exist(
    db_initialization,
    tmp_db_name,
    app_role_names,
    app_role_pswds,
    owner_role_name,
    owner_role_pswd,
):
    async with DB.get_db_connection(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as owner_conn:
        # assert that no read/write connections exist
        assert not await active_connections_exist(owner_conn, app_role_names)

        # open a read/write connection and assert a connection exists
        async with DB.get_db_connection(
            database=tmp_db_name, user=app_role_names[0], password=app_role_pswds[0]
        ):
            assert await active_connections_exist(owner_conn, app_role_names)


@pytest.mark.asyncio
async def test_force_close_connections(
    db_initialization,
    tmp_db_name,
    app_role_names,
    app_role_pswds,
    owner_role_name,
    owner_role_pswd,
):
    async with DB.get_db_connection(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as owner_conn:
        # open a read/write connection
        async with DB.get_db_connection(
            database=tmp_db_name, user=app_role_names[0], password=app_role_pswds[0]
        ):
            # assert the read/write connection exists
            assert await active_connections_exist(owner_conn, [app_role_names[0]])
            # force the read/write connection to close
            await force_close_connections(owner_conn, [app_role_names[0]])
            # assert the read/write connection no longer exists
            assert not await active_connections_exist(owner_conn, [app_role_names[0]])


@pytest.mark.asyncio
async def test_advisory_lock_context_manager(
    db_initialization,
    tmp_db_name,
    empty_project,
    owner_role_name,
    owner_role_pswd,
):
    async def advisory_lock_exists(conn: asyncpg.Connection) -> bool:
        q = "SELECT EXISTS(SELECT * FROM pg_locks WHERE locktype='advisory')"
        return bool(await conn.fetchval(q))

    # assert that no advisory locks exist
    async with empty_project.get_db_connection(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as conn:
        assert not await advisory_lock_exists(conn)

    # assert that an advisory lock is obtained within the migration_lock context manager
    async with empty_project.migration_lock(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as conn:
        assert await advisory_lock_exists(conn)

    # assert that no advisory locks exist
    async with empty_project.get_db_connection(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    ) as conn:
        assert not await advisory_lock_exists(conn)


@pytest.mark.asyncio
async def test_migrate_up(
    db_initialization,
    test_logger,
    tmp_db_name,
    project,
    owner_role_name,
    owner_role_pswd,
):
    helper_config = MigrationHelperConfig()
    helper = MigrationHelper(
        dbami_db=project,
        logger=test_logger,
        helper_config=helper_config,
        database=tmp_db_name,
        user=owner_role_name,
        password=owner_role_pswd,
    )
    await helper.run()

    assert (
        await project.get_current_version(
            database=tmp_db_name,
            user=owner_role_name,
            password=owner_role_pswd,
        )
        == 4
    )


@pytest.mark.asyncio
async def test_migrate_down(
    db_initialization,
    test_logger,
    tmp_db_name,
    project,
    owner_role_name,
    owner_role_pswd,
):
    await project.load_schema(
        database=tmp_db_name, user=owner_role_name, password=owner_role_pswd
    )

    helper_config = MigrationHelperConfig(do_rollback=True, target_migration_version=2)
    helper = MigrationHelper(
        dbami_db=project,
        logger=test_logger,
        helper_config=helper_config,
        database=tmp_db_name,
        user=owner_role_name,
        password=owner_role_pswd,
    )
    await helper.run()

    assert (
        await project.get_current_version(
            database=tmp_db_name,
            user=owner_role_name,
            password=owner_role_pswd,
        )
        == 2
    )
