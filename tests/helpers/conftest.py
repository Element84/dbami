import logging
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from buildpg import V, render

from dbami.db import DB


async def create_owner_role(
    conn: asyncpg.Connection, owner_role_name: str, owner_role_password: str
) -> None:
    q, p = render(
        "CREATE ROLE :un WITH LOGIN PASSWORD ':pw'",
        un=V(owner_role_name),
        pw=V(owner_role_password),
    )
    await conn.execute(q, *p)


async def create_rw_role(conn: asyncpg.Connection, rw_role_name) -> None:
    q, p = render("CREATE ROLE :un", un=V(rw_role_name))
    await conn.execute(q, *p)


async def create_app_role(
    conn: asyncpg.Connection, app_role_name: str, app_password: str, groupname: str
) -> None:
    q, p = render(
        "CREATE ROLE :un WITH LOGIN IN ROLE :ir PASSWORD ':pw'",
        un=V(app_role_name),
        pw=V(app_password),
        ir=V(groupname),
    )
    await conn.execute(q, *p)


async def drop_roles(conn: asyncpg.Connection, roles: list[str]) -> None:
    for role in roles:
        q, p = render(
            "DROP ROLE :role",
            role=V(role),
        )
        try:
            await conn.execute(q, *p)
        except asyncpg.InvalidRoleSpecificationError:
            pass


async def create_database(
    conn: asyncpg.Connection, db_name: str, owner_role_name: str
) -> None:
    await conn.execute(f'CREATE DATABASE "{db_name}" WITH OWNER {owner_role_name}')


async def drop_database(conn: asyncpg.Connection, db_name: str) -> None:
    await conn.execute(f'DROP DATABASE "{db_name}"')


async def post_create_init(
    conn: asyncpg.Connection, db_name: str, owner_role_name: str, rw_role_name: str
) -> None:
    await conn.execute(
        f"""
        REVOKE ALL ON DATABASE "{db_name}" FROM PUBLIC;
        REVOKE CREATE ON SCHEMA public FROM PUBLIC;
        CREATE SCHEMA IF NOT EXISTS "{db_name}" AUTHORIZATION {owner_role_name};
        GRANT pg_signal_backend TO {owner_role_name};
        GRANT {owner_role_name} TO current_user;
        SET ROLE {owner_role_name};
        GRANT CONNECT ON DATABASE "{db_name}" TO {rw_role_name};
        GRANT USAGE, CREATE ON SCHEMA "{db_name}" TO {rw_role_name};
        ALTER DEFAULT PRIVILEGES IN SCHEMA "{db_name}" GRANT SELECT,
            INSERT, UPDATE, DELETE ON TABLES TO {rw_role_name};
        ALTER DEFAULT PRIVILEGES IN SCHEMA "{db_name}" GRANT USAGE ON
            SEQUENCES TO {rw_role_name};
        """
    )


@pytest.fixture
def owner_role_name() -> str:
    return "test_owner_role"


@pytest.fixture
def owner_role_pswd() -> str:
    return "test_owner_pass"


@pytest.fixture
def rw_role_name() -> str:
    return "test_readwrite_role"


@pytest.fixture
def app_role_names() -> list[str]:
    return ["test_app_role_1", "test_app_role_2"]


@pytest.fixture
def app_role_pswds() -> list[str]:
    return ["foo_pass", "bar_pass"]


@pytest_asyncio.fixture
async def db_initialization(
    tmp_db_name,
    owner_role_name,
    owner_role_pswd,
    rw_role_name,
    app_role_names,
    app_role_pswds,
) -> AsyncGenerator[None, None]:
    try:
        async with DB.get_db_connection(database="postgres") as conn:
            # create test roles
            await create_owner_role(conn, owner_role_name, owner_role_pswd)
            await create_rw_role(conn, rw_role_name)
            for app_role_name, app_password in zip(app_role_names, app_role_pswds):
                await create_app_role(conn, app_role_name, app_password, rw_role_name)
            # create test database
            await create_database(conn, tmp_db_name, owner_role_name)

        # assign role privileges in test database
        async with DB.get_db_connection(database=tmp_db_name) as conn:
            await post_create_init(conn, tmp_db_name, owner_role_name, rw_role_name)

        yield

    finally:
        async with DB.get_db_connection(database="postgres") as conn:
            # drop test database
            await drop_database(conn, tmp_db_name)
            # clean up roles
            await drop_roles(conn, [owner_role_name, rw_role_name, *app_role_names])


@pytest.fixture
def test_logger() -> logging.Logger:
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)
    return logger
