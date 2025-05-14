import logging
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from buildpg import V, render

from dbami.db import DB
from dbami.util import printe


async def create_owner_role(
    conn: asyncpg.Connection, owner_role_name: str, owner_role_password: str
) -> None:
    q, _ = render(
        "CREATE ROLE :un WITH LOGIN PASSWORD ':pw'",
        un=V(owner_role_name),
        pw=V(owner_role_password),
    )
    try:
        await conn.execute(q)
    except asyncpg.DuplicateObjectError:
        printe(f"Role '{owner_role_name}' already exists, skipping.")


async def create_rw_role(conn: asyncpg.Connection, rw_role_name) -> None:
    q, _ = render("CREATE ROLE :un", un=V(rw_role_name))
    try:
        await conn.execute(q)
    except asyncpg.DuplicateObjectError:
        printe(f"Role '{rw_role_name}' already exists, skipping.")


async def create_app_role(
    conn: asyncpg.Connection, app_role_name: str, app_password: str, groupname: str
) -> None:
    q, _ = render(
        "CREATE ROLE :un WITH LOGIN PASSWORD ':pw' IN ROLE :ir",
        un=V(app_role_name),
        pw=V(app_password),
        ir=V(groupname),
    )
    try:
        await conn.execute(q)
    except asyncpg.DuplicateObjectError:
        printe(f"Role '{app_role_name}' already exists, skipping.")


async def drop_roles(conn: asyncpg.Connection, roles: list[str]) -> None:
    for role in roles:
        q, _ = render(
            "DROP ROLE :r",
            r=V(role),
        )
        try:
            await conn.execute(q)
        except asyncpg.UndefinedObjectError:
            printe(f"Role {role}' already dropped, skipping.")


async def create_database(
    conn: asyncpg.Connection, db_name: str, owner_role_name: str
) -> None:
    # in AWS RDS, the user creating a database must be granted the role that will be the
    # owner of the database (not necessary in our case here, but including for those who
    # use this as a reference)
    grant_q, _ = render("GRANT :o TO current_user", o=V(owner_role_name))
    # using a separate statement since CREATE DATABASE cannot be executed inside a
    # transaction block
    create_q, _ = render(
        'CREATE DATABASE ":db" WITH OWNER :o',
        db=V(db_name),
        o=V(owner_role_name),
    )
    try:
        await conn.execute(grant_q)
        await conn.execute(create_q)
    except asyncpg.DuplicateDatabaseError:
        printe(f"Database '{db_name}' already exists, skipping.")


async def drop_database(conn: asyncpg.Connection, db_name: str) -> None:
    q, _ = render(
        'DROP DATABASE ":db"',
        db=V(db_name),
    )
    try:
        await conn.execute(q)
    except asyncpg.InvalidCatalogNameError:
        printe(f"Database '{db_name}' already dropped, skipping.")


async def post_create_init(
    conn: asyncpg.Connection, db_name: str, owner_role_name: str, rw_role_name: str
) -> None:
    q, _ = render(
        """
        -- clear the default permissions, so we can setup tighter ones
        REVOKE ALL ON DATABASE ":db" FROM PUBLIC;
        REVOKE CREATE ON SCHEMA public FROM PUBLIC;

        -- grant database-level permissions.
        -- in AWS RDS, the GRANT CONNECT must be done as psuedo-superuser
        GRANT ALL ON DATABASE ":db" TO :o;
        GRANT CONNECT ON DATABASE ":db" TO :rw;

        -- allow owner role to force close read/write member users connections
        GRANT pg_signal_backend TO :o;

        -- run remaining commands as if we are the owner role
        SET ROLE :o;

        -- create schema and grant basic permissions
        CREATE SCHEMA IF NOT EXISTS ":db" AUTHORIZATION :o;
        GRANT USAGE, CREATE ON SCHEMA ":db" TO :rw;

        -- alter default privileges so users will have access to new tables
        ALTER DEFAULT PRIVILEGES IN SCHEMA ":db" GRANT
            SELECT, INSERT, UPDATE, DELETE ON TABLES TO :rw;
        ALTER DEFAULT PRIVILEGES IN SCHEMA ":db" GRANT
            USAGE ON SEQUENCES TO :rw;
        """,
        db=V(db_name),
        o=V(owner_role_name),
        rw=V(rw_role_name),
    )
    await conn.execute(q)


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
